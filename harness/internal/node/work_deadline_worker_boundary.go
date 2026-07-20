package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	eventpkg "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type WorkDeadlineWorkerStore interface {
	ScanWorkDeadlines(context.Context, time.Time) (store.WorkDeadlineScan, error)
	PrepareWorkExpiry(context.Context, store.WorkDeadlineCandidate, time.Time) (store.WorkExpiryPreparation, error)
	CommitWorkExpiry(context.Context, store.WorkExpiryCommitSpec, time.Time) (store.WorkExpiryCommitResult, error)
}

type WorkDeadlineWorkerOptions struct {
	Store              WorkDeadlineWorkerStore
	Signer             eventpkg.PublicationSigner
	Clock              WorkDeadlineClock
	PublicationTrigger WorkDeadlinePublicationTrigger
	Period             time.Duration
	timers             workDeadlineTimerFactory
}

type durableWorkDeadlineBackend struct {
	store  WorkDeadlineWorkerStore
	signer eventpkg.PublicationSigner
}

type workExpiryEventAuthority struct {
	work       model.ReviewWork
	cause      model.EventKey
	eventID    model.EventID
	node       model.Node
	profile    model.Profile
	scope      model.EventScope
	acceptedAt time.Time
}

func NewWorkDeadlineWorker(options WorkDeadlineWorkerOptions) (*WorkDeadlineWorker, error) {
	if options.Store == nil || options.Signer == nil || options.PublicationTrigger == nil {
		return nil, fmt.Errorf("%w: Store, signer and publisher are required", ErrWorkDeadlineWorker)
	}
	if options.Clock == nil {
		options.Clock = wallWorkDeadlineClock{}
	}
	if options.Period == 0 {
		options.Period = workDeadlineReconcilePeriod
	}
	if options.timers == nil {
		options.timers = wallWorkDeadlineTimerFactory{}
	}
	backend := durableWorkDeadlineBackend{store: options.Store, signer: options.Signer}
	return newWorkDeadlineWorker(backend, options.Clock, options.PublicationTrigger,
		options.timers, options.Period)
}

func (backend durableWorkDeadlineBackend) scan(ctx context.Context,
	at time.Time,
) (workDeadlineScan, error) {
	result, err := backend.store.ScanWorkDeadlines(ctx, at)
	if err != nil {
		return workDeadlineScan{}, err
	}
	due := result.Due()
	if len(due) > store.WorkDeadlineScanLimit {
		return workDeadlineScan{}, fmt.Errorf("%w: Store returned an oversized deadline batch",
			store.ErrWorkDeadlineInvariant)
	}
	candidates := make([]workDeadlineCandidate, len(due))
	for index, candidate := range due {
		work := candidate.Work()
		if work.Ref().IsZero() || candidate.Cause().IsZero() {
			return workDeadlineScan{}, fmt.Errorf("%w: Store returned incomplete deadline authority",
				store.ErrWorkDeadlineInvariant)
		}
		candidates[index] = workDeadlineCandidate{durable: candidate, work: work}
	}
	return workDeadlineScan{due: candidates, moreDue: result.MoreDue(),
		exhaustedCount:    result.ExhaustedCount(),
		nextDeadlineNanos: result.NextDeadlineUnixNano()}, nil
}

func (backend durableWorkDeadlineBackend) expire(ctx context.Context,
	candidate workDeadlineCandidate, clock WorkDeadlineClock,
) (bool, error) {
	acceptedAt, err := workDeadlineNow(clock)
	if err != nil {
		return false, err
	}
	preparation, err := backend.store.PrepareWorkExpiry(ctx, candidate.durable, acceptedAt)
	if err != nil {
		return false, err
	}
	authority, err := workExpiryAuthority(preparation)
	if err != nil {
		return false, err
	}
	item, err := assembleWorkExpiry(ctx, backend.signer, authority)
	if err != nil {
		return false, err
	}
	commitAt, err := workDeadlineNow(clock)
	if err != nil {
		return false, err
	}
	_, err = backend.store.CommitWorkExpiry(ctx, store.WorkExpiryCommitSpec{
		Preparation: preparation, Expiry: item}, commitAt)
	if err != nil {
		return false, err
	}
	return true, nil
}

func workExpiryAuthority(preparation store.WorkExpiryPreparation) (workExpiryEventAuthority, error) {
	work := preparation.Work()
	scope := preparation.Scope()
	eventID, err := store.WorkExpiryEventID(preparation)
	if err != nil || eventID != preparation.EventID() || work.Ref().IsZero() ||
		preparation.Cause().IsZero() || preparation.AcceptedAt().IsZero() {
		return workExpiryEventAuthority{}, fmt.Errorf("%w: invalid prepared expiry identity",
			store.ErrWorkDeadlineInvariant)
	}
	eventScope, err := scope.EventScope(0, work.Ref())
	if err != nil {
		return workExpiryEventAuthority{}, fmt.Errorf("%w: prepared expiry scope: %v",
			store.ErrWorkDeadlineInvariant, err)
	}
	return workExpiryEventAuthority{work: work, cause: preparation.Cause(), eventID: eventID,
		node: scope.Node(), profile: scope.Profile(), scope: eventScope,
		acceptedAt: preparation.AcceptedAt()}, nil
}

func assembleWorkExpiry(ctx context.Context, signer eventpkg.PublicationSigner,
	authority workExpiryEventAuthority,
) (store.LocalAcceptanceItem, error) {
	work := authority.work
	if ctx == nil || signer == nil || work.Ref().IsZero() || authority.cause.IsZero() ||
		authority.eventID.IsZero() || authority.acceptedAt.IsZero() ||
		authority.scope.WorkRef() != work.Ref() {
		return store.LocalAcceptanceItem{}, fmt.Errorf("%w: incomplete Event authority",
			store.ErrWorkDeadlineInvariant)
	}
	result, err := eventpkg.AdmitWorkExpiry(ctx, signer, eventpkg.WorkExpirySpec{
		Work: work, Cause: authority.cause, EventID: authority.eventID,
		Node: authority.node, Profile: authority.profile, Scope: authority.scope,
		AcceptedAt: authority.acceptedAt,
	})
	if err != nil {
		return store.LocalAcceptanceItem{}, mapWorkExpiryError(err)
	}
	mutation, err := store.NewWorkTransition(result.Work(), work.Version(), work.State())
	if err != nil {
		return store.LocalAcceptanceItem{}, fmt.Errorf("%w: expiry mutation: %v",
			store.ErrWorkDeadlineInvariant, err)
	}
	return store.LocalAcceptanceItem{Publication: result.Publication(), Work: &mutation}, nil
}

func mapWorkExpiryError(err error) error {
	switch {
	case errors.Is(err, eventpkg.ErrWorkExpiryStale):
		return fmt.Errorf("%w: %v", store.ErrWorkDeadlineStale, err)
	case errors.Is(err, eventpkg.ErrWorkExpiryInvariant):
		return fmt.Errorf("%w: %v", store.ErrWorkDeadlineInvariant, err)
	default:
		return err
	}
}
