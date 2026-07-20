package semantic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	peerInboxSemanticWorkerPeriod       = 250 * time.Millisecond
	peerInboxSemanticWorkerBatch        = 8
	peerInboxSemanticWorkerRetryMinimum = time.Second
	peerInboxSemanticWorkerRetryMaximum = 30 * time.Second
	peerInboxSemanticWorkerOwnerBytes   = 12
)

var (
	ErrPeerInboxSemanticWorker        = errors.New("Peer Inbox semantic worker")
	ErrPeerInboxSemanticWorkerRunning = fmt.Errorf("%w: worker has already run", ErrPeerInboxSemanticWorker)
)

type PeerInboxSemanticWorkerStore interface {
	ClaimPeerInboxSemantic(context.Context, store.ClaimPeerInboxSemanticSpec) (store.PeerInboxSemanticClaimResult, error)
	ProbePeerInboxSemanticAuthority(context.Context, store.ProbePeerInboxSemanticAuthoritySpec) error
	RetryPeerInboxSemantic(context.Context, store.RetryPeerInboxSemanticSpec) (store.PeerInboxSemanticRetry, error)
	PrepareLocalAdmission(context.Context, model.ChannelID, model.Audience, uint8) (store.LocalAdmissionScope, error)
	CommitPeerInboxSemantic(context.Context, store.CommitPeerInboxSemanticSpec, time.Time) (store.PeerInboxSemanticCommitResult, error)
}

type PeerInboxSemanticWorkerClock interface{ Now() time.Time }
type PeerInboxSemanticPublicationTrigger interface{ Trigger() }
type PeerInboxSemanticSigner interface {
	Sign(context.Context, []byte) ([]byte, error)
}

type PeerInboxSemanticWorkerOptions struct {
	Store              PeerInboxSemanticWorkerStore
	Signer             PeerInboxSemanticSigner
	Clock              PeerInboxSemanticWorkerClock
	PublicationTrigger PeerInboxSemanticPublicationTrigger
	Period             time.Duration
}

type PeerInboxSemanticWorkerState string

const (
	PeerInboxSemanticWorkerIdle    PeerInboxSemanticWorkerState = "idle"
	PeerInboxSemanticWorkerRunning PeerInboxSemanticWorkerState = "running"
	PeerInboxSemanticWorkerStopped PeerInboxSemanticWorkerState = "stopped"
	PeerInboxSemanticWorkerFailed  PeerInboxSemanticWorkerState = "failed"
)

type PeerInboxSemanticWorkerSnapshot struct {
	State         PeerInboxSemanticWorkerState
	Cycles        uint64
	Claims        uint64
	Committed     uint64
	Replayed      uint64
	Retries       uint64
	Stale         uint64
	InFlight      int
	MaximumActive int
	LastCycleAt   time.Time
}

type PeerInboxSemanticWorker struct {
	backend   peerInboxSemanticWorkerBackend
	planner   peerInboxSemanticWorkerPlanner
	signer    PeerInboxSemanticSigner
	clock     PeerInboxSemanticWorkerClock
	publisher PeerInboxSemanticPublicationTrigger
	period    time.Duration
	owner     string
	trigger   chan struct{}

	mu       sync.Mutex
	started  bool
	snapshot PeerInboxSemanticWorkerSnapshot
}

type peerInboxSemanticWorkerBackend interface {
	claim(context.Context, string, time.Time) (peerInboxSemanticWorkerClaim, bool, error)
	retry(context.Context, peerInboxSemanticWorkerClaim, store.PeerInboxSemanticRetryDiagnostic,
		time.Duration, time.Time) error
	probe(context.Context, peerInboxSemanticWorkerClaim, time.Time) error
	prepare(context.Context, peerInboxSemanticWorkerClaim, uint8) (peerInboxSemanticWorkerAdmission, error)
	commit(context.Context, peerInboxSemanticWorkerClaim, peerInboxSemanticWorkerDecision,
		peerInboxSemanticWorkerAdmission, []model.SignedPublication, time.Time) (peerInboxSemanticWorkerCommit, error)
}

type peerInboxSemanticWorkerPlanner interface {
	plan(peerInboxSemanticWorkerClaim, time.Time) (peerInboxSemanticWorkerDecision, error)
}

type peerInboxSemanticWorkerClaim struct {
	value        store.PeerInboxSemanticClaim
	imported     model.Event
	decisionSeed model.Digest
	attempt      uint32
}

type peerInboxSemanticWorkerIntent struct {
	eventType model.EventType
	payload   model.JSON
	cause     model.EventKey
}

type peerInboxSemanticWorkerDecision struct {
	plan       store.PeerInboxSemanticPlan
	decisionAt time.Time
	responses  []peerInboxSemanticWorkerIntent
	retry      bool
	diagnostic string
}

type peerInboxSemanticResponseScope interface {
	eventScope(uint8, model.WorkRef) (model.EventScope, error)
	principal() string
}

type peerInboxSemanticWorkerAdmission struct {
	value store.LocalAdmissionScope
	scope peerInboxSemanticResponseScope
}

type peerInboxSemanticWorkerCommit struct{ changed, replayed bool }
type durablePeerInboxSemanticWorkerBackend struct{ store PeerInboxSemanticWorkerStore }
type teamworkPeerInboxSemanticWorkerPlanner struct{}
type wallPeerInboxSemanticWorkerClock struct{}
type storePeerInboxSemanticResponseScope struct{ value store.LocalAdmissionScope }

func (wallPeerInboxSemanticWorkerClock) Now() time.Time { return time.Now() }

func NewPeerInboxSemanticWorker(options PeerInboxSemanticWorkerOptions) (*PeerInboxSemanticWorker, error) {
	if options.Store == nil || options.Signer == nil || options.PublicationTrigger == nil {
		return nil, fmt.Errorf("%w: Store, signer and publication trigger are required",
			ErrPeerInboxSemanticWorker)
	}
	clock := options.Clock
	if clock == nil {
		clock = wallPeerInboxSemanticWorkerClock{}
	}
	period := options.Period
	if period == 0 {
		period = peerInboxSemanticWorkerPeriod
	}
	owner, err := newPeerInboxSemanticWorkerOwner()
	if err != nil {
		return nil, fmt.Errorf("%w: generate owner: %v", ErrPeerInboxSemanticWorker, err)
	}
	return newPeerInboxSemanticWorker(durablePeerInboxSemanticWorkerBackend{options.Store},
		teamworkPeerInboxSemanticWorkerPlanner{}, options.Signer, clock,
		options.PublicationTrigger, period, owner)
}

func newPeerInboxSemanticWorker(backend peerInboxSemanticWorkerBackend,
	planner peerInboxSemanticWorkerPlanner, signer PeerInboxSemanticSigner,
	clock PeerInboxSemanticWorkerClock, publisher PeerInboxSemanticPublicationTrigger,
	period time.Duration, owner string,
) (*PeerInboxSemanticWorker, error) {
	if backend == nil || planner == nil || signer == nil || clock == nil || publisher == nil ||
		period <= 0 || period > peerInboxSemanticWorkerPeriod || owner == "" || len(owner) > 512 {
		return nil, fmt.Errorf("%w: complete bounded configuration is required",
			ErrPeerInboxSemanticWorker)
	}
	return &PeerInboxSemanticWorker{backend: backend, planner: planner, signer: signer,
		clock: clock, publisher: publisher, period: period, owner: owner,
		trigger:  make(chan struct{}, 1),
		snapshot: PeerInboxSemanticWorkerSnapshot{State: PeerInboxSemanticWorkerIdle}}, nil
}

func newPeerInboxSemanticWorkerOwner() (string, error) {
	value := make([]byte, peerInboxSemanticWorkerOwnerBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "peer-inbox-semantic-" + hex.EncodeToString(value), nil
}

func (worker *PeerInboxSemanticWorker) Trigger() {
	if worker == nil || worker.trigger == nil {
		return
	}
	select {
	case worker.trigger <- struct{}{}:
	default:
	}
}

func (worker *PeerInboxSemanticWorker) Snapshot() PeerInboxSemanticWorkerSnapshot {
	if worker == nil {
		return PeerInboxSemanticWorkerSnapshot{State: PeerInboxSemanticWorkerFailed}
	}
	worker.mu.Lock()
	defer worker.mu.Unlock()
	return worker.snapshot
}

func (worker *PeerInboxSemanticWorker) Run(ctx context.Context) error {
	if worker == nil || worker.backend == nil || worker.planner == nil || worker.signer == nil ||
		worker.clock == nil || worker.publisher == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrPeerInboxSemanticWorker)
	}
	if !worker.start() {
		return ErrPeerInboxSemanticWorkerRunning
	}
	failed := false
	defer worker.stop(&failed)
	ticker := time.NewTicker(worker.period)
	defer ticker.Stop()
	for {
		if err := worker.runCycle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failed = true
			worker.fail()
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-worker.trigger:
		}
	}
}

func (worker *PeerInboxSemanticWorker) runCycle(ctx context.Context) error {
	at, err := worker.now(ctx)
	if err != nil {
		return err
	}
	worker.recordCycle(at)
	for count := 0; count < peerInboxSemanticWorkerBatch; count++ {
		found, err := worker.processOne(ctx)
		if err != nil || !found {
			return err
		}
		if count == peerInboxSemanticWorkerBatch-1 {
			worker.Trigger()
		}
	}
	return nil
}

func (worker *PeerInboxSemanticWorker) processOne(ctx context.Context) (bool, error) {
	claimAt, err := worker.now(ctx)
	if err != nil {
		return false, err
	}
	claim, found, err := worker.backend.claim(ctx, worker.owner, claimAt)
	if err != nil {
		return false, worker.expectedAuthority(err)
	}
	if !found {
		return false, nil
	}
	worker.recordClaim(true)
	defer worker.recordClaim(false)
	decisionAt, err := worker.now(ctx)
	if err != nil {
		return true, err
	}
	decision, err := worker.planner.plan(claim, decisionAt)
	if err != nil {
		return true, fmt.Errorf("%w: plan claimed Event: %v", ErrPeerInboxSemanticWorker, err)
	}
	if decision.retry {
		if !validPeerInboxSemanticPolicyRetry(decision.diagnostic) {
			return true, fmt.Errorf("%w: planner returned unknown retry diagnostic",
				ErrPeerInboxSemanticWorker)
		}
		return true, worker.retry(ctx, claim, store.PeerInboxSemanticRetryDependencyUnavailable)
	}
	responses, admission, err := worker.prepareResponses(ctx, claim, decision)
	if err != nil {
		if errors.Is(err, store.ErrChannelUnavailable) || errors.Is(err, store.ErrAudienceUnavailable) {
			return true, worker.retry(ctx, claim, store.PeerInboxSemanticRetryDependencyUnavailable)
		}
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		return true, fmt.Errorf("%w: assemble responses: %v", ErrPeerInboxSemanticWorker, err)
	}
	probeAt, err := worker.now(ctx)
	if err != nil {
		return true, err
	}
	if err := worker.backend.probe(ctx, claim, probeAt); err != nil {
		return true, worker.expectedAuthority(err)
	}
	commitAt, err := worker.now(ctx)
	if err != nil {
		return true, err
	}
	result, err := worker.backend.commit(ctx, claim, decision, admission, responses, commitAt)
	if err != nil {
		// Multiple bounded semantic lanes may prepare the same optimistic local
		// sequence range. The winner advances authority; a loser durably releases
		// its Inbox fence and retries from a fresh snapshot instead of failing the
		// Node for expected local admission contention.
		if errors.Is(err, store.ErrAdmissionConflict) {
			return true, worker.retry(ctx, claim, store.PeerInboxSemanticRetryBusy)
		}
		return true, worker.expectedAuthority(err)
	}
	worker.recordCommit(result)
	if len(responses) != 0 {
		worker.publisher.Trigger()
	}
	return true, nil
}

func (worker *PeerInboxSemanticWorker) prepareResponses(ctx context.Context,
	claim peerInboxSemanticWorkerClaim, decision peerInboxSemanticWorkerDecision,
) ([]model.SignedPublication, peerInboxSemanticWorkerAdmission, error) {
	if decision.retry || decision.decisionAt.IsZero() || len(decision.responses) > 2 {
		return nil, peerInboxSemanticWorkerAdmission{}, errors.New("invalid terminal semantic decision")
	}
	if len(decision.responses) == 0 {
		return nil, peerInboxSemanticWorkerAdmission{}, nil
	}
	admission, err := worker.backend.prepare(ctx, claim, uint8(len(decision.responses)))
	if err != nil {
		return nil, peerInboxSemanticWorkerAdmission{}, err
	}
	responses, err := assemblePeerInboxSemanticResponses(ctx, worker.signer, claim,
		decision, admission.scope)
	return responses, admission, err
}

func (worker *PeerInboxSemanticWorker) retry(ctx context.Context,
	claim peerInboxSemanticWorkerClaim, diagnostic store.PeerInboxSemanticRetryDiagnostic,
) error {
	at, err := worker.now(ctx)
	if err != nil {
		return err
	}
	err = worker.backend.retry(ctx, claim, diagnostic,
		peerInboxSemanticWorkerBackoff(claim.attempt), at)
	if err != nil {
		return worker.expectedAuthority(err)
	}
	worker.recordRetry()
	return nil
}

func (worker *PeerInboxSemanticWorker) expectedAuthority(err error) error {
	if errors.Is(err, store.ErrPeerInboxSemanticStale) ||
		errors.Is(err, store.ErrPeerInboxSemanticAuthority) {
		worker.recordStale()
		return nil
	}
	return err
}

func (worker *PeerInboxSemanticWorker) now(ctx context.Context) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	at := worker.clock.Now().Round(0).UTC()
	if at.IsZero() {
		return time.Time{}, fmt.Errorf("%w: trusted clock returned zero", ErrPeerInboxSemanticWorker)
	}
	return at, nil
}

func peerInboxSemanticWorkerBackoff(attempt uint32) time.Duration {
	delay := peerInboxSemanticWorkerRetryMinimum
	for current := uint32(1); current < attempt && delay < peerInboxSemanticWorkerRetryMaximum; current++ {
		delay *= 2
	}
	if delay > peerInboxSemanticWorkerRetryMaximum {
		return peerInboxSemanticWorkerRetryMaximum
	}
	return delay
}

func validPeerInboxSemanticPolicyRetry(diagnostic string) bool {
	switch diagnostic {
	case "missing_work", "local_work_behind", "missing_cause":
		return true
	default:
		return false
	}
}
