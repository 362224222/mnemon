package event

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

var (
	ErrWorkExpiryStale     = errors.New("Work expiry authority is stale")
	ErrWorkExpiryInvariant = errors.New("Work expiry authority is inconsistent")
)

// WorkExpirySpec is the Store-prepared, immutable authority needed to plan,
// admit, and sign one canonical review.expired Event. It deliberately contains
// no persistence types so the Event layer remains independent of Store.
type WorkExpirySpec struct {
	Work       model.ReviewWork
	Cause      model.EventKey
	EventID    model.EventID
	Node       model.Node
	Profile    model.Profile
	Scope      model.EventScope
	AcceptedAt time.Time
}

// WorkExpiryResult carries the signed Event and canonical next Work while
// leaving the persistence mutation and its predecessor CAS to the caller.
type WorkExpiryResult struct {
	publication model.SignedPublication
	work        model.ReviewWork
}

func (result WorkExpiryResult) Publication() model.SignedPublication { return result.publication }
func (result WorkExpiryResult) Work() model.ReviewWork               { return result.work }

type workExpiryClock struct{ at time.Time }

func (clock workExpiryClock) Now() time.Time { return clock.at }

// AdmitWorkExpiry applies the Teamwork expiry policy before admitting and
// signing its Event. The caller must still commit the result against the exact
// durable preparation from which spec was derived.
func AdmitWorkExpiry(ctx context.Context, signer PublicationSigner,
	spec WorkExpirySpec,
) (WorkExpiryResult, error) {
	if err := validateWorkExpirySpec(ctx, signer, spec); err != nil {
		return WorkExpiryResult{}, err
	}
	intent, err := teamwork.PlanExpiry(teamwork.ExpirySpec{
		Work: spec.Work, HomePeerID: spec.Node.PeerID(),
		ExpectedVersion: spec.Work.Version(), NowUnixNano: spec.AcceptedAt.UnixNano(),
	})
	if err != nil {
		return WorkExpiryResult{}, classifyWorkExpiryPlanError(err)
	}
	audience, err := model.NewAudience([]model.PeerID{spec.Work.Participants().ReviewerPeerID()})
	if err != nil {
		return WorkExpiryResult{}, fmt.Errorf("%w: audience: %v", ErrWorkExpiryInvariant, err)
	}
	stamp, err := NewAdmissionStamp(AdmissionStampSpec{
		Node: spec.Node, Profile: spec.Profile, EventID: spec.EventID,
		ChannelID: spec.Scope.ChannelID(), WorkRef: spec.Work.Ref(),
		OriginSequence: spec.Scope.OriginSequence(), ChannelSequence: spec.Scope.ChannelSequence(),
		OriginMember: spec.Scope.OriginMember(), PublicationRoster: spec.Scope.PublicationRoster(),
		Audience: audience, WorkVersion: spec.Work.Version(), Iteration: spec.Work.Iteration(),
		WorkDeadlineUnixNano: spec.Work.DeadlineUnixNano(), CausedBy: []model.EventKey{spec.Cause},
	})
	if err != nil {
		return WorkExpiryResult{}, fmt.Errorf("%w: admission stamp: %v", ErrWorkExpiryInvariant, err)
	}
	factory, err := NewFactory(workExpiryClock{spec.AcceptedAt}, signer)
	if err != nil {
		return WorkExpiryResult{}, err
	}
	bundle, err := factory.AdmitController(ctx, stamp, ExpiredDecision{})
	if err != nil {
		return WorkExpiryResult{}, err
	}
	nextSpec := spec.Work.Spec()
	nextSpec.Version, nextSpec.Iteration = intent.NextVersion(), intent.NextIteration()
	nextSpec.State, nextSpec.StateData = intent.NextState(), bundle.Event().Payload()
	nextSpec.UpdatedBy, nextSpec.UpdatedAt = bundle.Event().ID(), bundle.Event().AcceptedAt()
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		return WorkExpiryResult{}, fmt.Errorf("%w: next Work: %v", ErrWorkExpiryInvariant, err)
	}
	return WorkExpiryResult{publication: bundle.Publication(), work: next}, nil
}

func validateWorkExpirySpec(ctx context.Context, signer PublicationSigner,
	spec WorkExpirySpec,
) error {
	work := spec.Work
	if ctx == nil || signer == nil || work.Ref().IsZero() || spec.Cause.IsZero() ||
		spec.EventID.IsZero() || spec.AcceptedAt.IsZero() ||
		spec.Scope.WorkRef() != work.Ref() || spec.Scope.ChannelID() != work.ChannelID() ||
		spec.Scope.OriginPeerID() != spec.Node.PeerID() ||
		spec.Scope.OriginEpoch() != spec.Node.OriginEpoch() {
		return fmt.Errorf("%w: incomplete or mismatched prepared authority", ErrWorkExpiryInvariant)
	}
	return nil
}

func classifyWorkExpiryPlanError(err error) error {
	if errors.Is(err, teamwork.ErrWorkNotDue) ||
		errors.Is(err, teamwork.ErrWorkNotExpirable) ||
		errors.Is(err, teamwork.ErrVersionConflict) {
		return fmt.Errorf("%w: %v", ErrWorkExpiryStale, err)
	}
	return fmt.Errorf("%w: policy: %v", ErrWorkExpiryInvariant, err)
}
