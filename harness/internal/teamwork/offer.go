package teamwork

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	DefaultOfferDeadline = model.DefaultReviewDeadline
	MinimumOfferDeadline = model.MinimumReviewDeadline
	MaximumOfferDeadline = model.MaximumReviewDeadline
)

var (
	ErrInvalidOffer       = errors.New("invalid teamwork offer")
	ErrDeadlineOutOfRange = errors.New("teamwork deadline out of range")
)

// OfferPlanSpec is the already-authorized input to offer planning. Reviewer
// aliases and Channel membership are resolved before this policy boundary.
type OfferPlanSpec struct {
	ChannelID      model.ChannelID
	RosterRevision uint64
	HomePeerID     model.PeerID
	ReviewerPeerID model.PeerID
	AcceptedAt     time.Time
	Deadline       time.Duration
}

// OfferPlan is deterministic for a given accepted time and explicit reviewer.
type OfferPlan struct {
	acceptedAt       time.Time
	deadlineDuration time.Duration
	deadlineUnixNano int64
	participants     model.ParticipantSnapshot
}

func (p OfferPlan) AcceptedAt() time.Time                   { return p.acceptedAt }
func (p OfferPlan) DeadlineDuration() time.Duration         { return p.deadlineDuration }
func (p OfferPlan) DeadlineUnixNano() int64                 { return p.deadlineUnixNano }
func (p OfferPlan) Participants() model.ParticipantSnapshot { return p.participants }

// PlanOffer freezes one explicit single-reviewer Work. A zero Deadline selects
// the frozen 24-hour default.
func PlanOffer(spec OfferPlanSpec) (OfferPlan, error) {
	if spec.ChannelID.IsZero() || spec.HomePeerID.IsZero() || spec.RosterRevision == 0 {
		return OfferPlan{}, fmt.Errorf("%w: Channel, roster revision and home PeerID are required", ErrInvalidOffer)
	}
	if _, err := model.CanonicalPeerIDBytes(spec.ReviewerPeerID); err != nil {
		return OfferPlan{}, fmt.Errorf("%w: reviewer PeerID: %v", ErrInvalidOffer, err)
	}
	if spec.ReviewerPeerID == spec.HomePeerID {
		return OfferPlan{}, fmt.Errorf("%w: self review is forbidden", ErrInvalidOffer)
	}

	acceptedAt := spec.AcceptedAt.Round(0).UTC()
	if spec.AcceptedAt.IsZero() || !time.Unix(0, acceptedAt.UnixNano()).UTC().Equal(acceptedAt) {
		return OfferPlan{}, fmt.Errorf("%w: accepted time must fit positive Unix nanoseconds", ErrInvalidOffer)
	}
	acceptedUnixNano := acceptedAt.UnixNano()
	if acceptedUnixNano <= 0 {
		return OfferPlan{}, fmt.Errorf("%w: accepted time must fit positive Unix nanoseconds", ErrInvalidOffer)
	}

	deadline := spec.Deadline
	if deadline == 0 {
		deadline = DefaultOfferDeadline
	}
	if deadline < MinimumOfferDeadline || deadline > MaximumOfferDeadline {
		return OfferPlan{}, fmt.Errorf("%w: got %s, want %s..%s", ErrDeadlineOutOfRange, deadline, MinimumOfferDeadline, MaximumOfferDeadline)
	}
	if int64(deadline) > math.MaxInt64-acceptedUnixNano {
		return OfferPlan{}, fmt.Errorf("%w: deadline overflows Unix nanoseconds", ErrInvalidOffer)
	}
	deadlineUnixNano := acceptedUnixNano + int64(deadline)

	participants, err := model.NewParticipantSnapshot(
		spec.ChannelID,
		spec.RosterRevision,
		spec.HomePeerID,
		spec.ReviewerPeerID,
	)
	if err != nil {
		return OfferPlan{}, fmt.Errorf("%w: participant snapshot: %v", ErrInvalidOffer, err)
	}

	return OfferPlan{
		acceptedAt:       acceptedAt,
		deadlineDuration: deadline,
		deadlineUnixNano: deadlineUnixNano,
		participants:     participants,
	}, nil
}
