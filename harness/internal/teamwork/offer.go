package teamwork

import (
	"errors"
	"fmt"
	"math"
	"sort"
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
	ChannelID       model.ChannelID
	RosterRevision  uint64
	HomePeerID      model.PeerID
	ReviewerPeerIDs []model.PeerID
	AcceptedAt      time.Time
	Deadline        time.Duration
}

// PlannedOffer describes one single-reviewer Work. Work and Event identities
// are deliberately allocated by the admission transaction, not this policy.
type PlannedOffer struct {
	ordinal      uint8
	participants model.ParticipantSnapshot
}

func (o PlannedOffer) Ordinal() uint8                          { return o.ordinal }
func (o PlannedOffer) Participants() model.ParticipantSnapshot { return o.participants }

// OfferPlan is deterministic for a given accepted time and selected PeerIDs.
// Its reviewer order is canonical PeerID byte order.
type OfferPlan struct {
	acceptedAt       time.Time
	deadlineDuration time.Duration
	deadlineUnixNano int64
	offers           []PlannedOffer
}

func (p OfferPlan) AcceptedAt() time.Time           { return p.acceptedAt }
func (p OfferPlan) DeadlineDuration() time.Duration { return p.deadlineDuration }
func (p OfferPlan) DeadlineUnixNano() int64         { return p.deadlineUnixNano }
func (p OfferPlan) Offers() []PlannedOffer {
	return append([]PlannedOffer(nil), p.offers...)
}

// PlanOffer expands one action into one to seven independent single-reviewer
// plans. A zero Deadline selects the frozen 24-hour default.
func PlanOffer(spec OfferPlanSpec) (OfferPlan, error) {
	if spec.ChannelID.IsZero() || spec.HomePeerID.IsZero() || spec.RosterRevision == 0 {
		return OfferPlan{}, fmt.Errorf("%w: Channel, roster revision and home PeerID are required", ErrInvalidOffer)
	}
	if len(spec.ReviewerPeerIDs) == 0 || len(spec.ReviewerPeerIDs) > model.MaxChildWorks {
		return OfferPlan{}, fmt.Errorf("%w: reviewer count must be between 1 and %d", ErrInvalidOffer, model.MaxChildWorks)
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

	reviewers := append([]model.PeerID(nil), spec.ReviewerPeerIDs...)
	sort.Slice(reviewers, func(i, j int) bool {
		return reviewers[i].String() < reviewers[j].String()
	})

	offers := make([]PlannedOffer, len(reviewers))
	for index, reviewer := range reviewers {
		if reviewer.IsZero() {
			return OfferPlan{}, fmt.Errorf("%w: reviewer %d has a zero PeerID", ErrInvalidOffer, index)
		}
		if reviewer == spec.HomePeerID {
			return OfferPlan{}, fmt.Errorf("%w: self review is forbidden", ErrInvalidOffer)
		}
		if index > 0 && reviewer == reviewers[index-1] {
			return OfferPlan{}, fmt.Errorf("%w: duplicate reviewer %q", ErrInvalidOffer, reviewer.String())
		}
		participants, err := model.NewParticipantSnapshot(
			spec.ChannelID,
			spec.RosterRevision,
			spec.HomePeerID,
			reviewer,
		)
		if err != nil {
			return OfferPlan{}, fmt.Errorf("%w: participant snapshot: %v", ErrInvalidOffer, err)
		}
		offers[index] = PlannedOffer{ordinal: uint8(index), participants: participants}
	}

	return OfferPlan{
		acceptedAt:       acceptedAt,
		deadlineDuration: deadline,
		deadlineUnixNano: deadlineUnixNano,
		offers:           offers,
	}, nil
}
