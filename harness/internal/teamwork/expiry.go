package teamwork

import (
	"errors"
	"fmt"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrWorkNotDue        = errors.New("Work deadline not due")
	ErrWorkNotExpirable  = errors.New("Work state is not deadline eligible")
	ErrInvalidExpiryScan = errors.New("invalid expiry scan")
)

// ExpirySpec identifies one exact home-owned Work version for timer or
// restart-scan expiry planning.
type ExpirySpec struct {
	Work            model.ReviewWork
	HomePeerID      model.PeerID
	ExpectedVersion uint64
	NowUnixNano     int64
}

// PlanExpiry returns the same CAS shape used by ordinary home transitions.
// Equality at the deadline is due; DELIVERED and terminal Works never expire.
func PlanExpiry(spec ExpirySpec) (TransitionIntent, error) {
	if err := validateHomeCAS(spec.Work, spec.HomePeerID, spec.ExpectedVersion, spec.NowUnixNano); err != nil {
		return TransitionIntent{}, err
	}
	if spec.Work.Version() == model.MaxSQLiteInteger {
		return TransitionIntent{}, ErrWorkVersionExhausted
	}
	if !spec.Work.State().DeadlineEligible() {
		return TransitionIntent{}, fmt.Errorf("%w: %s", ErrWorkNotExpirable, spec.Work.State())
	}
	if spec.NowUnixNano < spec.Work.DeadlineUnixNano() {
		return TransitionIntent{}, fmt.Errorf("%w: now %d, deadline %d", ErrWorkNotDue, spec.NowUnixNano, spec.Work.DeadlineUnixNano())
	}
	return planExpiryIntent(spec.Work, spec.ExpectedVersion, spec.NowUnixNano, model.EventReviewExpired), nil
}

// ExpiryScanPlan is the canonical earliest-deadline ordering a serve/restart
// controller can apply one-by-one with Work-version CAS.
type ExpiryScanPlan struct {
	intents   []TransitionIntent
	exhausted []model.WorkRef
}

func (p ExpiryScanPlan) Intents() []TransitionIntent {
	return append([]TransitionIntent(nil), p.intents...)
}

// Exhausted reports due Works whose SQLite-bounded version cannot advance.
// They need operator repair, but one corrupt/exhausted record must not stop
// restart recovery from expiring every other eligible Work.
func (p ExpiryScanPlan) Exhausted() []model.WorkRef {
	return append([]model.WorkRef(nil), p.exhausted...)
}

// PlanExpiryScan is restart-friendly: it derives due work solely from durable
// integer deadlines and current Work versions. Remote mirrors, DELIVERED and
// terminal Works are intentionally not scheduled by this home.
func PlanExpiryScan(homePeerID model.PeerID, nowUnixNano int64, works []model.ReviewWork) (ExpiryScanPlan, error) {
	if homePeerID.IsZero() || nowUnixNano <= 0 {
		return ExpiryScanPlan{}, fmt.Errorf("%w: home PeerID and positive trusted time are required", ErrInvalidExpiryScan)
	}

	seen := make(map[string]struct{}, len(works))
	intents := make([]TransitionIntent, 0, len(works))
	exhausted := make([]model.WorkRef, 0)
	for _, work := range works {
		if work.Ref().IsZero() || work.ChannelID().IsZero() || work.Version() == 0 || work.DeadlineUnixNano() <= 0 {
			return ExpiryScanPlan{}, fmt.Errorf("%w: zero or incomplete Work", ErrInvalidExpiryScan)
		}
		key := work.Ref().HomePeerID().String() + "\x00" + work.Ref().WorkID().String()
		if _, exists := seen[key]; exists {
			return ExpiryScanPlan{}, fmt.Errorf("%w: duplicate WorkRef", ErrInvalidExpiryScan)
		}
		seen[key] = struct{}{}
		if work.Ref().HomePeerID() != homePeerID || !work.State().DeadlineEligible() || nowUnixNano < work.DeadlineUnixNano() {
			continue
		}
		if work.Version() == model.MaxSQLiteInteger {
			exhausted = append(exhausted, work.Ref())
			continue
		}
		intent, err := PlanExpiry(ExpirySpec{
			Work:            work,
			HomePeerID:      homePeerID,
			ExpectedVersion: work.Version(),
			NowUnixNano:     nowUnixNano,
		})
		if err != nil {
			return ExpiryScanPlan{}, fmt.Errorf("%w: %v", ErrInvalidExpiryScan, err)
		}
		intents = append(intents, intent)
	}

	sort.Slice(intents, func(i, j int) bool {
		if intents[i].DeadlineUnixNano() != intents[j].DeadlineUnixNano() {
			return intents[i].DeadlineUnixNano() < intents[j].DeadlineUnixNano()
		}
		if intents[i].WorkRef().HomePeerID().String() != intents[j].WorkRef().HomePeerID().String() {
			return intents[i].WorkRef().HomePeerID().String() < intents[j].WorkRef().HomePeerID().String()
		}
		return intents[i].WorkRef().WorkID().String() < intents[j].WorkRef().WorkID().String()
	})
	sort.Slice(exhausted, func(i, j int) bool {
		if exhausted[i].HomePeerID().String() != exhausted[j].HomePeerID().String() {
			return exhausted[i].HomePeerID().String() < exhausted[j].HomePeerID().String()
		}
		return exhausted[i].WorkID().String() < exhausted[j].WorkID().String()
	})
	return ExpiryScanPlan{intents: intents, exhausted: exhausted}, nil
}

func planExpiryIntent(work model.ReviewWork, expectedVersion uint64, nowUnixNano int64, requested model.EventType) TransitionIntent {
	return newTransitionIntent(
		work,
		expectedVersion,
		nowUnixNano,
		requested,
		model.EventReviewExpired,
		model.WorkExpired,
		work.Iteration(),
	)
}
