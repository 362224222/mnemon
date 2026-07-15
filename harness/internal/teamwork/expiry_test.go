package teamwork

import (
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPlanExpiryIsHomeOnlyAndUsesIntegerBoundary(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 123, time.UTC).UnixNano()
	work := newTestWork(t, "work-expiry", "peer-home", model.WorkRework, 9, 2, deadline)
	tests := []struct {
		name    string
		home    model.PeerID
		now     int64
		wantErr error
	}{
		{"one nanosecond early", work.Ref().HomePeerID(), deadline - 1, ErrWorkNotDue},
		{"at boundary", work.Ref().HomePeerID(), deadline, nil},
		{"after boundary", work.Ref().HomePeerID(), deadline + 1, nil},
		{"remote cannot expire", parsePeer(t, "peer-remote"), deadline, ErrNotWorkHome},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan, err := PlanExpiry(ExpirySpec{work, test.home, work.Version(), test.now})
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("PlanExpiry() error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("PlanExpiry() error = %v", err)
			}
			if plan.ExpectedVersion() != 9 || plan.ExpectedState() != model.WorkRework || plan.NextVersion() != 10 || plan.NextState() != model.WorkExpired {
				t.Fatalf("expiry CAS plan = %d/%s -> %d/%s", plan.ExpectedVersion(), plan.ExpectedState(), plan.NextVersion(), plan.NextState())
			}
			if plan.DeadlineUnixNano() != deadline || plan.ObservedAtUnixNano() != test.now {
				t.Fatalf("expiry lost integer times: deadline=%d now=%d", plan.DeadlineUnixNano(), plan.ObservedAtUnixNano())
			}
			if plan.AuthoritativeEventType() != model.EventReviewExpired || plan.DeadlineWon() {
				t.Fatalf("direct expiry Event mapping mismatch")
			}
		})
	}
}

func TestPlanExpiryRejectsDeliveredAndTerminalWorks(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	for _, state := range []model.WorkState{
		model.WorkDelivered,
		model.WorkClosed,
		model.WorkDeclined,
		model.WorkExpired,
		model.WorkCancelled,
	} {
		state := state
		t.Run(string(state), func(t *testing.T) {
			work := newTestWork(t, "work-no-expire-"+string(state), "peer-home", state, 4, 1, deadline)
			_, err := PlanExpiry(ExpirySpec{work, work.Ref().HomePeerID(), work.Version(), deadline})
			if !errors.Is(err, ErrWorkNotExpirable) {
				t.Fatalf("PlanExpiry(%s) error = %v, want ErrWorkNotExpirable", state, err)
			}
		})
	}
}

func TestPlanExpiryRejectsSQLiteVersionExhaustion(t *testing.T) {
	t.Parallel()
	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	work := newTestWork(t, "work-expiry-version-max", "peer-home", model.WorkActive, model.MaxSQLiteInteger, 1, deadline)
	_, err := PlanExpiry(ExpirySpec{Work: work, HomePeerID: work.Ref().HomePeerID(),
		ExpectedVersion: work.Version(), NowUnixNano: deadline})
	if !errors.Is(err, ErrWorkVersionExhausted) {
		t.Fatalf("PlanExpiry() error = %v, want ErrWorkVersionExhausted", err)
	}
}

func TestPlanExpiryScanIsRestartDeterministic(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	home := parsePeer(t, "peer-home")
	works := []model.ReviewWork{
		newTestWork(t, "work-z", "peer-home", model.WorkOffered, 1, 1, now-10),
		newTestWork(t, "work-future", "peer-home", model.WorkActive, 3, 1, now+1),
		newTestWork(t, "work-delivered-scan", "peer-home", model.WorkDelivered, 4, 1, now-100),
		newTestWork(t, "work-remote", "peer-other", model.WorkActive, 2, 1, now-100),
		newTestWork(t, "work-b", "peer-home", model.WorkRework, 5, 2, now-20),
		newTestWork(t, "work-a", "peer-home", model.WorkActive, 6, 1, now-20),
	}

	plan, err := PlanExpiryScan(home, now, works)
	if err != nil {
		t.Fatalf("PlanExpiryScan() error = %v", err)
	}
	intents := plan.Intents()
	if len(intents) != 3 {
		t.Fatalf("len(Intents()) = %d, want 3", len(intents))
	}
	for index, want := range []string{"work-a", "work-b", "work-z"} {
		if got := intents[index].WorkRef().WorkID().String(); got != want {
			t.Errorf("intent[%d] WorkID = %q, want %q", index, got, want)
		}
		if intents[index].WorkRef().HomePeerID() != home || intents[index].AuthoritativeEventType() != model.EventReviewExpired {
			t.Errorf("intent[%d] is not a home expiry", index)
		}
	}

	reversed := append([]model.ReviewWork(nil), works...)
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	restarted, err := PlanExpiryScan(home, now, reversed)
	if err != nil {
		t.Fatalf("restart PlanExpiryScan() error = %v", err)
	}
	for index, intent := range restarted.Intents() {
		if intent.WorkRef() != intents[index].WorkRef() || intent.ExpectedVersion() != intents[index].ExpectedVersion() {
			t.Fatalf("restart order changed at %d", index)
		}
	}

	intents[0] = TransitionIntent{}
	if plan.Intents()[0].WorkRef().IsZero() {
		t.Fatalf("Intents() exposed mutable scan storage")
	}
}

func TestPlanExpiryScanRejectsDuplicateWork(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	work := newTestWork(t, "work-duplicate", "peer-home", model.WorkActive, 2, 1, now)
	_, err := PlanExpiryScan(work.Ref().HomePeerID(), now, []model.ReviewWork{work, work})
	if !errors.Is(err, ErrInvalidExpiryScan) {
		t.Fatalf("duplicate scan error = %v, want ErrInvalidExpiryScan", err)
	}
}

func TestPlanExpiryScanIsolatesVersionExhaustion(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	home := parsePeer(t, "peer-home")
	exhausted := newTestWork(t, "work-exhausted", "peer-home", model.WorkActive, model.MaxSQLiteInteger, 1, now-1)
	healthy := newTestWork(t, "work-healthy", "peer-home", model.WorkActive, 3, 1, now-2)

	plan, err := PlanExpiryScan(home, now, []model.ReviewWork{exhausted, healthy})
	if err != nil {
		t.Fatalf("PlanExpiryScan() error = %v", err)
	}
	if got := plan.Intents(); len(got) != 1 || got[0].WorkRef() != healthy.Ref() {
		t.Fatalf("healthy intents = %#v", got)
	}
	if got := plan.Exhausted(); len(got) != 1 || got[0] != exhausted.Ref() {
		t.Fatalf("exhausted diagnostics = %#v", got)
	}

	copy := plan.Exhausted()
	copy[0] = model.WorkRef{}
	if plan.Exhausted()[0].IsZero() {
		t.Fatal("Exhausted() exposed mutable scan storage")
	}
}
