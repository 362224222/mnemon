package teamwork

import (
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPlanHomeTransitionClosedPolicy(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	tests := []struct {
		name          string
		state         model.WorkState
		version       uint64
		iteration     uint8
		eventType     model.EventType
		wantState     model.WorkState
		wantIteration uint8
	}{
		{"accept offer", model.WorkOffered, 1, 1, model.EventReviewAccepted, model.WorkActive, 1},
		{"decline offer", model.WorkOffered, 1, 1, model.EventReviewDeclined, model.WorkDeclined, 1},
		{"cancel offer", model.WorkOffered, 1, 1, model.EventReviewCancelled, model.WorkCancelled, 1},
		{"deliver active", model.WorkActive, 2, 1, model.EventReviewDelivered, model.WorkDelivered, 1},
		{"cancel active", model.WorkActive, 2, 1, model.EventReviewCancelled, model.WorkCancelled, 1},
		{"deliver rework", model.WorkRework, 4, 2, model.EventReviewDelivered, model.WorkDelivered, 2},
		{"cancel rework", model.WorkRework, 4, 2, model.EventReviewCancelled, model.WorkCancelled, 2},
		{"request one rework", model.WorkDelivered, 3, 1, model.EventReviewReworkRequested, model.WorkRework, 2},
		{"close first delivery", model.WorkDelivered, 3, 1, model.EventReviewClosed, model.WorkClosed, 1},
		{"close second delivery", model.WorkDelivered, 5, 2, model.EventReviewClosed, model.WorkClosed, 2},
		{"cancel first delivery", model.WorkDelivered, 3, 1, model.EventReviewCancelled, model.WorkCancelled, 1},
		{"cancel final delivery", model.WorkDelivered, 5, 2, model.EventReviewCancelled, model.WorkCancelled, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			work := newTestWork(t, "work-policy", "peer-home", test.state, test.version, test.iteration, deadline)
			plan, err := PlanHomeTransition(HomeTransitionSpec{
				Work:            work,
				ActorPeerID:     work.Ref().HomePeerID(),
				ExpectedVersion: work.Version(),
				EventType:       test.eventType,
				NowUnixNano:     deadline - 1,
			})
			if err != nil {
				t.Fatalf("PlanHomeTransition() error = %v", err)
			}
			if plan.WorkRef() != work.Ref() || plan.ChannelID() != work.ChannelID() {
				t.Fatalf("plan lost Work scope")
			}
			if plan.ExpectedVersion() != test.version || plan.ExpectedState() != test.state || plan.ExpectedIteration() != test.iteration {
				t.Fatalf("CAS expectation = (%d,%s,%d), want (%d,%s,%d)", plan.ExpectedVersion(), plan.ExpectedState(), plan.ExpectedIteration(), test.version, test.state, test.iteration)
			}
			if plan.NextVersion() != test.version+1 || plan.NextState() != test.wantState || plan.NextIteration() != test.wantIteration {
				t.Fatalf("next = (%d,%s,%d), want (%d,%s,%d)", plan.NextVersion(), plan.NextState(), plan.NextIteration(), test.version+1, test.wantState, test.wantIteration)
			}
			if plan.RequestedEventType() != test.eventType || plan.AuthoritativeEventType() != test.eventType || plan.DeadlineWon() {
				t.Fatalf("ordinary transition Event mapping is not identity")
			}
			if plan.DeadlineUnixNano() != deadline {
				t.Fatalf("transition changed frozen deadline")
			}
		})
	}
}

func TestPlanHomeTransitionRejectsUnauthorizedOrInvalidInputs(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	offered := newTestWork(t, "work-invalid", "peer-home", model.WorkOffered, 1, 1, deadline)
	tests := []struct {
		name      string
		work      model.ReviewWork
		actor     model.PeerID
		version   uint64
		eventType model.EventType
		wantErr   error
	}{
		{"remote actor", offered, parsePeer(t, "peer-reviewer"), 1, model.EventReviewAccepted, ErrNotWorkHome},
		{"stale version", offered, offered.Ref().HomePeerID(), 2, model.EventReviewAccepted, ErrVersionConflict},
		{"accept request is input", offered, offered.Ref().HomePeerID(), 1, model.EventReviewAcceptRequested, ErrParticipantInput},
		{"decline request is input", offered, offered.Ref().HomePeerID(), 1, model.EventReviewDeclineRequested, ErrParticipantInput},
		{"delivery ready is input", offered, offered.Ref().HomePeerID(), 1, model.EventReviewDeliveryReady, ErrParticipantInput},
		{"offer creates, not transitions", offered, offered.Ref().HomePeerID(), 1, model.EventReviewOffered, ErrTransitionNotAllowed},
		{"accept rejected is receipt", offered, offered.Ref().HomePeerID(), 1, model.EventReviewAcceptRejected, ErrTransitionNotAllowed},
		{"outcome is fallback receipt", offered, offered.Ref().HomePeerID(), 1, model.EventReviewOutcome, ErrTransitionNotAllowed},
		{"close offered", offered, offered.Ref().HomePeerID(), 1, model.EventReviewClosed, ErrTransitionNotAllowed},
		{"second rework forbidden", newTestWork(t, "work-rework-once", "peer-home", model.WorkDelivered, 5, 2, deadline), offered.Ref().HomePeerID(), 5, model.EventReviewReworkRequested, ErrTransitionNotAllowed},
		{"terminal immutable", newTestWork(t, "work-terminal", "peer-home", model.WorkClosed, 4, 1, deadline), offered.Ref().HomePeerID(), 4, model.EventReviewCancelled, ErrTerminalWork},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PlanHomeTransition(HomeTransitionSpec{
				Work:            test.work,
				ActorPeerID:     test.actor,
				ExpectedVersion: test.version,
				EventType:       test.eventType,
				NowUnixNano:     deadline - 1,
			})
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("PlanHomeTransition() error = %v, want %v", err, test.wantErr)
			}
		})
	}

	exhausted := newTestWork(t, "work-version-max", "peer-home", model.WorkActive, model.MaxSQLiteInteger, 1, deadline)
	_, err := PlanHomeTransition(HomeTransitionSpec{exhausted, exhausted.Ref().HomePeerID(), exhausted.Version(), model.EventReviewDelivered, deadline - 1})
	if !errors.Is(err, ErrWorkVersionExhausted) {
		t.Fatalf("version exhaustion error = %v", err)
	}
}

func TestPlanHomeTransitionDeadlineWinsRaces(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC).UnixNano()
	active := newTestWork(t, "work-race", "peer-home", model.WorkActive, 7, 1, deadline)
	var plans []TransitionIntent
	for _, requested := range []model.EventType{model.EventReviewDelivered, model.EventReviewCancelled, model.EventReviewExpired} {
		plan, err := PlanHomeTransition(HomeTransitionSpec{
			Work:            active,
			ActorPeerID:     active.Ref().HomePeerID(),
			ExpectedVersion: active.Version(),
			EventType:       requested,
			NowUnixNano:     deadline,
		})
		if err != nil {
			t.Fatalf("PlanHomeTransition(%s) error = %v", requested, err)
		}
		if plan.AuthoritativeEventType() != model.EventReviewExpired || plan.NextState() != model.WorkExpired || plan.NextVersion() != 8 {
			t.Fatalf("deadline plan for %s = (%s,%s,v%d)", requested, plan.AuthoritativeEventType(), plan.NextState(), plan.NextVersion())
		}
		if got, want := plan.DeadlineWon(), requested != model.EventReviewExpired; got != want {
			t.Fatalf("DeadlineWon() for %s = %t, want %t", requested, got, want)
		}
		plans = append(plans, plan)
	}
	for _, plan := range plans[1:] {
		if plan.ExpectedVersion() != plans[0].ExpectedVersion() || plan.ExpectedState() != plans[0].ExpectedState() || plan.NextVersion() != plans[0].NextVersion() || plan.NextState() != plans[0].NextState() || plan.AuthoritativeEventType() != plans[0].AuthoritativeEventType() {
			t.Fatalf("racing home decisions did not derive the same expiry CAS intent")
		}
	}

	before, err := PlanHomeTransition(HomeTransitionSpec{active, active.Ref().HomePeerID(), active.Version(), model.EventReviewDelivered, deadline - 1})
	if err != nil || before.NextState() != model.WorkDelivered {
		t.Fatalf("pre-deadline delivery = (%v, %v), want DELIVERED", before.NextState(), err)
	}
	_, err = PlanHomeTransition(HomeTransitionSpec{active, active.Ref().HomePeerID(), active.Version(), model.EventReviewExpired, deadline - 1})
	if !errors.Is(err, ErrWorkNotDue) {
		t.Fatalf("early expiry error = %v, want ErrWorkNotDue", err)
	}

	// Participant publications remain controller inputs even at the boundary.
	_, err = PlanHomeTransition(HomeTransitionSpec{active, active.Ref().HomePeerID(), active.Version(), model.EventReviewDeliveryReady, deadline})
	if !errors.Is(err, ErrParticipantInput) {
		t.Fatalf("participant input at deadline error = %v", err)
	}
	_, err = PlanHomeTransition(HomeTransitionSpec{active, active.Ref().HomePeerID(), active.Version(), model.EventReviewAcceptRejected, deadline})
	if !errors.Is(err, ErrTransitionNotAllowed) {
		t.Fatalf("non-mutating receipt at deadline error = %v", err)
	}

	// DELIVERED has a result closure and is explicitly outside timer expiry.
	delivered := newTestWork(t, "work-delivered", "peer-home", model.WorkDelivered, 8, 1, deadline)
	closed, err := PlanHomeTransition(HomeTransitionSpec{delivered, delivered.Ref().HomePeerID(), delivered.Version(), model.EventReviewClosed, deadline})
	if err != nil || closed.NextState() != model.WorkClosed || closed.AuthoritativeEventType() != model.EventReviewClosed {
		t.Fatalf("delivered close at deadline = (%v,%v,%v)", closed.NextState(), closed.AuthoritativeEventType(), err)
	}
	_, err = PlanHomeTransition(HomeTransitionSpec{delivered, delivered.Ref().HomePeerID(), delivered.Version(), model.EventReviewExpired, deadline})
	if !errors.Is(err, ErrWorkNotExpirable) {
		t.Fatalf("delivered explicit expiry error = %v, want ErrWorkNotExpirable", err)
	}
}

func newTestWork(t *testing.T, workName, homeName string, state model.WorkState, version uint64, iteration uint8, deadline int64) model.ReviewWork {
	return newTestWorkInChannel(t, "channel-alpha", workName, homeName, state, version, iteration, deadline)
}

func newTestWorkInChannel(t *testing.T, channelName, workName, homeName string, state model.WorkState, version uint64, iteration uint8, deadline int64) model.ReviewWork {
	t.Helper()
	home := parsePeer(t, homeName)
	reviewer := parsePeer(t, homeName+"-reviewer")
	channel := parseChannel(t, channelName)
	workID, err := model.ParseWorkID(workName)
	if err != nil {
		t.Fatalf("ParseWorkID(%q): %v", workName, err)
	}
	ref, err := model.NewWorkRef(home, workID)
	if err != nil {
		t.Fatalf("NewWorkRef(): %v", err)
	}
	participants, err := model.NewParticipantSnapshot(channel, 3, home, reviewer)
	if err != nil {
		t.Fatalf("NewParticipantSnapshot(): %v", err)
	}
	stateData, err := model.NewJSON([]byte(`{"test":true}`))
	if err != nil {
		t.Fatalf("NewJSON(): %v", err)
	}
	updatedBy, err := model.ParseEventID("event-" + workName + "-" + string(state))
	if err != nil {
		t.Fatalf("ParseEventID(): %v", err)
	}
	work, err := model.NewReviewWork(model.ReviewWorkSpec{
		Ref:              ref,
		ChannelID:        channel,
		Participants:     participants,
		Version:          version,
		Iteration:        iteration,
		DeadlineUnixNano: deadline,
		State:            state,
		StateData:        stateData,
		UpdatedBy:        updatedBy,
		UpdatedAt:        time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("NewReviewWork(%s): %v", state, err)
	}
	return work
}
