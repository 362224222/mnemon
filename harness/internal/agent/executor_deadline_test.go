package agent

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestTeamworkActionExecutorAtomicallyLetsDeadlineWin(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := executorDueWork(t, fixture)
	fixture.backend.work = work
	action := executorAction(t, "cancel", true, "deadline race", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)

	response, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodeWorkExpired || apiErr.Replayed ||
		apiErr.OperationID == nil || response.OperationID != "" || fixture.backend.deadlines != 1 ||
		fixture.backend.commits != 0 || fixture.backend.rejects != 0 ||
		fixture.backend.deadlineAt != fixture.clock.now {
		t.Fatalf("deadline winner = (%#v, %#v), backend=%#v", response, apiErr, fixture.backend)
	}
	assertExecutorDeadlineMutation(t, fixture, work)
	assertExecutorDeadlineAuthority(t, fixture, reservation)
	assertExecutorDeadlineReplay(t, fixture, action, apiErr)
}

func executorDueWork(t *testing.T, fixture *executorFixture) model.ReviewWork {
	t.Helper()
	work := fixture.work(t, model.WorkActive, 2, 1, false)
	workSpec := work.Spec()
	workSpec.DeadlineUnixNano = fixture.at.Add(500 * time.Millisecond).UnixNano()
	work, err := model.NewReviewWork(workSpec)
	if err != nil {
		t.Fatal(err)
	}
	return work
}

func assertExecutorDeadlineMutation(t *testing.T, fixture *executorFixture,
	work model.ReviewWork,
) {
	t.Helper()
	item := fixture.backend.deadline.expiry
	if item.Work == nil || item.Work.Work.State() != model.WorkExpired ||
		item.Work.Work.Version() != work.Version()+1 || item.Work.Work.Iteration() != work.Iteration() {
		t.Fatalf("deadline Work mutation = %#v", item.Work)
	}
}

func assertExecutorDeadlineAuthority(t *testing.T, fixture *executorFixture,
	reservation store.ManagedOperationReservation,
) {
	t.Helper()
	item := fixture.backend.deadline.expiry
	expiry := item.Publication.Event()
	wantEventID, _ := derivedDeadlineEventID(reservation.Operation.ID())
	contextHash, _ := reservation.Operation.ContextHash()
	if expiry.ID() != wantEventID || expiry.Type() != model.EventReviewExpired ||
		!expiry.AcceptedAt().Equal(fixture.clock.now) || len(expiry.CausedBy()) != 1 ||
		expiry.CausedBy()[0] != executorCurrent(t, reservation.Run).SourceEvent() ||
		fixture.backend.deadline.contextHash != contextHash ||
		fixture.backend.deadline.operation != localExecutionAuthority(reservation.Operation) {
		t.Fatalf("deadline Event/authority = %#v / %#v", expiry, fixture.backend.deadline)
	}
}

func assertExecutorDeadlineReplay(t *testing.T, fixture *executorFixture,
	action ValidatedAction, apiErr *ControlError,
) {
	t.Helper()
	_, replayErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: store.ManagedOperationReservation{Operation: fixture.backend.rejected,
			Replayed: true}, At: fixture.at.Add(2 * time.Second),
	})
	if replayErr == nil || replayErr.Code != apiErr.Code || replayErr.Message != apiErr.Message ||
		!replayErr.Replayed || fixture.backend.deadlines != 1 || fixture.backend.rejects != 0 {
		t.Fatalf("deadline replay = %#v", replayErr)
	}
}
