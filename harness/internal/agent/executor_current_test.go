package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type executorCurrentActionCase struct {
	name        string
	kind        model.OperationKind
	state       model.WorkState
	version     uint64
	iteration   uint8
	content     string
	wantEvent   model.EventType
	wantState   model.WorkState
	wantIter    uint8
	participant bool
	artifacts   bool
}

func TestTeamworkActionExecutorExecutesAllCurrentActions(t *testing.T) {
	t.Parallel()
	tests := []executorCurrentActionCase{
		{name: "accept", kind: model.OperationTeamworkAccept, state: model.WorkOffered, version: 1,
			iteration: 1, wantEvent: model.EventReviewAcceptRequested, participant: true},
		{name: "decline", kind: model.OperationTeamworkDecline, state: model.WorkOffered, version: 1,
			iteration: 1, content: "not suitable", wantEvent: model.EventReviewDeclineRequested, participant: true},
		{name: "deliver", kind: model.OperationTeamworkDeliver, state: model.WorkActive, version: 2,
			iteration: 1, content: "review complete", wantEvent: model.EventReviewDeliveryReady,
			participant: true, artifacts: true},
		{name: "rework", kind: model.OperationTeamworkRework, state: model.WorkDelivered, version: 3,
			iteration: 1, content: "fix edge case", wantEvent: model.EventReviewReworkRequested,
			wantState: model.WorkRework, wantIter: 2, artifacts: true},
		{name: "close", kind: model.OperationTeamworkClose, state: model.WorkDelivered, version: 3,
			iteration: 1, wantEvent: model.EventReviewClosed, wantState: model.WorkClosed, wantIter: 1},
		{name: "cancel", kind: model.OperationTeamworkCancel, state: model.WorkDelivered, version: 3,
			iteration: 1, content: "superseded", wantEvent: model.EventReviewCancelled,
			wantState: model.WorkCancelled, wantIter: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { runExecutorCurrentActionCase(t, test) })
	}
}

func runExecutorCurrentActionCase(t *testing.T, test executorCurrentActionCase) {
	t.Helper()
	fixture := newExecutorFixture(t, 1)
	work := executorCurrentActionWork(t, fixture, test)
	fixture.backend.work = work
	paths := executorCurrentActionArtifacts(fixture, test)
	action := executorAction(t, test.name, true, test.content, "", "", paths)
	reservation := executorReservation(t, fixture, action, work, true)
	response, controlErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: TeamworkActionRequest{Action: test.name, Content: test.content, Artifacts: paths},
		Action:  action, Reservation: reservation, At: fixture.at})
	if controlErr != nil {
		t.Fatalf("ExecuteTeamwork(%s) error = %v", test.name, controlErr)
	}
	if response.Handling == nil || response.Handling.Status != "completed" ||
		len(response.Results) != 1 || fixture.artifacts.calls != 1 || fixture.backend.deadlines != 0 {
		t.Fatalf("%s response = %#v", test.name, response)
	}
	item := fixture.backend.committed.items[0]
	eventValue := item.Publication.Event()
	assertExecutorCurrentActionEvent(t, fixture, test, reservation, eventValue)
	assertExecutorCurrentArtifactAuthority(t, fixture, test, reservation, paths, eventValue)
	assertExecutorCurrentTransition(t, test, work, item, eventValue)
}

func executorCurrentActionWork(t *testing.T, fixture *executorFixture,
	test executorCurrentActionCase,
) model.ReviewWork {
	t.Helper()
	work := fixture.work(t, test.state, test.version, test.iteration, test.participant)
	if test.participant {
		return work
	}
	spec := work.Spec()
	spec.DeadlineUnixNano = fixture.at.Add(500 * time.Millisecond).UnixNano()
	work, err := model.NewReviewWork(spec)
	if err != nil {
		t.Fatal(err)
	}
	return work
}

func executorCurrentActionArtifacts(fixture *executorFixture,
	test executorCurrentActionCase,
) []string {
	if !test.artifacts {
		return nil
	}
	ref, _ := model.NewArtifactRef(model.Sum([]byte("artifact-"+test.name)), model.ArtifactProduced)
	fixture.artifacts.result.References = []model.ArtifactRef{ref}
	return []string{"result.txt"}
}

func assertExecutorCurrentActionEvent(t *testing.T, fixture *executorFixture,
	test executorCurrentActionCase, reservation store.ManagedOperationReservation,
	eventValue model.Event,
) {
	t.Helper()
	wantEventID, _ := derivedActionEventID(reservation.Operation.ID())
	if eventValue.Type() != test.wantEvent || len(eventValue.CausedBy()) != 1 ||
		eventValue.ID() != wantEventID ||
		eventValue.CausedBy()[0] != executorCurrent(t, reservation.Run).SourceEvent() ||
		eventValue.Scope().OriginSequence() != fixture.scope.firstOriginSequence ||
		eventValue.Scope().ChannelSequence() != fixture.scope.firstChannelSequence ||
		eventValue.Scope().PublicationRoster() != fixture.scope.publicationRoster ||
		!eventValue.AcceptedAt().Equal(fixture.at) {
		t.Fatalf("%s Event = %#v", test.name, eventValue)
	}
}

func assertExecutorCurrentArtifactAuthority(t *testing.T, fixture *executorFixture,
	test executorCurrentActionCase, reservation store.ManagedOperationReservation,
	paths []string, eventValue model.Event,
) {
	t.Helper()
	spec := fixture.artifacts.last
	if spec.Reservation.Operation.ID() != reservation.Operation.ID() || spec.Action != test.kind ||
		!spec.HasCurrent || spec.Current.SourceEvent() != executorCurrent(t, reservation.Run).SourceEvent() ||
		strings.Join(spec.Paths, "\x00") != strings.Join(paths, "\x00") {
		t.Fatalf("%s Artifact authority = %#v", test.name, spec)
	}
	if test.artifacts != (len(eventValue.Artifacts()) == 1) {
		t.Fatalf("%s Artifact refs = %#v", test.name, eventValue.Artifacts())
	}
}

func assertExecutorCurrentTransition(t *testing.T, test executorCurrentActionCase,
	work model.ReviewWork, item store.LocalAcceptanceItem, eventValue model.Event,
) {
	t.Helper()
	if test.participant {
		if item.Work != nil || eventValue.Scope().OriginPeerID() != work.Participants().ReviewerPeerID() ||
			!eventValue.Audience().Contains(work.Ref().HomePeerID()) {
			t.Fatalf("participant action mutated Work or changed participants: %#v", item)
		}
		return
	}
	if item.Work == nil || item.Work.Work.State() != test.wantState ||
		item.Work.Work.Version() != work.Version()+1 || item.Work.Work.Iteration() != test.wantIter ||
		eventValue.Scope().OriginPeerID() != work.Ref().HomePeerID() ||
		!eventValue.Audience().Contains(work.Participants().ReviewerPeerID()) {
		t.Fatalf("home transition = %#v", item.Work)
	}
}
