package agent

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestTeamworkActionExecutorFinalizesCommittedArtifactBeforeReplay(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	root := model.Sum([]byte("executor-finalize-artifact"))
	ref, _ := model.NewArtifactRef(root, model.ArtifactProduced)
	fixture.artifacts.result.References = []model.ArtifactRef{ref}
	action := executorAction(t, "offer", false, "artifact result", "30m",
		AgentParticipantAuto, []string{"result.txt"})
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	pending := NewControlError(CodeOperationPending, "Artifact publication remains pending")
	fixture.artifacts.publishErr = pending
	if _, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	}); apiErr != pending || fixture.artifacts.publishCalls != 1 ||
		fixture.backend.lastReceipt.IsZero() {
		t.Fatalf("fresh pending publication = (%#v,%d calls)", apiErr,
			fixture.artifacts.publishCalls)
	}
	checkpoint, err := buildArtifactCaptureCheckpoint([]ArtifactCaptureRoot{{
		RootDigest: root,
		ManifestDigest: model.Sum(
			[]byte("manifest-" + root.String())),
	}})
	if err != nil {
		t.Fatal(err)
	}
	terminal := executorTerminalOperationWithCapture(t, reservation.Operation,
		fixture.backend.lastReceipt, checkpoint, fixture.at)
	replaySpec := TeamworkExecutionSpec{Action: action,
		Reservation: store.ManagedOperationReservation{
			Operation: terminal, Replayed: true,
		}}
	if replay, apiErr := fixture.executor.ExecuteTeamwork(
		context.Background(), replaySpec); apiErr != pending ||
		replay.Status != "" || fixture.artifacts.publishCalls != 2 {
		t.Fatalf("pending committed replay = (%#v,%#v,%d calls)",
			replay, apiErr, fixture.artifacts.publishCalls)
	}
	fixture.artifacts.publishErr = nil
	replay, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), replaySpec)
	if apiErr != nil || !replay.Replayed || replay.Status != "accepted" ||
		fixture.artifacts.publishCalls != 3 {
		t.Fatalf("finalized committed replay = (%#v,%#v,%d calls)",
			replay, apiErr, fixture.artifacts.publishCalls)
	}
}

func TestServiceRoutesCommittedArtifactReplayThroughFinalizer(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	root := model.Sum([]byte("service-finalize-artifact"))
	ref, _ := model.NewArtifactRef(root, model.ArtifactProduced)
	fixture.artifacts.result.References = []model.ArtifactRef{ref}
	request := TeamworkActionRequest{Action: "offer", Channel: "alpha",
		To: AgentParticipantAuto, Deadline: "30m", Content: "artifact result",
		Artifacts: []string{"result.txt"}}
	action := executorAction(t, request.Action, false, request.Content,
		request.Deadline, request.To, request.Artifacts)
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	first, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: request, Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	checkpoint, err := buildArtifactCaptureCheckpoint([]ArtifactCaptureRoot{{
		RootDigest: root, ManifestDigest: model.Sum([]byte("manifest-" + root.String())),
	}})
	if err != nil {
		t.Fatal(err)
	}
	terminal := executorTerminalOperationWithCapture(t, reservation.Operation,
		fixture.backend.lastReceipt, checkpoint, fixture.at)
	probeCalls, executorCalls := 0, 0
	views := &fakeAgentCurrentViews{}
	service, err := NewService(&fakeControlStore{
		operation: exactTerminalProbe(t, terminal, &probeCalls),
	}, ServiceOptions{Actions: testActionHandlers(t), CurrentViews: views,
		Executor: fakeTeamworkExecutor{execute: func(ctx context.Context,
			spec TeamworkExecutionSpec,
		) (OperationResponse, *ControlError) {
			executorCalls++
			if ctx == nil || spec.Reservation.Operation.ID() != terminal.ID() ||
				!spec.Reservation.Replayed {
				t.Fatalf("terminal finalization authority = %#v", spec.Reservation)
			}
			response := first
			response.Replayed = true
			return response, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, apiErr := service.TeamworkAction(context.Background(), ControlMetadata{
		Profile:          disabledReplayProfile(t, fixture.profile),
		OperationKeyHash: terminal.ClientKeyHash(), HasOperationKey: true,
	}, request)
	want := first
	want.Replayed = true
	if apiErr != nil || !reflect.DeepEqual(got, want) || probeCalls != 1 ||
		executorCalls != 1 || len(views.cleanups) != 1 ||
		views.cleanups[0] != terminal.AgentRunID() {
		t.Fatalf("service finalized replay = (%#v,%#v), probe/executor/cleanup %d/%d/%v",
			got, apiErr, probeCalls, executorCalls, views.cleanups)
	}
}

func executorTerminalOperationWithCapture(t *testing.T, base model.Operation,
	result, capture model.JSON, at time.Time,
) model.Operation {
	t.Helper()
	contextHash, hasContext := base.ContextHash()
	var contextValue *model.Digest
	if hasContext {
		contextValue = &contextHash
	}
	operation, err := model.NewOperation(model.OperationSpec{ID: base.ID(),
		ProfileID: base.ProfileID(), AgentRunID: base.AgentRunID(),
		ClientKeyHash: base.ClientKeyHash(), ContextHash: contextValue,
		Kind: base.Kind(), RequestDigest: base.RequestDigest(),
		Status: model.OperationCommitted, Capture: &capture, Result: &result,
		CreatedAt: base.CreatedAt(), FinishedAt: &at})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}
