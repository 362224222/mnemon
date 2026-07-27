package agent

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestTeamworkActionExecutorReportsAsyncRejectionWinnerAsReplay(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := fixture.work(t, model.WorkDelivered, 3, 1, false)
	fixture.backend.work, fixture.backend.commitErr = work, store.ErrWorkCASConflict
	action := executorAction(t, "close", true, "", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)
	const message = "managed operation context expired with its Agent Runtime claim"
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: reservation.Operation.ID(), Code: string(CodeContextStale), Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.rejected = executorTerminalOperation(t, reservation.Operation,
		model.OperationRejected, receipt.JSON(), fixture.clock.now)
	fixture.backend.rejectReplay = true
	_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodeContextStale || apiErr.Message != message ||
		!apiErr.Replayed || apiErr.OperationID == nil ||
		*apiErr.OperationID != reservation.Operation.ID().String() || fixture.backend.rejects != 1 {
		t.Fatalf("async rejection winner = %#v", apiErr)
	}
}

func TestTeamworkActionExecutorReportsCommittedRejectionWinnerAsReplay(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := fixture.work(t, model.WorkDelivered, 3, 1, false)
	fixture.backend.work = work
	action := executorAction(t, "close", true, "", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)
	first, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr != nil || first.Status != "accepted" {
		t.Fatalf("prepare committed winner = (%#v, %#v)", first, apiErr)
	}
	fixture.backend.rejected = executorTerminalOperation(t, reservation.Operation,
		model.OperationCommitted, fixture.backend.lastReceipt, fixture.clock.now)
	fixture.backend.commitErr, fixture.backend.rejectReplay = store.ErrWorkCASConflict, true
	replay, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr != nil || replay.Status != first.Status || replay.OperationID != first.OperationID ||
		!replay.Replayed || fixture.backend.rejects != 1 {
		t.Fatalf("committed rejection winner = (%#v, %#v)", replay, apiErr)
	}
}

func TestTeamworkActionExecutorReprobesTerminalAfterRejectClockFailure(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := fixture.work(t, model.WorkDelivered, 3, 1, false)
	action := executorAction(t, "close", true, "", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)
	fixture.backend.work = model.ReviewWork{}
	const message = "managed operation context expired with its Agent Runtime claim"
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: reservation.Operation.ID(), Code: string(CodeContextStale), Message: message,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.backend.probeTerminal = executorTerminalOperation(t, reservation.Operation,
		model.OperationRejected, receipt.JSON(), fixture.clock.now)
	fixture.backend.probeMisses, fixture.clock.now = 1, time.Time{}
	_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodeContextStale || apiErr.Message != message ||
		!apiErr.Replayed || fixture.backend.probes != 2 || fixture.backend.rejects != 0 {
		t.Fatalf("terminal replay after clock failure = %#v, backend=%#v", apiErr, fixture.backend)
	}
}

func TestTeamworkActionExecutorProbesTerminalBeforeRetryableError(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	fixture.selector.err = ErrAgentSelectionParticipantUnavailable
	action := executorAction(t, "offer", false, "terminal before retry", "30m",
		"reviewer-0", nil)
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	receipt := mustExecutorRejectionReceipt(t, reservation.Operation.ID(), CodeInvalidArgument,
		"durable rejection won before retry")
	fixture.backend.probeTerminal = executorTerminalOperation(t, reservation.Operation,
		model.OperationRejected, receipt, fixture.clock.now)
	_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr == nil || apiErr.Code != CodeInvalidArgument || !apiErr.Replayed ||
		fixture.backend.probes != 1 || fixture.backend.rejects != 0 {
		t.Fatalf("terminal before retryable error = %#v, backend=%#v", apiErr, fixture.backend)
	}
}

func TestTeamworkActionExecutorFailsClosedForRejectBackendDrift(t *testing.T) {
	t.Parallel()
	t.Run("substituted operation", func(t *testing.T) {
		fixture := newExecutorFixture(t, 1)
		work := fixture.work(t, model.WorkDelivered, 3, 1, false)
		fixture.backend.work, fixture.backend.commitErr = work, store.ErrWorkCASConflict
		action := executorAction(t, "close", true, "", "", "", nil)
		reservation := executorReservation(t, fixture, action, work, true)
		wrongID, _ := model.ParseOperationID("operation-executor-substituted")
		wrong := executorOperationWithID(t, reservation.Operation, wrongID)
		receipt := mustExecutorRejectionReceipt(t, wrongID, CodeContextStale, "substituted receipt")
		fixture.backend.rejected = executorTerminalOperation(t, wrong, model.OperationRejected,
			receipt, fixture.clock.now)
		fixture.backend.rejectReplay = true
		_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
			Action: action, Reservation: reservation, At: fixture.at,
		})
		if apiErr == nil || apiErr.Code != CodeInternal || !apiErr.Replayed ||
			apiErr.OperationID == nil || *apiErr.OperationID != reservation.Operation.ID().String() {
			t.Fatalf("substituted terminal authority = %#v", apiErr)
		}
	})

	t.Run("fresh receipt differs", func(t *testing.T) {
		fixture := newExecutorFixture(t, 1)
		work := fixture.work(t, model.WorkDelivered, 3, 1, false)
		fixture.backend.work, fixture.backend.commitErr = work, store.ErrWorkCASConflict
		action := executorAction(t, "close", true, "", "", "", nil)
		reservation := executorReservation(t, fixture, action, work, true)
		receipt := mustExecutorRejectionReceipt(t, reservation.Operation.ID(), CodeContextStale,
			"different fresh receipt")
		fixture.backend.rejected = executorTerminalOperation(t, reservation.Operation,
			model.OperationRejected, receipt, fixture.clock.now)
		fixture.backend.rejectFreshTerminal = true
		_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
			Action: action, Reservation: reservation, At: fixture.at,
		})
		if apiErr == nil || apiErr.Code != CodeInternal || apiErr.Replayed {
			t.Fatalf("different fresh terminal receipt = %#v", apiErr)
		}
	})

	t.Run("non-terminal success", func(t *testing.T) {
		fixture := newExecutorFixture(t, 1)
		work := fixture.work(t, model.WorkDelivered, 3, 1, false)
		fixture.backend.work, fixture.backend.commitErr = work, store.ErrWorkCASConflict
		action := executorAction(t, "close", true, "", "", "", nil)
		reservation := executorReservation(t, fixture, action, work, true)
		fixture.backend.rejected, fixture.backend.rejectFreshTerminal = reservation.Operation, true
		_, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
			Action: action, Reservation: reservation, At: fixture.at,
		})
		if apiErr == nil || apiErr.Code != CodeInternal || apiErr.Replayed {
			t.Fatalf("non-terminal rejection success = %#v", apiErr)
		}
	})
}

func TestServiceTeamworkTerminalReplayPrecedesLiveDependencies(t *testing.T) {
	t.Parallel()
	fixture, request, first, terminal := committedTeamworkReplayFixture(t, "goal")
	profile := disabledReplayProfile(t, fixture.profile)
	probeCalls, reserveCalls, gateCalls := 0, 0, 0
	fake := &fakeControlStore{
		operation: exactTerminalProbe(t, terminal, &probeCalls),
		reserve: func(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error) {
			reserveCalls++
			return store.ManagedOperationReservation{}, nil
		},
	}
	clock := &countingServiceClock{}
	random := &countingServiceRandom{}
	views := &fakeAgentCurrentViews{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t), Clock: clock,
		Random: random, CurrentViews: views,
		ActivationGate: serviceActivationGateFunc(func(context.Context, model.Profile) *ControlError {
			gateCalls++
			return NewControlError(CodeAssetRevisionMismatch, "live activation must not run")
		})})
	if err != nil {
		t.Fatal(err)
	}
	response, apiErr := service.TeamworkAction(context.Background(), ControlMetadata{Profile: profile,
		OperationKeyHash: terminal.ClientKeyHash(), HasOperationKey: true}, request)
	want := first
	want.Replayed = true
	if apiErr != nil || !reflect.DeepEqual(response, want) {
		t.Fatalf("terminal Teamwork replay = (%#v, %v), want %#v", response, apiErr, want)
	}
	if probeCalls != 1 || reserveCalls != 0 || gateCalls != 0 || clock.calls != 0 || random.calls != 0 ||
		len(views.cleanups) != 1 || views.cleanups[0] != terminal.AgentRunID() {
		t.Fatalf("terminal Teamwork dependencies = probe %d reserve %d gate %d clock %d random %d cleanup %v",
			probeCalls, reserveCalls, gateCalls, clock.calls, random.calls, views.cleanups)
	}
}

func TestServiceRejectedTeamworkReplayRequiresExactStaleContext(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	work := fixture.work(t, model.WorkOffered, 1, 1, true)
	action := executorAction(t, "accept", true, "", "", "", nil)
	reservation := executorReservation(t, fixture, action, work, true)
	wantError := NewControlError(CodeWorkConflict, "the original Work changed")
	receipt, err := buildTeamworkRejection(reservation.Operation, wantError)
	if err != nil {
		t.Fatal(err)
	}
	terminal := executorTerminalOperation(t, reservation.Operation, model.OperationRejected,
		receipt, fixture.at)
	profile := disabledReplayProfile(t, fixture.profile)
	probeCalls, gateCalls := 0, 0
	fake := &fakeControlStore{operation: exactTerminalProbe(t, terminal, &probeCalls)}
	clock := &countingServiceClock{}
	random := &countingServiceRandom{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t), Clock: clock,
		Random: random, CurrentViews: &fakeAgentCurrentViews{},
		ActivationGate: serviceActivationGateFunc(func(context.Context, model.Profile) *ControlError {
			gateCalls++
			return NewControlError(CodeAssetRevisionMismatch, "live activation must not run")
		})})
	if err != nil {
		t.Fatal(err)
	}
	contextHash, _ := terminal.ContextHash()
	metadata := ControlMetadata{Profile: profile, OperationKeyHash: terminal.ClientKeyHash(),
		HasOperationKey: true, ClaimContextHash: contextHash, HasClaimContext: true}
	_, replayErr := service.TeamworkAction(context.Background(), metadata,
		TeamworkActionRequest{Action: "accept"})
	if replayErr == nil || replayErr.Code != wantError.Code || replayErr.Message != wantError.Message ||
		!replayErr.Replayed || replayErr.OperationID == nil ||
		*replayErr.OperationID != terminal.ID().String() {
		t.Fatalf("rejected Teamwork replay = %#v", replayErr)
	}
	if probeCalls != 1 || gateCalls != 0 || clock.calls != 0 || random.calls != 0 {
		t.Fatalf("rejected replay dependencies = probe %d gate %d clock %d random %d",
			probeCalls, gateCalls, clock.calls, random.calls)
	}

	metadata.ClaimContextHash = model.Sum([]byte("wrong-context"))
	if _, apiErr := service.TeamworkAction(context.Background(), metadata,
		TeamworkActionRequest{Action: "accept"}); apiErr == nil || apiErr.Code != CodeOperationMismatch {
		t.Fatalf("changed replay context error = %#v", apiErr)
	}
	metadata.ClaimContextHash, metadata.HasClaimContext = model.Digest{}, false
	if _, apiErr := service.TeamworkAction(context.Background(), metadata,
		TeamworkActionRequest{Action: "accept"}); apiErr == nil || apiErr.Code != CodeContextRequired {
		t.Fatalf("missing replay context error = %#v", apiErr)
	}
}

func TestServiceResolutionTerminalReplayPrecedesLiveDependencies(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 19, 14, 0, 0, 0, time.UTC)
	profile := disabledReplayProfile(t, serviceTestProfile(t, at))
	contextHash := model.Sum([]byte("resolution-replay-context"))
	request := AgentResolveRequest{Decision: "retry", Content: "retry after repair"}
	requestDigest, err := store.ManagedResolutionRequestDigest(testActionHandlers(t).AssetRevision(),
		contextHash, model.OperationResolveRetry, request.Content)
	if err != nil {
		t.Fatal(err)
	}
	operationID, _ := model.ParseOperationID("operation-resolution-replay")
	runID, _ := model.ParseRunID("run-resolution-replay")
	response := OperationResponse{SchemaVersion: model.SchemaVersion, Status: "resolved",
		Action: string(model.OperationResolveRetry), OperationID: operationID.String(),
		Handling: &HandlingReceipt{Status: "requeued"}, Results: []OperationResult{},
		Receipt: model.Sum([]byte("resolution-evidence")).String()}
	receipt, err := model.JSONFrom(response)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: profile.ID(),
		AgentRunID: runID, ClientKeyHash: model.Sum([]byte("resolution-replay-key")),
		ContextHash: &contextHash, Kind: model.OperationResolveRetry, RequestDigest: requestDigest,
		Status: model.OperationCommitted, Result: &receipt, CreatedAt: at.Add(-time.Minute), FinishedAt: &at})
	if err != nil {
		t.Fatal(err)
	}
	probeCalls, reserveCalls, resolveCalls, gateCalls := 0, 0, 0, 0
	fake := &fakeControlStore{operation: exactTerminalProbe(t, terminal, &probeCalls),
		reserve: func(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error) {
			reserveCalls++
			return store.ManagedOperationReservation{}, nil
		},
		resolve: func(context.Context, store.ManagedResolutionSpec) (store.ManagedResolutionResult, error) {
			resolveCalls++
			return store.ManagedResolutionResult{}, nil
		}}
	clock := &countingServiceClock{}
	random := &countingServiceRandom{}
	views := &fakeAgentCurrentViews{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t), Clock: clock,
		Random: random, CurrentViews: views,
		ActivationGate: serviceActivationGateFunc(func(context.Context, model.Profile) *ControlError {
			gateCalls++
			return NewControlError(CodeAssetRevisionMismatch, "live activation must not run")
		})})
	if err != nil {
		t.Fatal(err)
	}
	got, apiErr := service.AgentResolve(context.Background(), ControlMetadata{Profile: profile,
		OperationKeyHash: terminal.ClientKeyHash(), HasOperationKey: true,
		ClaimContextHash: contextHash, HasClaimContext: true}, request)
	response.Replayed = true
	if apiErr != nil || !reflect.DeepEqual(got, response) {
		t.Fatalf("terminal resolution replay = (%#v, %v), want %#v", got, apiErr, response)
	}
	if probeCalls != 1 || reserveCalls != 0 || resolveCalls != 0 || gateCalls != 0 ||
		clock.calls != 0 || random.calls != 0 || len(views.cleanups) != 1 {
		t.Fatalf("resolution replay dependencies = probe %d reserve %d resolve %d gate %d clock %d random %d cleanup %v",
			probeCalls, reserveCalls, resolveCalls, gateCalls, clock.calls, random.calls, views.cleanups)
	}
}

func TestServiceRejectedResolutionReplayPreservesOriginalError(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 19, 15, 0, 0, 0, time.UTC)
	profile := disabledReplayProfile(t, serviceTestProfile(t, at))
	contextHash := model.Sum([]byte("rejected-resolution-context"))
	request := AgentResolveRequest{Decision: "reject", Content: "the request is not actionable"}
	requestDigest, err := store.ManagedResolutionRequestDigest(testActionHandlers(t).AssetRevision(),
		contextHash, model.OperationResolveReject, request.Content)
	if err != nil {
		t.Fatal(err)
	}
	operationID, _ := model.ParseOperationID("operation-rejected-resolution")
	runID, _ := model.ParseRunID("run-rejected-resolution")
	lease := at.Add(time.Minute)
	started, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: profile.ID(),
		AgentRunID: runID, ClientKeyHash: model.Sum([]byte("rejected-resolution-key")),
		ContextHash: &contextHash, Kind: model.OperationResolveReject, RequestDigest: requestDigest,
		Status: model.OperationStarted, LeaseOwner: "owner-rejected-resolution", LeaseUntil: &lease,
		CreatedAt: at.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	wantError := NewControlError(CodeWorkConflict, "the original resolution lost its Work race")
	receipt, err := buildTeamworkRejection(started, wantError)
	if err != nil {
		t.Fatal(err)
	}
	terminal := executorTerminalOperation(t, started, model.OperationRejected, receipt, at)
	probeCalls := 0
	service, err := NewService(&fakeControlStore{operation: exactTerminalProbe(t, terminal, &probeCalls)},
		ServiceOptions{Actions: testActionHandlers(t), CurrentViews: &fakeAgentCurrentViews{}})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := service.AgentResolve(context.Background(), ControlMetadata{Profile: profile,
		OperationKeyHash: terminal.ClientKeyHash(), HasOperationKey: true,
		ClaimContextHash: contextHash, HasClaimContext: true}, request)
	if apiErr == nil || apiErr.Code != wantError.Code || apiErr.Message != wantError.Message ||
		!apiErr.Replayed || apiErr.OperationID == nil || *apiErr.OperationID != terminal.ID().String() ||
		probeCalls != 1 {
		t.Fatalf("rejected resolution replay = %#v, probe calls %d", apiErr, probeCalls)
	}
}

func TestManagedAsyncRejectionReceiptsPreserveClosedPublicErrors(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	action := executorAction(t, "accept", true, "", "", "", nil)
	work := fixture.work(t, model.WorkOffered, 1, 1, true)
	started := executorReservation(t, fixture, action, work, true).Operation
	tests := []struct {
		name    string
		code    ControlErrorCode
		message string
	}{
		{name: "claim expired", code: CodeContextStale,
			message: "managed operation context expired with its Agent Runtime claim"},
		{name: "runtime failed", code: CodeInternal,
			message: "managed Agent Runtime failed before operation completion"},
		{name: "runtime orphaned", code: CodeInternal,
			message: "managed Agent Runtime was orphaned before operation completion"},
		{name: "Work cancelled", code: CodeWorkConflict,
			message: "Work was cancelled before action commit"},
		{name: "Work expired", code: CodeWorkExpired,
			message: "Work deadline reached before action commit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
				OperationID: started.ID(), Code: string(test.code), Message: test.message,
			})
			if err != nil {
				t.Fatal(err)
			}
			terminal := executorTerminalOperation(t, started, model.OperationRejected,
				receipt.JSON(), fixture.at)
			apiErr := decodeRejectedTeamworkOperation(testActionHandlers(t), terminal, true)
			if apiErr == nil || apiErr.Code != test.code || apiErr.Message != test.message ||
				!apiErr.Replayed || apiErr.OperationID == nil || *apiErr.OperationID != started.ID().String() {
				t.Fatalf("async rejection replay = %#v", apiErr)
			}
		})
	}
}

func TestManagedRejectionReceiptRejectsCodeOutsideAgentUnion(t *testing.T) {
	t.Parallel()
	fixture := newExecutorFixture(t, 1)
	action := executorAction(t, "accept", true, "", "", "", nil)
	work := fixture.work(t, model.WorkOffered, 1, 1, true)
	started := executorReservation(t, fixture, action, work, true).Operation
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: started.ID(), Code: "runtime_made_this_up", Message: "unknown code",
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal := executorTerminalOperation(t, started, model.OperationRejected, receipt.JSON(), fixture.at)
	apiErr := decodeRejectedTeamworkOperation(testActionHandlers(t), terminal, true)
	if apiErr == nil || apiErr.Code != CodeInternal || apiErr.Message != "rejected managed receipt is invalid" ||
		!apiErr.Replayed {
		t.Fatalf("unknown rejection code = %#v", apiErr)
	}
}

func TestServiceReserveTerminalRaceUsesPureReplayDecoder(t *testing.T) {
	t.Parallel()
	fixture, request, first, terminal := committedTeamworkReplayFixture(t, "race")
	executorCalls := 0
	fake := &fakeControlStore{reserve: func(_ context.Context,
		spec store.ManagedOperationSpec,
	) (store.ManagedOperationReservation, error) {
		if spec.RequestDigest != terminal.RequestDigest() {
			t.Fatalf("race reservation digest = %s, want %s", spec.RequestDigest, terminal.RequestDigest())
		}
		return store.ManagedOperationReservation{Operation: terminal, Replayed: true}, nil
	}}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock:  serviceTestClock{fixture.at},
		Random: bytes.NewReader(bytes.Repeat([]byte{0x71}, managedSecretBytes)),
		Executor: fakeTeamworkExecutor{execute: func(context.Context,
			TeamworkExecutionSpec,
		) (OperationResponse, *ControlError) {
			executorCalls++
			return OperationResponse{}, nil
		}}, CurrentViews: &fakeAgentCurrentViews{}})
	if err != nil {
		t.Fatal(err)
	}
	got, apiErr := service.TeamworkAction(context.Background(), ControlMetadata{Profile: fixture.profile,
		OperationKeyHash: terminal.ClientKeyHash(), HasOperationKey: true}, request)
	want := first
	want.Replayed = true
	if apiErr != nil || !reflect.DeepEqual(got, want) || executorCalls != 0 {
		t.Fatalf("reserve terminal race = (%#v, %v), executor calls %d", got, apiErr, executorCalls)
	}
}

func committedTeamworkReplayFixture(t *testing.T,
	content string,
) (*executorFixture, TeamworkActionRequest, OperationResponse, model.Operation) {
	t.Helper()
	fixture := newExecutorFixture(t, 1)
	request := TeamworkActionRequest{Action: "offer", Channel: "alpha", To: "reviewer-0",
		Deadline: "30m", Content: content}
	action := executorAction(t, "offer", false, request.Content, request.Deadline, request.To, nil)
	reservation := executorReservation(t, fixture, action, model.ReviewWork{}, false)
	response, apiErr := fixture.executor.ExecuteTeamwork(context.Background(), TeamworkExecutionSpec{
		Request: request, Action: action, Reservation: reservation, At: fixture.at,
	})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	terminal := executorTerminalOperation(t, reservation.Operation, model.OperationCommitted,
		fixture.backend.lastReceipt, fixture.at)
	return fixture, request, response, terminal
}

func exactTerminalProbe(t *testing.T, terminal model.Operation,
	calls *int,
) func(context.Context, store.ManagedOperationProbeSpec) (store.ManagedOperationProbe, error) {
	t.Helper()
	return func(_ context.Context, spec store.ManagedOperationProbeSpec) (store.ManagedOperationProbe, error) {
		*calls = *calls + 1
		contextHash, hasContext := terminal.ContextHash()
		if spec.Profile.ID() != terminal.ProfileID() || spec.ClientKeyHash != terminal.ClientKeyHash() ||
			spec.RequestDigest != terminal.RequestDigest() || spec.Kind != terminal.Kind() ||
			spec.HasClaimContext != hasContext || (hasContext && spec.ClaimContextHash != contextHash) {
			return store.ManagedOperationProbe{}, store.ErrOperationMismatch
		}
		return store.ManagedOperationProbe{Operation: terminal, Found: true}, nil
	}
}

func disabledReplayProfile(t *testing.T, profile model.Profile) model.Profile {
	t.Helper()
	spec := profile.Spec()
	spec.Enabled = false
	disabled, err := model.NewProfile(spec)
	if err != nil {
		t.Fatal(err)
	}
	return disabled
}
