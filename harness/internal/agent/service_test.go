package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type serviceTestClock struct{ now time.Time }

func (clock serviceTestClock) Now() time.Time { return clock.now }

type fakeControlStore struct {
	probe   func(context.Context, store.AgentClaimProbeSpec) (store.AgentClaimStatus, error)
	claim   func(context.Context, store.AgentClaimSpec) (store.AgentClaimResult, error)
	current func(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error)
	reserve func(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error)
	resolve func(context.Context, store.ManagedResolutionSpec) (store.ManagedResolutionResult, error)
}

func (fake *fakeControlStore) ProbeAgentClaim(ctx context.Context,
	spec store.AgentClaimProbeSpec,
) (store.AgentClaimStatus, error) {
	if fake.probe == nil {
		return "", errors.New("unexpected ProbeAgentClaim")
	}
	return fake.probe(ctx, spec)
}

func (fake *fakeControlStore) ClaimAgentCurrent(ctx context.Context,
	spec store.AgentClaimSpec,
) (store.AgentClaimResult, error) {
	if fake.claim == nil {
		return store.AgentClaimResult{}, errors.New("unexpected ClaimAgentCurrent")
	}
	return fake.claim(ctx, spec)
}

func (fake *fakeControlStore) FinalizeAgentCurrentRead(ctx context.Context,
	spec store.AgentCurrentReadSpec,
) (store.AgentCurrentReadResult, error) {
	if fake.current == nil {
		return store.AgentCurrentReadResult{}, errors.New("unexpected FinalizeAgentCurrentRead")
	}
	return fake.current(ctx, spec)
}

func (fake *fakeControlStore) ReserveManagedOperation(ctx context.Context,
	spec store.ManagedOperationSpec,
) (store.ManagedOperationReservation, error) {
	if fake.reserve == nil {
		return store.ManagedOperationReservation{}, errors.New("unexpected ReserveManagedOperation")
	}
	return fake.reserve(ctx, spec)
}

func (fake *fakeControlStore) CommitManagedResolution(ctx context.Context,
	spec store.ManagedResolutionSpec,
) (store.ManagedResolutionResult, error) {
	if fake.resolve == nil {
		return store.ManagedResolutionResult{}, errors.New("unexpected CommitManagedResolution")
	}
	return fake.resolve(ctx, spec)
}

type fakeTeamworkExecutor struct {
	execute func(context.Context, TeamworkExecutionSpec) (localapi.OperationResponse, *localapi.APIError)
}

func (fake fakeTeamworkExecutor) ExecuteTeamwork(ctx context.Context,
	spec TeamworkExecutionSpec,
) (localapi.OperationResponse, *localapi.APIError) {
	return fake.execute(ctx, spec)
}

func TestServiceHookAndCurrentKeepClaimCapabilityPrivate(t *testing.T) {
	at := time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	secret := bytes.Repeat([]byte{0x11}, managedSecretBytes)
	ownerBytes := bytes.Repeat([]byte{0x22}, managedSecretBytes)
	entropy := append(append([]byte{}, secret...), ownerBytes...)
	projection := serviceTestProjection(t, at)
	runID, _ := model.ParseRunID("run-service-current")
	handlingID, _ := model.ParseHandlingID("handling-service-current")

	var claimSpec store.AgentClaimSpec
	var readSpec store.AgentCurrentReadSpec
	fake := &fakeControlStore{
		probe: func(_ context.Context, spec store.AgentClaimProbeSpec) (store.AgentClaimStatus, error) {
			if spec.ProfileID != profile.ID() || spec.ExpectedAssetRevision != "asset-service" || !spec.At.Equal(at) {
				t.Fatalf("probe spec = %#v", spec)
			}
			return store.AgentClaimActionable, nil
		},
		claim: func(_ context.Context, spec store.AgentClaimSpec) (store.AgentClaimResult, error) {
			claimSpec = spec
			claimFence := spec.ClaimTokenHash
			run, err := model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: profile.ID(),
				HandlingID: &handlingID, Cause: mustServiceJSON(t, `{"cause":"service-test"}`),
				HandlingAttempt: 1, ClaimFenceHash: &claimFence, LeaseUntil: &spec.LeaseUntil,
				Launcher: "service-test", Runtime: profile.Runtime(),
				LauncherDiagnostic: mustServiceJSON(t, `{}`), RuntimeIDs: mustServiceJSON(t, `{}`),
				Status: model.AgentRunRunning, StartedAt: spec.At})
			if err != nil {
				t.Fatal(err)
			}
			return store.AgentClaimResult{Status: store.AgentClaimActionable, Run: run}, nil
		},
		current: func(_ context.Context, spec store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error) {
			readSpec = spec
			return store.AgentCurrentReadResult{Projection: projection}, nil
		},
	}
	service, err := NewService(fake, ServiceOptions{AssetRevision: "asset-service",
		Clock: serviceTestClock{at}, Random: bytes.NewReader(entropy)})
	if err != nil {
		t.Fatal(err)
	}
	metadata := localapi.RequestMetadata{Profile: profile}
	hook, apiErr := service.HookCheck(context.Background(), metadata, localapi.HookCheckRequest{})
	if apiErr != nil || !hook.Pending {
		t.Fatalf("HookCheck() = (%#v, %v)", hook, apiErr)
	}
	current, apiErr := service.AgentCurrent(context.Background(), metadata, localapi.AgentCurrentRequest{})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	if current.Status != string(store.AgentClaimActionable) || current.RunID != runID.String() ||
		current.ClaimSecret != base64.RawURLEncoding.EncodeToString(secret) ||
		!bytes.Equal(current.Projection, projection.CanonicalJSON().Bytes()) {
		t.Fatalf("AgentCurrent() = %#v", current)
	}
	wantOwner := "claim-" + base64.RawURLEncoding.EncodeToString(ownerBytes)
	if claimSpec.ClaimOwner != wantOwner || claimSpec.ClaimTokenHash != model.Sum(secret) ||
		!claimSpec.LeaseUntil.Equal(at.Add(5*time.Minute)) {
		t.Fatalf("claim spec = %#v", claimSpec)
	}
	if readSpec.RunID != runID || readSpec.ClaimTokenHash != model.Sum(secret) || !readSpec.At.Equal(at) {
		t.Fatalf("current read spec = %#v", readSpec)
	}
	metadata.HasRunAttachment = true
	if _, apiErr := service.HookCheck(context.Background(), metadata, localapi.HookCheckRequest{}); apiErr == nil ||
		apiErr.Code != localapi.CodeContextInvalid {
		t.Fatalf("attached HookCheck error = %v", apiErr)
	}
}

func TestServiceTeamworkActionReservesServerOwnedOperation(t *testing.T) {
	at := time.Date(2026, 7, 16, 16, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	operationKey := model.Sum([]byte("operation-key"))
	request := localapi.TeamworkActionRequest{Action: "offer", Channel: "design",
		To: "auto", Deadline: "30m", Content: "Review the architecture"}
	var reserved store.ManagedOperationSpec
	var executed TeamworkExecutionSpec
	fake := &fakeControlStore{reserve: func(_ context.Context,
		spec store.ManagedOperationSpec,
	) (store.ManagedOperationReservation, error) {
		reserved = spec
		return store.ManagedOperationReservation{Acquired: true}, nil
	}}
	wantResponse := localapi.OperationResponse{Status: "accepted", Action: "teamwork.offer",
		OperationID: "operation-service-offer", Results: []localapi.OperationResult{}, Receipt: `{}`}
	executor := fakeTeamworkExecutor{execute: func(_ context.Context,
		spec TeamworkExecutionSpec,
	) (localapi.OperationResponse, *localapi.APIError) {
		executed = spec
		return wantResponse, nil
	}}
	service, err := NewService(fake, ServiceOptions{AssetRevision: "asset-service",
		Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0x33}, managedSecretBytes)),
		OperationLease: time.Minute, Executor: executor})
	if err != nil {
		t.Fatal(err)
	}
	metadata := localapi.RequestMetadata{Profile: profile, HasOperationKey: true,
		OperationKeyHash: operationKey}
	response, apiErr := service.TeamworkAction(context.Background(), metadata, request)
	if apiErr != nil || response.OperationID != wantResponse.OperationID {
		t.Fatalf("TeamworkAction() = (%#v, %v)", response, apiErr)
	}
	raw, _ := model.CanonicalMarshal(request)
	if reserved.Profile.ID() != profile.ID() || reserved.Kind != model.OperationTeamworkOffer ||
		reserved.ClientKeyHash != operationKey || reserved.RequestDigest != model.Sum(raw) ||
		reserved.HasClaimContext || !reserved.At.Equal(at) || !reserved.LeaseUntil.Equal(at.Add(time.Minute)) {
		t.Fatalf("reservation = %#v", reserved)
	}
	if executed.Action.Name != "offer" || executed.Action.Deadline != 30*time.Minute ||
		executed.Action.Candidate == nil || !executed.At.Equal(at) {
		t.Fatalf("execution spec = %#v", executed)
	}
	if _, err := model.ParseRunID(reserved.LeaseOwner); err != nil {
		t.Fatalf("server lease owner = %q: %v", reserved.LeaseOwner, err)
	}

	request.Action = "accept"
	request.Channel, request.To, request.Deadline, request.Content = "", "", "", ""
	if _, apiErr := service.TeamworkAction(context.Background(), metadata, request); apiErr == nil ||
		apiErr.Code != localapi.CodeContextRequired {
		t.Fatalf("contextless accept error = %v", apiErr)
	}
}

func TestServiceResolveBindsDigestAndValidatesDurableReceipt(t *testing.T) {
	at := time.Date(2026, 7, 16, 17, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	contextHash := model.Sum([]byte("claim-context"))
	operationKey := model.Sum([]byte("resolve-operation"))
	var reserved store.ManagedOperationSpec
	fake := &fakeControlStore{}
	fake.reserve = func(_ context.Context, spec store.ManagedOperationSpec) (store.ManagedOperationReservation, error) {
		reserved = spec
		operationID, _ := model.ParseOperationID("operation-service-resolve")
		runID, _ := model.ParseRunID("run-service-resolve")
		leaseUntil := spec.LeaseUntil
		operation, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: profile.ID(),
			AgentRunID: runID, ClientKeyHash: spec.ClientKeyHash, ContextHash: &contextHash,
			Kind: spec.Kind, RequestDigest: spec.RequestDigest, Status: model.OperationStarted,
			LeaseOwner: spec.LeaseOwner, LeaseUntil: &leaseUntil, CreatedAt: spec.At})
		if err != nil {
			t.Fatal(err)
		}
		return store.ManagedOperationReservation{Operation: operation, Acquired: true}, nil
	}
	fake.resolve = func(_ context.Context, spec store.ManagedResolutionSpec) (store.ManagedResolutionResult, error) {
		started := spec.Reservation.Operation
		response := localapi.OperationResponse{SchemaVersion: localapi.SchemaVersion, Status: "resolved",
			Action: string(started.Kind()), OperationID: started.ID().String(),
			Handling: &localapi.HandlingReceipt{Status: "requeued"},
			Results:  []localapi.OperationResult{}, Receipt: `{"evidence":true}`}
		receipt, err := model.JSONFrom(response)
		if err != nil {
			t.Fatal(err)
		}
		finishedAt := spec.At
		committed, err := model.NewOperation(model.OperationSpec{ID: started.ID(), ProfileID: started.ProfileID(),
			AgentRunID: started.AgentRunID(), ClientKeyHash: started.ClientKeyHash(), ContextHash: &contextHash,
			Kind: started.Kind(), RequestDigest: started.RequestDigest(), Status: model.OperationCommitted,
			Result: &receipt, CreatedAt: started.CreatedAt(), FinishedAt: &finishedAt})
		if err != nil {
			t.Fatal(err)
		}
		return store.ManagedResolutionResult{Operation: committed, Receipt: receipt, Replayed: true}, nil
	}
	service, err := NewService(fake, ServiceOptions{AssetRevision: "asset-service",
		Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0x44}, managedSecretBytes))})
	if err != nil {
		t.Fatal(err)
	}
	metadata := localapi.RequestMetadata{Profile: profile, HasOperationKey: true,
		OperationKeyHash: operationKey, HasClaimContext: true, ClaimContextHash: contextHash}
	response, apiErr := service.AgentResolve(context.Background(), metadata,
		localapi.AgentResolveRequest{Decision: "retry", Content: "try after correction"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	wantDigest, _ := store.ManagedResolutionRequestDigest(contextHash,
		model.OperationResolveRetry, "try after correction")
	if reserved.Kind != model.OperationResolveRetry || reserved.RequestDigest != wantDigest ||
		!reserved.HasClaimContext || reserved.ClaimContextHash != contextHash {
		t.Fatalf("resolution reservation = %#v", reserved)
	}
	if response.Action != string(model.OperationResolveRetry) || response.Handling.Status != "requeued" ||
		!response.Replayed {
		t.Fatalf("resolution response = %#v", response)
	}
}

func serviceTestProfile(t *testing.T, at time.Time) model.Profile {
	t.Helper()
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-service", WorkspaceRoot: "/workspace", Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("service-credential")),
		ActiveAssetRevision: "asset-service", HandlingBudget: model.DefaultHandlingBudget().JSON(),
		Enabled: true, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func serviceTestProjection(t *testing.T, at time.Time) model.CurrentProjection {
	t.Helper()
	home, _ := model.ParsePeerID("peer-service-home")
	origin, _ := model.ParsePeerID("peer-service-origin")
	epoch, _ := model.ParseOriginEpoch("epoch-service")
	eventID, _ := model.ParseEventID("event-service-current")
	key, _ := model.NewEventKey(origin, epoch, eventID)
	workID, _ := model.ParseWorkID("work-service-current")
	workRef, _ := model.NewWorkRef(home, workID)
	payload := mustServiceJSON(t, `{"content":"review this","deadline":"2026-07-17T15:00:00Z","iteration":1,"work_version":1}`)
	event, err := model.NewCurrentEvent(model.CurrentEventSpec{Key: key,
		Digest: model.Sum([]byte("service-current-event")), Type: model.EventReviewOffered,
		WorkRef: workRef, Summary: "Review this", Payload: payload, AcceptedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	work, err := model.NewCurrentWork(model.CurrentWorkSpec{Ref: workRef, Version: 1, Iteration: 1,
		DeadlineUnixNano: at.Add(24 * time.Hour).UnixNano(), State: model.WorkOffered,
		StateData: mustServiceJSON(t, `{"content":"review this"}`), LocalRole: model.CurrentReviewer})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := model.NewCurrentProjection(model.CurrentProjectionSpec{SourceEvent: event,
		ActionWork: work, AllowedActions: []model.OperationKind{model.OperationTeamworkAccept}})
	if err != nil {
		t.Fatal(err)
	}
	return projection
}

func mustServiceJSON(t *testing.T, value string) model.JSON {
	t.Helper()
	result, err := model.NewJSON([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
