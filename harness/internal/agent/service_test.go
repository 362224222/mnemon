package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type serviceTestClock struct{ now time.Time }

func (clock serviceTestClock) Now() time.Time { return clock.now }

type serviceActivationGateFunc func(context.Context, model.Profile) *ControlError

func (check serviceActivationGateFunc) Check(ctx context.Context,
	profile model.Profile,
) *ControlError {
	return check(ctx, profile)
}

type countingServiceClock struct {
	now   time.Time
	calls int
}

func (clock *countingServiceClock) Now() time.Time {
	clock.calls++
	return clock.now
}

type countingServiceRandom struct{ calls int }

func (random *countingServiceRandom) Read(_ []byte) (int, error) {
	random.calls++
	return 0, errors.New("unexpected entropy read")
}

type fakeControlStore struct {
	probe     func(context.Context, store.AgentClaimProbeSpec) (store.AgentClaimStatus, error)
	claim     func(context.Context, store.AgentClaimSpec) (store.AgentClaimResult, error)
	peek      func(context.Context, store.AgentAttachmentSpec) error
	consume   func(context.Context, store.AgentAttachmentSpec) (store.AgentClaimResult, error)
	plan      func(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadPlan, error)
	current   func(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error)
	initiate  func(context.Context, model.Profile, time.Time) (store.AgentInitiationContext, error)
	operation func(context.Context, store.ManagedOperationProbeSpec) (store.ManagedOperationProbe, error)
	reserve   func(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error)
	resolve   func(context.Context, store.ManagedResolutionSpec) (store.ManagedResolutionResult, error)
}

func (fake *fakeControlStore) PeekAgentRunAttachment(ctx context.Context,
	spec store.AgentAttachmentSpec,
) error {
	if fake.peek == nil {
		return errors.New("unexpected PeekAgentRunAttachment")
	}
	return fake.peek(ctx, spec)
}

func (fake *fakeControlStore) ConsumeAgentRunAttachment(ctx context.Context,
	spec store.AgentAttachmentSpec,
) (store.AgentClaimResult, error) {
	if fake.consume == nil {
		return store.AgentClaimResult{}, errors.New("unexpected ConsumeAgentRunAttachment")
	}
	return fake.consume(ctx, spec)
}

func (fake *fakeControlStore) PlanAgentCurrentRead(ctx context.Context,
	spec store.AgentCurrentReadSpec,
) (store.AgentCurrentReadPlan, error) {
	if fake.plan == nil {
		return store.AgentCurrentReadPlan{}, errors.New("unexpected PlanAgentCurrentRead")
	}
	return fake.plan(ctx, spec)
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

func (fake *fakeControlStore) ReadAgentInitiationContext(ctx context.Context, profile model.Profile,
	at time.Time,
) (store.AgentInitiationContext, error) {
	if fake.initiate == nil {
		return store.AgentInitiationContext{}, errors.New("unexpected ReadAgentInitiationContext")
	}
	return fake.initiate(ctx, profile, at)
}

func (fake *fakeControlStore) ReserveManagedOperation(ctx context.Context,
	spec store.ManagedOperationSpec,
) (store.ManagedOperationReservation, error) {
	if fake.reserve == nil {
		return store.ManagedOperationReservation{}, errors.New("unexpected ReserveManagedOperation")
	}
	return fake.reserve(ctx, spec)
}

func (fake *fakeControlStore) ProbeManagedOperation(ctx context.Context,
	spec store.ManagedOperationProbeSpec,
) (store.ManagedOperationProbe, error) {
	if fake.operation == nil {
		return store.ManagedOperationProbe{}, nil
	}
	return fake.operation(ctx, spec)
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
	execute func(context.Context, TeamworkExecutionSpec) (OperationResponse, *ControlError)
}

type fakeAgentCurrentViews struct {
	materialize func(context.Context, store.AgentCurrentReadPlan) ([]model.CurrentArtifactRef, error)
	plans       []store.AgentCurrentReadPlan
	cleanups    []model.RunID
}

func (fake *fakeAgentCurrentViews) Materialize(ctx context.Context,
	plan store.AgentCurrentReadPlan,
) ([]model.CurrentArtifactRef, error) {
	fake.plans = append(fake.plans, plan)
	if fake.materialize != nil {
		return fake.materialize(ctx, plan)
	}
	return []model.CurrentArtifactRef{}, nil
}

func (fake *fakeAgentCurrentViews) CleanupRun(_ context.Context, run model.RunID) error {
	fake.cleanups = append(fake.cleanups, run)
	return nil
}

func (fake fakeTeamworkExecutor) ExecuteTeamwork(ctx context.Context,
	spec TeamworkExecutionSpec,
) (OperationResponse, *ControlError) {
	return fake.execute(ctx, spec)
}

func TestServiceRejectsMissingActionHandlers(t *testing.T) {
	t.Parallel()
	if _, err := NewService(&fakeControlStore{}, ServiceOptions{
		CurrentViews: &fakeAgentCurrentViews{},
	}); err == nil {
		t.Fatal("service accepted missing Action handlers")
	}
}

func TestServiceActivationGateBlocksAllAgentRoutesBeforeSideEffects(t *testing.T) {
	at := time.Date(2026, 7, 17, 4, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	gateErr := NewControlError(CodeAssetRevisionMismatch,
		"managed projection is not active")
	gateCalls := 0
	gate := serviceActivationGateFunc(func(ctx context.Context,
		got model.Profile,
	) *ControlError {
		gateCalls++
		if ctx == nil || got.ID() != profile.ID() {
			t.Fatalf("activation gate authority = (%v, %s)", ctx, got.ID())
		}
		return gateErr
	})
	storeCalls := 0
	called := func() { storeCalls++ }
	fake := &fakeControlStore{
		probe: func(context.Context, store.AgentClaimProbeSpec) (store.AgentClaimStatus, error) {
			called()
			return store.AgentClaimNone, nil
		},
		claim: func(context.Context, store.AgentClaimSpec) (store.AgentClaimResult, error) {
			called()
			return store.AgentClaimResult{}, nil
		},
		plan: func(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadPlan, error) {
			called()
			return store.AgentCurrentReadPlan{}, nil
		},
		current: func(context.Context, store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error) {
			called()
			return store.AgentCurrentReadResult{}, nil
		},
		initiate: func(context.Context, model.Profile, time.Time) (store.AgentInitiationContext, error) {
			called()
			return store.AgentInitiationContext{}, nil
		},
		reserve: func(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error) {
			called()
			return store.ManagedOperationReservation{}, nil
		},
		resolve: func(context.Context, store.ManagedResolutionSpec) (store.ManagedResolutionResult, error) {
			called()
			return store.ManagedResolutionResult{}, nil
		},
	}
	executorCalls := 0
	executor := fakeTeamworkExecutor{execute: func(context.Context,
		TeamworkExecutionSpec,
	) (OperationResponse, *ControlError) {
		executorCalls++
		return OperationResponse{}, nil
	}}
	clock := &countingServiceClock{now: at}
	random := &countingServiceRandom{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: clock, Random: random, Executor: executor, CurrentViews: &fakeAgentCurrentViews{},
		ActivationGate: gate})
	if err != nil {
		t.Fatal(err)
	}
	metadata := ControlMetadata{Profile: profile, HasOperationKey: true,
		OperationKeyHash: model.Sum([]byte("blocked-operation")), HasClaimContext: true,
		ClaimContextHash: model.Sum([]byte("blocked-context"))}
	assertGateError := func(name string, apiErr *ControlError) {
		t.Helper()
		if apiErr != gateErr {
			t.Fatalf("%s error = %p %v, want original gate error %p", name, apiErr, apiErr, gateErr)
		}
	}
	_, apiErr := service.HookCheck(context.Background(), metadata)
	assertGateError("HookCheck", apiErr)
	_, apiErr = service.AgentCurrent(context.Background(), metadata)
	assertGateError("AgentCurrent", apiErr)
	_, apiErr = service.TeamworkAction(context.Background(), metadata,
		TeamworkActionRequest{Action: "accept"})
	assertGateError("TeamworkAction", apiErr)
	_, apiErr = service.AgentResolve(context.Background(), metadata,
		AgentResolveRequest{Decision: "retry"})
	assertGateError("AgentResolve", apiErr)
	if gateCalls != 4 || storeCalls != 0 || executorCalls != 0 || clock.calls != 0 || random.calls != 0 {
		t.Fatalf("blocked calls: gate=%d Store=%d executor=%d clock=%d random=%d",
			gateCalls, storeCalls, executorCalls, clock.calls, random.calls)
	}
}

func TestServiceActivationGateIsOptionalAndDisabledProfileFailsClosed(t *testing.T) {
	at := time.Date(2026, 7, 17, 4, 30, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	probeCalls := 0
	fake := &fakeControlStore{probe: func(context.Context,
		store.AgentClaimProbeSpec,
	) (store.AgentClaimStatus, error) {
		probeCalls++
		return store.AgentClaimNone, nil
	}}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: serviceTestClock{now: at}, CurrentViews: &fakeAgentCurrentViews{}})
	if err != nil {
		t.Fatal(err)
	}
	response, apiErr := service.HookCheck(context.Background(),
		ControlMetadata{Profile: profile})
	if apiErr != nil || response.Pending || probeCalls != 1 {
		t.Fatalf("nil-gate HookCheck = (%#v, %v), probes=%d", response, apiErr, probeCalls)
	}
	disabled, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: profile.Principal(), WorkspaceRoot: profile.WorkspaceRoot(), Host: profile.Host(),
		Runtime: profile.Runtime(), CredentialHash: profile.CredentialHash(),
		ActiveAssetRevision: profile.ActiveAssetRevision(), HandlingBudget: profile.HandlingBudget(),
		Enabled: false, CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr = service.HookCheck(context.Background(),
		ControlMetadata{Profile: disabled})
	if apiErr == nil || apiErr.Code != CodeAssetRevisionMismatch || probeCalls != 1 {
		t.Fatalf("disabled Profile error = %v, probes=%d", apiErr, probeCalls)
	}
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
	var planSpec store.AgentCurrentReadSpec
	var readSpec store.AgentCurrentReadSpec
	views := &fakeAgentCurrentViews{}
	fake := &fakeControlStore{
		probe: func(_ context.Context, spec store.AgentClaimProbeSpec) (store.AgentClaimStatus, error) {
			if spec.ProfileID != profile.ID() || spec.ExpectedAssetRevision != profile.ActiveAssetRevision() || !spec.At.Equal(at) {
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
		plan: func(_ context.Context, spec store.AgentCurrentReadSpec) (store.AgentCurrentReadPlan, error) {
			planSpec = spec
			return store.AgentCurrentReadPlan{RunID: runID,
				Artifacts: []store.AgentCurrentArtifactMaterialization{}}, nil
		},
		current: func(_ context.Context, spec store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error) {
			readSpec = spec
			return store.AgentCurrentReadResult{Projection: projection}, nil
		},
	}
	currentActions := testActionHandlers(t)
	service, err := NewService(fake, ServiceOptions{Actions: currentActions,
		Clock: serviceTestClock{at}, Random: bytes.NewReader(entropy), CurrentViews: views})
	if err != nil {
		t.Fatal(err)
	}
	metadata := ControlMetadata{Profile: profile}
	hook, apiErr := service.HookCheck(context.Background(), metadata)
	if apiErr != nil || !hook.Pending {
		t.Fatalf("HookCheck() = (%#v, %v)", hook, apiErr)
	}
	current, apiErr := service.AgentCurrent(context.Background(), metadata)
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
	if planSpec.RunID != runID || planSpec.ClaimTokenHash != model.Sum(secret) ||
		len(planSpec.ArtifactViews) != 0 || readSpec.RunID != runID ||
		readSpec.ClaimTokenHash != model.Sum(secret) || !readSpec.At.Equal(at) ||
		readSpec.ArtifactViews == nil || len(views.plans) != 1 {
		t.Fatalf("current read spec = %#v", readSpec)
	}
	assertCurrentActionPolicy(t, planSpec, readSpec, currentActions)
}

func assertCurrentActionPolicy(t testing.TB, plan, final store.AgentCurrentReadSpec,
	actions ActionHandlers,
) {
	t.Helper()
	if plan.ActionPolicy.AssetRevision() != actions.AssetRevision() ||
		final.ActionPolicy.AssetRevision() != actions.AssetRevision() ||
		len(plan.ActionPolicy.Entries()) != model.TeamworkActionCount ||
		len(final.ActionPolicy.Entries()) != model.TeamworkActionCount {
		t.Fatalf("current Action policies = (%#v, %#v)", plan.ActionPolicy, final.ActionPolicy)
	}
}

func TestServiceAttachmentHookPeeksAndCurrentConsumesExactPreclaim(t *testing.T) {
	at := time.Date(2026, 7, 16, 15, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	tokenHash := model.Sum([]byte("service-attachment"))
	runID, _ := model.ParseRunID("run-service-attachment")
	handlingID, _ := model.ParseHandlingID("handling-service-attachment")
	lease := at.Add(5 * time.Minute)
	projection := serviceTestProjection(t, at)
	makeRun := func(attached bool) model.AgentRun {
		var attachedAt *time.Time
		var runtimeStartedAt *time.Time
		status := model.AgentRunStarting
		diagnostic := mustServiceJSON(t, `{}`)
		runtimeIDs := mustServiceJSON(t, `{}`)
		if attached {
			attachedAt, status = &at, model.AgentRunRunning
			runtimeStartedAt = &at
			diagnostic = mustServiceJSON(t, `{"adapter":"codex-app-server"}`)
			runtimeIDs = mustServiceJSON(t, `{"process_id":42}`)
		}
		run, err := model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: profile.ID(),
			HandlingID: &handlingID, Cause: mustServiceJSON(t, `{"kind":"wake"}`), HandlingAttempt: 1,
			ClaimFenceHash: &tokenHash, LeaseUntil: &lease, AttachmentTokenHash: &tokenHash,
			AttachmentExpiresAt: &lease, AttachedAt: attachedAt, Launcher: "mnemond-wake",
			Runtime: profile.Runtime(), LauncherDiagnostic: diagnostic,
			RuntimeIDs: runtimeIDs, Status: status, RuntimeStartedAt: runtimeStartedAt, StartedAt: at})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	peekCalls, consumeCalls := 0, 0
	fake := &fakeControlStore{
		peek: func(_ context.Context, spec store.AgentAttachmentSpec) error {
			peekCalls++
			if spec.ProfileID != profile.ID() || spec.ExpectedAssetRevision != profile.ActiveAssetRevision() ||
				spec.AttachmentTokenHash != tokenHash || !spec.At.Equal(at) {
				t.Fatalf("peek attachment spec = %#v", spec)
			}
			return nil
		},
		consume: func(_ context.Context, spec store.AgentAttachmentSpec) (store.AgentClaimResult, error) {
			consumeCalls++
			if spec.AttachmentTokenHash != tokenHash {
				t.Fatalf("consume attachment spec = %#v", spec)
			}
			return store.AgentClaimResult{Status: store.AgentClaimActionable, Run: makeRun(true)}, nil
		},
		plan: func(_ context.Context, spec store.AgentCurrentReadSpec) (store.AgentCurrentReadPlan, error) {
			if spec.RunID != runID || spec.ClaimTokenHash != tokenHash {
				t.Fatalf("attached current plan spec = %#v", spec)
			}
			return store.AgentCurrentReadPlan{RunID: runID,
				Artifacts: []store.AgentCurrentArtifactMaterialization{}}, nil
		},
		current: func(_ context.Context, spec store.AgentCurrentReadSpec) (store.AgentCurrentReadResult, error) {
			if spec.RunID != runID || spec.ClaimTokenHash != tokenHash {
				t.Fatalf("attached current finalize spec = %#v", spec)
			}
			return store.AgentCurrentReadResult{Projection: projection}, nil
		},
	}
	random := &countingServiceRandom{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: serviceTestClock{at}, Random: random, CurrentViews: &fakeAgentCurrentViews{}})
	if err != nil {
		t.Fatal(err)
	}
	metadata := ControlMetadata{Profile: profile, HasRunAttachment: true,
		RunAttachmentHash: tokenHash}
	hook, apiErr := service.HookCheck(context.Background(), metadata)
	if apiErr != nil || !hook.Pending || peekCalls != 1 {
		t.Fatalf("attached HookCheck() = (%#v, %v), peeks=%d", hook, apiErr, peekCalls)
	}
	current, apiErr := service.AgentCurrent(context.Background(), metadata)
	if apiErr != nil || current.Status != "actionable" || current.RunID != runID.String() ||
		current.ClaimSecret != "" || !bytes.Equal(current.Projection, projection.CanonicalJSON().Bytes()) ||
		consumeCalls != 1 || random.calls != 0 {
		t.Fatalf("attached AgentCurrent() = (%#v, %v), consumes=%d entropy=%d",
			current, apiErr, consumeCalls, random.calls)
	}
}

func TestServiceCurrentNoneCarriesOnlyIdentityFreeInitiationContext(t *testing.T) {
	at := time.Date(2026, 7, 17, 2, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	fake := &fakeControlStore{
		claim: func(_ context.Context, spec store.AgentClaimSpec) (store.AgentClaimResult, error) {
			if spec.ProfileID != profile.ID() || !spec.At.Equal(at) {
				t.Fatalf("claim probe spec = %#v", spec)
			}
			return store.AgentClaimResult{Status: store.AgentClaimNone}, nil
		},
		initiate: func(_ context.Context, got model.Profile, gotAt time.Time) (store.AgentInitiationContext, error) {
			if got.ID() != profile.ID() || !gotAt.Equal(at) {
				t.Fatalf("initiation authority = %s at %s", got.ID(), gotAt)
			}
			return store.AgentInitiationContext{}, nil
		},
	}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0x62}, 2*managedSecretBytes)),
		CurrentViews: &fakeAgentCurrentViews{}})
	if err != nil {
		t.Fatal(err)
	}
	response, apiErr := service.AgentCurrent(context.Background(),
		ControlMetadata{Profile: profile})
	if apiErr != nil || response.Status != "none" || response.RunID != "" || response.ClaimSecret != "" {
		t.Fatalf("none current = (%#v, %v)", response, apiErr)
	}
	wantProjection := []byte(`{"initiation_context":{"channels":[]},"schema_version":1}`)
	if !bytes.Equal(response.Projection, wantProjection) ||
		bytes.Contains(response.Projection, []byte("peer_id")) {
		t.Fatalf("none initiation projection = %s", response.Projection)
	}
}

func TestServiceTeamworkActionReservesServerOwnedOperation(t *testing.T) {
	at := time.Date(2026, 7, 16, 16, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	operationKey := model.Sum([]byte("operation-key"))
	request := TeamworkActionRequest{Action: "offer", Channel: "design",
		To: "auto", Deadline: "30m", Content: "Review the architecture"}
	var reserved store.ManagedOperationSpec
	var executed TeamworkExecutionSpec
	views := &fakeAgentCurrentViews{}
	fake := &fakeControlStore{reserve: func(_ context.Context,
		spec store.ManagedOperationSpec,
	) (store.ManagedOperationReservation, error) {
		reserved = spec
		operationID, _ := model.ParseOperationID("operation-service-offer")
		runID, _ := model.ParseRunID("run-service-offer")
		lease := spec.LeaseUntil
		operation, err := model.NewOperation(model.OperationSpec{ID: operationID, ProfileID: profile.ID(),
			AgentRunID: runID, ClientKeyHash: spec.ClientKeyHash, Kind: spec.Kind,
			RequestDigest: spec.RequestDigest, Status: model.OperationStarted,
			LeaseOwner: spec.LeaseOwner, LeaseUntil: &lease, CreatedAt: spec.At})
		if err != nil {
			t.Fatal(err)
		}
		return store.ManagedOperationReservation{Operation: operation, Acquired: true}, nil
	}}
	wantResponse := OperationResponse{Status: "accepted", Action: "teamwork.offer",
		OperationID: "operation-service-offer", Results: []OperationResult{}, Receipt: `{}`}
	executor := fakeTeamworkExecutor{execute: func(_ context.Context,
		spec TeamworkExecutionSpec,
	) (OperationResponse, *ControlError) {
		executed = spec
		return wantResponse, nil
	}}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0x33}, managedSecretBytes)),
		OperationLease: time.Minute, Executor: executor, CurrentViews: views})
	if err != nil {
		t.Fatal(err)
	}
	metadata := ControlMetadata{Profile: profile, HasOperationKey: true,
		OperationKeyHash: operationKey}
	response, apiErr := service.TeamworkAction(context.Background(), metadata, request)
	if apiErr != nil || response.OperationID != wantResponse.OperationID {
		t.Fatalf("TeamworkAction() = (%#v, %v)", response, apiErr)
	}
	wantRequestDigest, _ := executed.Action.requestDigest(model.Digest{}, false)
	if reserved.Profile.ID() != profile.ID() || reserved.Kind != model.OperationTeamworkOffer ||
		reserved.ClientKeyHash != operationKey || reserved.RequestDigest != wantRequestDigest ||
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
	wantCleanup, _ := model.ParseRunID("run-service-offer")
	if len(views.cleanups) != 1 || views.cleanups[0] != wantCleanup {
		t.Fatalf("accepted action view cleanups = %v", views.cleanups)
	}

	request.Action = "accept"
	request.Channel, request.To, request.Deadline, request.Content = "", "", "", ""
	if _, apiErr := service.TeamworkAction(context.Background(), metadata, request); apiErr == nil ||
		apiErr.Code != CodeContextRequired {
		t.Fatalf("contextless accept error = %v", apiErr)
	}
}

func TestServiceTeamworkActionRejectsUnknownAssetActionBeforeSideEffects(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	profile := serviceTestProfile(t, at)
	storeCalls := 0
	fake := &fakeControlStore{
		operation: func(context.Context, store.ManagedOperationProbeSpec) (store.ManagedOperationProbe, error) {
			storeCalls++
			return store.ManagedOperationProbe{}, nil
		},
		reserve: func(context.Context, store.ManagedOperationSpec) (store.ManagedOperationReservation, error) {
			storeCalls++
			return store.ManagedOperationReservation{}, nil
		},
	}
	activationCalls := 0
	gate := serviceActivationGateFunc(func(context.Context, model.Profile) *ControlError {
		activationCalls++
		return nil
	})
	executorCalls := 0
	executor := fakeTeamworkExecutor{execute: func(context.Context,
		TeamworkExecutionSpec,
	) (OperationResponse, *ControlError) {
		executorCalls++
		return OperationResponse{}, nil
	}}
	clock := &countingServiceClock{now: at}
	random := &countingServiceRandom{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: clock, Random: random, Executor: executor, CurrentViews: &fakeAgentCurrentViews{},
		ActivationGate: gate})
	if err != nil {
		t.Fatal(err)
	}
	_, apiErr := service.TeamworkAction(context.Background(), ControlMetadata{
		Profile: profile, HasOperationKey: true,
		OperationKeyHash: model.Sum([]byte("unknown-action-operation")),
	}, TeamworkActionRequest{Action: "future-action"})
	if apiErr == nil || apiErr.Code != CodeUnknownAction {
		t.Fatalf("unknown Action error = %v", apiErr)
	}
	if storeCalls != 0 || activationCalls != 0 || executorCalls != 0 ||
		clock.calls != 0 || random.calls != 0 {
		t.Fatalf("unknown Action side effects: Store=%d activation=%d executor=%d clock=%d random=%d",
			storeCalls, activationCalls, executorCalls, clock.calls, random.calls)
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
		response := OperationResponse{SchemaVersion: model.SchemaVersion, Status: "resolved",
			Action: string(started.Kind()), OperationID: started.ID().String(),
			Handling: &HandlingReceipt{Status: "requeued"},
			Results:  []OperationResult{}, Receipt: `{"evidence":true}`}
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
	views := &fakeAgentCurrentViews{}
	service, err := NewService(fake, ServiceOptions{Actions: testActionHandlers(t),
		Clock: serviceTestClock{at}, Random: bytes.NewReader(bytes.Repeat([]byte{0x44}, managedSecretBytes)),
		CurrentViews: views})
	if err != nil {
		t.Fatal(err)
	}
	metadata := ControlMetadata{Profile: profile, HasOperationKey: true,
		OperationKeyHash: operationKey, HasClaimContext: true, ClaimContextHash: contextHash}
	response, apiErr := service.AgentResolve(context.Background(), metadata,
		AgentResolveRequest{Decision: "retry", Content: "try after correction"})
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	wantDigest, _ := store.ManagedResolutionRequestDigest(testActionHandlers(t).AssetRevision(),
		contextHash, model.OperationResolveRetry, "try after correction")
	if reserved.Kind != model.OperationResolveRetry || reserved.RequestDigest != wantDigest ||
		!reserved.HasClaimContext || reserved.ClaimContextHash != contextHash {
		t.Fatalf("resolution reservation = %#v", reserved)
	}
	if response.Action != string(model.OperationResolveRetry) || response.Handling.Status != "requeued" ||
		!response.Replayed {
		t.Fatalf("resolution response = %#v", response)
	}
	wantCleanup, _ := model.ParseRunID("run-service-resolve")
	if len(views.cleanups) != 1 || views.cleanups[0] != wantCleanup {
		t.Fatalf("resolution view cleanups = %v", views.cleanups)
	}
}

func serviceTestProfile(t *testing.T, at time.Time) model.Profile {
	t.Helper()
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-service", WorkspaceRoot: "/workspace", Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum([]byte("service-credential")),
		ActiveAssetRevision: testActionHandlers(t).AssetRevision().String(),
		HandlingBudget:      model.DefaultHandlingBudget().JSON(),
		Enabled:             true, CreatedAt: at, UpdatedAt: at})
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
