package node

import (
	"bytes"
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type localAPIAdapterService struct {
	hook    func(context.Context, agent.ControlMetadata) (agent.HookCheckResponse, *agent.ControlError)
	current func(context.Context, agent.ControlMetadata) (agent.AgentCurrentResponse, *agent.ControlError)
	action  func(context.Context, agent.ControlMetadata, agent.TeamworkActionRequest) (agent.OperationResponse, *agent.ControlError)
	resolve func(context.Context, agent.ControlMetadata, agent.AgentResolveRequest) (agent.OperationResponse, *agent.ControlError)
}

func (service localAPIAdapterService) HookCheck(ctx context.Context,
	metadata agent.ControlMetadata,
) (agent.HookCheckResponse, *agent.ControlError) {
	return service.hook(ctx, metadata)
}

func (service localAPIAdapterService) AgentCurrent(ctx context.Context,
	metadata agent.ControlMetadata,
) (agent.AgentCurrentResponse, *agent.ControlError) {
	return service.current(ctx, metadata)
}

func (service localAPIAdapterService) TeamworkAction(ctx context.Context,
	metadata agent.ControlMetadata, request agent.TeamworkActionRequest,
) (agent.OperationResponse, *agent.ControlError) {
	return service.action(ctx, metadata, request)
}

func (service localAPIAdapterService) AgentResolve(ctx context.Context,
	metadata agent.ControlMetadata, request agent.AgentResolveRequest,
) (agent.OperationResponse, *agent.ControlError) {
	return service.resolve(ctx, metadata, request)
}

func TestLocalAPIServiceAdapterMapsMetadataAndRequestsExactly(t *testing.T) {
	t.Parallel()
	operationHash := model.Sum([]byte("operation"))
	contextHash := model.Sum([]byte("context"))
	attachmentHash := model.Sum([]byte("attachment"))
	metadata := RequestMetadata{OperationKeyHash: operationHash, HasOperationKey: true,
		ClaimContextHash: contextHash, HasClaimContext: true,
		RunAttachmentHash: attachmentHash, HasRunAttachment: true}
	assertMetadata := func(got agent.ControlMetadata) {
		t.Helper()
		if got.Profile.ID() != metadata.Profile.ID() || got.OperationKeyHash != operationHash ||
			!got.HasOperationKey || got.ClaimContextHash != contextHash || !got.HasClaimContext ||
			got.RunAttachmentHash != attachmentHash || !got.HasRunAttachment {
			t.Fatalf("Agent metadata = %#v", got)
		}
	}
	actionRequest := TeamworkActionRequest{Action: "offer", Channel: "alpha", To: "reviewer",
		Deadline: "24h", Content: "review", Artifacts: []string{"a.md"}}
	resolveRequest := AgentResolveRequest{Decision: "retry", Content: "later"}
	service := localAPIAdapterService{
		hook: func(ctx context.Context, got agent.ControlMetadata) (agent.HookCheckResponse, *agent.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			return agent.HookCheckResponse{Pending: true}, nil
		},
		current: func(ctx context.Context, got agent.ControlMetadata) (agent.AgentCurrentResponse, *agent.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			return agent.AgentCurrentResponse{Status: "none"}, nil
		},
		action: func(ctx context.Context, got agent.ControlMetadata,
			request agent.TeamworkActionRequest,
		) (agent.OperationResponse, *agent.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			assertCanonicalAdapterRequest(t, actionRequest, request)
			request.Artifacts[0] = "changed-in-domain"
			return agent.OperationResponse{Results: []agent.OperationResult{}}, nil
		},
		resolve: func(ctx context.Context, got agent.ControlMetadata,
			request agent.AgentResolveRequest,
		) (agent.OperationResponse, *agent.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			if request.Decision != resolveRequest.Decision || request.Content != resolveRequest.Content {
				t.Fatalf("Agent resolve request = %#v", request)
			}
			return agent.OperationResponse{Results: []agent.OperationResult{}}, nil
		},
	}
	adapter := newLocalAPIServiceAdapter(service)
	ctx := context.WithValue(context.Background(), adapterContextKey{}, "present")
	if response, controlErr := adapter.HookCheck(ctx, metadata, HookCheckRequest{}); controlErr != nil ||
		!response.Pending || response.SchemaVersion != SchemaVersion {
		t.Fatalf("HookCheck() = (%#v, %v)", response, controlErr)
	}
	if response, controlErr := adapter.AgentCurrent(ctx, metadata, AgentCurrentRequest{}); controlErr != nil ||
		response.Status != "none" || response.SchemaVersion != SchemaVersion {
		t.Fatalf("AgentCurrent() = (%#v, %v)", response, controlErr)
	}
	if _, controlErr := adapter.TeamworkAction(ctx, metadata, actionRequest); controlErr != nil {
		t.Fatal(controlErr)
	}
	if actionRequest.Artifacts[0] != "a.md" {
		t.Fatalf("transport Artifact slice was aliased: %#v", actionRequest.Artifacts)
	}
	if _, controlErr := adapter.AgentResolve(ctx, metadata, resolveRequest); controlErr != nil {
		t.Fatal(controlErr)
	}
}

func TestLocalAPIServiceAdapterPreservesResultNullabilityAndPrivateFields(t *testing.T) {
	t.Parallel()
	projection := []byte(`{"status":"actionable"}`)
	current := agent.AgentCurrentResponse{Status: "actionable", RunID: "run-adapter",
		ClaimSecret: "private", Projection: projection}
	service := localAPIAdapterService{current: func(context.Context,
		agent.ControlMetadata,
	) (agent.AgentCurrentResponse, *agent.ControlError) {
		return current, nil
	}}
	adapter := &localAPIServiceAdapter{next: service}
	gotCurrent, controlErr := adapter.AgentCurrent(context.Background(), RequestMetadata{},
		AgentCurrentRequest{})
	if controlErr != nil || gotCurrent.Status != current.Status || gotCurrent.RunID != current.RunID ||
		gotCurrent.ClaimSecret != current.ClaimSecret || !bytes.Equal(gotCurrent.Projection, projection) ||
		gotCurrent.SchemaVersion != SchemaVersion {
		t.Fatalf("AgentCurrent() = (%#v, %v)", gotCurrent, controlErr)
	}
	gotCurrent.Projection[0] = 'x'
	if projection[0] == 'x' {
		t.Fatal("Agent current projection was not copied")
	}

	nilResult := localAPIOperationResponse(agent.OperationResponse{})
	if nilResult.Results != nil || nilResult.Handling != nil {
		t.Fatalf("nil result nullability = %#v", nilResult)
	}
	emptyResult := localAPIOperationResponse(agent.OperationResponse{SchemaVersion: model.SchemaVersion,
		Status: "accepted", Action: "teamwork.offer", OperationID: "operation-adapter",
		Replayed: true, Handling: &agent.HandlingReceipt{Status: "completed"},
		Results: []agent.OperationResult{{EventID: "event-adapter", EventType: "review.offered",
			Work: agent.WorkReceipt{Ref: "peer/work", Version: 3, State: "active"}}}, Receipt: "digest"})
	if emptyResult.Results == nil || len(emptyResult.Results) != 1 || emptyResult.Handling == nil ||
		emptyResult.Handling.Status != "completed" || !emptyResult.Replayed ||
		emptyResult.Results[0].Work.Version != 3 {
		t.Fatalf("mapped operation result = %#v", emptyResult)
	}
	explicitEmpty := localAPIOperationResponse(agent.OperationResponse{Results: []agent.OperationResult{}})
	if explicitEmpty.Results == nil || len(explicitEmpty.Results) != 0 {
		t.Fatalf("explicit empty results = %#v", explicitEmpty.Results)
	}
}

func TestLocalAPIServiceAdapterMapsAllControlErrorsExhaustively(t *testing.T) {
	t.Parallel()
	codes := agent.ControlErrorCodes()
	if len(localAPIErrorMappings) != len(codes) {
		t.Fatalf("local API mappings = %d, Agent codes = %d", len(localAPIErrorMappings), len(codes))
	}
	seen := make(map[agent.ControlErrorCode]struct{}, len(localAPIErrorMappings))
	for _, mapping := range localAPIErrorMappings {
		if _, duplicate := seen[mapping.domain]; duplicate || !mapping.transport.Valid() {
			t.Fatalf("invalid or duplicate local API mapping = %#v", mapping)
		}
		seen[mapping.domain] = struct{}{}
	}
	for _, code := range codes {
		assertLocalAPIControlError(t, code)
	}
}

func TestLocalAPIServiceAdapterFailsClosedOnMalformedControlErrors(t *testing.T) {
	t.Parallel()
	unknown := localAPIControlError(&agent.ControlError{Code: "unknown", Message: "diagnostic"})
	malformedRetry := localAPIControlError(&agent.ControlError{Code: agent.CodeOperationPending,
		Message: "diagnostic", Retryable: false})
	malformedReplay := localAPIControlError(&agent.ControlError{Code: agent.CodeInternal,
		Message: "diagnostic", Replayed: true})
	invalidOperation := "bad id"
	malformedOperation := localAPIControlError(&agent.ControlError{Code: agent.CodeInternal,
		Message: "diagnostic", OperationID: &invalidOperation})
	for _, mapped := range []*APIError{unknown, malformedRetry, malformedReplay, malformedOperation} {
		if mapped == nil || mapped.Code != CodeInternal || mapped.Message != "internal control error" ||
			mapped.Retryable || mapped.Replayed || mapped.OperationID != nil {
			t.Fatalf("invalid Control error mapping = %#v", mapped)
		}
	}
	if adapter := newLocalAPIServiceAdapter(nil); adapter != nil {
		t.Fatalf("newLocalAPIServiceAdapter(nil) = %#v", adapter)
	}
}

func TestLocalAPIServiceAdapterPrioritizesControlErrorOverResult(t *testing.T) {
	t.Parallel()
	service := localAPIAdapterService{action: func(context.Context, agent.ControlMetadata,
		agent.TeamworkActionRequest,
	) (agent.OperationResponse, *agent.ControlError) {
		return agent.OperationResponse{Status: "accepted", OperationID: "operation-must-not-leak",
				Results: []agent.OperationResult{}}, agent.NewControlError(agent.CodeWorkConflict,
				"current Work changed")
	}}
	adapter := newLocalAPIServiceAdapter(service)
	response, apiErr := adapter.TeamworkAction(context.Background(), RequestMetadata{},
		TeamworkActionRequest{Action: "offer"})
	if apiErr == nil || apiErr.Code != CodeWorkConflict || response.Status != "" ||
		response.OperationID != "" || response.Results != nil {
		t.Fatalf("error-priority result = (%#v, %#v)", response, apiErr)
	}
}

func assertLocalAPIControlError(t *testing.T, code agent.ControlErrorCode) {
	t.Helper()
	operationID := "operation-adapter-error"
	controlErr := agent.NewControlError(code, "bounded diagnostic")
	controlErr.OperationID = &operationID
	controlErr.Replayed = true
	mapped := localAPIControlError(controlErr)
	if string(mapped.Code) != string(code) || mapped.Retryable != code.Retryable() ||
		!mapped.Replayed || mapped.OperationID == nil || *mapped.OperationID != operationID ||
		mapped.SchemaVersion != SchemaVersion || mapped.Status != "error" {
		t.Fatalf("mapped Control error %q = %#v", code, mapped)
	}
}

type adapterContextKey struct{}

func assertAdapterContext(t *testing.T, ctx context.Context) {
	t.Helper()
	if ctx == nil || ctx.Value(adapterContextKey{}) != "present" {
		t.Fatalf("Agent context = %#v", ctx)
	}
}

func assertCanonicalAdapterRequest(t *testing.T, transport TeamworkActionRequest,
	domain agent.TeamworkActionRequest,
) {
	t.Helper()
	transportRaw, transportErr := model.CanonicalMarshal(transport)
	domainRaw, domainErr := model.CanonicalMarshal(domain)
	if transportErr != nil || domainErr != nil || !bytes.Equal(transportRaw, domainRaw) {
		t.Fatalf("request canonical bytes = %s / %s, errors = %v / %v",
			transportRaw, domainRaw, transportErr, domainErr)
	}
}
