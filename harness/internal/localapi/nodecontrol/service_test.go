package nodecontrol

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type controlAdapterService struct {
	hook    func(context.Context, node.ControlMetadata) (node.HookCheckResponse, *node.ControlError)
	current func(context.Context, node.ControlMetadata) (node.AgentCurrentResponse, *node.ControlError)
	action  func(context.Context, node.ControlMetadata, node.TeamworkActionRequest) (node.OperationResponse, *node.ControlError)
	resolve func(context.Context, node.ControlMetadata, node.AgentResolveRequest) (node.OperationResponse, *node.ControlError)
}

func (service controlAdapterService) HookCheck(ctx context.Context,
	metadata node.ControlMetadata,
) (node.HookCheckResponse, *node.ControlError) {
	return service.hook(ctx, metadata)
}

func (service controlAdapterService) AgentCurrent(ctx context.Context,
	metadata node.ControlMetadata,
) (node.AgentCurrentResponse, *node.ControlError) {
	return service.current(ctx, metadata)
}

func (service controlAdapterService) TeamworkAction(ctx context.Context,
	metadata node.ControlMetadata, request node.TeamworkActionRequest,
) (node.OperationResponse, *node.ControlError) {
	return service.action(ctx, metadata, request)
}

func (service controlAdapterService) AgentResolve(ctx context.Context,
	metadata node.ControlMetadata, request node.AgentResolveRequest,
) (node.OperationResponse, *node.ControlError) {
	return service.resolve(ctx, metadata, request)
}

func TestServiceAdapterMapsMetadataAndRequestsExactly(t *testing.T) {
	operationHash := model.Sum([]byte("operation"))
	contextHash := model.Sum([]byte("context"))
	attachmentHash := model.Sum([]byte("attachment"))
	metadata := localapi.RequestMetadata{OperationKeyHash: operationHash, HasOperationKey: true,
		ClaimContextHash: contextHash, HasClaimContext: true,
		RunAttachmentHash: attachmentHash, HasRunAttachment: true}
	assertMetadata := func(got node.ControlMetadata) {
		t.Helper()
		if got.Profile.ID() != metadata.Profile.ID() || got.OperationKeyHash != operationHash ||
			!got.HasOperationKey || got.ClaimContextHash != contextHash || !got.HasClaimContext ||
			got.RunAttachmentHash != attachmentHash || !got.HasRunAttachment {
			t.Fatalf("control metadata = %#v", got)
		}
	}
	actionRequest := localapi.TeamworkActionRequest{Action: "offer", Channel: "alpha", To: "reviewer",
		Deadline: "24h", Content: "review", Artifacts: []string{"a.md"}}
	resolveRequest := localapi.AgentResolveRequest{Decision: "retry", Content: "later"}
	service := controlAdapterService{
		hook: func(ctx context.Context, got node.ControlMetadata) (node.HookCheckResponse, *node.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			return node.HookCheckResponse{Pending: true}, nil
		},
		current: func(ctx context.Context, got node.ControlMetadata) (node.AgentCurrentResponse, *node.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			return node.AgentCurrentResponse{Status: "none"}, nil
		},
		action: func(ctx context.Context, got node.ControlMetadata,
			request node.TeamworkActionRequest,
		) (node.OperationResponse, *node.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			assertCanonicalAdapterRequest(t, actionRequest, request)
			request.Artifacts[0] = "changed-in-domain"
			return emptyOperationResponse(), nil
		},
		resolve: func(ctx context.Context, got node.ControlMetadata,
			request node.AgentResolveRequest,
		) (node.OperationResponse, *node.ControlError) {
			assertAdapterContext(t, ctx)
			assertMetadata(got)
			if request.Decision != resolveRequest.Decision || request.Content != resolveRequest.Content {
				t.Fatalf("control resolve request = %#v", request)
			}
			return emptyOperationResponse(), nil
		},
	}
	adapter := newServiceAdapter(service)
	ctx := context.WithValue(context.Background(), adapterContextKey{}, "present")
	if response, controlErr := adapter.HookCheck(ctx, metadata, localapi.HookCheckRequest{}); controlErr != nil ||
		!response.Pending || response.SchemaVersion != localapi.SchemaVersion {
		t.Fatalf("HookCheck() = (%#v, %v)", response, controlErr)
	}
	if response, controlErr := adapter.AgentCurrent(ctx, metadata, localapi.AgentCurrentRequest{}); controlErr != nil ||
		response.Status != "none" || response.SchemaVersion != localapi.SchemaVersion {
		t.Fatalf("AgentCurrent() = (%#v, %v)", response, controlErr)
	}
	if _, controlErr := adapter.TeamworkAction(ctx, metadata, actionRequest); controlErr != nil {
		t.Fatal(controlErr)
	}
	if actionRequest.Artifacts[0] != "a.md" {
		t.Fatalf("transport artifact slice was aliased: %#v", actionRequest.Artifacts)
	}
	if _, controlErr := adapter.AgentResolve(ctx, metadata, resolveRequest); controlErr != nil {
		t.Fatal(controlErr)
	}
}

func TestServiceAdapterPreservesResultNullabilityAndPrivateFields(t *testing.T) {
	projection := []byte(`{"status":"actionable"}`)
	current := node.AgentCurrentResponse{Status: "actionable", RunID: "run-adapter",
		ClaimSecret: "private", Projection: projection}
	service := controlAdapterService{current: func(context.Context,
		node.ControlMetadata,
	) (node.AgentCurrentResponse, *node.ControlError) {
		return current, nil
	}}
	adapter := &serviceAdapter{next: service}
	gotCurrent, controlErr := adapter.AgentCurrent(context.Background(), localapi.RequestMetadata{},
		localapi.AgentCurrentRequest{})
	if controlErr != nil || gotCurrent.Status != current.Status || gotCurrent.RunID != current.RunID ||
		gotCurrent.ClaimSecret != current.ClaimSecret || !bytes.Equal(gotCurrent.Projection, projection) ||
		gotCurrent.SchemaVersion != localapi.SchemaVersion {
		t.Fatalf("AgentCurrent() = (%#v, %v)", gotCurrent, controlErr)
	}
	gotCurrent.Projection[0] = 'x'
	if projection[0] == 'x' {
		t.Fatal("agent current projection was not copied")
	}

	nilResult := operationResponse(node.OperationResponse{})
	if nilResult.Results != nil || nilResult.Handling != nil {
		t.Fatalf("nil result nullability = %#v", nilResult)
	}
	domainResult := decodeOperationResponse(t, `{
		"handling":{"status":"completed"},
		"results":[{"event_id":"event-adapter","event_type":"review.offered",
			"work":{"ref":"peer/work","version":3,"state":"active"}}]
	}`)
	domainResult.SchemaVersion = model.SchemaVersion
	domainResult.Status = "accepted"
	domainResult.Action = "teamwork.offer"
	domainResult.OperationID = "operation-adapter"
	domainResult.Replayed = true
	domainResult.Receipt = "digest"
	mappedResult := operationResponse(domainResult)
	if mappedResult.Results == nil || len(mappedResult.Results) != 1 || mappedResult.Handling == nil ||
		mappedResult.Handling.Status != "completed" || !mappedResult.Replayed ||
		mappedResult.Results[0].Work.Version != 3 {
		t.Fatalf("mapped operation result = %#v", mappedResult)
	}
	explicitEmpty := operationResponse(emptyOperationResponse())
	if explicitEmpty.Results == nil || len(explicitEmpty.Results) != 0 {
		t.Fatalf("explicit empty results = %#v", explicitEmpty.Results)
	}
}

func TestServiceAdapterMapsAllControlErrorsExhaustively(t *testing.T) {
	if len(controlErrorMappings) != len(controlErrorCodesForAdapterTest) {
		t.Fatalf("local API mappings = %d, control codes = %d",
			len(controlErrorMappings), len(controlErrorCodesForAdapterTest))
	}
	seen := make(map[node.ControlErrorCode]struct{}, len(controlErrorMappings))
	for _, mapping := range controlErrorMappings {
		if _, duplicate := seen[mapping.domain]; duplicate || !mapping.domain.Valid() ||
			!mapping.transport.Valid() {
			t.Fatalf("invalid or duplicate local API mapping = %#v", mapping)
		}
		seen[mapping.domain] = struct{}{}
	}
	for _, code := range controlErrorCodesForAdapterTest {
		if _, ok := seen[code]; !ok {
			t.Errorf("control code %q is not mapped", code)
			continue
		}
		assertControlAPIError(t, code)
	}
}

func TestServiceAdapterFailsClosedOnMalformedControlErrors(t *testing.T) {
	unknown := controlAPIError(&node.ControlError{Code: "unknown", Message: "diagnostic"})
	malformedRetry := controlAPIError(&node.ControlError{Code: node.ControlCodeOperationPending,
		Message: "diagnostic", Retryable: false})
	malformedReplay := controlAPIError(&node.ControlError{Code: node.ControlCodeInternal,
		Message: "diagnostic", Replayed: true})
	invalidOperation := "bad id"
	malformedOperation := controlAPIError(&node.ControlError{Code: node.ControlCodeInternal,
		Message: "diagnostic", OperationID: &invalidOperation})
	for _, mapped := range []*localapi.APIError{unknown, malformedRetry, malformedReplay, malformedOperation} {
		if mapped == nil || mapped.Code != localapi.CodeInternal || mapped.Message != "internal control error" ||
			mapped.Retryable || mapped.Replayed || mapped.OperationID != nil {
			t.Fatalf("invalid control error mapping = %#v", mapped)
		}
	}
	if adapter := newServiceAdapter(nil); adapter != nil {
		t.Fatalf("newServiceAdapter(nil) = %#v", adapter)
	}
}

func TestServiceAdapterPrioritizesControlErrorOverResult(t *testing.T) {
	service := controlAdapterService{action: func(context.Context, node.ControlMetadata,
		node.TeamworkActionRequest,
	) (node.OperationResponse, *node.ControlError) {
		response := emptyOperationResponse()
		response.Status = "accepted"
		response.OperationID = "operation-must-not-leak"
		return response, newControlError(node.ControlCodeWorkConflict, "current Work changed")
	}}
	adapter := newServiceAdapter(service)
	response, apiErr := adapter.TeamworkAction(context.Background(), localapi.RequestMetadata{},
		localapi.TeamworkActionRequest{Action: "offer"})
	if apiErr == nil || apiErr.Code != localapi.CodeWorkConflict || response.Status != "" ||
		response.OperationID != "" || response.Results != nil {
		t.Fatalf("error-priority result = (%#v, %#v)", response, apiErr)
	}
}

var controlErrorCodesForAdapterTest = node.ControlErrorCodes()

func assertControlAPIError(t *testing.T, code node.ControlErrorCode) {
	t.Helper()
	operationID := "operation-adapter-error"
	controlErr := newControlError(code, "bounded diagnostic")
	controlErr.OperationID = &operationID
	controlErr.Replayed = true
	mapped := controlAPIError(controlErr)
	if string(mapped.Code) != string(code) || mapped.Retryable != code.Retryable() ||
		!mapped.Replayed || mapped.OperationID == nil || *mapped.OperationID != operationID ||
		mapped.SchemaVersion != localapi.SchemaVersion || mapped.Status != "error" {
		t.Fatalf("mapped control error %q = %#v", code, mapped)
	}
}

func newControlError(code node.ControlErrorCode, message string) *node.ControlError {
	return &node.ControlError{Code: code, Retryable: code.Retryable(), Message: message}
}

func emptyOperationResponse() node.OperationResponse {
	var response node.OperationResponse
	response.Results = nonNilEmpty(response.Results)
	return response
}

func nonNilEmpty[T any](_ []T) []T {
	return make([]T, 0)
}

func decodeOperationResponse(t *testing.T, raw string) node.OperationResponse {
	t.Helper()
	var response node.OperationResponse
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("decode operation response: %v", err)
	}
	return response
}

type adapterContextKey struct{}

func assertAdapterContext(t *testing.T, ctx context.Context) {
	t.Helper()
	if ctx == nil || ctx.Value(adapterContextKey{}) != "present" {
		t.Fatalf("control context = %#v", ctx)
	}
}

func assertCanonicalAdapterRequest(t *testing.T, transport localapi.TeamworkActionRequest,
	domain node.TeamworkActionRequest,
) {
	t.Helper()
	transportRaw, transportErr := model.CanonicalMarshal(transport)
	domainRaw, domainErr := model.CanonicalMarshal(domain)
	if transportErr != nil || domainErr != nil || !bytes.Equal(transportRaw, domainRaw) {
		t.Fatalf("request canonical bytes = %s / %s, errors = %v / %v",
			transportRaw, domainRaw, transportErr, domainErr)
	}
}
