package nodecontrol

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type serviceAdapter struct {
	next node.ManagedControlService
}

func newServiceAdapter(next node.ManagedControlService) *serviceAdapter {
	if next == nil {
		return nil
	}
	return &serviceAdapter{next: next}
}

func (adapter *serviceAdapter) HookCheck(ctx context.Context,
	metadata localapi.RequestMetadata, _ localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	response, controlErr := adapter.next.HookCheck(ctx, controlMetadata(metadata))
	if controlErr != nil {
		return localapi.HookCheckResponse{}, controlAPIError(controlErr)
	}
	return localapi.HookCheckResponse{SchemaVersion: localapi.SchemaVersion,
		Pending: response.Pending}, nil
}

func (adapter *serviceAdapter) AgentCurrent(ctx context.Context,
	metadata localapi.RequestMetadata, _ localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	response, controlErr := adapter.next.AgentCurrent(ctx, controlMetadata(metadata))
	if controlErr != nil {
		return localapi.AgentCurrentResponse{}, controlAPIError(controlErr)
	}
	return localapi.AgentCurrentResponse{SchemaVersion: localapi.SchemaVersion,
		Status: response.Status, RunID: response.RunID, ClaimSecret: response.ClaimSecret,
		Projection: append(response.Projection[:0:0], response.Projection...)}, nil
}

func (adapter *serviceAdapter) TeamworkAction(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	response, controlErr := adapter.next.TeamworkAction(ctx, controlMetadata(metadata),
		node.TeamworkActionRequest{Action: request.Action, Channel: request.Channel,
			To: request.To, Deadline: request.Deadline, Content: request.Content,
			Artifacts: append([]string(nil), request.Artifacts...)})
	if controlErr != nil {
		return localapi.OperationResponse{}, controlAPIError(controlErr)
	}
	return operationResponse(response), nil
}

func (adapter *serviceAdapter) AgentResolve(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	response, controlErr := adapter.next.AgentResolve(ctx, controlMetadata(metadata),
		node.AgentResolveRequest{Decision: request.Decision, Content: request.Content})
	if controlErr != nil {
		return localapi.OperationResponse{}, controlAPIError(controlErr)
	}
	return operationResponse(response), nil
}

func controlMetadata(metadata localapi.RequestMetadata) node.ControlMetadata {
	return node.ControlMetadata{Profile: metadata.Profile,
		OperationKeyHash: metadata.OperationKeyHash, HasOperationKey: metadata.HasOperationKey,
		ClaimContextHash: metadata.ClaimContextHash, HasClaimContext: metadata.HasClaimContext,
		RunAttachmentHash: metadata.RunAttachmentHash, HasRunAttachment: metadata.HasRunAttachment}
}

func operationResponse(response node.OperationResponse) localapi.OperationResponse {
	var handling *localapi.HandlingReceipt
	if response.Handling != nil {
		handling = &localapi.HandlingReceipt{Status: response.Handling.Status}
	}
	var results []localapi.OperationResult
	if response.Results != nil {
		results = make([]localapi.OperationResult, len(response.Results))
		for index, result := range response.Results {
			results[index] = localapi.OperationResult{EventID: result.EventID,
				EventType: result.EventType, Work: localapi.WorkReceipt{Ref: result.Work.Ref,
					Version: result.Work.Version, State: result.Work.State}}
		}
	}
	return localapi.OperationResponse{SchemaVersion: response.SchemaVersion,
		Status: response.Status, Action: response.Action, OperationID: response.OperationID,
		Replayed: response.Replayed, Handling: handling, Results: results, Receipt: response.Receipt}
}

func controlAPIError(controlErr *node.ControlError) *localapi.APIError {
	if controlErr == nil {
		return nil
	}
	code, known := controlErrorCode(controlErr.Code)
	if !known || controlErr.Retryable != controlErr.Code.Retryable() ||
		controlErr.Replayed && controlErr.OperationID == nil {
		return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
	}
	if controlErr.OperationID != nil {
		if _, err := model.ParseOperationID(*controlErr.OperationID); err != nil {
			return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
		}
	}
	mapped := localapi.NewAPIError(code, controlErr.Message)
	if mapped.Code != code || mapped.Message != controlErr.Message {
		return localapi.NewAPIError(localapi.CodeInternal, "internal control error")
	}
	mapped.Replayed = controlErr.Replayed
	if controlErr.OperationID != nil {
		operationID := *controlErr.OperationID
		mapped.OperationID = &operationID
	}
	return mapped
}

type controlErrorMapping struct {
	domain    node.ControlErrorCode
	transport localapi.ErrorCode
}

var controlErrorMappings = [...]controlErrorMapping{
	{node.ControlCodeInvalidArgument, localapi.CodeInvalidArgument},
	{node.ControlCodeContentRequired, localapi.CodeContentRequired},
	{node.ControlCodeContentTooLarge, localapi.CodeContentTooLarge},
	{node.ControlCodeArtifactInvalid, localapi.CodeArtifactInvalid},
	{node.ControlCodeArtifactTooLarge, localapi.CodeArtifactTooLarge},
	{node.ControlCodeAmbiguousChannel, localapi.CodeAmbiguousChannel},
	{node.ControlCodeAmbiguousParticipant, localapi.CodeAmbiguousParticipant},
	{node.ControlCodeUnknownAction, localapi.CodeUnknownAction},
	{node.ControlCodeAuthenticationFailed, localapi.CodeAuthenticationFailed},
	{node.ControlCodeContextRequired, localapi.CodeContextRequired},
	{node.ControlCodeContextInvalid, localapi.CodeContextInvalid},
	{node.ControlCodeContextStale, localapi.CodeContextStale},
	{node.ControlCodeAssetRevisionMismatch, localapi.CodeAssetRevisionMismatch},
	{node.ControlCodeActionNotAllowed, localapi.CodeActionNotAllowed},
	{node.ControlCodeCurrentTooLarge, localapi.CodeCurrentTooLarge},
	{node.ControlCodeOperationMismatch, localapi.CodeOperationMismatch},
	{node.ControlCodeWorkConflict, localapi.CodeWorkConflict},
	{node.ControlCodeWorkExpired, localapi.CodeWorkExpired},
	{node.ControlCodeProfileHostMismatch, localapi.CodeProfileHostMismatch},
	{node.ControlCodeOperationPending, localapi.CodeOperationPending},
	{node.ControlCodePeerUnavailable, localapi.CodePeerUnavailable},
	{node.ControlCodeMnemondUnavailable, localapi.CodeMnemondUnavailable},
	{node.ControlCodeInternal, localapi.CodeInternal},
}

func controlErrorCode(code node.ControlErrorCode) (localapi.ErrorCode, bool) {
	for _, mapping := range controlErrorMappings {
		if mapping.domain == code {
			return mapping.transport, true
		}
	}
	return localapi.CodeInternal, false
}

var _ localapi.Service = (*serviceAdapter)(nil)
