package node

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// localAPIServiceAdapter is the only translation boundary between the Agent
// control domain and the local HTTP protocol. Every field and error code is
// mapped explicitly so transport concerns cannot leak into Agent policy.
type localAPIServiceAdapter struct {
	next agent.ControlService
}

func newLocalAPIServiceAdapter(next agent.ControlService) *localAPIServiceAdapter {
	if next == nil {
		return nil
	}
	return &localAPIServiceAdapter{next: next}
}

func (adapter *localAPIServiceAdapter) HookCheck(ctx context.Context,
	metadata RequestMetadata, _ HookCheckRequest,
) (HookCheckResponse, *APIError) {
	response, controlErr := adapter.next.HookCheck(ctx, agentControlMetadata(metadata))
	if controlErr != nil {
		return HookCheckResponse{}, localAPIControlError(controlErr)
	}
	return HookCheckResponse{SchemaVersion: SchemaVersion,
		Pending: response.Pending}, nil
}

func (adapter *localAPIServiceAdapter) AgentCurrent(ctx context.Context,
	metadata RequestMetadata, _ AgentCurrentRequest,
) (AgentCurrentResponse, *APIError) {
	response, controlErr := adapter.next.AgentCurrent(ctx, agentControlMetadata(metadata))
	if controlErr != nil {
		return AgentCurrentResponse{}, localAPIControlError(controlErr)
	}
	return AgentCurrentResponse{SchemaVersion: SchemaVersion,
		Status: response.Status, RunID: response.RunID, ClaimSecret: response.ClaimSecret,
		Projection: append(response.Projection[:0:0], response.Projection...)}, nil
}

func (adapter *localAPIServiceAdapter) TeamworkAction(ctx context.Context,
	metadata RequestMetadata, request TeamworkActionRequest,
) (OperationResponse, *APIError) {
	response, controlErr := adapter.next.TeamworkAction(ctx, agentControlMetadata(metadata),
		agent.TeamworkActionRequest{Action: request.Action, Channel: request.Channel,
			To: request.To, Deadline: request.Deadline, Content: request.Content,
			Artifacts: append([]string(nil), request.Artifacts...)})
	if controlErr != nil {
		return OperationResponse{}, localAPIControlError(controlErr)
	}
	return localAPIOperationResponse(response), nil
}

func (adapter *localAPIServiceAdapter) AgentResolve(ctx context.Context,
	metadata RequestMetadata, request AgentResolveRequest,
) (OperationResponse, *APIError) {
	response, controlErr := adapter.next.AgentResolve(ctx, agentControlMetadata(metadata),
		agent.AgentResolveRequest{Decision: request.Decision, Content: request.Content})
	if controlErr != nil {
		return OperationResponse{}, localAPIControlError(controlErr)
	}
	return localAPIOperationResponse(response), nil
}

func agentControlMetadata(metadata RequestMetadata) agent.ControlMetadata {
	return agent.ControlMetadata{Profile: metadata.Profile,
		OperationKeyHash: metadata.OperationKeyHash, HasOperationKey: metadata.HasOperationKey,
		ClaimContextHash: metadata.ClaimContextHash, HasClaimContext: metadata.HasClaimContext,
		RunAttachmentHash: metadata.RunAttachmentHash, HasRunAttachment: metadata.HasRunAttachment}
}

func localAPIOperationResponse(response agent.OperationResponse) OperationResponse {
	var handling *HandlingReceipt
	if response.Handling != nil {
		handling = &HandlingReceipt{Status: response.Handling.Status}
	}
	var results []OperationResult
	if response.Results != nil {
		results = make([]OperationResult, len(response.Results))
		for index, result := range response.Results {
			results[index] = OperationResult{EventID: result.EventID,
				EventType: result.EventType, Work: WorkReceipt{Ref: result.Work.Ref,
					Version: result.Work.Version, State: result.Work.State}}
		}
	}
	return OperationResponse{SchemaVersion: response.SchemaVersion,
		Status: response.Status, Action: response.Action, OperationID: response.OperationID,
		Replayed: response.Replayed, Handling: handling, Results: results, Receipt: response.Receipt}
}

func localAPIControlError(controlErr *agent.ControlError) *APIError {
	if controlErr == nil {
		return nil
	}
	code, known := localAPIErrorCode(controlErr.Code)
	if !known || controlErr.Retryable != controlErr.Code.Retryable() ||
		controlErr.Replayed && controlErr.OperationID == nil {
		return NewAPIError(CodeInternal, "internal control error")
	}
	if controlErr.OperationID != nil {
		if _, err := model.ParseOperationID(*controlErr.OperationID); err != nil {
			return NewAPIError(CodeInternal, "internal control error")
		}
	}
	mapped := NewAPIError(code, controlErr.Message)
	if mapped.Code != code || mapped.Message != controlErr.Message {
		return NewAPIError(CodeInternal, "internal control error")
	}
	mapped.Replayed = controlErr.Replayed
	if controlErr.OperationID != nil {
		operationID := *controlErr.OperationID
		mapped.OperationID = &operationID
	}
	return mapped
}

type localAPIErrorMapping struct {
	domain    agent.ControlErrorCode
	transport ErrorCode
}

var localAPIErrorMappings = [...]localAPIErrorMapping{
	{agent.CodeInvalidArgument, CodeInvalidArgument},
	{agent.CodeContentRequired, CodeContentRequired},
	{agent.CodeContentTooLarge, CodeContentTooLarge},
	{agent.CodeArtifactInvalid, CodeArtifactInvalid},
	{agent.CodeArtifactTooLarge, CodeArtifactTooLarge},
	{agent.CodeAmbiguousChannel, CodeAmbiguousChannel},
	{agent.CodeAmbiguousParticipant, CodeAmbiguousParticipant},
	{agent.CodeUnknownAction, CodeUnknownAction},
	{agent.CodeAuthenticationFailed, CodeAuthenticationFailed},
	{agent.CodeContextRequired, CodeContextRequired},
	{agent.CodeContextInvalid, CodeContextInvalid},
	{agent.CodeContextStale, CodeContextStale},
	{agent.CodeAssetRevisionMismatch, CodeAssetRevisionMismatch},
	{agent.CodeActionNotAllowed, CodeActionNotAllowed},
	{agent.CodeCurrentTooLarge, CodeCurrentTooLarge},
	{agent.CodeOperationMismatch, CodeOperationMismatch},
	{agent.CodeWorkConflict, CodeWorkConflict},
	{agent.CodeWorkExpired, CodeWorkExpired},
	{agent.CodeProfileHostMismatch, CodeProfileHostMismatch},
	{agent.CodeOperationPending, CodeOperationPending},
	{agent.CodePeerUnavailable, CodePeerUnavailable},
	{agent.CodeMnemondUnavailable, CodeMnemondUnavailable},
	{agent.CodeInternal, CodeInternal},
}

func localAPIErrorCode(code agent.ControlErrorCode) (ErrorCode, bool) {
	for _, mapping := range localAPIErrorMappings {
		if mapping.domain == code {
			return mapping.transport, true
		}
	}
	return CodeInternal, false
}

var _ Service = (*localAPIServiceAdapter)(nil)
