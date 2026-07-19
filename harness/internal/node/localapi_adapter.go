package node

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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
	metadata localapi.RequestMetadata, _ localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	response, controlErr := adapter.next.HookCheck(ctx, agentControlMetadata(metadata))
	if controlErr != nil {
		return localapi.HookCheckResponse{}, localAPIControlError(controlErr)
	}
	return localapi.HookCheckResponse{SchemaVersion: localapi.SchemaVersion,
		Pending: response.Pending}, nil
}

func (adapter *localAPIServiceAdapter) AgentCurrent(ctx context.Context,
	metadata localapi.RequestMetadata, _ localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	response, controlErr := adapter.next.AgentCurrent(ctx, agentControlMetadata(metadata))
	if controlErr != nil {
		return localapi.AgentCurrentResponse{}, localAPIControlError(controlErr)
	}
	return localapi.AgentCurrentResponse{SchemaVersion: localapi.SchemaVersion,
		Status: response.Status, RunID: response.RunID, ClaimSecret: response.ClaimSecret,
		Projection: append(response.Projection[:0:0], response.Projection...)}, nil
}

func (adapter *localAPIServiceAdapter) TeamworkAction(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	response, controlErr := adapter.next.TeamworkAction(ctx, agentControlMetadata(metadata),
		agent.TeamworkActionRequest{Action: request.Action, Channel: request.Channel,
			To: request.To, Deadline: request.Deadline, Content: request.Content,
			Artifacts: append([]string(nil), request.Artifacts...)})
	if controlErr != nil {
		return localapi.OperationResponse{}, localAPIControlError(controlErr)
	}
	return localAPIOperationResponse(response), nil
}

func (adapter *localAPIServiceAdapter) AgentResolve(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	response, controlErr := adapter.next.AgentResolve(ctx, agentControlMetadata(metadata),
		agent.AgentResolveRequest{Decision: request.Decision, Content: request.Content})
	if controlErr != nil {
		return localapi.OperationResponse{}, localAPIControlError(controlErr)
	}
	return localAPIOperationResponse(response), nil
}

func agentControlMetadata(metadata localapi.RequestMetadata) agent.ControlMetadata {
	return agent.ControlMetadata{Profile: metadata.Profile,
		OperationKeyHash: metadata.OperationKeyHash, HasOperationKey: metadata.HasOperationKey,
		ClaimContextHash: metadata.ClaimContextHash, HasClaimContext: metadata.HasClaimContext,
		RunAttachmentHash: metadata.RunAttachmentHash, HasRunAttachment: metadata.HasRunAttachment}
}

func localAPIOperationResponse(response agent.OperationResponse) localapi.OperationResponse {
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

func localAPIControlError(controlErr *agent.ControlError) *localapi.APIError {
	if controlErr == nil {
		return nil
	}
	code, known := localAPIErrorCode(controlErr.Code)
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

type localAPIErrorMapping struct {
	domain    agent.ControlErrorCode
	transport localapi.ErrorCode
}

var localAPIErrorMappings = [...]localAPIErrorMapping{
	{agent.CodeInvalidArgument, localapi.CodeInvalidArgument},
	{agent.CodeContentRequired, localapi.CodeContentRequired},
	{agent.CodeContentTooLarge, localapi.CodeContentTooLarge},
	{agent.CodeArtifactInvalid, localapi.CodeArtifactInvalid},
	{agent.CodeArtifactTooLarge, localapi.CodeArtifactTooLarge},
	{agent.CodeAmbiguousChannel, localapi.CodeAmbiguousChannel},
	{agent.CodeAmbiguousParticipant, localapi.CodeAmbiguousParticipant},
	{agent.CodeUnknownAction, localapi.CodeUnknownAction},
	{agent.CodeAuthenticationFailed, localapi.CodeAuthenticationFailed},
	{agent.CodeContextRequired, localapi.CodeContextRequired},
	{agent.CodeContextInvalid, localapi.CodeContextInvalid},
	{agent.CodeContextStale, localapi.CodeContextStale},
	{agent.CodeAssetRevisionMismatch, localapi.CodeAssetRevisionMismatch},
	{agent.CodeActionNotAllowed, localapi.CodeActionNotAllowed},
	{agent.CodeCurrentTooLarge, localapi.CodeCurrentTooLarge},
	{agent.CodeOperationMismatch, localapi.CodeOperationMismatch},
	{agent.CodeWorkConflict, localapi.CodeWorkConflict},
	{agent.CodeWorkExpired, localapi.CodeWorkExpired},
	{agent.CodeProfileHostMismatch, localapi.CodeProfileHostMismatch},
	{agent.CodeOperationPending, localapi.CodeOperationPending},
	{agent.CodePeerUnavailable, localapi.CodePeerUnavailable},
	{agent.CodeMnemondUnavailable, localapi.CodeMnemondUnavailable},
	{agent.CodeInternal, localapi.CodeInternal},
}

func localAPIErrorCode(code agent.ControlErrorCode) (localapi.ErrorCode, bool) {
	for _, mapping := range localAPIErrorMappings {
		if mapping.domain == code {
			return mapping.transport, true
		}
	}
	return localapi.CodeInternal, false
}

var _ localapi.Service = (*localAPIServiceAdapter)(nil)
