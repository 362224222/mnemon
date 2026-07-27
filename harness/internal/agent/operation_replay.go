package agent

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func buildTeamworkRejection(operation model.Operation, apiErr *ControlError) (model.JSON, error) {
	if operation.ID().IsZero() || apiErr == nil || !apiErr.Code.Valid() || apiErr.Code.Retryable() ||
		apiErr.Message == "" {
		return model.JSON{}, errors.New("incomplete Teamwork rejection")
	}
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		Code: string(apiErr.Code), Message: apiErr.Message, OperationID: operation.ID(),
	})
	if err != nil {
		return model.JSON{}, err
	}
	return receipt.JSON(), nil
}

func (e *TeamworkActionExecutor) matchesTeamworkOperation(action ValidatedAction,
	operation model.Operation,
) bool {
	contextHash, hasContext := operation.ContextHash()
	requestDigest, err := action.requestDigest(contextHash, hasContext)
	return operation.ProfileID() == e.profile.ID() && action.matches(e.actions, operation.Kind()) &&
		err == nil && operation.RequestDigest() == requestDigest
}

func sameTeamworkOperationIdentity(started, terminal model.Operation) bool {
	startedContext, startedHasContext := started.ContextHash()
	terminalContext, terminalHasContext := terminal.ContextHash()
	return started.ID() == terminal.ID() && started.ProfileID() == terminal.ProfileID() &&
		started.AgentRunID() == terminal.AgentRunID() &&
		started.ClientKeyHash() == terminal.ClientKeyHash() && started.Kind() == terminal.Kind() &&
		started.RequestDigest() == terminal.RequestDigest() &&
		started.CreatedAt().Equal(terminal.CreatedAt()) && startedHasContext == terminalHasContext &&
		(!startedHasContext || startedContext == terminalContext)
}

func (e *TeamworkActionExecutor) probeRejectionTerminal(ctx context.Context,
	operation model.Operation,
) (OperationResponse, *ControlError, bool) {
	contextHash, hasContext := operation.ContextHash()
	probe, err := e.backend.Probe(ctx, store.ManagedOperationProbeSpec{Profile: e.profile,
		ClientKeyHash: operation.ClientKeyHash(), RequestDigest: operation.RequestDigest(),
		Kind: operation.Kind(), ClaimContextHash: contextHash, HasClaimContext: hasContext})
	if err != nil {
		return OperationResponse{}, mapTeamworkExecutionError(err), true
	}
	if !probe.Found {
		if !probe.Operation.ID().IsZero() {
			return OperationResponse{}, operationAPIError(CodeInternal,
				"Teamwork operation probe returned live authority", operation.ID(), false), true
		}
		return OperationResponse{}, nil, false
	}
	response, apiErr := e.projectRejectionTerminal(ctx, operation,
		store.OperationRejectionResult{Operation: probe.Operation, Replayed: true}, model.JSON{})
	return response, apiErr, true
}

func (e *TeamworkActionExecutor) projectRejectionTerminal(ctx context.Context,
	operation model.Operation,
	result store.OperationRejectionResult, freshEvidence model.JSON,
) (OperationResponse, *ControlError) {
	terminal := result.Operation
	if !sameTeamworkOperationIdentity(operation, terminal) {
		return OperationResponse{}, operationAPIError(CodeInternal,
			"terminal Teamwork operation authority differs", operation.ID(), result.Replayed)
	}
	if !terminal.Status().Terminal() {
		return OperationResponse{}, operationAPIError(CodeInternal,
			"operation rejection backend returned non-terminal authority", operation.ID(), result.Replayed)
	}
	if !result.Replayed {
		stored, hasResult := terminal.Result()
		if terminal.Status() != model.OperationRejected || freshEvidence.IsZero() || !hasResult ||
			stored.String() != freshEvidence.String() {
			return OperationResponse{}, operationAPIError(CodeInternal,
				"fresh Teamwork rejection receipt differs", operation.ID(), false)
		}
	}
	switch terminal.Status() {
	case model.OperationCommitted:
		return e.projectCommittedTeamworkOperation(ctx, terminal, result.Replayed)
	case model.OperationRejected:
		return OperationResponse{}, decodeRejectedTeamworkOperation(e.actions, terminal, result.Replayed)
	}
	return OperationResponse{}, operationAPIError(CodeInternal,
		"terminal Teamwork operation status is invalid", operation.ID(), result.Replayed)
}

func (e *TeamworkActionExecutor) replayTerminalTeamworkOperation(ctx context.Context,
	action ValidatedAction, operation model.Operation,
) (OperationResponse, *ControlError, bool) {
	if !operation.Status().Terminal() {
		return OperationResponse{}, nil, false
	}
	if !e.matchesTeamworkOperation(action, operation) {
		return OperationResponse{}, operationAPIError(CodeOperationMismatch,
			"reserved operation differs from Teamwork action", operation.ID(), true), true
	}
	if operation.Status() == model.OperationRejected {
		return OperationResponse{}, decodeRejectedTeamworkOperation(e.actions, operation, true), true
	}
	response, apiErr := e.projectCommittedTeamworkOperation(ctx, operation, true)
	return response, apiErr, true
}

func (e *TeamworkActionExecutor) projectCommittedTeamworkOperation(ctx context.Context,
	operation model.Operation, replayed bool,
) (OperationResponse, *ControlError) {
	required, err := operationArtifactPublicationRequired(operation)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"committed Teamwork Artifact checkpoint is invalid")
	}
	if required {
		if apiErr := e.publishAccepted(ctx, operation.ID()); apiErr != nil {
			return OperationResponse{}, apiErr
		}
	}
	response, err := decodeCommittedTeamworkOperation(e.actions, operation, replayed)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"committed Teamwork receipt is invalid")
	}
	return response, nil
}

func operationArtifactPublicationRequired(operation model.Operation) (bool, error) {
	roots, err := operationArtifactCaptureRoots(operation)
	return len(roots) != 0, err
}

func operationArtifactCaptureRoots(operation model.Operation) ([]ArtifactCaptureRoot, error) {
	checkpoint, found := operation.Capture()
	if !found {
		return nil, nil
	}
	return parseArtifactCaptureCheckpoint(checkpoint)
}

func decodeRejectedTeamworkOperation(handlers ActionHandlers, operation model.Operation,
	replayed bool,
) *ControlError {
	handler, supported := handlers.Operation(operation.Kind())
	if !supported || !handler.EventType().Valid() {
		return operationAPIError(CodeInternal, "rejected Teamwork receipt is invalid",
			operation.ID(), replayed)
	}
	return decodeRejectedManagedOperation(operation, replayed)
}

func decodeRejectedManagedOperation(operation model.Operation, replayed bool) *ControlError {
	result, ok := operation.Result()
	if operation.Status() != model.OperationRejected || !ok || result.IsZero() || operation.ID().IsZero() {
		return operationAPIError(CodeInternal, "rejected managed receipt is invalid",
			operation.ID(), replayed)
	}
	return decodeManagedRejectionReceipt(result, operation.ID(), replayed)
}

func decodeManagedRejectionReceipt(result model.JSON, operationID model.OperationID,
	replayed bool,
) *ControlError {
	if result.IsZero() || operationID.IsZero() {
		return operationAPIError(CodeInternal, "rejected managed receipt is invalid",
			operationID, replayed)
	}
	receipt, err := model.ParseOperationRejectionReceipt(result.Bytes(), operationID)
	code := ControlErrorCode(receipt.Code())
	if err != nil || !code.Valid() || code.Retryable() {
		return operationAPIError(CodeInternal, "rejected managed receipt is invalid",
			operationID, replayed)
	}
	return operationAPIError(code, receipt.Message(), operationID, replayed)
}

func requireOperationCapabilities(metadata ControlMetadata,
	contextRequired bool,
) *ControlError {
	if !metadata.HasOperationKey || metadata.OperationKeyHash.IsZero() {
		return NewControlError(CodeInvalidArgument, "operation key is required")
	}
	if (contextRequired && !metadata.HasClaimContext) ||
		(metadata.HasClaimContext && metadata.ClaimContextHash.IsZero()) {
		return NewControlError(CodeContextRequired, "managed context is required")
	}
	return nil
}

func (s *Service) probeManagedOperation(ctx context.Context, metadata ControlMetadata,
	kind model.OperationKind, requestDigest model.Digest,
) (model.Operation, bool, *ControlError) {
	probe, err := s.store.ProbeManagedOperation(ctx, store.ManagedOperationProbeSpec{
		Profile: metadata.Profile, ClientKeyHash: metadata.OperationKeyHash,
		RequestDigest: requestDigest, Kind: kind,
		ClaimContextHash: metadata.ClaimContextHash, HasClaimContext: metadata.HasClaimContext,
	})
	if err != nil {
		return model.Operation{}, false, mapControlError(err)
	}
	if !probe.Found {
		if !probe.Operation.ID().IsZero() {
			return model.Operation{}, false, NewControlError(CodeInternal,
				"managed operation probe returned live authority")
		}
		return model.Operation{}, false, nil
	}
	operation := probe.Operation
	contextHash, hasContext := operation.ContextHash()
	if operation.ID().IsZero() || !operation.Status().Terminal() ||
		operation.ProfileID() != metadata.Profile.ID() ||
		operation.ClientKeyHash() != metadata.OperationKeyHash || operation.Kind() != kind ||
		operation.RequestDigest() != requestDigest || hasContext != metadata.HasClaimContext ||
		(hasContext && contextHash != metadata.ClaimContextHash) {
		return model.Operation{}, false, NewControlError(CodeInternal,
			"managed operation probe returned mismatched authority")
	}
	return operation, true, nil
}

func (s *Service) replayTeamworkOperation(ctx context.Context, action ValidatedAction,
	operation model.Operation,
) (OperationResponse, *ControlError) {
	switch operation.Status() {
	case model.OperationCommitted:
		required, err := operationArtifactPublicationRequired(operation)
		if err != nil {
			return OperationResponse{}, NewControlError(CodeInternal,
				"committed Teamwork Artifact checkpoint is invalid")
		}
		if required {
			if s.executor == nil {
				return OperationResponse{}, NewControlError(CodeInternal,
					"Teamwork action executor is unavailable")
			}
			response, apiErr := s.executor.ExecuteTeamwork(ctx, TeamworkExecutionSpec{
				Action: action, Reservation: store.ManagedOperationReservation{
					Operation: operation, Replayed: true,
				},
			})
			if apiErr != nil {
				return OperationResponse{}, apiErr
			}
			s.cleanupCurrentViews(operation.AgentRunID())
			return response, nil
		}
		response, err := decodeCommittedTeamworkOperation(s.actions, operation, true)
		if err != nil {
			return OperationResponse{}, NewControlError(CodeInternal,
				"committed Teamwork receipt is invalid")
		}
		s.cleanupCurrentViews(operation.AgentRunID())
		return response, nil
	case model.OperationRejected:
		return OperationResponse{}, decodeRejectedTeamworkOperation(s.actions, operation, true)
	default:
		return OperationResponse{}, NewControlError(CodeInternal,
			"managed operation probe returned a nonterminal Teamwork operation")
	}
}

func (s *Service) replayResolutionOperation(operation model.Operation) (OperationResponse, *ControlError) {
	if !validResolutionOperationKind(operation.Kind()) {
		return OperationResponse{}, NewControlError(CodeInternal,
			"managed resolution replay has an invalid kind")
	}
	switch operation.Status() {
	case model.OperationCommitted:
		receipt, ok := operation.Result()
		if !ok {
			return OperationResponse{}, NewControlError(CodeInternal,
				"committed resolution receipt is missing")
		}
		response, err := decodeResolutionResponse(receipt, operation)
		if err != nil {
			return OperationResponse{}, NewControlError(CodeInternal,
				"committed resolution receipt is invalid")
		}
		response.Replayed = true
		s.cleanupCurrentViews(operation.AgentRunID())
		return response, nil
	case model.OperationRejected:
		return OperationResponse{}, decodeRejectedManagedOperation(operation, true)
	default:
		return OperationResponse{}, NewControlError(CodeInternal,
			"managed operation probe returned a nonterminal resolution")
	}
}

func decodeResolutionResponse(receipt model.JSON, operation model.Operation) (OperationResponse, error) {
	var response OperationResponse
	if receipt.IsZero() || operation.ID().IsZero() || operation.Status() != model.OperationCommitted {
		return response, errors.New("zero resolution receipt")
	}
	if err := json.Unmarshal(receipt.Bytes(), &response); err != nil {
		return OperationResponse{}, err
	}
	if response.SchemaVersion != model.SchemaVersion || response.Status != "resolved" ||
		response.OperationID != operation.ID().String() || response.Action != string(operation.Kind()) ||
		response.Results == nil || len(response.Results) != 0 || response.Handling == nil ||
		response.Receipt == "" {
		return OperationResponse{}, errors.New("invalid resolution receipt shape")
	}
	wantHandling := map[model.OperationKind]string{
		model.OperationResolveNoAction: "completed",
		model.OperationResolveRetry:    "requeued",
		model.OperationResolveReject:   "rejected",
	}[operation.Kind()]
	if wantHandling == "" || response.Handling.Status != wantHandling {
		return OperationResponse{}, errors.New("invalid resolution receipt lifecycle")
	}
	return response, nil
}
