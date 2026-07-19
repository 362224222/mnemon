package agent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type teamworkRejectionWire struct {
	SchemaVersion int              `json:"schema_version"`
	Status        string           `json:"status"`
	Code          ControlErrorCode `json:"code"`
	Retryable     bool             `json:"retryable"`
	Replayed      bool             `json:"replayed"`
	Message       string           `json:"message"`
	OperationID   string           `json:"operation_id"`
}

func buildTeamworkRejection(operation model.Operation, apiErr *ControlError) (model.JSON, error) {
	if operation.ID().IsZero() || apiErr == nil || !apiErr.Code.Valid() || apiErr.Message == "" {
		return model.JSON{}, errors.New("incomplete Teamwork rejection")
	}
	return model.JSONFrom(teamworkRejectionWire{SchemaVersion: model.SchemaVersion, Status: "error",
		Code: apiErr.Code, Retryable: apiErr.Code.Retryable(), Replayed: false,
		Message: apiErr.Message, OperationID: operation.ID().String()})
}

func (e *TeamworkActionExecutor) matchesTeamworkOperation(action ValidatedAction,
	operation model.Operation,
) bool {
	contextHash, hasContext := operation.ContextHash()
	requestDigest, err := action.requestDigest(contextHash, hasContext)
	return operation.ProfileID() == e.profile.ID() && action.matches(e.actions, operation.Kind()) &&
		err == nil && operation.RequestDigest() == requestDigest
}

func (e *TeamworkActionExecutor) replayTerminalTeamworkOperation(action ValidatedAction,
	operation model.Operation,
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
	response, err := decodeCommittedTeamworkOperation(e.actions, operation, true)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"committed Teamwork receipt is invalid"), true
	}
	return response, nil, true
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
	var wire teamworkRejectionWire
	decoder := json.NewDecoder(strings.NewReader(result.String()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil || wire.SchemaVersion != model.SchemaVersion ||
		requireExecutionJSONEOF(decoder) != nil || wire.Status != "error" || wire.Replayed ||
		wire.OperationID != operationID.String() || !wire.Code.Valid() || wire.Message == "" ||
		wire.Retryable != wire.Code.Retryable() {
		return operationAPIError(CodeInternal, "rejected managed receipt is invalid",
			operationID, replayed)
	}
	return operationAPIError(wire.Code, wire.Message, operationID, replayed)
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

func (s *Service) replayTeamworkOperation(operation model.Operation) (OperationResponse, *ControlError) {
	switch operation.Status() {
	case model.OperationCommitted:
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
