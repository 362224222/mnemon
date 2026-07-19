package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const MaxControlDiagnosticBytes = model.MaxOperationRejectionMessageBytes

// ControlErrorCode is the closed Agent-owned error vocabulary. Transport
// adapters must map every value explicitly and fail closed on unknown values.
type ControlErrorCode string

const (
	CodeInvalidArgument       ControlErrorCode = "invalid_argument"
	CodeContentRequired       ControlErrorCode = "content_required"
	CodeContentTooLarge       ControlErrorCode = "content_too_large"
	CodeArtifactInvalid       ControlErrorCode = "artifact_invalid"
	CodeArtifactTooLarge      ControlErrorCode = "artifact_too_large"
	CodeAmbiguousChannel      ControlErrorCode = "ambiguous_channel"
	CodeAmbiguousParticipant  ControlErrorCode = "ambiguous_participant"
	CodeUnknownAction         ControlErrorCode = "unknown_action"
	CodeAuthenticationFailed  ControlErrorCode = "authentication_failed"
	CodeContextRequired       ControlErrorCode = "context_required"
	CodeContextInvalid        ControlErrorCode = "context_invalid"
	CodeContextStale          ControlErrorCode = "context_stale"
	CodeAssetRevisionMismatch ControlErrorCode = "asset_revision_mismatch"
	CodeActionNotAllowed      ControlErrorCode = "action_not_allowed"
	CodeCurrentTooLarge       ControlErrorCode = "current_too_large"
	CodeOperationMismatch     ControlErrorCode = "operation_mismatch"
	CodeWorkConflict          ControlErrorCode = "work_conflict"
	CodeWorkExpired           ControlErrorCode = "work_expired"
	CodeProfileHostMismatch   ControlErrorCode = "profile_host_mismatch"
	CodeOperationPending      ControlErrorCode = "operation_pending"
	CodePeerUnavailable       ControlErrorCode = "peer_unavailable"
	CodeMnemondUnavailable    ControlErrorCode = "mnemond_unavailable"
	CodeInternal              ControlErrorCode = "internal"
)

var controlErrorCodes = [...]ControlErrorCode{
	CodeInvalidArgument, CodeContentRequired, CodeContentTooLarge,
	CodeArtifactInvalid, CodeArtifactTooLarge, CodeAmbiguousChannel,
	CodeAmbiguousParticipant, CodeUnknownAction, CodeAuthenticationFailed,
	CodeContextRequired, CodeContextInvalid, CodeContextStale,
	CodeAssetRevisionMismatch, CodeActionNotAllowed, CodeCurrentTooLarge,
	CodeOperationMismatch, CodeWorkConflict, CodeWorkExpired,
	CodeProfileHostMismatch, CodeOperationPending, CodePeerUnavailable,
	CodeMnemondUnavailable, CodeInternal,
}

func (code ControlErrorCode) Valid() bool {
	for _, candidate := range controlErrorCodes {
		if code == candidate {
			return true
		}
	}
	return false
}

func (code ControlErrorCode) Retryable() bool {
	return code == CodeOperationPending || code == CodePeerUnavailable ||
		code == CodeMnemondUnavailable
}

// ControlErrorCodes returns an immutable snapshot for exhaustive adapter
// parity tests; callers cannot mutate the canonical registry.
func ControlErrorCodes() []ControlErrorCode {
	return append([]ControlErrorCode(nil), controlErrorCodes[:]...)
}

type ControlError struct {
	Code        ControlErrorCode
	Retryable   bool
	Replayed    bool
	Message     string
	OperationID *string
}

func NewControlError(code ControlErrorCode, message string) *ControlError {
	message = strings.TrimSpace(message)
	if !code.Valid() || message == "" || len([]byte(message)) > MaxControlDiagnosticBytes {
		code = CodeInternal
		message = "internal control error"
	}
	return &ControlError{Code: code, Retryable: code.Retryable(), Message: message}
}

func (controlErr *ControlError) Error() string {
	if controlErr == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", controlErr.Code, controlErr.Message)
}

type ControlMetadata struct {
	Profile           model.Profile
	OperationKeyHash  model.Digest
	HasOperationKey   bool
	ClaimContextHash  model.Digest
	HasClaimContext   bool
	RunAttachmentHash model.Digest
	HasRunAttachment  bool
}

type HookCheckResponse struct {
	Pending bool
}

type AgentCurrentResponse struct {
	Status      string
	RunID       string
	ClaimSecret string
	Projection  json.RawMessage
}

type TeamworkActionRequest struct {
	Action    string   `json:"action"`
	Channel   string   `json:"channel,omitempty"`
	To        string   `json:"to,omitempty"`
	Deadline  string   `json:"deadline,omitempty"`
	Content   string   `json:"content,omitempty"`
	Artifacts []string `json:"artifacts,omitempty"`
}

type AgentResolveRequest struct {
	Decision string `json:"decision"`
	Content  string `json:"content,omitempty"`
}

type OperationResult struct {
	EventID   string      `json:"event_id"`
	EventType string      `json:"event_type"`
	Work      WorkReceipt `json:"work"`
}

type WorkReceipt struct {
	Ref     string `json:"ref"`
	Version uint64 `json:"version"`
	State   string `json:"state"`
}

type HandlingReceipt struct {
	Status string `json:"status"`
}

type OperationResponse struct {
	SchemaVersion int               `json:"schema_version"`
	Status        string            `json:"status"`
	Action        string            `json:"action"`
	OperationID   string            `json:"operation_id"`
	Replayed      bool              `json:"replayed"`
	Handling      *HandlingReceipt  `json:"handling"`
	Results       []OperationResult `json:"results"`
	Receipt       string            `json:"receipt"`
}

// ControlService is the Agent-owned local control surface. Empty HTTP request
// objects are transport details and therefore do not enter HookCheck or
// AgentCurrent.
type ControlService interface {
	HookCheck(context.Context, ControlMetadata) (HookCheckResponse, *ControlError)
	AgentCurrent(context.Context, ControlMetadata) (AgentCurrentResponse, *ControlError)
	TeamworkAction(context.Context, ControlMetadata, TeamworkActionRequest) (OperationResponse, *ControlError)
	AgentResolve(context.Context, ControlMetadata, AgentResolveRequest) (OperationResponse, *ControlError)
}
