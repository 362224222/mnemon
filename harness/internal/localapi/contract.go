package localapi

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	SchemaVersion       = 1
	MaxRequestBodyBytes = 64 << 10
	MaxDiagnosticBytes  = 512
)

type ErrorCode string

const (
	CodeInvalidArgument       ErrorCode = "invalid_argument"
	CodeContentRequired       ErrorCode = "content_required"
	CodeContentTooLarge       ErrorCode = "content_too_large"
	CodeArtifactInvalid       ErrorCode = "artifact_invalid"
	CodeArtifactTooLarge      ErrorCode = "artifact_too_large"
	CodeAmbiguousChannel      ErrorCode = "ambiguous_channel"
	CodeAmbiguousParticipant  ErrorCode = "ambiguous_participant"
	CodeUnknownAction         ErrorCode = "unknown_action"
	CodeAuthenticationFailed  ErrorCode = "authentication_failed"
	CodeContextRequired       ErrorCode = "context_required"
	CodeContextInvalid        ErrorCode = "context_invalid"
	CodeContextStale          ErrorCode = "context_stale"
	CodeAssetRevisionMismatch ErrorCode = "asset_revision_mismatch"
	CodeActionNotAllowed      ErrorCode = "action_not_allowed"
	CodeCurrentTooLarge       ErrorCode = "current_too_large"
	CodeOperationMismatch     ErrorCode = "operation_mismatch"
	CodeWorkConflict          ErrorCode = "work_conflict"
	CodeWorkExpired           ErrorCode = "work_expired"
	CodeProfileHostMismatch   ErrorCode = "profile_host_mismatch"
	CodeOperationPending      ErrorCode = "operation_pending"
	CodePeerUnavailable       ErrorCode = "peer_unavailable"
	CodeMnemondUnavailable    ErrorCode = "mnemond_unavailable"
	CodeInternal              ErrorCode = "internal"
)

func (c ErrorCode) Valid() bool {
	switch c {
	case CodeInvalidArgument, CodeContentRequired, CodeContentTooLarge,
		CodeArtifactInvalid, CodeArtifactTooLarge, CodeAmbiguousChannel,
		CodeAmbiguousParticipant, CodeUnknownAction, CodeAuthenticationFailed,
		CodeContextRequired, CodeContextInvalid, CodeContextStale,
		CodeAssetRevisionMismatch, CodeActionNotAllowed, CodeCurrentTooLarge,
		CodeOperationMismatch, CodeWorkConflict, CodeWorkExpired,
		CodeProfileHostMismatch, CodeOperationPending, CodePeerUnavailable,
		CodeMnemondUnavailable, CodeInternal:
		return true
	default:
		return false
	}
}

func (c ErrorCode) ExitStatus() int {
	switch c {
	case CodeInvalidArgument, CodeContentRequired, CodeContentTooLarge,
		CodeArtifactInvalid, CodeArtifactTooLarge, CodeAmbiguousChannel,
		CodeAmbiguousParticipant, CodeUnknownAction:
		return 2
	case CodeAuthenticationFailed, CodeContextRequired, CodeContextInvalid,
		CodeContextStale, CodeAssetRevisionMismatch:
		return 3
	case CodeActionNotAllowed, CodeCurrentTooLarge, CodeOperationMismatch,
		CodeWorkConflict, CodeWorkExpired, CodeProfileHostMismatch:
		return 4
	case CodeOperationPending, CodePeerUnavailable, CodeMnemondUnavailable:
		return 5
	default:
		return 1
	}
}

func (c ErrorCode) Retryable() bool {
	return c == CodeOperationPending || c == CodePeerUnavailable || c == CodeMnemondUnavailable
}

type APIError struct {
	SchemaVersion int       `json:"schema_version"`
	Status        string    `json:"status"`
	Code          ErrorCode `json:"code"`
	Retryable     bool      `json:"retryable"`
	Replayed      bool      `json:"replayed"`
	Message       string    `json:"message"`
	OperationID   *string   `json:"operation_id"`
}

func NewAPIError(code ErrorCode, message string) *APIError {
	message = strings.TrimSpace(message)
	if !code.Valid() || message == "" || len([]byte(message)) > MaxDiagnosticBytes {
		code = CodeInternal
		message = "internal control error"
	}
	return &APIError{SchemaVersion: SchemaVersion, Status: "error", Code: code,
		Retryable: code.Retryable(), Message: message}
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *APIError) ExitStatus() int {
	if e == nil {
		return 0
	}
	return e.Code.ExitStatus()
}

type HookCheckRequest struct{}

type HookCheckResponse struct {
	SchemaVersion int  `json:"schema_version"`
	Pending       bool `json:"pending"`
}

type AgentCurrentRequest struct{}

// AgentCurrentResponse is the private server-to-client envelope. ClaimSecret
// and RunID are consumed by mnemon-harness and must never be copied into the
// Agent-visible projection.
type AgentCurrentResponse struct {
	SchemaVersion int             `json:"schema_version"`
	Status        string          `json:"status"`
	RunID         string          `json:"run_id,omitempty"`
	ClaimSecret   string          `json:"claim_secret,omitempty"`
	Projection    json.RawMessage `json:"projection,omitempty"`
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
