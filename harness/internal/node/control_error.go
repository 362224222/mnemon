package node

import (
	"context"
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	SchemaVersion       = 1
	MaxRequestBodyBytes = 64 << 10
	MaxDiagnosticBytes  = 512
)

type ErrorCode string

const (
	CodeInvalidArgument        ErrorCode = "invalid_argument"
	CodeContentRequired        ErrorCode = "content_required"
	CodeContentTooLarge        ErrorCode = "content_too_large"
	CodeArtifactInvalid        ErrorCode = "artifact_invalid"
	CodeArtifactTooLarge       ErrorCode = "artifact_too_large"
	CodeAmbiguousChannel       ErrorCode = "ambiguous_channel"
	CodeAmbiguousParticipant   ErrorCode = "ambiguous_participant"
	CodeUnknownAction          ErrorCode = "unknown_action"
	CodeAuthenticationFailed   ErrorCode = "authentication_failed"
	CodeContextRequired        ErrorCode = "context_required"
	CodeContextInvalid         ErrorCode = "context_invalid"
	CodeContextStale           ErrorCode = "context_stale"
	CodeAssetRevisionMismatch  ErrorCode = "asset_revision_mismatch"
	CodeActionNotAllowed       ErrorCode = "action_not_allowed"
	CodeCurrentTooLarge        ErrorCode = "current_too_large"
	CodeOperationMismatch      ErrorCode = "operation_mismatch"
	CodeWorkConflict           ErrorCode = "work_conflict"
	CodeWorkExpired            ErrorCode = "work_expired"
	CodeProfileHostMismatch    ErrorCode = "profile_host_mismatch"
	CodeHostActivationRequired ErrorCode = "host_activation_required"
	CodeOperationPending       ErrorCode = "operation_pending"
	CodePeerUnavailable        ErrorCode = "peer_unavailable"
	CodeMnemondUnavailable     ErrorCode = "mnemond_unavailable"
	CodeOwnerUnreachable       ErrorCode = "owner_unreachable"
	CodeBusy                   ErrorCode = "busy"
	CodeInvalidToken           ErrorCode = "invalid_token"
	CodeWrongOwner             ErrorCode = "wrong_owner"
	CodeTokenExpired           ErrorCode = "token_expired"
	CodeTokenClosed            ErrorCode = "token_closed"
	CodeTokenExhausted         ErrorCode = "token_exhausted"
	CodeChannelFull            ErrorCode = "channel_full"
	CodeNodeChannelLimit       ErrorCode = "node_channel_limit"
	CodeBadProof               ErrorCode = "bad_proof"
	CodeIncompatibleProtocol   ErrorCode = "incompatible_protocol"
	CodeNotMember              ErrorCode = "not_member"
	CodeMemberRevoked          ErrorCode = "member_revoked"
	CodeChannelClosed          ErrorCode = "channel_closed"
	CodeBaselineConflict       ErrorCode = "baseline_conflict"
	CodeOriginEpochMismatch    ErrorCode = "origin_epoch_mismatch"
	CodeRosterGap              ErrorCode = "roster_gap"
	CodeRosterConflict         ErrorCode = "roster_conflict"
	CodeInternal               ErrorCode = "internal"
)

func (c ErrorCode) Valid() bool {
	switch c {
	case CodeInvalidArgument, CodeContentRequired, CodeContentTooLarge,
		CodeArtifactInvalid, CodeArtifactTooLarge, CodeAmbiguousChannel,
		CodeAmbiguousParticipant, CodeUnknownAction, CodeAuthenticationFailed,
		CodeContextRequired, CodeContextInvalid, CodeContextStale,
		CodeAssetRevisionMismatch, CodeActionNotAllowed, CodeCurrentTooLarge,
		CodeOperationMismatch, CodeWorkConflict, CodeWorkExpired,
		CodeProfileHostMismatch, CodeHostActivationRequired, CodeOperationPending, CodePeerUnavailable,
		CodeMnemondUnavailable, CodeOwnerUnreachable, CodeBusy, CodeInvalidToken,
		CodeWrongOwner, CodeTokenExpired, CodeTokenClosed, CodeTokenExhausted,
		CodeChannelFull, CodeNodeChannelLimit, CodeBadProof, CodeIncompatibleProtocol,
		CodeNotMember, CodeMemberRevoked, CodeChannelClosed, CodeBaselineConflict, CodeOriginEpochMismatch,
		CodeRosterGap, CodeRosterConflict, CodeInternal:
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
		CodeWorkConflict, CodeWorkExpired, CodeProfileHostMismatch, CodeHostActivationRequired,
		CodeInvalidToken, CodeWrongOwner, CodeTokenExpired, CodeTokenClosed,
		CodeTokenExhausted, CodeChannelFull, CodeNodeChannelLimit, CodeBadProof,
		CodeIncompatibleProtocol, CodeNotMember, CodeMemberRevoked, CodeChannelClosed,
		CodeBaselineConflict, CodeOriginEpochMismatch, CodeRosterConflict:
		return 4
	case CodeOperationPending, CodePeerUnavailable, CodeMnemondUnavailable,
		CodeOwnerUnreachable, CodeBusy, CodeRosterGap:
		return 5
	default:
		return 1
	}
}

func (c ErrorCode) Retryable() bool {
	return c == CodeOperationPending || c == CodePeerUnavailable || c == CodeMnemondUnavailable ||
		c == CodeOwnerUnreachable || c == CodeBusy || c == CodeRosterGap
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

type Authenticator interface {
	AuthenticateProfile(context.Context, model.Digest) (model.Profile, error)
}

type RequestMetadata struct {
	Profile           model.Profile
	OperationKeyHash  model.Digest
	HasOperationKey   bool
	ClaimContextHash  model.Digest
	HasClaimContext   bool
	RunAttachmentHash model.Digest
	HasRunAttachment  bool
}
