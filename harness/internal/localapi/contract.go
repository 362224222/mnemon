package localapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

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

type InitiationParticipant struct {
	EffectiveAlias string `json:"effective_alias"`
	Eligible       bool   `json:"eligible"`
	Reachable      bool   `json:"reachable"`
}

type InitiationChannel struct {
	AllowTeam    bool                    `json:"allow_team"`
	LocalAlias   string                  `json:"local_alias"`
	Participants []InitiationParticipant `json:"participants"`
}

type InitiationProjection struct {
	InitiationContext struct {
		Channels []InitiationChannel `json:"channels"`
	} `json:"initiation_context"`
	SchemaVersion int `json:"schema_version"`
}

func ParseInitiationProjection(raw []byte) (InitiationProjection, error) {
	var projection InitiationProjection
	canonical, err := model.CanonicalizeJSON(raw)
	if err != nil || !bytes.Equal(canonical, raw) {
		return projection, errors.New("initiation projection is not canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&projection); err != nil {
		return InitiationProjection{}, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return InitiationProjection{}, errors.New("initiation projection has trailing input")
	}
	closed, err := model.CanonicalMarshal(projection)
	if err != nil || !bytes.Equal(closed, raw) || projection.SchemaVersion != SchemaVersion ||
		projection.InitiationContext.Channels == nil ||
		len(projection.InitiationContext.Channels) > model.MaxChannelsPerNode {
		return InitiationProjection{}, errors.New("initiation projection has an invalid schema")
	}
	previousChannel := ""
	for _, channel := range projection.InitiationContext.Channels {
		if !validInitiationAlias(channel.LocalAlias) ||
			(previousChannel != "" && previousChannel >= channel.LocalAlias) ||
			channel.Participants == nil || len(channel.Participants) > model.MaxChildWorks {
			return InitiationProjection{}, errors.New("initiation Channel projection is invalid")
		}
		previousChannel = channel.LocalAlias
		previousParticipant := ""
		eligible := false
		for _, participant := range channel.Participants {
			if !validInitiationAlias(participant.EffectiveAlias) ||
				participant.EffectiveAlias == "auto" || participant.EffectiveAlias == "team" ||
				(previousParticipant != "" && previousParticipant >= participant.EffectiveAlias) {
				return InitiationProjection{}, errors.New("initiation participant projection is invalid")
			}
			previousParticipant = participant.EffectiveAlias
			eligible = eligible || participant.Eligible
		}
		if channel.AllowTeam != eligible {
			return InitiationProjection{}, errors.New("initiation team projection is invalid")
		}
	}
	return projection, nil
}

func validInitiationAlias(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxLabelBytes ||
		strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
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
