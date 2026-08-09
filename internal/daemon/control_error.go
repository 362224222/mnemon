package daemon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/mnemon-dev/mnemon/internal/agency"
	"github.com/mnemon-dev/mnemon/internal/authority"
	"github.com/mnemon-dev/mnemon/internal/cas"
)

type controlErrorCode string

const (
	codeInvalidArgument      controlErrorCode = "invalid_argument"
	codeArtifactInvalid      controlErrorCode = "artifact_invalid"
	codeArtifactTooLarge     controlErrorCode = "artifact_too_large"
	codeAuthenticationFailed controlErrorCode = "authentication_failed"
	codeContextStale         controlErrorCode = "context_stale"
	codeActionNotAllowed     controlErrorCode = "action_not_allowed"
	codeOperationMismatch    controlErrorCode = "operation_mismatch"
	codeMnemondUnavailable   controlErrorCode = "mnemond_unavailable"
	codeInternal             controlErrorCode = "internal"
)

type controlError struct {
	Code          controlErrorCode `json:"code"`
	Message       string           `json:"message"`
	OperationID   *string          `json:"operation_id"`
	Replayed      bool             `json:"replayed"`
	Retryable     bool             `json:"retryable"`
	SchemaVersion int              `json:"schema_version"`
	Status        string           `json:"status"`
}

func newControlError(code controlErrorCode, message string) *controlError {
	message = strings.TrimSpace(message)
	if !code.valid() || message == "" || len(message) > maxControlDiagnostic {
		code, message = codeInternal, "internal control error"
	}
	return &controlError{Code: code, Message: message, Retryable: code == codeMnemondUnavailable,
		SchemaVersion: 1, Status: "error"}
}

func (code controlErrorCode) valid() bool {
	switch code {
	case codeInvalidArgument, codeArtifactInvalid, codeArtifactTooLarge,
		codeAuthenticationFailed, codeContextStale, codeActionNotAllowed,
		codeOperationMismatch, codeMnemondUnavailable, codeInternal:
		return true
	default:
		return false
	}
}

func (value *controlError) Error() string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", value.Code, value.Message)
}

func writeControlError(writer http.ResponseWriter, value *controlError) {
	if value == nil {
		value = newControlError(codeInternal, "internal control error")
	}
	writeControlJSON(writer, value.httpStatus(), value)
}

func (value *controlError) httpStatus() int {
	if value == nil {
		return http.StatusInternalServerError
	}
	switch value.Code {
	case codeAuthenticationFailed:
		return http.StatusUnauthorized
	case codeMnemondUnavailable:
		return http.StatusServiceUnavailable
	case codeContextStale, codeActionNotAllowed, codeOperationMismatch:
		return http.StatusConflict
	case codeInternal:
		return http.StatusInternalServerError
	default:
		return http.StatusBadRequest
	}
}

func classifyServiceError(err error) *controlError {
	switch {
	case errors.Is(err, authority.ErrAttachmentAuth), errors.Is(err, authority.ErrPrincipalUnavailable):
		return newControlError(codeAuthenticationFailed, "Agency attachment authentication failed")
	case errors.Is(err, authority.ErrAttachmentExpired), errors.Is(err, authority.ErrAttachmentEnded),
		errors.Is(err, authority.ErrCurrentUnavailable):
		return newControlError(codeContextStale, "Agency context is stale")
	case errors.Is(err, authority.ErrOperationConflict):
		return newControlError(codeOperationMismatch, "Agency operation conflicts with its prior request")
	case errors.Is(err, authority.ErrArtifactUnavailable), errors.Is(err, cas.ErrCorruption),
		errors.Is(err, os.ErrNotExist):
		return newControlError(codeArtifactInvalid, "Artifact is unavailable")
	case errors.Is(err, agency.ErrLimit), errors.Is(err, cas.ErrInput):
		return newControlError(codeInvalidArgument, "Agency input exceeds a closed bound")
	case errors.Is(err, agency.ErrInvalid):
		return newControlError(codeInvalidArgument, "Agency input is invalid")
	case errors.Is(err, agency.ErrInvariant):
		return newControlError(codeActionNotAllowed, "Intent is not allowed by the current View")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, authority.ErrClosed):
		return newControlError(codeMnemondUnavailable, "local Agency authority is unavailable")
	default:
		return newControlError(codeInternal, "internal Agency error")
	}
}
