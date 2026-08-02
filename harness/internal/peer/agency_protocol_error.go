package peer

import (
	"bytes"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const agencyProtocolErrorBytes = 1 << 10

var ErrAgencyFrame = errors.New("invalid Mnemon Agency frame")

type AgencyProtocolErrorCode string

const (
	AgencyErrorBusy          AgencyProtocolErrorCode = "busy"
	AgencyErrorUnavailable   AgencyProtocolErrorCode = "unavailable"
	AgencyErrorNotAuthorized AgencyProtocolErrorCode = "not_authorized"
	AgencyErrorInvalid       AgencyProtocolErrorCode = "invalid_request"
	AgencyErrorConflict      AgencyProtocolErrorCode = "conflict"
	AgencyErrorNotFound      AgencyProtocolErrorCode = "not_found"
)

func (code AgencyProtocolErrorCode) Valid() bool {
	switch code {
	case AgencyErrorBusy, AgencyErrorUnavailable, AgencyErrorNotAuthorized,
		AgencyErrorInvalid, AgencyErrorConflict, AgencyErrorNotFound:
		return true
	default:
		return false
	}
}

func (code AgencyProtocolErrorCode) retryable() bool {
	return code == AgencyErrorBusy || code == AgencyErrorUnavailable
}

type AgencyProtocolErrorSpec struct {
	Code       AgencyProtocolErrorCode
	RetryAfter time.Duration
}

// AgencyProtocolError is a closed transport failure with no remote text. It
// never represents an admission decision; a semantic rejection travels only
// in a signed admission Receipt.
type AgencyProtocolError struct {
	code       AgencyProtocolErrorCode
	retryAfter time.Duration
	canonical  model.JSON
}

type agencyProtocolErrorWire struct {
	Code         AgencyProtocolErrorCode `json:"code"`
	RetryAfterMS int64                   `json:"retry_after_ms"`
}

func NewAgencyProtocolError(spec AgencyProtocolErrorSpec) (AgencyProtocolError, error) {
	retryable := spec.Code.retryable()
	if !spec.Code.Valid() || spec.RetryAfter < 0 ||
		spec.RetryAfter%time.Millisecond != 0 || spec.RetryAfter > time.Minute ||
		(retryable && spec.RetryAfter == 0) || (!retryable && spec.RetryAfter != 0) {
		return AgencyProtocolError{}, agencyFrameError("invalid protocol failure", nil)
	}
	canonical, err := canonicalAgencyJSON(agencyProtocolErrorWire{Code: spec.Code,
		RetryAfterMS: spec.RetryAfter.Milliseconds()}, agencyProtocolErrorBytes)
	if err != nil {
		return AgencyProtocolError{}, err
	}
	return AgencyProtocolError{code: spec.Code, retryAfter: spec.RetryAfter,
		canonical: canonical}, nil
}

func parseAgencyProtocolError(raw []byte) (AgencyProtocolError, error) {
	var wire agencyProtocolErrorWire
	if err := decodeExactAgencyJSON(raw, agencyProtocolErrorBytes, &wire); err != nil {
		return AgencyProtocolError{}, err
	}
	payload, err := NewAgencyProtocolError(AgencyProtocolErrorSpec{Code: wire.Code,
		RetryAfter: time.Duration(wire.RetryAfterMS) * time.Millisecond})
	if err != nil {
		return AgencyProtocolError{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return AgencyProtocolError{}, agencyFrameError("protocol failure is not canonical", nil)
	}
	return payload, nil
}

func (failure AgencyProtocolError) Code() AgencyProtocolErrorCode { return failure.code }
func (failure AgencyProtocolError) Retryable() bool               { return failure.code.retryable() }
func (failure AgencyProtocolError) RetryAfter() time.Duration     { return failure.retryAfter }
func (failure AgencyProtocolError) CanonicalJSON() model.JSON     { return failure.canonical }
func (failure AgencyProtocolError) IsZero() bool {
	return !failure.code.Valid() || failure.canonical.IsZero()
}
func (AgencyProtocolError) agencyDeliveryReply() {}
func (AgencyProtocolError) agencyObjectReply()   {}
