package peer

import (
	"bytes"
	"time"
)

type ArtifactProtocolErrorCode string

const (
	ArtifactErrorBusy ArtifactProtocolErrorCode = "busy"
	// ArtifactErrorNotAuthorized intentionally coalesces unknown objects,
	// missing authority, and cross-Channel requests.
	ArtifactErrorNotAuthorized ArtifactProtocolErrorCode = "not_authorized"
	// ArtifactErrorCorrupt reveals no object identity or implementation detail;
	// it only closes an authorized transfer whose source bytes no longer match
	// their immutable content address.
	ArtifactErrorCorrupt ArtifactProtocolErrorCode = "corrupt"
)

func (code ArtifactProtocolErrorCode) Valid() bool {
	switch code {
	case ArtifactErrorBusy, ArtifactErrorNotAuthorized, ArtifactErrorCorrupt:
		return true
	default:
		return false
	}
}

func (code ArtifactProtocolErrorCode) retryable() bool { return code == ArtifactErrorBusy }

type ArtifactProtocolErrorSpec struct {
	Code       ArtifactProtocolErrorCode
	Retryable  bool
	RetryAfter time.Duration
}

type ArtifactProtocolError struct {
	code       ArtifactProtocolErrorCode
	retryable  bool
	retryAfter time.Duration
	canonical  ArtifactJSON
}

type artifactProtocolErrorWire struct {
	Code         ArtifactProtocolErrorCode `json:"code"`
	RetryAfterMS int64                     `json:"retry_after_ms"`
	Retryable    bool                      `json:"retryable"`
}

func NewArtifactProtocolError(spec ArtifactProtocolErrorSpec) (ArtifactProtocolError, error) {
	if !spec.Code.Valid() || spec.Retryable != spec.Code.retryable() ||
		spec.RetryAfter < 0 || spec.RetryAfter > HermeticLimits().ArtifactRequestTimeout ||
		spec.RetryAfter%time.Millisecond != 0 ||
		(spec.Retryable && spec.RetryAfter == 0) || (!spec.Retryable && spec.RetryAfter != 0) {
		return ArtifactProtocolError{}, artifactFrameError("ProtocolError code or retry policy is invalid", nil)
	}
	canonical, err := artifactJSONFrom(artifactProtocolErrorWire{Code: spec.Code,
		RetryAfterMS: spec.RetryAfter.Milliseconds(), Retryable: spec.Retryable},
		artifactSmallFrameBytes)
	if err != nil {
		return ArtifactProtocolError{}, artifactFrameError("encode ProtocolError", err)
	}
	return ArtifactProtocolError{code: spec.Code, retryable: spec.Retryable,
		retryAfter: spec.RetryAfter, canonical: canonical}, nil
}

func parseArtifactProtocolError(raw []byte) (ArtifactProtocolError, error) {
	var wire artifactProtocolErrorWire
	if err := decodeExactArtifactJSON(raw, &wire, artifactSmallFrameBytes); err != nil {
		return ArtifactProtocolError{}, err
	}
	if wire.RetryAfterMS < 0 || wire.RetryAfterMS > HermeticLimits().ArtifactRequestTimeout.Milliseconds() {
		return ArtifactProtocolError{}, artifactFrameError("ProtocolError retry_after is out of range", nil)
	}
	payload, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: wire.Code,
		Retryable: wire.Retryable, RetryAfter: time.Duration(wire.RetryAfterMS) * time.Millisecond})
	if err != nil {
		return ArtifactProtocolError{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return ArtifactProtocolError{}, artifactFrameError("ProtocolError bytes are not canonical", nil)
	}
	return payload, nil
}

func (payload ArtifactProtocolError) Code() ArtifactProtocolErrorCode { return payload.code }
func (payload ArtifactProtocolError) Retryable() bool                 { return payload.retryable }
func (payload ArtifactProtocolError) RetryAfter() time.Duration       { return payload.retryAfter }
func (payload ArtifactProtocolError) CanonicalJSON() ArtifactJSON     { return payload.canonical }
func (payload ArtifactProtocolError) IsZero() bool {
	return !payload.code.Valid() || payload.canonical.IsZero()
}
func (ArtifactProtocolError) artifactFrameType() ArtifactFrameType {
	return ArtifactFrameProtocolError
}
