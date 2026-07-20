package peer

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestArtifactProtocolErrorIsClosedAndHasStableRetryPolicy(t *testing.T) {
	t.Parallel()

	for _, code := range []ArtifactProtocolErrorCode{ArtifactErrorNotAuthorized, ArtifactErrorCorrupt} {
		payload, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: code})
		if err != nil || payload.Code() != code || payload.Retryable() || payload.RetryAfter() != 0 {
			t.Fatalf("NewArtifactProtocolError(%s) = (%#v, %v)", code, payload, err)
		}
	}
	busy, err := NewArtifactProtocolError(ArtifactProtocolErrorSpec{Code: ArtifactErrorBusy,
		Retryable: true, RetryAfter: HermeticLimits().ArtifactRequestTimeout})
	if err != nil || !busy.Retryable() {
		t.Fatalf("bounded busy ProtocolError = (%#v, %v)", busy, err)
	}

	invalid := []ArtifactProtocolErrorSpec{
		{},
		{Code: "future"},
		{Code: "not_found"},
		{Code: "corrupt_source"},
		{Code: ArtifactErrorBusy},
		{Code: ArtifactErrorBusy, Retryable: true},
		{Code: ArtifactErrorBusy, Retryable: true, RetryAfter: time.Microsecond},
		{Code: ArtifactErrorBusy, Retryable: true,
			RetryAfter: HermeticLimits().ArtifactRequestTimeout + time.Millisecond},
		{Code: ArtifactErrorNotAuthorized, Retryable: true, RetryAfter: time.Millisecond},
		{Code: ArtifactErrorNotAuthorized, RetryAfter: time.Millisecond},
	}
	for index, spec := range invalid {
		if _, err := NewArtifactProtocolError(spec); !errors.Is(err, ErrArtifactFrame) {
			t.Errorf("invalid ProtocolError %d error = %v", index, err)
		}
	}

	for _, code := range []string{"future", "not_found", "corrupt_source"} {
		openError := []byte(fmt.Sprintf(
			`{"payload":{"code":"%s","retry_after_ms":0,"retryable":false},"type":"protocol_error","version":1}`,
			code))
		if _, err := ParseArtifactFrame(openError); !errors.Is(err, ErrArtifactFrame) {
			t.Fatalf("open ProtocolError code %q error = %v", code, err)
		}
	}
}
