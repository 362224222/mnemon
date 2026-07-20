package peer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestChannelEnrollmentMapsOnlyTypedStableControllerFailures(t *testing.T) {
	tests := []struct {
		code       ChannelProtocolErrorCode
		retryAfter time.Duration
	}{
		{ChannelErrorBusy, channelEnrollmentBusyRetry},
		{ChannelErrorInvalidToken, 0},
		{ChannelErrorWrongOwner, 0},
		{ChannelErrorBadProof, 0},
		{ChannelErrorTokenExpired, 0},
		{ChannelErrorTokenClosed, 0},
		{ChannelErrorTokenExhausted, 0},
		{ChannelErrorChannelFull, 0},
		{ChannelErrorChannelClosed, 0},
		{ChannelErrorMemberRevoked, 0},
		{ChannelErrorRosterGap, channelEnrollmentGapRetry},
		{ChannelErrorRosterConflict, 0},
	}
	for _, test := range tests {
		failure, err := NewChannelEnrollmentControlFailure(test.code)
		if err != nil || failure.Code() != test.code ||
			!errors.Is(failure, ErrChannelEnrollmentControlFailure) {
			t.Fatalf("NewChannelEnrollmentControlFailure(%q) = (%#v,%v)",
				test.code, failure, err)
		}
		cause := fmt.Errorf("controller detail: %w", failure)
		code, retryAfter, ok := channelEnrollmentControllerFailure(cause)
		if !ok || code != test.code || retryAfter != test.retryAfter {
			t.Errorf("channelEnrollmentControllerFailure(%v) = (%q,%s,%t)",
				failure, code, retryAfter, ok)
		}
	}
	for _, code := range []ChannelProtocolErrorCode{"", ChannelErrorOwnerUnreachable,
		ChannelErrorNodeChannelLimit, ChannelErrorIncompatibleProtocol, ChannelErrorNotMember} {
		if failure, err := NewChannelEnrollmentControlFailure(code); failure != nil || err == nil {
			t.Errorf("invalid owner failure %q = (%#v,%v)", code, failure, err)
		}
	}
	if code, retryAfter, ok := channelEnrollmentControllerFailure(
		errors.New("sqlite path and secret detail")); ok || code != "" || retryAfter != 0 {
		t.Fatalf("untyped controller failure leaked to wire = (%q,%s,%t)",
			code, retryAfter, ok)
	}
	if code, retryAfter, ok := channelEnrollmentControllerFailure(
		context.DeadlineExceeded); ok || code != "" || retryAfter != 0 {
		t.Fatalf("ambiguous controller timeout leaked as a rejection = (%q,%s,%t)",
			code, retryAfter, ok)
	}
	secret := "sqlite path and secret detail"
	err := (&ChannelEnrollmentOwner{}).respondControllerFailure(nil, ChannelRequestID{},
		errors.New(secret))
	if !errors.Is(err, ErrChannelEnrollmentProtocol) || strings.Contains(err.Error(), secret) {
		t.Fatalf("untyped controller failure response = %q", err)
	}
}
