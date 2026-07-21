package localapi

import (
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelValueValidationAcceptsClosedPublicValues(t *testing.T) {
	if !validChannelInboxOutcome("accepted") || validChannelInboxOutcome("delivered") {
		t.Fatal("Channel inbox outcome validation is not closed")
	}
	if !validChannelLabel("Release 1") || validChannelLabel(" Release 1") ||
		validChannelLabel(strings.Repeat("x", model.MaxLabelBytes+1)) ||
		validChannelLabel("bad\nlabel") {
		t.Fatal("Channel label validation accepted an unsafe label")
	}
}

func TestValidateChannelInviteViewRequiresCanonicalExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 7, 18, 12, 0, 0, 123, time.UTC).Format(time.RFC3339Nano)
	valid := ChannelInviteView{ExpiresAt: expiresAt, RemainingUses: 1, Status: "open"}
	if apiErr := validateChannelInviteView(valid); apiErr != nil {
		t.Fatalf("valid invite rejected: %#v", apiErr)
	}
	valid.ExpiresAt = "2026-07-18T12:00:00+00:00"
	if apiErr := validateChannelInviteView(valid); apiErr == nil {
		t.Fatal("noncanonical invite expiry accepted")
	}
}
