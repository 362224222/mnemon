package model

import (
	"errors"
	"testing"
)

func assertCurrentReceiptUpdateEvidence(t testing.TB, receipt CurrentReadReceipt,
	event CurrentEvent,
) {
	t.Helper()
	if receipt.ActionWorkUpdatedBy() != event.Key().EventID() ||
		!receipt.ActionWorkUpdatedAt().Equal(event.AcceptedAt()) {
		t.Fatalf("receipt Work update evidence = (%s, %s)",
			receipt.ActionWorkUpdatedBy(), receipt.ActionWorkUpdatedAt())
	}
}

func TestParseCurrentReceiptAuthorityRequiresExactUpdaterAndTimes(t *testing.T) {
	valid := currentReceiptWire{ActionWorkUpdatedBy: "event-current-update",
		ActionWorkUpdatedAt: "2026-07-19T09:00:00Z", ReadAt: "2026-07-19T09:00:01Z"}
	if updatedBy, updatedAt, readAt, err := parseCurrentReceiptAuthority(valid); err != nil ||
		updatedBy.String() != valid.ActionWorkUpdatedBy || updatedAt.IsZero() || readAt.IsZero() {
		t.Fatalf("parseCurrentReceiptAuthority() = (%s, %s, %s, %v)",
			updatedBy, updatedAt, readAt, err)
	}
	for _, mutate := range []func(*currentReceiptWire){
		func(wire *currentReceiptWire) { wire.ActionWorkUpdatedBy = "" },
		func(wire *currentReceiptWire) { wire.ActionWorkUpdatedAt = "not-time" },
		func(wire *currentReceiptWire) { wire.ReadAt = "not-time" },
	} {
		wire := valid
		mutate(&wire)
		if _, _, _, err := parseCurrentReceiptAuthority(wire); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid receipt authority error = %v", err)
		}
	}
}
