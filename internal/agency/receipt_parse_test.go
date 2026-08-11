package agency

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestParseReceiptCanonicalJSONRoundTripsAcceptedAndRejected(t *testing.T) {
	request := mustBoundRoot(t, "operation:parse-receipt")
	event, err := NewEvent(request, EventStamp{
		ID: mustEventID(t, "event:parse-receipt"), AcceptedAt: testTime, OriginSequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := NewAcceptedReceipt(request, event, testTime.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := NewRejectedReceipt(request, mustLabel(t, "admission.rejected"),
		"Bounded diagnostic.", testTime.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, original := range []Receipt{accepted, rejected} {
		parsed, parseErr := ParseReceiptCanonicalJSON(original.CanonicalJSON())
		if parseErr != nil {
			t.Fatalf("ParseReceiptCanonicalJSON() error = %v", parseErr)
		}
		if !bytes.Equal(parsed.CanonicalJSON(), original.CanonicalJSON()) || parsed.Digest() != original.Digest() ||
			parsed.OperationKey() != original.OperationKey() || parsed.RequestDigest() != original.RequestDigest() ||
			parsed.Outcome() != original.Outcome() || !parsed.RecordedAt().Equal(original.RecordedAt()) {
			t.Fatalf("parsed Receipt differs\n got: %s\nwant: %s", parsed.CanonicalJSON(), original.CanonicalJSON())
		}
	}
}

func TestParseReceiptCanonicalJSONRejectsMalformedAndNoncanonicalData(t *testing.T) {
	request := mustBoundRoot(t, "operation:malformed-receipt")
	rejected, err := NewRejectedReceipt(request, mustLabel(t, "admission.rejected"), "No.", testTime)
	if err != nil {
		t.Fatal(err)
	}
	canonical := rejected.CanonicalJSON()
	unknown := bytes.Replace(canonical, []byte("{"), []byte(`{"unknown":true,`), 1)
	duplicate := bytes.Replace(canonical, []byte(`"outcome":"rejected"`),
		[]byte(`"outcome":"rejected","outcome":"accepted"`), 1)
	missingCode, _, err := canonicalJSON(receiptWire{
		SchemaVersion: 1, OperationKey: request.OperationKey().String(),
		RequestDigest: request.RequestDigest().String(), Outcome: "rejected",
		RecordedAt: testTime.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptedWithRejection, _, err := canonicalJSON(receiptWire{
		SchemaVersion: 1, OperationKey: request.OperationKey().String(),
		RequestDigest: request.RequestDigest().String(), Outcome: "accepted",
		RecordedAt: testTime.Format(time.RFC3339Nano),
		Event:      &eventRefWire{ID: "event:accepted", Digest: Sum([]byte("event")).String()},
		Code:       "forged.rejection",
	})
	if err != nil {
		t.Fatal(err)
	}
	nonUTCTime := bytes.Replace(canonical, []byte(testTime.Format(time.RFC3339Nano)),
		[]byte("2026-08-03T16:00:00+08:00"), 1)
	cases := map[string][]byte{
		"unknown field":             unknown,
		"duplicate field":           duplicate,
		"trailing value":            append(append([]byte(nil), canonical...), []byte(` {}`)...),
		"leading whitespace":        append([]byte(" \n"), canonical...),
		"rejection without code":    missingCode,
		"accepted rejection fields": acceptedWithRejection,
		"noncanonical timestamp":    nonUTCTime,
		"corrupted JSON":            append(append([]byte(nil), canonical[:len(canonical)-1]...), '!'),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, parseErr := ParseReceiptCanonicalJSON(data); parseErr == nil {
				t.Fatalf("ParseReceiptCanonicalJSON(%s) unexpectedly succeeded", data)
			}
		})
	}
}

func TestProjectAgentReceiptRejectsMalformedPrivateValue(t *testing.T) {
	if _, err := ProjectAgentReceipt(Receipt{}, false); !errors.Is(err, ErrInvariant) {
		t.Fatalf("ProjectAgentReceipt(zero) error = %v, want ErrInvariant", err)
	}
}

func FuzzParseReceiptCanonicalJSON(f *testing.F) {
	f.Add([]byte(`{"schema_version":1,"operation_key":"operation:fuzz","request_digest":"sha256:5e4d4c6f1f1fcca50ac6aa9b04b5ce3a95f05d059e09c3dfc5478f24fb16b69e","outcome":"rejected","recorded_at":"2026-08-03T08:00:00Z","code":"fuzz.rejected","diagnostic":"bounded"}`))
	f.Add([]byte(`{"outcome":"accepted","outcome":"rejected"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		receipt, err := ParseReceiptCanonicalJSON(data)
		if err != nil {
			return
		}
		if !bytes.Equal(receipt.CanonicalJSON(), data) || receipt.Digest() != Sum(data) {
			t.Fatalf("successful parse was not exact canonical input: %s", data)
		}
		if _, err := ParseReceiptCanonicalJSON(receipt.CanonicalJSON()); err != nil {
			t.Fatalf("canonical reparse failed: %v", err)
		}
	})
}
