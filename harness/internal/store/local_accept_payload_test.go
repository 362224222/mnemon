package store

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestDecodeClosedEventPayloadUsesCanonicalWireDeadline(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	for _, deadline := range []time.Time{
		now.Add(24 * time.Hour),
		now.Add(24*time.Hour + 123450000*time.Nanosecond),
	} {
		wire := deadline.UTC().Format(time.RFC3339Nano)
		event := newCodecEvent(t, model.EventReviewOffered,
			`{"content":"review","deadline":"`+wire+`","iteration":1,"work_version":1}`, now)
		facts, err := decodeClosedEventPayload(event)
		if err != nil || facts.DeadlineUnixNano != deadline.UnixNano() || facts.WorkVersion != 1 {
			t.Fatalf("deadline %s facts = (%#v, %v)", wire, facts, err)
		}
	}

	nonCanonical := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"review","deadline":"2026-07-17T13:00:00.000000000Z","iteration":1,"work_version":1}`, now)
	if _, err := decodeClosedEventPayload(nonCanonical); err == nil {
		t.Fatal("fixed-width wire deadline was accepted")
	}
	unknown := newCodecEvent(t, model.EventReviewOffered,
		`{"content":"review","deadline":"2026-07-17T13:00:00Z","iteration":1,"unknown":true,"work_version":1}`, now)
	if _, err := decodeClosedEventPayload(unknown); err == nil {
		t.Fatal("unknown payload field was accepted")
	}
}

func TestDecodeClosedEventPayloadRejectsWrongShapeAndContent(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	tests := []struct {
		typeValue model.EventType
		payload   string
	}{
		{model.EventReviewOffered, `{"content":"","deadline":"2026-07-17T13:00:00Z","iteration":1,"work_version":1}`},
		{model.EventReviewDeliveryReady, `{"content":"","iteration":1,"work_version":2}`},
		{model.EventReviewAccepted, `{"iteration":1,"work_version":0}`},
		{model.EventReviewOutcome, `{"decision_ref":"d","diagnostic_code":"x","iteration":1,"status":"other","work_version":1}`},
	}
	for _, test := range tests {
		event := newCodecEvent(t, test.typeValue, test.payload, now)
		if _, err := decodeClosedEventPayload(event); err == nil {
			t.Errorf("decodeClosedEventPayload(%s) accepted", test.typeValue)
		}
	}
}

func TestDecodeClosedEventPayloadPreservesOutcomeDecisionRef(t *testing.T) {
	t.Parallel()
	event := newCodecEvent(t, model.EventReviewOutcome,
		`{"decision_ref":"event-source","diagnostic_code":"conflict","iteration":1,"status":"rejected","work_version":1}`,
		time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC))
	facts, err := decodeClosedEventPayload(event)
	if err != nil {
		t.Fatal(err)
	}
	if facts.DecisionRef != "event-source" {
		t.Fatalf("decision ref = %q, want event-source", facts.DecisionRef)
	}
}
