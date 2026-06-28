package contract_test

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func TestObservationEnvelopeAdaptsToObservedEventEnvelope(t *testing.T) {
	env := contract.ObservationEnvelope{
		Source:     "codex-a@project",
		ExternalID: "observe-1",
		Event: contract.Event{
			SchemaVersion: 1,
			ID:            "ev-1",
			TS:            "2026-06-24T00:00:00Z",
			Type:          "assignment.write_candidate.observed",
			Actor:         "untrusted@payload",
			ResourceRefs:  []contract.ResourceRef{{Kind: "assignment", ID: "asg1"}},
			CorrelationID: "corr-1",
			CausedBy:      "teamwork_signal/sig1",
			Payload: eventmodel.BuildPayload(map[string]any{
				"assignment_id": "asg1",
				"ttl":           "20m",
				"scope":         "review implementation",
			}, map[string]any{"expected_work": "review implementation"}, nil),
		},
	}

	out, err := contract.EventEnvelopeFromObservation(env, "codex", "nudge")
	if err != nil {
		t.Fatalf("EventEnvelopeFromObservation() error = %v", err)
	}
	if out.Phase != eventmodel.PhaseObserved {
		t.Fatalf("phase = %q, want observed", out.Phase)
	}
	if out.Event.Actor != string(env.Source) {
		t.Fatalf("actor must be adapted from ObservationEnvelope.Source, got %q", out.Event.Actor)
	}
	if out.Event.Subject != eventmodel.Subject("assignment", "asg1") {
		t.Fatalf("subject = %q", out.Event.Subject)
	}
	if out.Event.TTL != "20m" {
		t.Fatalf("ttl = %q", out.Event.TTL)
	}
	if out.Meta["external_id"] != "observe-1" || out.Meta["host"] != "codex" || out.Meta["lifecycle"] != "nudge" {
		t.Fatalf("observed meta mismatch: %+v", out.Meta)
	}
}

func TestEventEnvelopeRejectsPhaseMetaMismatchFromContractTest(t *testing.T) {
	ev := eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            "event-1",
		Type:          "assignment.accepted",
		Subject:       eventmodel.Subject("assignment", "asg1"),
		Actor:         "mnemond-a",
		Payload:       eventmodel.BuildPayload(nil, map[string]any{"summary": "accepted work"}, nil),
		CreatedAt:     "2026-06-24T00:00:00Z",
	}
	env := eventmodel.NewEnvelope(eventmodel.PhaseSynced, ev, map[string]any{
		"decision_id": "dec-1",
		"ingest_seq":  int64(1),
		"accepted_at": "2026-06-24T00:01:00Z",
		"accepted_by": "mnemond-a",
	})

	err := env.Validate()
	if err == nil || !strings.Contains(err.Error(), `unknown key`) {
		t.Fatalf("Validate() error = %v, want synced envelope to reject accepted meta", err)
	}
}
