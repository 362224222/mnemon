package event

import (
	"strings"
	"testing"
)

func validEvent() Event {
	return Event{
		SchemaVersion: SchemaVersion,
		ID:            "event-1",
		Type:          "assignment.work_available",
		Subject:       Subject("assignment", "asg1"),
		Actor:         "mnemond@local",
		Audience:      "codex-b@project",
		Payload:       BuildPayload(nil, map[string]any{"summary": "review the implementation"}, nil),
		CorrelationID: "corr-1",
		CreatedAt:     "2026-06-24T00:00:00Z",
	}
}

func TestEnvelopeValidationRejectsPhaseMetaMismatch(t *testing.T) {
	ev := validEvent()
	tests := []struct {
		name string
		env  EventEnvelope
		want string
	}{
		{
			name: "observed missing required host",
			env: NewEnvelope(PhaseObserved, ev, map[string]any{
				"external_id": "edge-1",
				"lifecycle":   "nudge",
			}),
			want: `missing key "host"`,
		},
		{
			name: "accepted rejects observed meta key",
			env: NewEnvelope(PhaseAccepted, ev, map[string]any{
				"decision_id": "dec-1",
				"ingest_seq":  int64(1),
				"accepted_at": "2026-06-24T00:01:00Z",
				"accepted_by": "mnemond-a",
				"external_id": "edge-1",
			}),
			want: `unknown key "external_id"`,
		},
		{
			name: "derived rejects wrong meta type",
			env: NewEnvelope(PhaseDerived, ev, map[string]any{
				"derived_at":            "2026-06-24T00:02:00Z",
				"expires_at":            "2026-06-24T00:22:00Z",
				"presentation_hint":     "work",
				"suggested_event_types": []any{"progress_digest.write_candidate.observed", 7},
			}),
			want: `must be a string list`,
		},
		{
			name: "unknown phase",
			env: NewEnvelope(EventPhase("commit"), ev, map[string]any{
				"decision_id": "dec-1",
			}),
			want: `unknown event envelope phase "commit"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.env.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestEventPayloadStaysPhaseAgnostic(t *testing.T) {
	ev := validEvent()
	envelopes := []EventEnvelope{
		ObservedEnvelope(ev, "edge-1", "codex", "nudge"),
		AcceptedEnvelope(ev, "dec-1", 1, "2026-06-24T00:01:00Z", "mnemond-a"),
		SyncedEnvelope(ev, "mnemond-a", "cursor-1", "sha256:abc", "2026-06-24T00:02:00Z"),
		DerivedEnvelope(ev, "2026-06-24T00:03:00Z", "2026-06-24T00:23:00Z", "work", []string{"progress_digest.write_candidate.observed"}),
	}
	for _, env := range envelopes {
		if err := env.Validate(); err != nil {
			t.Fatalf("%s envelope should validate: %v", env.Phase, err)
		}
		if _, ok := env.Event.Payload["external_id"]; ok {
			t.Fatalf("%s envelope leaked observed meta into payload", env.Phase)
		}
		if _, ok := env.Event.Payload["decision_id"]; ok {
			t.Fatalf("%s envelope leaked accepted meta into payload", env.Phase)
		}
	}

	leaky := validEvent()
	leaky.Payload[PayloadNarrativeKey].(map[string]any)["external_id"] = "edge-1"
	if err := ObservedEnvelope(leaky, "edge-1", "codex", "nudge").Validate(); err == nil {
		t.Fatal("Validate() must reject phase meta copied into event payload")
	}
}

func TestEventPayloadRequiresR2Sections(t *testing.T) {
	flat := validEvent()
	flat.Payload = map[string]any{"summary": "flat payload is no longer valid"}
	if err := ObservedEnvelope(flat, "edge-1", "codex", "nudge").Validate(); err == nil || !strings.Contains(err.Error(), "outside rule/narrative/refs") {
		t.Fatalf("flat payload must be rejected, got %v", err)
	}

	wrongSection := validEvent()
	wrongSection.Payload = map[string]any{PayloadRuleKey: "not an object"}
	if err := ObservedEnvelope(wrongSection, "edge-1", "codex", "nudge").Validate(); err == nil || !strings.Contains(err.Error(), "section \"rule\" must be an object") {
		t.Fatalf("non-object payload section must be rejected, got %v", err)
	}
}

func TestEventSchemaVersionIsPinnedToR2(t *testing.T) {
	ev := validEvent()
	ev.SchemaVersion = 1
	if err := ObservedEnvelope(ev, "edge-1", "codex", "nudge").Validate(); err == nil || !strings.Contains(err.Error(), "schema_version must be 2") {
		t.Fatalf("schema v1 must be rejected, got %v", err)
	}
}
