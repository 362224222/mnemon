package runtime

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

func TestIngestObservedEventEnvelopeUsesGovernedObservationPath(t *testing.T) {
	_, _, cs := newServerWith(t, rule.NewRuleSet(proposeRule()))
	env := eventmodel.ObservedEnvelope(eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            "client-id-must-not-survive",
		Type:          "memory.observed",
		Subject:       eventmodel.Subject("memory", "m1"),
		Actor:         "attacker@payload",
		Payload:       map[string]any{"content": "hostagent event"},
		CorrelationID: "corr-envelope",
		TTL:           "20m",
		CreatedAt:     "2026-06-24T00:00:00Z",
	}, "env-1", "codex", "nudge")

	seq, dup, err := cs.IngestObservedEnvelope("agent", env)
	if err != nil {
		t.Fatalf("ingest observed envelope: %v", err)
	}
	if dup || seq == 0 {
		t.Fatalf("new observed envelope must append once, got seq=%d dup=%v", seq, dup)
	}
	stored := mustEventAtSeq(t, cs, seq)
	if stored.Actor != "agent" {
		t.Fatalf("stored actor must be stamped from authenticated principal, got %q", stored.Actor)
	}
	if stored.ID == "client-id-must-not-survive" || stored.TS == "2026-06-24T00:00:00Z" {
		t.Fatalf("stored event id/ts must be server-stamped, got id=%q ts=%q", stored.ID, stored.TS)
	}
	if len(stored.ResourceRefs) != 1 || stored.ResourceRefs[0] != (contract.ResourceRef{Kind: "memory", ID: "m1"}) {
		t.Fatalf("observed envelope subject must bridge to resource ref, got %+v", stored.ResourceRefs)
	}
	if stored.Payload["ttl"] != "20m" {
		t.Fatalf("event ttl should bridge to legacy payload for existing validators, got %+v", stored.Payload)
	}

	ds, err := cs.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(ds) != 1 || ds[0].Status != contract.Accepted {
		t.Fatalf("observed envelope should flow through existing governance and accept once, got %+v", ds)
	}
}

func TestIngestObservedEventEnvelopeRejectsWrongPhase(t *testing.T) {
	_, _, cs := newServerWith(t, rule.NewRuleSet())
	ev := eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            "event-1",
		Type:          "memory.accepted",
		Subject:       eventmodel.Subject("memory", "m1"),
		Actor:         "agent",
		Payload:       map[string]any{"content": "not observed"},
		CreatedAt:     "2026-06-24T00:00:00Z",
	}
	_, _, err := cs.IngestObservedEnvelope("agent", eventmodel.AcceptedEnvelope(ev, "dec-1", 1, "2026-06-24T00:01:00Z", "mnemond-a"))
	if err == nil || !strings.Contains(err.Error(), "phase must be") {
		t.Fatalf("wrong phase error = %v", err)
	}
}
