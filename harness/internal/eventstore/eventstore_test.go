package eventstore

import (
	"testing"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func TestAppendAndReadAcceptedEnvelopeByIndexes(t *testing.T) {
	es, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open eventstore: %v", err)
	}
	t.Cleanup(func() { _ = es.Close() })

	ev := eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            "event-1",
		Type:          "assignment.accepted",
		Subject:       eventmodel.Subject("assignment", "asg1"),
		Actor:         "codex-a@project",
		Audience:      "codex-b@project",
		Payload:       eventmodel.BuildPayload(map[string]any{"scope": "review implementation"}, nil, nil),
		CorrelationID: "corr-1",
		CreatedAt:     "2026-06-24T00:00:00Z",
	}
	env := eventmodel.AcceptedEnvelope(ev, "dec-1", 7, "2026-06-24T00:01:00Z", "mnemond-a")

	seq, err := es.Append(env)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if seq != 1 {
		t.Fatalf("first envelope seq = %d, want 1", seq)
	}

	queries := []Query{
		{Phase: eventmodel.PhaseAccepted},
		{Type: "assignment.accepted"},
		{Subject: eventmodel.Subject("assignment", "asg1")},
		{Actor: "codex-a@project"},
		{Audience: "codex-b@project"},
		{CorrelationID: "corr-1"},
	}
	for _, q := range queries {
		got, err := es.Read(q)
		if err != nil {
			t.Fatalf("read %+v: %v", q, err)
		}
		if len(got) != 1 {
			t.Fatalf("read %+v returned %d records, want 1", q, len(got))
		}
		if got[0].Seq != seq || got[0].Envelope.Event.ID != "event-1" {
			t.Fatalf("read %+v returned wrong record: %+v", q, got[0])
		}
	}
}

func TestAppendRejectsInvalidEnvelope(t *testing.T) {
	es, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open eventstore: %v", err)
	}
	t.Cleanup(func() { _ = es.Close() })

	ev := eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            "event-1",
		Type:          "assignment.accepted",
		Subject:       eventmodel.Subject("assignment", "asg1"),
		Actor:         "mnemond-a",
		Payload:       map[string]any{"decision_id": "leaked-meta"},
		CreatedAt:     "2026-06-24T00:00:00Z",
	}
	if _, err := es.Append(eventmodel.AcceptedEnvelope(ev, "dec-1", 1, "2026-06-24T00:01:00Z", "mnemond-a")); err == nil {
		t.Fatal("append must reject envelope whose event payload carries phase meta")
	}
	got, err := es.Read(Query{})
	if err != nil {
		t.Fatalf("read after rejected append: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("invalid envelope must not be stored, got %d records", len(got))
	}
}

func TestReadHonorsCursorLimitAndPhase(t *testing.T) {
	es, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open eventstore: %v", err)
	}
	t.Cleanup(func() { _ = es.Close() })

	for i, typ := range []string{"assignment.accepted", "assignment.work_available", "assignment.progress_ready"} {
		ev := eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            typ,
			Type:          typ,
			Subject:       eventmodel.Subject("assignment", "asg1"),
			Actor:         "mnemond-a",
			Payload:       eventmodel.BuildPayload(map[string]any{"n": i}, nil, nil),
			CreatedAt:     "2026-06-24T00:00:00Z",
		}
		var env eventmodel.EventEnvelope
		if i == 0 {
			env = eventmodel.AcceptedEnvelope(ev, "dec-1", 1, "2026-06-24T00:01:00Z", "mnemond-a")
		} else {
			env = eventmodel.DerivedEnvelope(ev, "2026-06-24T00:02:00Z", "2026-06-24T00:22:00Z", "work", []string{"progress_digest.write_candidate.observed"})
		}
		if _, err := es.Append(env); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	got, err := es.Read(Query{AfterSeq: 1, Phase: eventmodel.PhaseDerived, Limit: 1})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read after cursor with limit returned %d records, want 1", len(got))
	}
	if got[0].Seq != 2 || got[0].Envelope.Phase != eventmodel.PhaseDerived {
		t.Fatalf("read returned wrong envelope: %+v", got[0])
	}
}
