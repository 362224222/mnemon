package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// S1: exactly-once ingest. A retried (Source,ExternalID) returns the same seq and never double-applies.
func TestIngestObservationDedupes(t *testing.T) {
	s := newTestStore(t)
	env := contract.ObservationEnvelope{
		Source:     "agent",
		ExternalID: "ext-1",
		Event:      contract.Event{Type: "memory.observed", Actor: "agent"},
	}
	seq1, dup1, err := s.IngestObservation(env)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if dup1 || seq1 == 0 {
		t.Fatalf("first ingest must be (seq>0, dup=false); got (%d,%v)", seq1, dup1)
	}
	// retry SAME key -> same seq, dup=true, no new append.
	seq2, dup2, err := s.IngestObservation(env)
	if err != nil {
		t.Fatalf("retry ingest: %v", err)
	}
	if !dup2 || seq2 != seq1 {
		t.Fatalf("retry must return (sameSeq, dup=true); got (%d,%v) vs first %d", seq2, dup2, seq1)
	}
	if evs, _ := s.PendingEvents(0); len(evs) != 1 {
		t.Fatalf("exactly one event must be appended across retries, got %d", len(evs))
	}
	// distinct external_id (same source) appends a fresh event.
	seq3, dup3, err := s.IngestObservation(contract.ObservationEnvelope{Source: "agent", ExternalID: "ext-2", Event: contract.Event{Type: "memory.observed"}})
	if err != nil || dup3 || seq3 == seq1 {
		t.Fatalf("distinct external_id must append a new event; got (%d,%v,%v)", seq3, dup3, err)
	}
}

// S1: an empty ExternalID must be rejected (fail-loud) — else two DISTINCT observations from the same source
// with no key would collapse on the ("",source) dedupe row and silently drop the second.
func TestIngestObservationRejectsEmptyExternalID(t *testing.T) {
	s := newTestStore(t)
	if _, _, err := s.IngestObservation(contract.ObservationEnvelope{Source: "agent", ExternalID: "", Event: contract.Event{Type: "memory.observed"}}); err == nil {
		t.Fatal("an empty ExternalID must be rejected (S1: an idempotency key is required for exactly-once ingest)")
	}
	if evs, _ := s.PendingEvents(0); len(evs) != 0 {
		t.Fatalf("a rejected empty-key ingest must append nothing; got %d", len(evs))
	}
}
