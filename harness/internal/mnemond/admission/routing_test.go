package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// R2#2: the event log carries BOTH observations and proposed operations. A non-proposal (observation)
// event must NOT be reconciled as a write attempt — it must not become a Rejected "empty op" decision
// that pollutes the decision log and muddies the cursor's meaning.
func TestObservationEventsAreNotReconciledAsWrites(t *testing.T) {
	s, k := newRecon(t)
	appendProposal(t, s, contract.Event{ID: "o1", Type: "memory.hot_write_observed", Actor: "codex", Payload: map[string]any{"note": "fyi"}})
	before := s.DecisionCount()
	ds := NewReconciler(s, k).RunOnce(casModes())
	if len(ds) != 0 {
		t.Fatalf("observation must not produce a decision, got %d", len(ds))
	}
	if s.DecisionCount() != before {
		t.Fatalf("observation polluted the decision log: %d -> %d", before, s.DecisionCount())
	}
}

// A proposal that follows an observation in the same RunOnce must still be reconciled (the cursor
// advances through the skipped observation).
func TestProposalAfterObservationStillReconciled(t *testing.T) {
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"}) // X@1
	appendProposal(t, s, contract.Event{ID: "o1", Type: "memory.hot_write_observed", Actor: "codex", Payload: map[string]any{}})
	appendProposal(t, s, updateProposal("p1", "codex", "c1", X, 1, map[string]any{"content": "v1"}, nil))
	ds := NewReconciler(s, k).RunOnce(casModes())
	if len(ds) != 1 || ds[0].Status != contract.Accepted || ds[0].OpID != "p1" {
		t.Fatalf("proposal after observation must be reconciled (Accepted, OpID p1), got %+v", ds)
	}
}
