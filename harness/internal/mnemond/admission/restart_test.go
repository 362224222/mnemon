package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// #1: the reconciler cursor must be durable. A fresh Reconciler over the same store (a "restart") must
// resume from the decision log, NOT re-consume already-decided events (which would pollute pull feedback
// with new deferred/rejected decisions for events that were already accepted).
func TestReconcilerResumesFromDecisionLogAfterRestart(t *testing.T) {
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"}) // X@1
	appendProposal(t, s, updateProposal("e1", "a1", "c1", X, 1, map[string]any{"content": "v1"}, nil))

	d1 := NewReconciler(s, k).RunOnce(casModes())
	if len(d1) != 1 || d1[0].Status != contract.Accepted {
		t.Fatalf("first run: want 1 Accepted, got %+v", d1)
	}
	countAfter1 := s.DecisionCount()

	// "restart": a brand-new reconciler over the SAME store
	d2 := NewReconciler(s, k).RunOnce(casModes())
	if len(d2) != 0 {
		t.Fatalf("restart re-consumed %d already-decided event(s) — cursor not durable", len(d2))
	}
	if s.DecisionCount() != countAfter1 {
		t.Fatalf("restart polluted the decision log: %d -> %d", countAfter1, s.DecisionCount())
	}
}

// A restart must still consume genuinely-new events appended after the prior run.
func TestReconcilerConsumesNewEventsAfterRestart(t *testing.T) {
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"})
	appendProposal(t, s, updateProposal("e1", "a1", "c1", X, 1, map[string]any{"content": "v1"}, nil))
	_ = NewReconciler(s, k).RunOnce(casModes()) // X -> @2, event 1 decided

	appendProposal(t, s, updateProposal("e2", "a1", "c2", X, 2, map[string]any{"content": "v2"}, nil))
	d2 := NewReconciler(s, k).RunOnce(casModes()) // restart must pick up event 2
	if len(d2) != 1 || d2[0].Status != contract.Accepted || d2[0].IngestSeq != 2 {
		t.Fatalf("restart must consume the new event (seq 2) Accepted, got %+v", d2)
	}
}
