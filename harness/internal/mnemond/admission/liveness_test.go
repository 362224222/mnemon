package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// ---- Arm D — liveness escalation (Invariant #10): same CorrelationID re-deferred across three passes ----
// pass1=rebase, pass2=rebase, pass3=human_review. The rebase counter PERSISTS on the Reconciler across passes.
func TestArmD_LivenessEscalation(t *testing.T) {
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"})    // X@1
	seedUpdate(t, k, X, 1, map[string]any{"content": "v1"}) // X@2 -> based_on 1 is permanently stale
	r := NewReconciler(s, k)
	const corr = "hot"
	pass := func(id string) contract.Decision {
		// the harness models "the proposer retried": each pass appends a fresh still-stale event with that CorrelationID.
		appendProposal(t, s, updateProposal(id, "codex", corr, X, 1, map[string]any{"content": "retry"}, nil))
		ds := r.RunOnce(casModes())
		if len(ds) != 1 {
			t.Fatalf("each pass processes exactly one new event, got %d", len(ds))
		}
		return ds[0]
	}
	if d := pass("d1"); d.Status != contract.Deferred || d.NextAction != "rebase" {
		t.Fatalf("pass1 want Deferred/rebase, got %s/%q", d.Status, d.NextAction)
	}
	if d := pass("d2"); d.Status != contract.Deferred || d.NextAction != "rebase" {
		t.Fatalf("pass2 want Deferred/rebase, got %s/%q", d.Status, d.NextAction)
	}
	if d := pass("d3"); d.Status != contract.Deferred || d.NextAction != "human_review" {
		t.Fatalf("pass3 want Deferred/human_review (escalation), got %s/%q", d.Status, d.NextAction)
	}
}
