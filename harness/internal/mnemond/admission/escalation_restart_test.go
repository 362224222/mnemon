package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// HIGH (verification finding): Invariant #10 (liveness escalation) must survive a restart. The cursor is
// durable, so escalation state must be too — otherwise a fresh Reconciler resets the deferral counter and
// a permanently-conflicting correlation is deferred with NextAction=rebase forever, never reaching
// human_review. Each pass here uses a BRAND-NEW Reconciler over the SAME store (a restart).
func TestEscalationSurvivesRestart(t *testing.T) {
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"})    // X@1
	seedUpdate(t, k, X, 1, map[string]any{"content": "v1"}) // X@2 -> based_on 1 permanently stale
	const corr = "hot"

	pass := func(id string) contract.Decision {
		appendProposal(t, s, updateProposal(id, "codex", corr, X, 1, map[string]any{"content": "retry"}, nil))
		ds := NewReconciler(s, k).RunOnce(casModes()) // FRESH reconciler each pass = restart
		if len(ds) != 1 {
			t.Fatalf("restart must process exactly the one new event, got %d", len(ds))
		}
		return ds[0]
	}
	if d := pass("r1"); d.NextAction != "rebase" {
		t.Fatalf("pass1 want rebase, got %q", d.NextAction)
	}
	if d := pass("r2"); d.NextAction != "rebase" {
		t.Fatalf("pass2 want rebase, got %q", d.NextAction)
	}
	if d := pass("r3"); d.NextAction != "human_review" {
		t.Fatalf("pass3 across restarts must escalate to human_review (Invariant #10 durable), got %q", d.NextAction)
	}
}
