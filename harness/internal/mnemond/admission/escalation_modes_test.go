package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// Round-2 MED: the durable escalation count must count ONLY rebase deferrals (NextAction=="rebase"),
// exactly as the removed in-memory map did — NOT every deferral. Otherwise an unrelated human_review
// deferral on the same CorrelationID (e.g. from a defer_to_human / auto_merge_disjoint pass) pre-seeds
// the count and escalates a later rebase retry one step early.
func TestEscalationCountsOnlyRebaseDeferrals(t *testing.T) {
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"})    // X@1
	seedUpdate(t, k, X, 1, map[string]any{"content": "v1"}) // X@2 -> base 1 stale
	const corr = "hot"

	// An unrelated human_review deferral on the SAME correlation (via defer_to_human mode).
	dh := k.Apply(contract.KernelOp{OpID: "h0", Actor: "codex", CorrelationID: corr,
		Writes: []contract.ResourceWrite{{Ref: X, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "h"}}}},
		contract.Modes{Conflict: contract.ConflictDeferToHuman, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict})
	if dh.Status != contract.Deferred || dh.NextAction != "human_review" {
		t.Fatalf("setup: want deferred/human_review, got %s/%q", dh.Status, dh.NextAction)
	}

	// Two rebase retries on the same corr. With rebase-only counting, both stay rebase (0 then 1, < 2).
	appendProposal(t, s, updateProposal("e1", "codex", corr, X, 1, map[string]any{"content": "a"}, nil))
	d1 := NewReconciler(s, k).RunOnce(casModes())
	appendProposal(t, s, updateProposal("e2", "codex", corr, X, 1, map[string]any{"content": "b"}, nil))
	d2 := NewReconciler(s, k).RunOnce(casModes())
	if d1[0].NextAction != "rebase" || d2[0].NextAction != "rebase" {
		t.Fatalf("an unrelated human_review deferral must NOT pre-seed rebase escalation; got %q,%q", d1[0].NextAction, d2[0].NextAction)
	}
}
