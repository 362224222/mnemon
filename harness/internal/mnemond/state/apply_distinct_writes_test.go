package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// A multi-write op must target DISTINCT resources (Invariant #5: multi-RESOURCE all-or-nothing). If two
// writes alias the same ref, the kernel applies them sequentially with last-write-wins and reports two
// NewVersions for one resource — letting a single "atomic" op self-cancel an earlier write in the SAME op
// (e.g. job.Reserve's budget OpUpdate followed by a data write aliased back to the budget ref that resets
// spent_usd, laundering the spend ceiling, S6; also audit-trail corruption — two versions for one resource).
// Reject duplicate write refs terminally up-front.
func TestApplyRejectsDuplicateWriteRefs(t *testing.T) {
	k := newKernel(t)
	mustCreate(t, k, "memory", "m1", map[string]any{"content": "a"})
	before := k.Store().DecisionCount()
	op := contract.KernelOp{OpID: "dup", Actor: "user", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "b"}},
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 2, Fields: map[string]any{"content": "c"}},
	}}
	d := k.Apply(op, p0Modes())
	if d.Status != contract.Rejected || d.NextAction != "" {
		t.Fatalf("duplicate write refs must be Rejected/'' (rebase cannot fix), got %s/%q", d.Status, d.NextAction)
	}
	if len(d.NewVersions) != 0 {
		t.Fatalf("a rejected op must report no NewVersions; got %+v", d.NewVersions)
	}
	if v, _ := k.Store().GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 1 {
		t.Fatalf("rejected op must not mutate state; m1=@%d want @1", v)
	}
	if k.Store().DecisionCount() != before+1 {
		t.Fatalf("exactly one terminal decision must be persisted")
	}
}

// Distinct refs in one op still work (no false positive): two different resources commit all-or-nothing.
func TestApplyAllowsDistinctWriteRefs(t *testing.T) {
	k := newKernel(t)
	mustCreate(t, k, "memory", "m1", map[string]any{"content": "a"})
	mustCreate(t, k, "goal", "g1", map[string]any{"statement": "ship"})
	d := k.Apply(contract.KernelOp{OpID: "two", Actor: "user", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "b"}},
		{Ref: contract.ResourceRef{Kind: "goal", ID: "g1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"statement": "x"}},
	}}, p0Modes())
	if d.Status != contract.Accepted || len(d.NewVersions) != 2 {
		t.Fatalf("two distinct-ref writes must commit together; got %s %+v", d.Status, d.NewVersions)
	}
}
