package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// newMaterializerTestStore is the kernel package's local test store ctor. It mirrors the helper that moved to
// the store package with store_test.go; kept in _test.go so the production store package never
// imports testing.
func newMaterializerTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func permissiveRules() AuthorityRules {
	return AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"user": {"memory", "goal", "skill"}}}
}
func p0Modes() contract.Modes {
	return contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict}
}
func mustCreate(t *testing.T, k *Materializer, kind contract.ResourceKind, id contract.ResourceID, f map[string]any) {
	t.Helper()
	d := k.Apply(contract.StateOp{OpID: "seed_" + string(id), Actor: "user",
		Writes: []contract.ResourceWrite{{Ref: contract.ResourceRef{Kind: kind, ID: id}, Kind: contract.OpCreate, Fields: f}}}, p0Modes())
	if d.Status != contract.Accepted {
		t.Fatalf("seed %s failed: %s", id, d.Reason)
	}
}
func newMaterializer(t *testing.T) *Materializer {
	return NewMaterializer(newMaterializerTestStore(t), SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), permissiveRules())
}

func TestApplyMultiResourceAllOrNothing(t *testing.T) {
	k := newMaterializer(t)
	mustCreate(t, k, "memory", "m1", map[string]any{"content": "a"})
	mustCreate(t, k, "goal", "g1", map[string]any{"statement": "ship"})
	op := contract.StateOp{OpID: "op1", Actor: "user", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "b"}},
		{Ref: contract.ResourceRef{Kind: "goal", ID: "g1"}, Kind: contract.OpUpdate, BasedOn: 99, Fields: map[string]any{"statement": "x"}},
	}}
	d := k.Apply(op, p0Modes())
	if d.Status != contract.Deferred {
		t.Fatalf("want deferred, got %s", d.Status)
	}
	if v, _ := k.Store().GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 1 {
		t.Fatalf("PARTIAL WRITE: m1 at %d despite g1 conflict", v) // Invariant #5
	}
}
func TestAuthzFailureIsRejectedNotDeferred(t *testing.T) {
	k := NewMaterializer(newMaterializerTestStore(t), SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), AuthorityRules{}) // nobody allowed
	d := k.Apply(contract.StateOp{OpID: "op2", Actor: "codex@x", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "a"}}}}, p0Modes())
	if d.Status != contract.Rejected || d.NextAction != "" {
		t.Fatalf("authz fail must be Rejected/'' (rebase can't fix), got %s/%q", d.Status, d.NextAction)
	}
}
func TestApplyPersistsExactlyOneDecision(t *testing.T) {
	k := newMaterializer(t)
	mustCreate(t, k, "memory", "m1", map[string]any{"content": "a"})
	before := k.Store().DecisionCount()
	_ = k.Apply(contract.StateOp{OpID: "op3", Actor: "user", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "b"}}}}, p0Modes())
	if k.Store().DecisionCount() != before+1 {
		t.Fatalf("want exactly one new decision")
	} // Invariant #7
}
