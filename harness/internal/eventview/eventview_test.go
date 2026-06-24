package eventview

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

var refs = []contract.ResourceRef{
	{Kind: "memory", ID: "m1"},
	{Kind: "goal", ID: "g1"},
}

func p1Rules() kernel.AuthorityRules {
	return kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{
		"user":    {"memory", "goal", "skill"},
		"codex@r": {"memory", "goal", "skill"},
	}}
}
func writeCASModes() contract.Modes {
	return contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict}
}
func newStoreKernel(t *testing.T) (*store.Store, *kernel.Kernel) {
	t.Helper()
	s, err := store.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	k := kernel.NewKernel(s, kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), p1Rules())
	return s, k
}
func createP(t *testing.T, k *kernel.Kernel, ref contract.ResourceRef, fields map[string]any) {
	t.Helper()
	d := k.Apply(contract.KernelOp{OpID: "seed_" + string(ref.ID), Actor: "user",
		Writes: []contract.ResourceWrite{{Ref: ref, Kind: contract.OpCreate, Fields: fields}}}, writeCASModes())
	if d.Status != contract.Accepted {
		t.Fatalf("create %s: %s", ref.ID, d.Reason)
	}
}
func updateP(t *testing.T, k *kernel.Kernel, ref contract.ResourceRef, basedOn contract.Version, fields map[string]any) {
	t.Helper()
	d := k.Apply(contract.KernelOp{OpID: "upd_" + string(ref.ID), Actor: "user",
		Writes: []contract.ResourceWrite{{Ref: ref, Kind: contract.OpUpdate, BasedOn: basedOn, Fields: fields}}}, writeCASModes())
	if d.Status != contract.Accepted {
		t.Fatalf("update %s: %s", ref.ID, d.Reason)
	}
}

// newStoreWith seeds m1@1, g1@5.
func newStoreWith(t *testing.T) *store.Store {
	t.Helper()
	s, k := newStoreKernel(t)
	createP(t, k, contract.ResourceRef{Kind: "memory", ID: "m1"}, map[string]any{"content": "a"}) // m1@1
	createP(t, k, contract.ResourceRef{Kind: "goal", ID: "g1"}, map[string]any{"statement": "s"}) // g1@1
	for v := contract.Version(1); v < 5; v++ {                                                    // bump g1 -> @5
		updateP(t, k, contract.ResourceRef{Kind: "goal", ID: "g1"}, v, map[string]any{"statement": "s"})
	}
	return s
}

// accept applies one accepted update against an existing store.
func accept(t *testing.T, s *store.Store, ref contract.ResourceRef, basedOn contract.Version, fields map[string]any) {
	t.Helper()
	k := kernel.NewKernel(s, kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), p1Rules())
	updateP(t, k, ref, basedOn, fields)
}

// deferOneFor produces exactly one Deferred decision for the given actor (a stale CAS under rebase mode).
func deferOneFor(t *testing.T, k *kernel.Kernel, actor contract.ActorID) {
	t.Helper()
	ref := contract.ResourceRef{Kind: "memory", ID: contract.ResourceID("d_" + string(actor))}
	c := k.Apply(contract.KernelOp{OpID: "dseed", Actor: actor,
		Writes: []contract.ResourceWrite{{Ref: ref, Kind: contract.OpCreate, Fields: map[string]any{"content": "x"}}}}, writeCASModes())
	if c.Status != contract.Accepted {
		t.Fatalf("defer seed create: %s", c.Reason)
	}
	d := k.Apply(contract.KernelOp{OpID: "dstale", Actor: actor,
		Writes: []contract.ResourceWrite{{Ref: ref, Kind: contract.OpUpdate, BasedOn: 99, Fields: map[string]any{"content": "y"}}}}, writeCASModes())
	if d.Status != contract.Deferred {
		t.Fatalf("expected deferred, got %s/%s", d.Status, d.Reason)
	}
}

func TestDigestChangesWithVersion(t *testing.T) {
	s := newStoreWith(t) // m1@1, g1@5
	d1 := Build(s, refs, "user").Digest
	accept(t, s, contract.ResourceRef{Kind: "memory", ID: "m1"}, 1, map[string]any{"content": "b"}) // m1 -> @2
	d2 := Build(s, refs, "user").Digest
	if d1 == d2 {
		t.Fatal("digest must change when an included resource version changes")
	}
}
func TestDeferredDecisionSurfacesAsPullFeedback(t *testing.T) {
	s, k := newStoreKernel(t)
	// before any reconcile: no feedback for codex@r
	if len(Build(s, refs, "codex@r").Feedback) != 0 {
		t.Fatal("feedback must be empty before a deferral")
	}
	deferOneFor(t, k, "codex@r")             // produce a Deferred decision for codex@r
	fb := Build(s, refs, "codex@r").Feedback // pull at the NEXT boundary (Invariant #8)
	if len(fb) != 1 || fb[0].Status != contract.Deferred {
		t.Fatal("deferred decision must surface in the actor's next projection")
	}
}
