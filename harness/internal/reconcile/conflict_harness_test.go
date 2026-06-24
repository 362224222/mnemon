package reconcile

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ---- shared harness helpers (simulated agents, deterministic fixtures, ZERO paid turns) ----

func rules() kernel.AuthorityRules {
	return kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{
		"user":  {"memory", "goal", "skill"},
		"a1":    {"memory", "goal", "skill"},
		"a2":    {"memory", "goal", "skill"},
		"codex": {"memory", "goal", "skill"},
	}}
}
func newRecon(t *testing.T) (*store.Store, *kernel.Kernel) {
	t.Helper()
	s, err := store.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	k := kernel.NewKernel(s, kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), rules())
	return s, k
}
func casModes() contract.Modes {
	return contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict}
}

// seedCreate / seedUpdate are TRUSTED setup writes (already-accepted state), applied directly via the kernel.
func seedCreate(t *testing.T, k *kernel.Kernel, ref contract.ResourceRef, fields map[string]any) {
	t.Helper()
	d := k.Apply(contract.KernelOp{OpID: "seed_" + string(ref.ID), Actor: "user",
		Writes: []contract.ResourceWrite{{Ref: ref, Kind: contract.OpCreate, Fields: fields}}}, casModes())
	if d.Status != contract.Accepted {
		t.Fatalf("seedCreate %s: %s", ref.ID, d.Reason)
	}
}
func seedUpdate(t *testing.T, k *kernel.Kernel, ref contract.ResourceRef, basedOn contract.Version, fields map[string]any) {
	t.Helper()
	d := k.Apply(contract.KernelOp{OpID: "sup_" + string(ref.ID), Actor: "user",
		Writes: []contract.ResourceWrite{{Ref: ref, Kind: contract.OpUpdate, BasedOn: basedOn, Fields: fields}}}, casModes())
	if d.Status != contract.Accepted {
		t.Fatalf("seedUpdate %s@%d: %s", ref.ID, basedOn, d.Reason)
	}
}

// updateProposal builds a *.proposed event for one OpUpdate write (the proposer path through the event log).
// Actor/CorrelationID/read-set live in the TRUSTED envelope; the write lives in the payload.
func updateProposal(id string, actor contract.ActorID, corr string, ref contract.ResourceRef, basedOn contract.Version, fields map[string]any, readSet []contract.ResourceVersion) contract.Event {
	return contract.Event{
		ID:            id,
		Type:          "memory.write.proposed",
		Actor:         actor,
		CorrelationID: corr,
		ResourceRefs:  []contract.ResourceRef{ref},
		BasedOn:       readSet,
		Payload: map[string]any{
			"writes": []contract.ResourceWrite{{Ref: ref, Kind: contract.OpUpdate, BasedOn: basedOn, Fields: fields}},
		},
	}
}
func appendProposal(t *testing.T, s *store.Store, ev contract.Event) {
	t.Helper()
	if _, err := s.AppendEvent(ev); err != nil {
		t.Fatalf("append: %v", err)
	}
}

type armResult struct {
	modes  contract.Modes
	winner contract.Decision
	loser  contract.Decision
}

// runArmA: two proposers both update X based_on 1; under any conflict mode exactly one wins the CAS.
// Reused by P2 to assert mode-selected loser handling.
func runArmA(t *testing.T, modes contract.Modes) armResult {
	t.Helper()
	s, k := newRecon(t)
	X := contract.ResourceRef{Kind: "memory", ID: "X"}
	seedCreate(t, k, X, map[string]any{"content": "v0"}) // X@1
	appendProposal(t, s, updateProposal("e1", "a1", "c1", X, 1, map[string]any{"content": "a1"}, nil))
	appendProposal(t, s, updateProposal("e2", "a2", "c2", X, 1, map[string]any{"content": "a2"}, nil))
	ds := NewReconciler(s, k).RunOnce(modes)
	if len(ds) != 2 {
		t.Fatalf("want 2 decisions, got %d", len(ds))
	}
	res := armResult{modes: modes}
	accepted := 0
	for _, d := range ds {
		if d.Status == contract.Accepted {
			res.winner = d
			accepted++
		} else {
			res.loser = d
		}
	}
	if accepted != 1 {
		t.Fatalf("want exactly one Accepted, got %d", accepted)
	}
	return res
}

// ---- Arm A — write-write (CAS catches) ----
func TestArmA_WriteWriteCASCatches(t *testing.T) {
	r := runArmA(t, casModes())
	if len(r.winner.NewVersions) != 1 || r.winner.NewVersions[0].Version != 2 {
		t.Fatalf("winner must advance X->2, got %+v", r.winner.NewVersions)
	}
	if r.loser.Status != contract.Deferred {
		t.Fatalf("loser must be Deferred, got %s", r.loser.Status)
	}
	if len(r.loser.Conflicts) != 1 || r.loser.Conflicts[0].Kind != contract.WriteWrite {
		t.Fatalf("loser must carry one WriteWrite conflict, got %+v", r.loser.Conflicts)
	}
}

// ---- Arm B — read-stale (read-set catches what CAS cannot; Invariant #6/6b) — fixture pinned ----
func TestArmB_ReadStaleVsWriteCAS(t *testing.T) {
	M := contract.ResourceRef{Kind: "memory", ID: "M"}
	G := contract.ResourceRef{Kind: "goal", ID: "G"}

	build := func(t *testing.T) (*store.Store, *kernel.Kernel) {
		s, k := newRecon(t)
		seedCreate(t, k, M, map[string]any{"content": "m0"})    // M@1
		seedUpdate(t, k, M, 1, map[string]any{"content": "m1"}) // M@2 (matches based_on M@2)
		seedCreate(t, k, G, map[string]any{"statement": "g0"})  // G@1
		for v := contract.Version(1); v < 5; v++ {              // bump G -> @5
			seedUpdate(t, k, G, v, map[string]any{"statement": "g"})
		}
		seedUpdate(t, k, G, 5, map[string]any{"statement": "g6"}) // disjoint accepted op: G@5->G@6, M untouched
		return s, k
	}
	// proposal: OpUpdate M based_on 2, ReadSet=[G@5]
	prop := func() contract.Event {
		return updateProposal("eb", "codex", "cb", M, 2, map[string]any{"content": "m2"}, []contract.ResourceVersion{{Ref: G, Version: 5}})
	}

	// (1) event_view_read_set: read-set [G@5] is stale (G is @6) -> Deferred{ReadStale on G}; M not written (still @2)
	{
		s, k := build(t)
		appendProposal(t, s, prop())
		ds := NewReconciler(s, k).RunOnce(contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationEventViewReadSet, Authz: contract.AuthzStrict})
		if len(ds) != 1 || ds[0].Status != contract.Deferred {
			t.Fatalf("read_set: want 1 Deferred, got %+v", ds)
		}
		c := ds[0].Conflicts
		if len(c) != 1 || c[0].Kind != contract.ReadStale || c[0].Ref != G || c[0].ExpectedVersion != 5 || c[0].ActualVersion != 6 {
			t.Fatalf("read_set: want ReadStale{G,exp5,act6}, got %+v", c)
		}
		if v, _ := s.GetVersion(M); v != 2 {
			t.Fatalf("read_set: M must stay @2 (no write), got %d", v)
		}
	}
	// (2) write_cas: read-set NOT validated -> M CAS based_on 2 hits -> Accepted M@3, no conflicts
	{
		s, k := build(t)
		appendProposal(t, s, prop())
		ds := NewReconciler(s, k).RunOnce(contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict})
		if len(ds) != 1 || ds[0].Status != contract.Accepted {
			t.Fatalf("write_cas: want 1 Accepted, got %+v", ds)
		}
		if len(ds[0].Conflicts) != 0 {
			t.Fatalf("write_cas: want no conflicts, got %+v", ds[0].Conflicts)
		}
		found := false
		for _, nv := range ds[0].NewVersions {
			if nv.Ref == M && nv.Version == 3 {
				found = true
			}
		}
		if !found {
			t.Fatalf("write_cas: NewVersions must include M@3, got %+v", ds[0].NewVersions)
		}
	}
}

// ---- Arm E — read-set granularity (resolves the §10 fork): per-resource, not whole-projection digest ----
func TestArmE_ReadSetGranularityPerResource(t *testing.T) {
	s, k := newRecon(t)
	M := contract.ResourceRef{Kind: "memory", ID: "M"}
	G := contract.ResourceRef{Kind: "goal", ID: "G"}
	H := contract.ResourceRef{Kind: "skill", ID: "H"}
	seedCreate(t, k, M, map[string]any{"content": "m0"})   // M@1
	seedCreate(t, k, G, map[string]any{"statement": "g0"}) // G@1
	for v := contract.Version(1); v < 5; v++ {             // G -> @5
		seedUpdate(t, k, G, v, map[string]any{"statement": "g"})
	}
	seedCreate(t, k, H, map[string]any{"name": "h0"})    // H@1
	seedUpdate(t, k, H, 1, map[string]any{"name": "h1"}) // H@1 -> H@2 (in-projection, but NOT in the read-set)

	// proposal: OpUpdate M based_on 1, ReadSet=[G@5] (per-resource; H deliberately omitted)
	appendProposal(t, s, updateProposal("ee", "codex", "ce", M, 1, map[string]any{"content": "m1"}, []contract.ResourceVersion{{Ref: G, Version: 5}}))
	ds := NewReconciler(s, k).RunOnce(contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationEventViewReadSet, Authz: contract.AuthzStrict})
	if len(ds) != 1 || ds[0].Status != contract.Accepted {
		t.Fatalf("per-resource read-set: G unchanged so proposal must be Accepted despite H changing; got %+v", ds)
	}
	// Documented decision: per-resource read-set [G@5] is the P0/P1 default. The whole-projection
	// context_digest IS stale here (H@1->H@2) — digest-granularity would defer; that is the deferred
	// `serializable` candidate (§10). This arm is the granularity fork's evidence.
}
