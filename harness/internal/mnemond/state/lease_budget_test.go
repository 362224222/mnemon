package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// D3/S5/S6: lease and budget are first-class versioned resources. Their per-resource Version IS the
// fencing token / CAS counter — no new locking mechanism, just the kernel's existing CAS.

func TestLeaseBudgetKindsRegistered(t *testing.T) {
	if !contract.KindCatalog["lease"] || !contract.KindCatalog["budget"] {
		t.Fatal("lease and budget must be versioned resource kinds (D3)")
	}
	g := DefaultSchemaGuard()
	want := map[contract.ResourceKind][]string{
		"lease":  {"job_id", "owner", "fence_until"},
		"budget": {"limit_usd", "spent_usd"},
	}
	for kind, fields := range want {
		got := g.Required[kind]
		if len(got) != len(fields) {
			t.Fatalf("%s required fields = %v, want %v", kind, got, fields)
		}
	}
}

func TestLeaseFenceIsVersion(t *testing.T) {
	k := NewMaterializer(newTestStore(t), DefaultSchemaGuard(),
		AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"worker": {"lease"}}})
	ref := contract.ResourceRef{Kind: "lease", ID: "job1"}
	modes := contract.Modes{Conflict: contract.ConflictReject, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict}
	mk := func(owner string, fence float64) map[string]any {
		return map[string]any{"job_id": "job1", "owner": owner, "fence_until": fence}
	}
	// create lease/job1 -> fence @1
	d := k.Apply(contract.StateOp{OpID: "claim", Actor: "worker", Writes: []contract.ResourceWrite{
		{Ref: ref, Kind: contract.OpCreate, Fields: mk("worker", 100)}}}, modes)
	if d.Status != contract.Accepted || len(d.NewVersions) != 1 || d.NewVersions[0].Version != 1 {
		t.Fatalf("create lease must Accept @1; got %+v", d)
	}
	// CAS based_on=1 -> fence @2
	d2 := k.Apply(contract.StateOp{OpID: "renew", Actor: "worker", Writes: []contract.ResourceWrite{
		{Ref: ref, Kind: contract.OpUpdate, BasedOn: 1, Fields: mk("worker", 200)}}}, modes)
	if d2.Status != contract.Accepted || d2.NewVersions[0].Version != 2 {
		t.Fatalf("CAS based_on=1 must advance the fence to @2; got %+v", d2)
	}
	// stale based_on=1 -> conflict (the fence already moved past 1)
	d3 := k.Apply(contract.StateOp{OpID: "stale", Actor: "worker", Writes: []contract.ResourceWrite{
		{Ref: ref, Kind: contract.OpUpdate, BasedOn: 1, Fields: mk("thief", 300)}}}, modes)
	if d3.Status != contract.Rejected {
		t.Fatalf("stale based_on=1 must conflict; got %+v", d3)
	}
}
