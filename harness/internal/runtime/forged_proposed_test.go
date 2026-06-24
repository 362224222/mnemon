package runtime

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// The wire boundary (access.ServerAPI.Ingest) admits ONLY observations. A *.proposed / *.diagnostic is an INTERNAL
// event class: a *.proposed is minted exclusively by the bridge AFTER the rule pre-gate + write-scope check
// (R11), a *.diagnostic only by the server (S7). The reconciler trusts every *.proposed in the log, so a
// client-supplied one would skip the rule pre-gate, the bridge write-scope, AND readback (S10) and be
// applied directly by the kernel (whose authz is actor×kind only). That is a within-kind cross-resource /
// cross-principal write-scope escalation. Ingest must reject reserved internal event types.

func TestIngestRejectsForgedProposed(t *testing.T) {
	s, _, cs := newServerWith(t, admission.NewRuleSet()) // empty rule set: no legitimate proposer exists
	_, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "forge1", Event: contract.Event{
		Type: "memory.write.proposed",
		Payload: map[string]any{"writes": []contract.ResourceWrite{
			{Ref: contract.ResourceRef{Kind: "memory", ID: "m_secret"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "stolen"}}}},
	}})
	if err == nil {
		t.Fatal("Ingest must reject a client-forged *.proposed event (R11/S9 write-scope bypass)")
	}
	if _, terr := cs.Tick(); terr != nil {
		t.Fatalf("tick: %v", terr)
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m_secret"}); v != 0 {
		t.Fatalf("forged out-of-scope write must not be applied; m_secret=@%d", v)
	}
}

func TestIngestRejectsForgedDiagnostic(t *testing.T) {
	_, _, cs := newServerWith(t, admission.NewRuleSet())
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "fd", Event: contract.Event{Type: "memory.diagnostic"}}); err == nil {
		t.Fatal("Ingest must reject a client-forged *.diagnostic event")
	}
}

// alice (scope {mem_a}) forges a *.proposed UPDATE to bob's mem_b (scope {mem_b}). Both are authorized for
// kind "memory", so kernel authz alone does not stop it — only the bridge write-scope would, and the forged
// proposed event bypasses the bridge. Ingest must reject it before it enters the log (D7/S9).
func TestIngestRejectsCrossPrincipalForgedProposed(t *testing.T) {
	s, err := store.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	rules := kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"alice": {"memory"}, "bob": {"memory"}}}
	k := kernel.NewKernel(s, kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), rules)
	subs := map[contract.ActorID]contract.Subscription{
		"alice": {Actor: "alice", Refs: []contract.ResourceRef{{Kind: "memory", ID: "mem_a"}}},
		"bob":   {Actor: "bob", Refs: []contract.ResourceRef{{Kind: "memory", ID: "mem_b"}}},
	}
	cs := New(s, k, admission.NewRuleSet(), subs, p0Modes(), seqGen(), fixedNow())
	if d := k.Apply(contract.KernelOp{OpID: "seed", Actor: "bob", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "mem_b"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "bob-secret"}}}}, p0Modes()); d.Status != contract.Accepted {
		t.Fatalf("seed: %s", d.Reason)
	}
	_, _, err = cs.Ingest("alice", contract.ObservationEnvelope{ExternalID: "x", Event: contract.Event{
		Type: "memory.write.proposed",
		Payload: map[string]any{"writes": []contract.ResourceWrite{
			{Ref: contract.ResourceRef{Kind: "memory", ID: "mem_b"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "alice-overwrote-bob"}}}},
	}})
	if err == nil {
		t.Fatal("Ingest must reject alice's forged cross-principal *.proposed write into bob's scope (S9/D7)")
	}
	if _, terr := cs.Tick(); terr != nil {
		t.Fatalf("tick: %v", terr)
	}
	_, fields, _ := s.GetResource(contract.ResourceRef{Kind: "memory", ID: "mem_b"})
	if fields == nil || fields["content"] != "bob-secret" {
		t.Fatalf("bob's mem_b must be untouched; got %v", fields["content"])
	}
}

// A legitimate observation that ends in neither reserved suffix still ingests normally (no false positive).
func TestIngestAllowsObservation(t *testing.T) {
	_, _, cs := newServerWith(t, admission.NewRuleSet())
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "ok", Event: contract.Event{Type: "memory.observed"}}); err != nil {
		t.Fatalf("a normal observation must still ingest; got %v", err)
	}
}
