package runtime

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

// The bridge must stamp the PRODUCING rule's actor. r1 (alice) and r2 (bob) share the same (handles, emits),
// but only r2 PROPOSES; r1 merely allows. Guessing the producer by scanning for the first rule matching
// (Handles(ev.Type), Emits()==proposal.Type) picks alice — misattributing bob's proposal to alice. The
// reduced decision carries the real origin (bob), so the stamped *.proposed event's Actor must be bob.
func TestProposeStampsProducingRuleActorNotFirstMatch(t *testing.T) {
	s, err := state.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	rules := state.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"alice": {"memory"}, "bob": {"memory"}}}
	k := state.NewKernel(s, state.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), rules)
	subs := map[contract.ActorID]contract.Subscription{
		"agent": {Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}},
	}
	r1 := admission.NewNativeRule("r1", "alice", "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
		})
	r2 := admission.NewNativeRule("r2", "bob", "memory.write.proposed", []string{"memory.observed"},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			rv := in.View.Resources[0]
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{Type: "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{{Ref: rv.Ref, Kind: contract.OpUpdate, BasedOn: rv.Version, Fields: map[string]any{"content": "by-bob"}}}}}}, nil
		})
	cs := New(s, k, admission.NewRuleSet(r1, r2), subs, p0Modes(), seqGen(), fixedNow())
	if d := k.Apply(contract.KernelOp{OpID: "seed", Actor: "bob", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "v0"}}}}, p0Modes()); d.Status != contract.Accepted {
		t.Fatalf("seed: %s", d.Reason)
	}
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := cs.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	evs, _ := s.PendingEvents(0)
	var found bool
	for _, ev := range evs {
		if ev.Type == "memory.write.proposed" {
			found = true
			if ev.Actor != "bob" {
				t.Fatalf("the proposed event must be attributed to the PRODUCING rule's actor (bob), not the first (handles,emits) match (alice); got %q", ev.Actor)
			}
		}
	}
	if !found {
		t.Fatal("expected a memory.write.proposed event to be minted")
	}
}
