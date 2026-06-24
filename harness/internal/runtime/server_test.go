package runtime

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func agentSubs() map[contract.ActorID]contract.Subscription {
	return map[contract.ActorID]contract.Subscription{
		"agent": {Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}},
	}
}

func p0Modes() contract.Modes {
	return contract.Modes{Conflict: contract.ConflictRebase, Isolation: contract.IsolationEventViewReadSet, Authz: contract.AuthzStrict}
}

// proposeRule updates the single in-scope memory resource based_on the version it saw.
func proposeRule() rule.Rule {
	return rule.NewNativeRule("writer", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(in rule.RuleInput) (contract.RuleDecision, error) {
			if len(in.View.Resources) == 0 {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"empty scope"}}, nil
			}
			rv := in.View.Resources[0]
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Type: "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{
					{Ref: rv.Ref, Kind: contract.OpUpdate, BasedOn: rv.Version, Fields: map[string]any{"content": "derived"}}}},
			}}, nil
		})
}

func denyRule() rule.Rule {
	return rule.NewNativeRule("denier", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(rule.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"denied for test"}}, nil
		})
}

func newServerWith(t *testing.T, rs rule.RuleSet) (*store.Store, *kernel.Kernel, *ControlServer) {
	t.Helper()
	s, err := store.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	rules := kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"agent": {"memory"}}}
	k := kernel.NewKernel(s, kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), rules)
	cs := New(s, k, rs, agentSubs(), p0Modes(), seqGen(), fixedNow())
	if d := k.Apply(contract.KernelOp{OpID: "seed", Actor: "agent", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "v0"}}}}, p0Modes()); d.Status != contract.Accepted {
		t.Fatalf("seed: %s", d.Reason)
	}
	return s, k, cs
}

func TestServerLoopProposeAccepts(t *testing.T) {
	s, _, cs := newServerWith(t, rule.NewRuleSet(proposeRule()))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ds, err := cs.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(ds) != 1 || ds[0].Status != contract.Accepted {
		t.Fatalf("propose-rule must lead to one Accepted decision; got %+v", ds)
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 2 {
		t.Fatalf("m1 must advance to @2; got %d", v)
	}
	claimed, _ := s.ClaimOutbox("w", time.Minute)
	if len(claimed) != 1 || claimed[0].Kind != "invalidation" {
		t.Fatalf("an accepted decision must enqueue an outbox invalidation; got %+v", claimed)
	}
}

func TestServerLoopDenyEmitsDiagnostic(t *testing.T) {
	s, _, cs := newServerWith(t, rule.NewRuleSet(denyRule()))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ds, err := cs.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(ds) != 0 {
		t.Fatalf("deny must produce no proposal/decision; got %+v", ds)
	}
	countDiag := func() int {
		evs, _ := s.PendingEvents(0)
		n := 0
		for _, ev := range evs {
			if ev.Type == "memory.diagnostic" {
				n++
			}
		}
		return n
	}
	if countDiag() != 1 {
		t.Fatalf("deny must emit exactly one memory.diagnostic event; got %d", countDiag())
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 1 {
		t.Fatalf("deny must not change state; m1 must stay @1, got %d", v)
	}
	// idempotent: a second Tick must not re-process the consumed observation (cursor advanced once).
	ds2, _ := cs.Tick()
	if len(ds2) != 0 {
		t.Fatalf("second tick must be a no-op; got %+v", ds2)
	}
	if countDiag() != 1 {
		t.Fatalf("diagnostic must not be re-emitted on a second tick; got %d", countDiag())
	}
}

func TestIngestOverwritesActorFromPrincipal(t *testing.T) {
	s, _, cs := newServerWith(t, rule.NewRuleSet(proposeRule()))
	seq, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", Actor: "admin"}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	evs, _ := s.PendingEvents(seq - 1)
	if len(evs) == 0 || evs[0].Actor != "agent" {
		t.Fatalf("ingested event actor must be the principal, not the payload claim; got %+v", evs)
	}
}
