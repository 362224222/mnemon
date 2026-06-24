package replay

import (
	"errors"
	"math"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

func alwaysAllow(id string, actor contract.ActorID) admission.Rule {
	return admission.NewNativeRule(id, actor, "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
		})
}

// proposeAtVersion proposes only when the scoped resource is at wantVer, else allows — a version-sensitive
// policy used to expose Shadow's view TIMING.
func proposeAtVersion(id string, actor contract.ActorID, wantVer contract.Version) admission.Rule {
	return admission.NewNativeRule(id, actor, "memory.write.proposed", []string{"memory.observed"},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			rv := in.View.Resources[0]
			if rv.Version != wantVer {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{Type: "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{{Ref: rv.Ref, Kind: contract.OpUpdate, BasedOn: rv.Version, Fields: map[string]any{"content": "x"}}}}}}, nil
		})
}

func mref(id string) contract.ResourceRef {
	return contract.ResourceRef{Kind: "memory", ID: contract.ResourceID(id)}
}

// observedLog is a log a real server would produce: a bootstrap proposal that creates m1 (giving the rules
// realistic canonical state) plus an OBSERVED event that drives the rule pre-gate.
func observedLog() ([]contract.Event, map[contract.ActorID]contract.Subscription) {
	events := []contract.Event{
		proposeWrite("boot", contract.ResourceWrite{Ref: mref("m1"), Kind: contract.OpCreate, Fields: map[string]any{"content": "v0"}}),
		{ID: "o1", Type: "memory.observed", Actor: "agent", CorrelationID: "c1"},
	}
	subs := map[contract.ActorID]contract.Subscription{"agent": {Actor: "agent", Refs: []contract.ResourceRef{mref("m1")}}}
	return events, subs
}

func proposeOnObserved(id string, actor contract.ActorID, content string) admission.Rule {
	return admission.NewNativeRule(id, actor, "memory.write.proposed", []string{"memory.observed"},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			rv := in.View.Resources[0]
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{Type: "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{{Ref: rv.Ref, Kind: contract.OpUpdate, BasedOn: rv.Version, Fields: map[string]any{"content": content}}}}}}, nil
		})
}

func denyOnObserved(id string, actor contract.ActorID) admission.Rule {
	return admission.NewNativeRule(id, actor, "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictDeny}, nil
		})
}

// S8: Shadow EXERCISES the candidate's rules over the OBSERVED events (the prior model only re-reconciled the
// already-minted .proposed events, never running the candidate's observed-handling rules — false-clean for
// every real rule change). A candidate that changes observed->decision behavior (propose -> deny) is non-clean;
// an identical policy is clean; Shadow never mutates a live store.
func TestShadowExercisesCandidateOverObservedEvents(t *testing.T) {
	events, subs := observedLog()
	live := admission.NewRuleSet(proposeOnObserved("p", "agent", "x"))
	candidate := admission.NewRuleSet(denyOnObserved("d", "agent"))

	liveStore, _ := liveDecisions(t, events)
	before := liveStore.DecisionCount()

	rep := Shadow(events, subs, live, candidate)
	if rep.Clean || rep.Diffs == 0 {
		t.Fatalf("a candidate that changes observed->decision behavior must be non-clean; got %+v", rep)
	}
	if liveStore.DecisionCount() != before {
		t.Fatalf("Shadow must not mutate a live store; decisions %d -> %d", before, liveStore.DecisionCount())
	}
	if c := Shadow(events, subs, live, live); !c.Clean || c.Diffs != 0 {
		t.Fatalf("an identical policy must be clean; got %+v", c)
	}
}

// S8: the comparison covers proposal CONTENT, not just the verdict — two policies that both propose but write
// different content diverge (the old kernel-diff false-clean class cannot recur: full payload is compared).
func TestShadowCatchesProposalContentDifference(t *testing.T) {
	events, subs := observedLog()
	rep := Shadow(events, subs, admission.NewRuleSet(proposeOnObserved("p", "agent", "alpha")), admission.NewRuleSet(proposeOnObserved("p", "agent", "beta")))
	if rep.Clean || rep.Diffs == 0 {
		t.Fatalf("a candidate proposing different CONTENT must be non-clean; got %+v", rep)
	}
}

// S8: a candidate that changes the proposal's trusted ORIGIN actor (same verdict + content, different write
// identity) diverges — the origin is part of the behavior (and the R2 misattribution fix carries it).
func TestShadowCatchesOriginActorDifference(t *testing.T) {
	events, subs := observedLog()
	rep := Shadow(events, subs, admission.NewRuleSet(proposeOnObserved("p", "agent", "x")), admission.NewRuleSet(proposeOnObserved("p", "other", "x")))
	if rep.Clean || rep.Diffs == 0 {
		t.Fatalf("a candidate stamping a different origin actor must be non-clean; got %+v", rep)
	}
}

// S8 (view TIMING): the server evaluates rules at DISPATCH time (before that tick's reconcile), so Shadow must
// evaluate each observed event against the state AS IT WAS at dispatch, not the FINAL state. The log bumps m1
// from @1 to @2 AFTER the observed event. live proposes only at @1; candidate always allows. They DIVERGE at
// the dispatch-time state (m1@1) but would MATCH at the final state (m1@2) — evaluating against final state
// yields a false-clean.
func TestShadowUsesDispatchTimeStateNotFinalState(t *testing.T) {
	events := []contract.Event{
		proposeWrite("boot", contract.ResourceWrite{Ref: mref("m1"), Kind: contract.OpCreate, Fields: map[string]any{"content": "v0"}}),
		{ID: "o1", Type: "memory.observed", Actor: "agent", CorrelationID: "c1"},
		proposeWrite("bump", contract.ResourceWrite{Ref: mref("m1"), Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "bumped"}}),
	}
	subs := map[contract.ActorID]contract.Subscription{"agent": {Actor: "agent", Refs: []contract.ResourceRef{mref("m1")}}}
	live := admission.NewRuleSet(proposeAtVersion("v1", "agent", 1)) // proposes at m1@1, allows otherwise
	candidate := admission.NewRuleSet(alwaysAllow("a", "agent"))     // always allows
	rep := Shadow(events, subs, live, candidate)
	if rep.Clean {
		t.Fatalf("a divergence at the DISPATCH-time state (m1@1) must be caught even though the FINAL state (m1@2) matches; got %+v", rep)
	}
}

// S8 (diagnostics): a candidate that ERRORS (or returns a borrowed-emit proposal) reduces to Verdict allow PLUS
// a durable diagnostic. It must NOT compare equal to live's clean allow — otherwise it passes Clean and emits
// diagnostics only after promotion. Shadow compares the decision AND the diagnostic slice.
func TestShadowComparesDiagnostics(t *testing.T) {
	events, subs := observedLog()
	live := admission.NewRuleSet(alwaysAllow("a", "agent"))
	candidate := admission.NewRuleSet(admission.NewNativeRule("err", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{}, errors.New("boom")
		}))
	rep := Shadow(events, subs, live, candidate)
	if rep.Clean {
		t.Fatalf("a candidate that errors (a durable diagnostic) must NOT compare equal to live's clean allow; got %+v", rep)
	}
}

// S8/S7 (Reasons): the server writes a decision's Reasons verbatim into durable *.diagnostic events (deny/warn
// audit trail). A candidate that changes ONLY the Reasons (e.g. blanking a security-relevant deny reason)
// rewrites the auditable trail — a behavior change the Clean gate must catch, not certify.
func TestShadowComparesDenyReasons(t *testing.T) {
	events, subs := observedLog()
	denyWithReason := func(reason string) admission.Rule {
		return admission.NewNativeRule("d", "agent", "memory.write.proposed", []string{"memory.observed"},
			func(admission.RuleInput) (contract.RuleDecision, error) {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{reason}}, nil
			})
	}
	rep := Shadow(events, subs, admission.NewRuleSet(denyWithReason("SECURITY: cross-tenant leak blocked")), admission.NewRuleSet(denyWithReason("ok")))
	if rep.Clean {
		t.Fatalf("a candidate that rewrites the durable deny reason must NOT be clean (S7 audit trail); got %+v", rep)
	}
	// identical reasons -> still clean (no false positive).
	if c := Shadow(events, subs, admission.NewRuleSet(denyWithReason("same")), admission.NewRuleSet(denyWithReason("same"))); !c.Clean {
		t.Fatalf("identical reasons must stay clean; got %+v", c)
	}
}

// S8 (marshal robustness): canonicalRuleResult must not collapse to "" on a non-marshalable value. A
// non-finite float in a Proposal payload (NaN/Inf, settable by a native rule) makes json.Marshal fail; if the
// error is swallowed, two DIVERGENT decisions both render "" and compare equal -> a false-clean that masks a
// reason/verdict change. The comparison must still distinguish the other fields.
func TestShadowComparesEvenWithNonFinitePayload(t *testing.T) {
	events, subs := observedLog()
	proposeWithReason := func(reason string, cost float64) admission.Rule {
		return admission.NewNativeRule("j", "agent", "memory.write.proposed", []string{"memory.observed"},
			func(admission.RuleInput) (contract.RuleDecision, error) {
				return contract.RuleDecision{Verdict: contract.VerdictPropose, Reasons: []string{reason},
					Proposal: &contract.ProposedEvent{Type: "memory.write.proposed",
						Payload: map[string]any{"est_cost_usd": cost}}}, nil
			})
	}
	nan := math.NaN()
	rep := Shadow(events, subs, admission.NewRuleSet(proposeWithReason("SECURITY: exfil blocked", nan)), admission.NewRuleSet(proposeWithReason("routine", nan)))
	if rep.Clean {
		t.Fatalf("a NaN payload value must not collapse divergent reasons to clean; got %+v", rep)
	}
	// identical policies (even with a NaN payload) stay clean (no false positive).
	if c := Shadow(events, subs, admission.NewRuleSet(proposeWithReason("same", nan)), admission.NewRuleSet(proposeWithReason("same", nan))); !c.Clean {
		t.Fatalf("identical policies must stay clean even with a NaN payload; got %+v", c)
	}
}
