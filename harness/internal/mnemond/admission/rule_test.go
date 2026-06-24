package admission

import (
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// evidenceRule denies if the observed event has no "evidence", else proposes a memory write.
func evidenceRule() Rule {
	return NewNativeRule("evidence-gate", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(in RuleInput) (contract.RuleDecision, error) {
			if _, ok := in.Event.Payload["evidence"]; !ok {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"missing evidence"}}, nil
			}
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Payload: map[string]any{"writes": []contract.ResourceWrite{}}}}, nil
		})
}

func TestNativeRuleDeniesOrProposes(t *testing.T) {
	r := evidenceRule()
	d, err := r.Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if err != nil || d.Verdict != contract.VerdictDeny {
		t.Fatalf("missing evidence -> deny; got %+v err=%v", d, err)
	}
	d2, _ := r.Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed", Payload: map[string]any{"evidence": "x"}}})
	if d2.Verdict != contract.VerdictPropose {
		t.Fatalf("evidence -> propose; got %+v", d2)
	}
	if d2.Proposal == nil || d2.Proposal.Type != "memory.write.proposed" {
		t.Fatalf("propose must carry a proposal stamped with the rule's emit type; got %+v", d2.Proposal)
	}
}

func TestRuleSetDenyBeatsAll(t *testing.T) {
	denier := NewNativeRule("denier", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{"nope"}}, nil
		})
	proposer := NewNativeRule("proposer", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{}}, nil
		})
	d, diags := NewRuleSet(proposer, denier).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if d.Verdict != contract.VerdictDeny {
		t.Fatalf("deny must beat propose regardless of order; got %+v", d)
	}
	if len(diags) != 0 {
		t.Fatalf("no erroring rule -> no diagnostics; got %+v", diags)
	}
}

func TestRuleSetWarnBeatsAllow(t *testing.T) {
	allow := NewNativeRule("a", "agent", "x.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
		})
	warn := NewNativeRule("w", "agent", "x.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictWarn, Reasons: []string{"heads up"}}, nil
		})
	d, _ := NewRuleSet(allow, warn).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if d.Verdict != contract.VerdictWarn {
		t.Fatalf("warn must beat allow; got %+v", d)
	}
}

func TestRuleSetErroringRuleContributesDiagnosticNotIntent(t *testing.T) {
	boomer := NewNativeRule("boomer", "agent", "x.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{}}, errors.New("boom")
		})
	d, diags := NewRuleSet(boomer).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if d.Verdict != contract.VerdictAllow {
		t.Fatalf("an erroring rule must contribute nothing (verdict stays allow); got %+v", d)
	}
	if len(diags) != 1 || diags[0].Ref != "boomer" {
		t.Fatalf("erroring rule must contribute exactly one diagnostic naming it; got %+v", diags)
	}
}

// S7 fail-closed: a rule returning an UNKNOWN verdict (e.g. the retired "enqueue_job") contributes
// zero intent and exactly one diagnostic naming it — never a silent zero-rank swallow.
func TestRuleSetUnknownVerdictContributesDiagnosticNotIntent(t *testing.T) {
	stale := NewNativeRule("stale", "agent", "x.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.RuleVerdict("enqueue_job")}, nil
		})
	d, diags := NewRuleSet(stale).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if d.Verdict != contract.VerdictAllow {
		t.Fatalf("an unknown verdict must contribute nothing (verdict stays allow); got %+v", d)
	}
	if len(diags) != 1 || diags[0].Ref != "stale" || diags[0].Stage != "rule" {
		t.Fatalf("an unknown verdict must contribute exactly one stage:rule diagnostic naming the rule; got %+v", diags)
	}
}

func TestRuleSetSkipsNonHandledTypes(t *testing.T) {
	d, _ := NewRuleSet(evidenceRule()).Evaluate(RuleInput{Event: contract.Event{Type: "goal.observed"}})
	if d.Verdict != contract.VerdictAllow {
		t.Fatalf("a rule that doesn't handle the type must not fire; got %+v", d)
	}
}
