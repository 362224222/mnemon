package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// The reducer must carry the PRODUCING rule's actor on the reduced decision, so the server stamps the bridge
// write identity from the actual producer instead of guessing by scanning for a rule with matching
// Handles/Emits (which can pick a different rule's actor).
func TestReducerCarriesProposalActor(t *testing.T) {
	proposer := NewNativeRule("p", "bob", "memory.write.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{Type: "memory.write.proposed"}}, nil
		})
	dec, _ := NewRuleSet(proposer).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if dec.ProposalActor != "bob" {
		t.Fatalf("reducer must carry the producing rule's actor; got %q", dec.ProposalActor)
	}
}

// A rule may only emit its DECLARED type. A proposal whose Type differs from the rule's Emits() (an attempt to
// borrow another rule's identity at the bridge) is rejected: no proposal carried + a diagnostic (S7).
func TestReducerRejectsBorrowedEmitType(t *testing.T) {
	borrow := NewNativeRule("b", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{Type: "goal.write.proposed"}}, nil
		})
	dec, diags := NewRuleSet(borrow).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if dec.Proposal != nil {
		t.Fatalf("a proposal whose type %q != the rule's declared Emits must not be carried", "goal.write.proposed")
	}
	if dec.ProposalActor != "" {
		t.Fatalf("a rejected proposal must carry no origin actor; got %q", dec.ProposalActor)
	}
	if len(diags) == 0 {
		t.Fatal("a borrowed-emit proposal must emit a diagnostic")
	}
}

// An empty proposal Type still defaults to the rule's Emits (and is carried with the rule's actor).
func TestReducerDefaultsEmptyProposalTypeToEmits(t *testing.T) {
	p := NewNativeRule("p", "carol", "memory.write.proposed", []string{"memory.observed"},
		func(RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{}}, nil
		})
	dec, _ := NewRuleSet(p).Evaluate(RuleInput{Event: contract.Event{Type: "memory.observed"}})
	if dec.Proposal == nil || dec.Proposal.Type != "memory.write.proposed" || dec.ProposalActor != "carol" {
		t.Fatalf("empty proposal type must default to Emits and carry the actor; got %+v actor=%q", dec.Proposal, dec.ProposalActor)
	}
}
