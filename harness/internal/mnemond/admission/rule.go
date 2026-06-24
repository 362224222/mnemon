// Package admission owns mnemond's event admission pipeline. Admission rules evaluate a typed
// input (the triggering event + the scoped view it was dispatched on) and return a rich
// RuleDecision: they may propose or deny, but they never write canonical state. The kernel remains
// the only materialized-state writer.
package admission

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

// RuleInput is the typed, read-only input to a rule: the triggering event and the scoped event view it was
// dispatched on. (Held by rule, not contract, so the view.View dependency lives off the wire — D11.)
type RuleInput struct {
	Event contract.Event
	View  view.View
}

// Rule is one admission rule. Actor()/Emits() let the server synthesize a trusted ResolvedBinding for the
// bridge when a rule proposes (the proposal's write identity + authorized emit type come from the rule, never
// the payload). Handles gates which event types it sees.
type Rule interface {
	ID() string
	Actor() contract.ActorID
	Emits() string
	Handles(eventType string) bool
	Evaluate(RuleInput) (contract.RuleDecision, error)
}

// NativeRule is a Go-implemented admission rule.
type NativeRule struct {
	id      string
	actor   contract.ActorID
	emits   string
	handles map[string]bool
	fn      func(RuleInput) (contract.RuleDecision, error)
}

func NewNativeRule(id string, actor contract.ActorID, emits string, handles []string, fn func(RuleInput) (contract.RuleDecision, error)) NativeRule {
	h := make(map[string]bool, len(handles))
	for _, t := range handles {
		h[t] = true
	}
	return NativeRule{id: id, actor: actor, emits: emits, handles: h, fn: fn}
}

func (r NativeRule) ID() string              { return r.id }
func (r NativeRule) Actor() contract.ActorID { return r.actor }
func (r NativeRule) Emits() string           { return r.emits }
func (r NativeRule) Handles(t string) bool   { return r.handles[t] }
func (r NativeRule) Evaluate(in RuleInput) (contract.RuleDecision, error) {
	d, err := r.fn(in)
	if err != nil {
		return contract.RuleDecision{}, err
	}
	// A propose without an explicit Type is stamped with the rule's authorized emit type, so the server can
	// match the proposal back to its originating rule (for the trusted bridge binding) deterministically.
	if d.Verdict == contract.VerdictPropose && d.Proposal != nil && d.Proposal.Type == "" {
		d.Proposal.Type = r.emits
	}
	return d, nil
}

// ShadowReport is the diff of a candidate policy vs the live policy over the same event log (S8).
// It is owned here so replay imports admission.ShadowReport, while admission never imports replay.
// It reports diffs, never pass/fail.
type ShadowReport struct {
	Clean bool
	Diffs int
}

// RuleSet is an ordered set of rules reduced by a DENY-PRIORITY policy.
type RuleSet struct{ rules []Rule }

func NewRuleSet(rules ...Rule) RuleSet { return RuleSet{rules: rules} }

// Rules exposes the member rules so the server can find the rule that produced a proposal (to stamp the
// trusted bridge binding from its Actor()/Emits()).
func (rs RuleSet) Rules() []Rule { return rs.rules }

// verdictRank orders the reduction: deny beats everything; propose beats warn/allow; warn beats
// allow. The highest-ranked verdict among the handling rules wins.
var verdictRank = map[contract.RuleVerdict]int{
	contract.VerdictAllow:   0,
	contract.VerdictWarn:    1,
	contract.VerdictPropose: 2,
	contract.VerdictDeny:    3,
}

// Evaluate reduces every handling rule into one decision (deny-priority) plus diagnostics. An erroring rule
// contributes ZERO intent and exactly one Diagnostic naming it (S7 / Invariant #13). Warn reasons attach to
// the final decision; the first proposal for the winning verdict is carried.
func (rs RuleSet) Evaluate(in RuleInput) (contract.RuleDecision, []contract.Diagnostic) {
	out := contract.RuleDecision{Verdict: contract.VerdictAllow}
	var diags []contract.Diagnostic
	var reasons []string
	for _, r := range rs.rules {
		if !r.Handles(in.Event.Type) {
			continue
		}
		d, err := r.Evaluate(in)
		if err != nil {
			diags = append(diags, contract.Diagnostic{Stage: "rule", Reason: err.Error(), Ref: r.ID()})
			continue
		}
		// S7 fail-closed: an UNKNOWN verdict (e.g. a stale rule still emitting a retired verdict string) must
		// not be silently swallowed by its zero rank — it contributes ZERO intent and exactly one diagnostic
		// naming the rule and the verdict, mirroring the erroring-rule treatment.
		if _, known := verdictRank[d.Verdict]; !known {
			diags = append(diags, contract.Diagnostic{Stage: "rule", Reason: fmt.Sprintf("rule %q returned unknown verdict %q", r.ID(), string(d.Verdict)), Ref: r.ID()})
			continue
		}
		reasons = append(reasons, d.Reasons...)
		// A rule may only emit its DECLARED type. An empty proposal Type defaults to the rule's Emits; a NON-empty
		// type that differs is a rule trying to borrow ANOTHER rule's identity at the bridge — reject it (zero
		// intent + a diagnostic, S7), and do not let a propose verdict stand on a rejected proposal.
		if d.Verdict == contract.VerdictPropose && d.Proposal != nil {
			if d.Proposal.Type == "" {
				d.Proposal.Type = r.Emits()
			}
			if d.Proposal.Type != r.Emits() {
				diags = append(diags, contract.Diagnostic{Stage: "rule", Reason: fmt.Sprintf("rule %q proposed type %q != declared emits %q", r.ID(), d.Proposal.Type, r.Emits()), Ref: r.ID()})
				continue
			}
		}
		if verdictRank[d.Verdict] > verdictRank[out.Verdict] {
			out.Verdict = d.Verdict
		}
		if d.Verdict == contract.VerdictPropose && d.Proposal != nil && out.Proposal == nil {
			out.Proposal = d.Proposal
			out.ProposalActor = r.Actor() // TRUSTED origin: the server stamps the bridge identity from this
		}
	}
	out.Reasons = reasons
	return out, diags
}
