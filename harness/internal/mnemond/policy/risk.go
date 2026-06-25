package policy

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// RiskEvidenceGate is the mid-risk governance gate (P3 three-tier risk): a candidate for this
// event package's kind must carry a non-empty `evidence` field, else it is DENIED with a durable
// diagnostic. It is a SEPARATE rule that handles the same observed type as the admission rule; when
// it denies, admission.Evaluate's deny-priority reduction makes the deny outrank the admission rule's
// propose, so the write is refused — no new kernel verdict or held state (M1 review correction). It
// gates on the cap's principal (a foreign principal's event passes through) and emits no proposal.
//
// High-risk (operator-only) gating is assembled only when a static event package selects the high tier
// and the local bindings include a control-agent path; a high-risk gate without an operator
// principal to exempt would make a kind ungovernable.
func RiskEvidenceGate(cap EventPackage, principal contract.ActorID) admission.Rule {
	return admission.NewNativeRule("risk-evidence:"+cap.Name+":"+string(principal), principal, "", []string{cap.ObservedType},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			if strings.TrimSpace(stringField(in.Event.Payload, "evidence")) == "" {
				return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{
					fmt.Sprintf("mid-risk %s candidate denied: evidence is required", cap.ResourceKind)}}, nil
			}
			return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
		})
}

// RiskOperatorGate is the high-risk governance gate: it DENIES the gated principal's candidate
// with a durable diagnostic — the agent's high-risk proposal lands in the Inbox, and a human/operator
// (a control-agent principal) re-submits the same candidate through the normal admission path. The
// assembler builds this gate ONLY for NON-operator (host-agent) principals, so the operator's own
// high-risk candidate is never gated. Like the evidence gate, the deny outranks the admission propose
// (admission.Evaluate is deny-priority) — no new kernel verdict or held state (the M1 correction).
func RiskOperatorGate(cap EventPackage, principal contract.ActorID) admission.Rule {
	return admission.NewNativeRule("risk-operator:"+cap.Name+":"+string(principal), principal, "", []string{cap.ObservedType},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			if in.Event.Actor != principal {
				return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
			}
			return contract.RuleDecision{Verdict: contract.VerdictDeny, Reasons: []string{
				fmt.Sprintf("high-risk %s candidate denied: needs operator approval (re-submit as a control-agent)", cap.ResourceKind)}}, nil
		})
}
