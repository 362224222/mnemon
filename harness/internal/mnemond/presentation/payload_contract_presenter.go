package presentation

import (
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

type payloadContractPresenter struct{}

func (payloadContractPresenter) Intent() string { return IntentPayloadContract }

func (payloadContractPresenter) Present(_ Request, _ view.View, _ time.Time) (PresentationBody, error) {
	return PresentationBody{Body: BuildPayloadContract()}, nil
}

func BuildPayloadContract() string {
	return strings.Join([]string{
		"[mnemon:payload-contract]",
		"Emit governed events through mnemon observe; do not write canonical state directly.",
		"- Payloads are R2 objects with rule, narrative, and refs sections; do not put business fields at the top level.",
		"- agent_profile.write_candidate.observed requires rule.actor, rule.availability, rule.ttl, narrative.focus, narrative.context_advantages, narrative.summary.",
		"- teamwork_signal.write_candidate.observed requires rule.scope, rule.ttl, narrative.statement, narrative.why_teamwork, refs.evidence_refs.",
		"- assignment.write_candidate.observed requires rule.assignee, rule.scope, rule.ttl, narrative.expected_work, narrative.expected_feedback, refs.evidence_refs.",
		"- progress_digest.write_candidate.observed requires rule.feedback_kind and narrative.summary; include rule.assignment_ref when reporting assignment feedback.",
	}, "\n")
}
