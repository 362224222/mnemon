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
		"- agent_profile.write_candidate.observed requires actor, focus, context_advantages, availability, ttl, summary.",
		"- teamwork_signal.write_candidate.observed requires scope, statement, why_teamwork, ttl.",
		"- assignment.write_candidate.observed requires assignee, scope, expected_work, expected_feedback, ttl.",
		"- progress_digest.write_candidate.observed requires summary; include assignment_ref when reporting assignment feedback.",
	}, "\n")
}
