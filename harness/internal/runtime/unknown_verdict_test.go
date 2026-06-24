package runtime

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// S7 fail-closed (new with the job-lane removal): a rule that returns an UNKNOWN verdict string —
// e.g. a stale rule still emitting the retired "enqueue_job" — must produce a durable stage:rule
// diagnostic event, never be silently swallowed (neither by the rule-set reduction's zero rank nor
// by dispatchOne's switch falling through).
func TestUnknownVerdictEmitsDiagnosticNotSilent(t *testing.T) {
	stale := admission.NewNativeRule("stale", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.RuleVerdict("enqueue_job")}, nil // retired => unknown
		})
	s, _, cs := newServerWith(t, admission.NewRuleSet(stale))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := cs.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if !hasDiagStage(t, s, "rule") {
		t.Fatal("an unknown verdict must emit a stage:rule diagnostic (S7 no-silent-drop), not fall through silently")
	}
	found := false
	for _, dg := range diagEvents(t, s) {
		if reason, _ := dg.Payload["reason"].(string); strings.Contains(reason, "enqueue_job") {
			found = true
		}
	}
	if !found {
		t.Fatal("the diagnostic must NAME the unknown verdict so the stale rule is debuggable")
	}
}

// S7: a propose verdict carrying no proposal is diagnosed, never silently dropped. (Re-pinned
// here after the job-lane test file that originally held this pin was deleted; the production
// branch in dispatchOne is alive and unrelated to the lane.)
func TestProposeNilProposalEmitsDiagnostic(t *testing.T) {
	nilProposer := admission.NewNativeRule("nil-proposer", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose}, nil // propose with no proposal
		})
	s, _, cs := newServerWith(t, admission.NewRuleSet(nilProposer))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := cs.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	found := false
	for _, dg := range diagEvents(t, s) {
		if reason, _ := dg.Payload["reason"].(string); strings.Contains(reason, "no proposal") {
			found = true
		}
	}
	if !found {
		t.Fatal("a propose verdict with a nil proposal must emit a stage:rule diagnostic (S7), not vanish")
	}
}
