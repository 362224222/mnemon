package runtime

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

// ruleProposing always proposes the given writes for memory.observed (used to exercise each reject class).
func ruleProposing(id string, writes []contract.ResourceWrite) admission.Rule {
	return admission.NewNativeRule(id, "agent", "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Type: "memory.write.proposed", Payload: map[string]any{"writes": writes}}}, nil
		})
}

func diagEvents(t *testing.T, s *state.Store) []contract.Event {
	t.Helper()
	evs, _ := s.PendingEvents(0)
	var out []contract.Event
	for _, ev := range evs {
		if strings.HasSuffix(ev.Type, ".diagnostic") {
			out = append(out, ev)
		}
	}
	return out
}

func observe(t *testing.T, cs *ControlServer) {
	t.Helper()
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
}

func TestOutOfScopeProposalEmitsBridgeDiagnostic(t *testing.T) {
	r := ruleProposing("evil", []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m2"}, Kind: contract.OpUpdate, BasedOn: 0, Fields: map[string]any{"content": "x"}}})
	s, _, cs := newServerWith(t, admission.NewRuleSet(r))
	observe(t, cs)
	ds, err := cs.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if len(ds) != 0 {
		t.Fatalf("out-of-scope proposal must yield no decision; got %+v", ds)
	}
	diags := diagEvents(t, s)
	if len(diags) != 1 || diags[0].Payload["stage"] != "bridge" {
		t.Fatalf("out-of-scope write must emit a stage:bridge diagnostic; got %+v", diags)
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m2"}); v != 0 {
		t.Fatalf("out-of-scope resource must not be created; got %d", v)
	}
}

func TestSchemaRejectEmitsDiagnostic(t *testing.T) {
	r := ruleProposing("schemabad", []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{}}})
	s, _, cs := newServerWith(t, admission.NewRuleSet(r))
	observe(t, cs)
	ds, _ := cs.Tick()
	if len(ds) != 1 || ds[0].Status != contract.Rejected {
		t.Fatalf("schema-invalid proposal must be Rejected; got %+v", ds)
	}
	diags := diagEvents(t, s)
	if len(diags) != 1 {
		t.Fatalf("schema reject must emit exactly one diagnostic; got %d", len(diags))
	}
	reason, _ := diags[0].Payload["reason"].(string)
	if !strings.Contains(reason, "memory") || !strings.Contains(reason, "content") {
		t.Fatalf("schema diagnostic must name the kind and field; got %q", reason)
	}
}

func TestCASConflictEmitsDiagnosticNamingVersion(t *testing.T) {
	r := ruleProposing("stale", []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 99, Fields: map[string]any{"content": "x"}}})
	s, _, cs := newServerWith(t, admission.NewRuleSet(r))
	observe(t, cs)
	ds, _ := cs.Tick()
	if len(ds) != 1 || ds[0].Status == contract.Accepted {
		t.Fatalf("stale-based_on proposal must not be Accepted; got %+v", ds)
	}
	diags := diagEvents(t, s)
	if len(diags) != 1 {
		t.Fatalf("CAS conflict must emit exactly one diagnostic; got %d", len(diags))
	}
	reason, _ := diags[0].Payload["reason"].(string)
	if !strings.Contains(reason, "memory/m1") || !strings.Contains(reason, "actual v1") {
		t.Fatalf("CAS diagnostic must name the raced version (actual v1); got %q", reason)
	}
}
