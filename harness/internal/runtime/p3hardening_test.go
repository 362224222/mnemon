package runtime

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

// #9: PullEventView must serve only the actor's CONFIGURED scope; client-named out-of-scope refs are denied.
func TestPullEventViewEnforcesConfiguredScope(t *testing.T) {
	_, k, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	// a resource the agent is NOT configured to see (agentSubs scope = {m1}).
	if d := k.Apply(contract.KernelOp{OpID: "secret", Actor: "agent", Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "secret"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "top"}}}}, p0Modes()); d.Status != contract.Accepted {
		t.Fatalf("seed secret: %s", d.Reason)
	}
	proj, err := cs.PullEventView("agent", contract.Subscription{Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "secret"}}})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	for _, rv := range proj.Resources {
		if rv.Ref.ID == "secret" {
			t.Fatal("a client-named out-of-scope ref must NOT be served (S9: server enforces the configured scope)")
		}
	}
}

// #5: concurrent Tick must be data-race-free and never double-dispatch (run under -race).
func TestConcurrentTickIsSafe(t *testing.T) {
	s, _, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = cs.Tick() }()
	}
	wg.Wait()
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 2 {
		t.Fatalf("concurrent Tick must dispatch the observation exactly once (m1 @2); got %d", v)
	}
}

// re-verify MED: a decision the kernel committed (advancing the reconciler cursor) but whose side-effects the
// server crashed before producing must be RECOVERABLE from the durable decision log — the S2 invalidation is
// not permanently lost.
func TestDecisionSideEffectsRecoveredFromLog(t *testing.T) {
	s, k, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	// simulate a committed-but-unprocessed decision: apply it directly (a reconciler-style Accepted decision
	// with IngestSeq>0), bypassing the server's side-effect step ("crash" before handleDecisions).
	d := k.Apply(contract.KernelOp{OpID: "p", Actor: "agent", IngestSeq: 99, Writes: []contract.ResourceWrite{
		{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "x"}}}}, p0Modes())
	if d.Status != contract.Accepted {
		t.Fatalf("setup apply: %s", d.Reason)
	}
	if c, _ := s.ClaimOutbox("probe", time.Minute, "invalidation"); len(c) != 0 {
		t.Fatalf("precondition: no invalidation should exist yet; got %d", len(c))
	}
	if _, err := cs.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	claimed, _ := s.ClaimOutbox("invalidation-worker", time.Minute, "invalidation")
	found := false
	for _, r := range claimed {
		if r.IdempotencyKey == "inv_"+d.DecisionID {
			found = true
		}
	}
	if !found {
		t.Fatalf("a committed decision's invalidation must be recoverable from the decision log; got %+v", claimed)
	}
}

// re-verify LOW: a VerdictWarn must surface its reasons as a diagnostic, not be silently dropped.
func TestWarnVerdictEmitsDiagnostic(t *testing.T) {
	warnRule := admission.NewNativeRule("w", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictWarn, Reasons: []string{"heads up"}}, nil
		})
	s, _, cs := newServerWith(t, admission.NewRuleSet(warnRule))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := cs.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	found := false
	for _, dg := range diagEvents(t, s) {
		if reason, _ := dg.Payload["reason"].(string); strings.Contains(reason, "heads up") {
			found = true
		}
	}
	if !found {
		t.Fatal("a warn verdict must surface its reasons as a diagnostic (no silent warn)")
	}
}

// re-verify LOW: warn reasons must surface even when a higher verdict (propose) wins — never a silent warn.
func TestWarnReasonsSurfacedWhenProposeWins(t *testing.T) {
	warn := admission.NewNativeRule("warn", "agent", "x.proposed", []string{"memory.observed"},
		func(admission.RuleInput) (contract.RuleDecision, error) {
			return contract.RuleDecision{Verdict: contract.VerdictWarn, Reasons: []string{"DANGER"}}, nil
		})
	s, _, cs := newServerWith(t, admission.NewRuleSet(warn, proposeRule()))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := cs.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 2 {
		t.Fatalf("propose must still win (m1 @2); got %d", v)
	}
	found := false
	for _, dg := range diagEvents(t, s) {
		if reason, _ := dg.Payload["reason"].(string); strings.Contains(reason, "DANGER") {
			found = true
		}
	}
	if !found {
		t.Fatal("warn reasons must surface as a diagnostic even when propose wins")
	}
}

// adversarial: re-scanning a *.proposed event (which carries a provenance digest, not an edge echo) on a
// later Tick must NOT emit a spurious stage:readback diagnostic.
func TestProposedEventReScanEmitsNoSpuriousReadback(t *testing.T) {
	s, _, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := cs.Tick(); err != nil { // propose accepted; a *.proposed event (with a stamped digest) is logged
		t.Fatalf("tick1: %v", err)
	}
	countReadback := func() int {
		n := 0
		for _, dg := range diagEvents(t, s) {
			if dg.Payload["stage"] == "readback" {
				n++
			}
		}
		return n
	}
	before := countReadback()
	if _, err := cs.Tick(); err != nil { // re-scans the *.proposed event
		t.Fatalf("tick2: %v", err)
	}
	if countReadback() != before {
		t.Fatalf("re-scanning a *.proposed event must not emit a spurious readback diagnostic; %d -> %d", before, countReadback())
	}
}

func hasDiagStage(t *testing.T, s *state.Store, stage string) bool {
	t.Helper()
	for _, dg := range diagEvents(t, s) {
		if dg.Payload["stage"] == stage {
			return true
		}
	}
	return false
}
