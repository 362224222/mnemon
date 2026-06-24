package runtime

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// S9/D7: a pull is scoped to the subscription and identity-bound — sub.Actor must equal the authenticated
// principal (a client cannot pull another actor's scope).
func TestPullEventViewIsScopedAndIdentityBound(t *testing.T) {
	_, _, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	proj, err := cs.PullEventView("agent", contract.Subscription{Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(proj.Resources) != 1 || proj.Resources[0].Ref.ID != "m1" {
		t.Fatalf("pull must be scoped to m1; got %+v", proj.Resources)
	}
	if _, err := cs.PullEventView("agent", contract.Subscription{Actor: "admin", Refs: nil}); err == nil {
		t.Fatal("pull with sub.Actor != principal must be rejected (forged identity, D7)")
	}
}

// S10/D8: a host that echoes a digest over tampered/stale content is caught on readback — the dependent
// write is NOT accepted; a correct echo passes through.
func TestContentTamperCaughtOnReadback(t *testing.T) {
	s, _, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	sub := contract.Subscription{Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}}
	proj, _ := cs.PullEventView("agent", sub)

	// 1) tampered echo -> mismatch -> blocked.
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "tampered", Event: contract.Event{
		Type: "memory.observed", CorrelationID: "c1", ContextDigest: "tampered-" + proj.Digest}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ds, _ := cs.Tick()
	if len(ds) != 0 {
		t.Fatalf("tampered readback must produce no decision; got %+v", ds)
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 1 {
		t.Fatalf("blocked proposal must not change state; m1 must stay @1, got %d", v)
	}
	foundReadback := false
	for _, dg := range diagEvents(t, s) {
		if dg.Payload["stage"] == "readback" {
			foundReadback = true
		}
	}
	if !foundReadback {
		t.Fatal("a tampered readback must emit a stage:readback diagnostic")
	}

	// 2) correct echo -> proposal proceeds to Accepted.
	if _, _, err := cs.Ingest("agent", contract.ObservationEnvelope{ExternalID: "correct", Event: contract.Event{
		Type: "memory.observed", CorrelationID: "c2", ContextDigest: proj.Digest}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ds2, _ := cs.Tick()
	if len(ds2) != 1 || ds2[0].Status != contract.Accepted {
		t.Fatalf("correct readback must let the proposal through to Accepted; got %+v", ds2)
	}
}
