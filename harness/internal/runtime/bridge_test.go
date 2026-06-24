package runtime

import (
	"strconv"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

func seqGen() func() string {
	n := 0
	return func() string { n++; return "id-" + strconv.Itoa(n) }
}
func fixedNow() func() string { return func() string { return "2026-06-04T00:00:00Z" } }
func newBridge() *Bridge      { return NewBridge(seqGen(), fixedNow()) }

func TestStampUsesTrustedSourcesNotPayload(t *testing.T) {
	br := newBridge()
	b := ResolvedBinding{Actor: "agent", Emits: "memory.write.proposed"}
	proj := eventview.EventView{Ref: "proj_abc", Digest: "abc",
		Resources: []contract.ResourceVersion{{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Version: 3}}}
	trigger := contract.Event{ID: "ev-trigger", Type: "memory.observed", CorrelationID: "corr-1"}
	// hostile intent tries to escalate identity / forge a read-set via payload; the write itself is in-scope:
	intent := contract.ProposedEvent{Type: "memory.write.proposed", Payload: map[string]any{
		"actor": "admin", "based_on": "forged",
		"writes": []contract.ResourceWrite{{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 3, Fields: map[string]any{"content": "x"}}},
	}}
	ev, err := br.Stamp(b, proj, trigger, intent)
	if err != nil {
		t.Fatalf("in-scope write must stamp: %v", err)
	}
	if ev.Actor != "agent" {
		t.Fatalf("Actor must come from binding, not payload; got %q", ev.Actor)
	}
	if len(ev.BasedOn) != 1 || ev.BasedOn[0].Version != 3 {
		t.Fatalf("BasedOn must be the dispatched event view's read-set; got %+v", ev.BasedOn)
	}
	if ev.EventViewRef != "proj_abc" || ev.ContextDigest != "abc" {
		t.Fatalf("provenance must come from the event view; got ref=%q digest=%q", ev.EventViewRef, ev.ContextDigest)
	}
	if ev.CorrelationID != "corr-1" || ev.CausedBy != "ev-trigger" {
		t.Fatalf("correlation/lineage must come from the trigger; got corr=%q causedBy=%q", ev.CorrelationID, ev.CausedBy)
	}
	if ev.Type != "memory.write.proposed" {
		t.Fatalf("Type must be the binding's Emits; got %q", ev.Type)
	}
	if ev.SchemaVersion != 1 || ev.TS == "" {
		t.Fatalf("envelope must be complete (schema_version=1, non-empty ts); got %d / %q", ev.SchemaVersion, ev.TS)
	}
}

func TestStampMintsCorrelationWhenTriggerEmpty(t *testing.T) {
	br := newBridge()
	b := ResolvedBinding{Actor: "agent", Emits: "memory.write.proposed"}
	// empty-writes intent passes the bridge (the kernel rejects it later as a malformed/empty op):
	ev, err := br.Stamp(b, eventview.EventView{}, contract.Event{ID: "t"}, contract.ProposedEvent{Type: "memory.write.proposed"})
	if err != nil {
		t.Fatalf("empty-writes intent must pass the bridge: %v", err)
	}
	if ev.CorrelationID == "" {
		t.Fatal("CorrelationID must be minted non-empty when the trigger has none (escalation requires it)")
	}
}

// R11: a write targeting a ref outside the actor's dispatched scope must be REJECTED at the bridge — the
// kernel's authz is actor/kind only, so the bridge is the sole ref-level gate.
func TestStampRejectsOutOfScopeWrite(t *testing.T) {
	br := newBridge()
	b := ResolvedBinding{Actor: "agent", Emits: "memory.write.proposed"}
	proj := eventview.EventView{Resources: []contract.ResourceVersion{{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Version: 1}}} // scope = {m1}
	intent := contract.ProposedEvent{Type: "memory.write.proposed", Payload: map[string]any{
		"writes": []contract.ResourceWrite{{Ref: contract.ResourceRef{Kind: "memory", ID: "m2"}, Kind: contract.OpUpdate, BasedOn: 0}}}} // m2 NOT in scope
	if _, err := br.Stamp(b, proj, contract.Event{ID: "t"}, intent); err == nil {
		t.Fatal("write to a ref outside the dispatched scope must be rejected (R11)")
	}
}
