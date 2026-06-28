package runtime

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// mustEventAtSeq reads the stored event at ingest seq via the durable log (re-stamped from rowid).
func mustEventAtSeq(t *testing.T, cs *ControlServer, seq int64) contract.Event {
	t.Helper()
	evs, err := cs.store.PendingEvents(0)
	if err != nil {
		t.Fatalf("pending events: %v", err)
	}
	for _, e := range evs {
		if e.IngestSeq == seq {
			return e
		}
	}
	t.Fatalf("no stored event at seq %d", seq)
	return contract.Event{}
}

// Intake must stamp the server-authoritative fields from the AUTHENTICATED principal and zero the
// client-forgeable provenance, never trusting the payload claim (D7/S9). A client cannot forge the
// actor, id, ts, schema version, read-set, or presentation-view ref.
func TestIngestStampsServerFields(t *testing.T) {
	_, _, cs := newServerWith(t, admission.NewRuleSet())
	seq, _, err := cs.Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "x1",
		Event: contract.Event{
			Type: "progress_digest.write_candidate.observed", Actor: "ATTACKER", ID: "forged", TS: "forged",
			BasedOn:             []contract.ResourceVersion{{Ref: contract.ResourceRef{Kind: "progress_digest", ID: "p"}, Version: 9}},
			PresentationViewRef: "forged-ref",
			CorrelationID:       "corr-keep",
			Payload:             runtimeR2Progress("x"),
		},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	ev := mustEventAtSeq(t, cs, seq)
	if ev.Actor != "codex@project" {
		t.Fatalf("actor must be stamped from the principal; got %q", ev.Actor)
	}
	if ev.ID == "forged" || ev.ID == "" {
		t.Fatalf("id must be server-minted; got %q", ev.ID)
	}
	if ev.TS == "forged" || ev.TS == "" {
		t.Fatalf("ts must be server-stamped; got %q", ev.TS)
	}
	if ev.SchemaVersion != 1 {
		t.Fatalf("schema_version must be stamped to 1; got %d", ev.SchemaVersion)
	}
	if len(ev.BasedOn) != 0 || ev.PresentationViewRef != "" {
		t.Fatalf("forgeable read-set/presentation-view ref must be zeroed; got based_on=%+v presentation_view_ref=%q", ev.BasedOn, ev.PresentationViewRef)
	}
	if ev.CorrelationID != "corr-keep" {
		t.Fatalf("correlation id must be preserved; got %q", ev.CorrelationID)
	}
}
