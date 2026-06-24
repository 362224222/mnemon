package runtime

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// Multi-machine SEMANTICS: two independent execution surfaces (edges) over real loopback HTTP hit ONE
// canonical writer; a cross-edge CAS conflict resolves deterministically (one accept, one defer).
func TestTwoEdgesConflictOverHTTP(t *testing.T) {
	s, _, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	srv := httptest.NewServer(access.NewHTTPHandler(cs))
	defer srv.Close()
	edgeA := access.NewClient(srv.URL, "agent")
	edgeB := access.NewClient(srv.URL, "agent")
	if _, _, err := edgeA.Ingest("agent", contract.ObservationEnvelope{ExternalID: "edgeA-1", Event: contract.Event{Type: "memory.observed", CorrelationID: "cA"}}); err != nil {
		t.Fatalf("edgeA ingest: %v", err)
	}
	if _, _, err := edgeB.Ingest("agent", contract.ObservationEnvelope{ExternalID: "edgeB-1", Event: contract.Event{Type: "memory.observed", CorrelationID: "cB"}}); err != nil {
		t.Fatalf("edgeB ingest: %v", err)
	}
	ds, err := cs.Tick()
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	var accepted, deferred int
	for _, d := range ds {
		switch d.Status {
		case contract.Accepted:
			accepted++
		case contract.Deferred:
			deferred++
			if d.NextAction != "rebase" {
				t.Fatalf("conflict loser must defer to rebase; got %q", d.NextAction)
			}
		}
	}
	if accepted != 1 || deferred != 1 {
		t.Fatalf("two racing edges must yield exactly one accept + one defer; got %d accept, %d defer (%+v)", accepted, deferred, ds)
	}
	if v, _ := s.GetVersion(contract.ResourceRef{Kind: "memory", ID: "m1"}); v != 2 {
		t.Fatalf("m1 must advance by exactly one; got %d", v)
	}
	found := false
	for _, dg := range diagEvents(t, s) {
		if reason, _ := dg.Payload["reason"].(string); strings.Contains(reason, "memory/m1") && strings.Contains(reason, "actual v2") {
			found = true
		}
	}
	if !found {
		t.Fatalf("deferred conflict must emit a diagnostic naming the raced version (actual v2); got %+v", diagEvents(t, s))
	}
}

func TestHTTPIngestTakesPrincipalFromHeaderNotBody(t *testing.T) {
	s, _, cs := newServerWith(t, admission.NewRuleSet(proposeRule()))
	srv := httptest.NewServer(access.NewHTTPHandler(cs))
	defer srv.Close()
	edge := access.NewClient(srv.URL, "agent")
	seq, _, err := edge.Ingest("agent", contract.ObservationEnvelope{ExternalID: "x", Event: contract.Event{Type: "memory.observed", Actor: "admin"}})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	evs, _ := s.PendingEvents(seq - 1)
	if len(evs) == 0 || evs[0].Actor != "agent" {
		t.Fatalf("actor must be the authenticated header principal, not the body claim; got %+v", evs)
	}
}
