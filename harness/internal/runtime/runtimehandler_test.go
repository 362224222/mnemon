package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// createOnObserve proposes creating memory/m1 the first time it sees a memory.observed; once m1
// exists it proposes nothing (so a re-tick is a harmless no-op).
func createOnObserve() admission.Rule {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	return admission.NewNativeRule("creator", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			for _, rv := range in.View.Resources {
				if rv.Ref == ref && rv.Version > 0 {
					return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
				}
			}
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Type: "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{
					{Ref: ref, Kind: contract.OpCreate, Fields: map[string]any{"content": "created"}}}},
			}}, nil
		})
}

// TestSyncTickAfterIngest pins P2.2: the product channel endpoint drives ONE Tick after a successful
// NEW observation (synchronous local mode), so a single observe closes the governed loop with no
// manual Tick — and the receipt tells the client the observation was recorded, whether it was a
// duplicate, that processing was attempted, and any processing error. A duplicate does not re-tick.
func TestSyncTickAfterIngest(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{
		Rules:     admission.NewRuleSet(createOnObserve()),
		Authority: kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"agent": {"memory"}}},
		Subs:      map[contract.ActorID]contract.Subscription{"agent": {Actor: "agent", Refs: []contract.ResourceRef{ref}}},
		NewID:     seqGen(), Now: fixedNow(),
		SchemaGuard: kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	srv := httptest.NewServer(NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	defer srv.Close()
	c := access.NewClient(srv.URL, "agent")

	rec, err := c.IngestObserve("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}})
	if err != nil {
		t.Fatalf("observe: %v", err)
	}
	if !rec.Ticked {
		t.Fatal("a new observation must trigger a synchronous tick (processing attempted)")
	}
	if rec.ProcessingError != "" {
		t.Fatalf("clean processing must carry no error; got %q", rec.ProcessingError)
	}
	if v, _, _ := rt.Resource(ref); v == 0 {
		t.Fatal("the sync tick must produce the canonical write without a manual Tick")
	}

	// A duplicate observation (same external id) is recorded as dup and does NOT re-tick.
	rec2, err := c.IngestObserve("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}})
	if err != nil {
		t.Fatalf("re-observe: %v", err)
	}
	if !rec2.Dup {
		t.Fatal("re-observing the same external id must report dup")
	}
	if rec2.Ticked {
		t.Fatal("a duplicate observation must not re-tick")
	}
}
