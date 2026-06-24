package replay

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func proposeWrite(id string, w contract.ResourceWrite) contract.Event {
	return contract.Event{ID: id, Type: "memory.write.proposed", Actor: "agent",
		Payload: map[string]any{"writes": []contract.ResourceWrite{w}}}
}

// liveDecisions produces decisions the canonical way: append the proposed events to a fresh kernel and
// reconcile (the same modes Replay uses), returning the store + decisions.
func liveDecisions(t *testing.T, events []contract.Event) (*store.Store, []contract.Decision) {
	t.Helper()
	s, err := store.OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	k := kernel.NewKernel(s, kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}), permissiveAuthority(events))
	r := admission.NewReconciler(s, k)
	for _, ev := range events {
		if _, err := s.AppendEvent(ev); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	return s, r.RunOnce(canonicalModes)
}

var sampleEvents = []contract.Event{
	proposeWrite("p1", contract.ResourceWrite{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpCreate, Fields: map[string]any{"content": "v1"}}),
	proposeWrite("p2", contract.ResourceWrite{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "v2"}}),
	proposeWrite("p3", contract.ResourceWrite{Ref: contract.ResourceRef{Kind: "memory", ID: "m1"}, Kind: contract.OpUpdate, BasedOn: 1, Fields: map[string]any{"content": "stale"}}), // stale -> conflict
}

// I6 determinism under data-driven kinds (PD2b): replay derives the kernel's schema guard from the
// LOG, not a compiled default — a proposal writing a kind absent from kernel.DefaultSchemaGuard
// (e.g. an external/declared kind, or a graduated memory/skill after PD5) must still reconcile to
// Accepted, exactly as the live run that produced the log did. A guard pinned to DefaultSchemaGuard
// would reject it as an unknown kind, silently breaking I6 once user kinds leave the compiled set.
func TestReplayDerivesGuardFromLogNotDefault(t *testing.T) {
	ev := contract.Event{ID: "w1", Type: "widget.write.proposed", Actor: "agent",
		Payload: map[string]any{"writes": []contract.ResourceWrite{{
			Ref: contract.ResourceRef{Kind: "widget", ID: "x1"}, Kind: contract.OpCreate,
			Fields: map[string]any{"content": "v"}}}}}
	decisions := Replay([]contract.Event{ev}, admission.RuleSet{})
	if len(decisions) != 1 {
		t.Fatalf("want 1 decision, got %d", len(decisions))
	}
	if decisions[0].Status != contract.Accepted {
		t.Fatalf("a kind present only in the log must reconcile to Accepted (replay guard is log-derived), got %q", decisions[0].Status)
	}
}

// S8: replaying the event log over a FRESH throwaway kernel reproduces the live decisions, identical after
// masking the dynamic fields (DecisionID/AppliedAt).
func TestReplayReproducesDecisionsMasked(t *testing.T) {
	_, live := liveDecisions(t, sampleEvents)
	replayed := Replay(sampleEvents, admission.RuleSet{})
	if len(replayed) != len(live) || len(live) == 0 {
		t.Fatalf("replay must reproduce %d decisions; got %d", len(live), len(replayed))
	}
	for i := range live {
		l, r := maskDynamic(live[i]), maskDynamic(replayed[i])
		if l.Status != r.Status || l.OpID != r.OpID || l.IngestSeq != r.IngestSeq || l.NextAction != r.NextAction {
			t.Fatalf("decision %d differs after masking:\n live=%+v\n repl=%+v", i, l, r)
		}
	}
}

// S8: replay never touches a live store/cursor and is a pure function of the events (twice -> identical).
func TestReplayIsReadOnly(t *testing.T) {
	liveStore, _ := liveDecisions(t, sampleEvents)
	before := liveStore.DecisionCount()
	_ = Replay(sampleEvents, admission.RuleSet{})
	if liveStore.DecisionCount() != before {
		t.Fatalf("Replay must not mutate any live store; decision count %d -> %d", before, liveStore.DecisionCount())
	}
	a, b := Replay(sampleEvents, admission.RuleSet{}), Replay(sampleEvents, admission.RuleSet{})
	if len(a) != len(b) {
		t.Fatalf("Replay must be deterministic; got %d vs %d", len(a), len(b))
	}
	for i := range a {
		if maskDynamic(a[i]).Status != maskDynamic(b[i]).Status {
			t.Fatalf("Replay non-deterministic at %d", i)
		}
	}
}
