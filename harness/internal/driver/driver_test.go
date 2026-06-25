package driver

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func createRule() admission.Rule {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	return admission.NewNativeRule("creator", "agent", "memory.write.proposed", []string{"memory.observed"},
		func(in admission.RuleInput) (contract.RuleDecision, error) {
			for _, rv := range in.View.Resources {
				if rv.Ref == ref && rv.Version > 0 {
					return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
				}
			}
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Type:    "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{{Ref: ref, Kind: contract.OpCreate, Fields: map[string]any{"content": "x"}}}},
			}}, nil
		})
}

func bootRuntime(t *testing.T) *runtime.Runtime {
	t.Helper()
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "s.db"), runtime.RuntimeConfig{
		Rules:       admission.NewRuleSet(createRule()),
		Authority:   state.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"agent": {"memory"}}},
		Subs:        map[contract.ActorID]contract.Subscription{"agent": {Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}}},
		SchemaGuard: state.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}, "goal": {"statement"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

// The co-hosted driver drives the governed Tick, drains a projection invalidation, and re-projects —
// out-of-band, over the runtime's OWN store (no second opener). Re-projection fires only when an
// invalidation was actually drained.
func TestDriverDrainsAndReprojectsOutOfBand(t *testing.T) {
	rt := bootRuntime(t)
	if _, _, err := rt.API().Ingest("agent", contract.ObservationEnvelope{
		ExternalID: "e1", Event: contract.Event{Type: "memory.observed", Payload: map[string]any{}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	reprojected := 0
	var gotRefs []contract.ResourceRef
	d := New(rt, func(refs []contract.ResourceRef) error { reprojected++; gotRefs = refs; return nil }, time.Hour)

	if err := d.Tick(context.Background()); err != nil {
		t.Fatalf("driver tick: %v", err)
	}
	if reprojected != 1 {
		t.Fatalf("the driver must re-project after draining an invalidation; got %d", reprojected)
	}
	if len(gotRefs) != 1 || gotRefs[0].Kind != "memory" {
		t.Fatalf("the reproject callback must receive the invalidated refs; got %v", gotRefs)
	}
	// the apply landed in the runtime's own store
	if v, _, _ := rt.Resource(contract.ResourceRef{Kind: "memory", ID: "m1"}); v == 0 {
		t.Fatal("the driver's Tick must have applied the proposal to the shared store")
	}

	if err := d.Tick(context.Background()); err != nil {
		t.Fatalf("driver tick 2: %v", err)
	}
	if reprojected != 1 {
		t.Fatalf("no new invalidation -> no re-projection; got %d", reprojected)
	}
}

// Run loops until the context is cancelled and returns cleanly (clean shutdown).
func TestDriverRunStopsOnContextCancel(t *testing.T) {
	rt := bootRuntime(t)
	d := New(rt, func(refs []contract.ResourceRef) error { return nil }, 10*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := d.Run(ctx); err != context.DeadlineExceeded {
		t.Fatalf("Run must return the context error on shutdown; got %v", err)
	}
}
