package runtime

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

// DrainOutbox is the driver's out-of-band claim of projection invalidations: an accepted apply
// enqueues an invalidation, the driver claims + acks it, and a
// re-drain finds nothing.
func TestDrainOutboxClaimsInvalidations(t *testing.T) {
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{
		Rules:       admission.NewRuleSet(createOnObserve()),
		Authority:   kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"agent": {"memory"}}},
		Subs:        map[contract.ActorID]contract.Subscription{"agent": {Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}}},
		SchemaGuard: kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	if _, _, err := rt.API().Ingest("agent", contract.ObservationEnvelope{
		ExternalID: "e1", Event: contract.Event{Type: "memory.observed", Payload: map[string]any{}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	refs, drained, err := rt.DrainOutbox()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if drained != 1 {
		t.Fatalf("an accepted apply must enqueue exactly one invalidation to drain; got %d", drained)
	}
	if len(refs) != 1 || refs[0].Kind != "memory" || refs[0].ID != "m1" {
		t.Fatalf("drain must return the invalidated refs (deduped); got %v", refs)
	}
	if _, drained2, err := rt.DrainOutbox(); err != nil || drained2 != 0 {
		t.Fatalf("a re-drain must find nothing; got %d (err %v)", drained2, err)
	}
}

// DrainOutbox must PRUNE what it acks: acked rows are never re-read, so leaving them accumulates
// one dead row per accepted decision for the life of the project.
func TestDrainOutboxPrunesAckedRows(t *testing.T) {
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{
		Rules:       admission.NewRuleSet(createOnObserve()),
		Authority:   kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"agent": {"memory"}}},
		Subs:        map[contract.ActorID]contract.Subscription{"agent": {Actor: "agent", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}}},
		SchemaGuard: kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("agent", contract.ObservationEnvelope{
		ExternalID: "e1", Event: contract.Event{Type: "memory.observed", Payload: map[string]any{}},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}
	if _, drained, err := rt.DrainOutbox(); err != nil || drained != 1 {
		t.Fatalf("drain: n=%d err=%v", drained, err)
	}
	if left, err := rt.store.DeleteAckedOutbox("invalidation"); err != nil || left != 0 {
		t.Fatalf("DrainOutbox must have pruned its acked rows; a manual prune still found %d (err %v)", left, err)
	}
}
