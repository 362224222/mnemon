package app

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assembler"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// The boot path (LocalRuntimeConfigFromBindings) must produce decision-equivalent outcomes to direct
// select-only assembly (assembler.Assemble over the in-memory config derived from the loops list).
// Before the cutover this pinned the old hand-rolled builders against Assemble; after the cutover it
// pins the app loops-derivation against direct assembly.
func TestAssembledBootMatchesBindingDerivedBoot(t *testing.T) {
	memRef := contract.ResourceRef{Kind: "memory", ID: "project"}
	skillRef := contract.ResourceRef{Kind: "skill", ID: "project"}

	mkBinding := func() access.ChannelBinding {
		b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{memRef, skillRef})
		b.AllowedObservedTypes = []string{
			"memory.write_candidate.observed",
			"skill.write_candidate.observed",
		}
		return b
	}

	drive := func(t *testing.T, rt *runtime.Runtime) {
		t.Helper()
		steps := []struct {
			id      string
			typ     string
			payload map[string]any
		}{
			{"m1", "memory.write_candidate.observed", map[string]any{"content": "parity fact", "source": "s", "confidence": "high"}},
			{"s1", "skill.write_candidate.observed", map[string]any{"skill_id": "parity-skill", "source": "s", "confidence": "high"}},
			{"m2", "memory.write_candidate.observed", map[string]any{"content": "password=hunter2", "source": "s", "confidence": "high"}},
		}
		// Tick after EACH ingest, mirroring the product's synchronous per-observe Tick (P2.2).
		// A single batched Tick would dispatch s1 against the pre-m1 view and reject its proposal
		// as read_stale — pinned dispatch-time-view semantics, identical on both paths, but not
		// the product sequence.
		for _, st := range steps {
			if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
				ExternalID: st.id,
				Event:      contract.Event{Type: st.typ, Payload: st.payload},
			}); err != nil {
				t.Fatalf("ingest %s: %v", st.id, err)
			}
			if _, err := rt.Tick(); err != nil {
				t.Fatalf("tick after %s: %v", st.id, err)
			}
		}
	}

	bootRC, err := LocalRuntimeConfigFromBindings([]access.ChannelBinding{mkBinding()}, nil)
	if err != nil {
		t.Fatalf("boot config: %v", err)
	}
	bootRT, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "boot.db"), bootRC)
	if err != nil {
		t.Fatalf("open boot runtime: %v", err)
	}
	defer bootRT.Close()

	asmRC, err := assembler.Assemble(capabilityFileFromLoops([]string{"memory", "skill"}), []access.ChannelBinding{mkBinding()}, nil)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	asmRT, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "asm.db"), asmRC)
	if err != nil {
		t.Fatalf("open assembled runtime: %v", err)
	}
	defer asmRT.Close()

	drive(t, bootRT)
	drive(t, asmRT)

	for _, ref := range []contract.ResourceRef{memRef, skillRef} {
		bv, bf, err := bootRT.Resource(ref)
		if err != nil {
			t.Fatalf("boot resource %s: %v", ref.Kind, err)
		}
		av, af, err := asmRT.Resource(ref)
		if err != nil {
			t.Fatalf("assembled resource %s: %v", ref.Kind, err)
		}
		if bv != av {
			t.Fatalf("%s version diverged: boot=%d assembled=%d", ref.Kind, bv, av)
		}
		if bv == 0 {
			t.Fatalf("%s candidate must be admitted on both paths", ref.Kind)
		}
		if !reflect.DeepEqual(bf, af) {
			t.Fatalf("%s fields diverged:\nboot:      %#v\nassembled: %#v", ref.Kind, bf, af)
		}
	}
	// The secret-like candidate must be denied on both paths: memory stays at the single admitted entry.
	if v, _, _ := bootRT.Resource(memRef); v != 1 {
		t.Fatalf("boot path admitted the denied candidate (memory v=%d)", v)
	}
}

// The hidden `local run --bindings` boot path has no localConfig: capability enablement is derived
// from the binding scope kinds ∩ EmbeddedCatalog(), so a memory/skill-scoped binding still boots both rules.
func TestLoopsFromBindingsDerivesEnablement(t *testing.T) {
	b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{
		{Kind: "memory", ID: "project"}, {Kind: "skill", ID: "project"},
	})
	got := loopsFromBindings([]access.ChannelBinding{b}, nil)
	want := []string{"memory", "skill"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loopsFromBindings = %v, want %v", got, want)
	}
}
