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
	assignmentRef := contract.ResourceRef{Kind: "assignment", ID: "project"}
	progressRef := contract.ResourceRef{Kind: "progress_digest", ID: "project"}

	mkBinding := func() access.ChannelBinding {
		b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{assignmentRef, progressRef})
		b.AllowedObservedTypes = []string{
			"assignment.write_candidate.observed",
			"progress_digest.write_candidate.observed",
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
			{"a1", "assignment.write_candidate.observed", map[string]any{"scope": "parity assignment", "ttl": "2h", "assignee": "codex@impl", "expected_work": "do the parity work", "expected_feedback": "progress_digest", "evidence": "test"}},
			{"p1", "progress_digest.write_candidate.observed", map[string]any{"summary": "parity progress"}},
			{"p2", "progress_digest.write_candidate.observed", map[string]any{"summary": "password=hunter2"}},
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

	asmRC, err := assembler.Assemble(eventPackageFileFromLoops([]string{"assignment", "progress_digest"}), []access.ChannelBinding{mkBinding()}, nil)
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

	for _, ref := range []contract.ResourceRef{assignmentRef, progressRef} {
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
	// The secret-like candidate must be denied on both paths: progress_digest stays at one entry.
	if v, _, _ := bootRT.Resource(progressRef); v != 1 {
		t.Fatalf("boot path admitted the denied candidate (progress_digest v=%d)", v)
	}
}

// The hidden `local run --bindings` boot path has no localConfig: event package enablement is derived
// from the binding scope kinds ∩ StandardRegistry().
func TestLoopsFromBindingsDerivesEnablement(t *testing.T) {
	b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{
		{Kind: "assignment", ID: "project"}, {Kind: "progress_digest", ID: "project"},
	})
	got := loopsFromBindings([]access.ChannelBinding{b}, nil)
	want := []string{"assignment", "progress_digest"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("loopsFromBindings = %v, want %v", got, want)
	}
}
