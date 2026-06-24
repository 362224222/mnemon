package access

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

// stubAPI is an inert inner ServerAPI: it records whether Ingest reached it, so a test can prove the
// authorizer rejected an observation BEFORE it crossed the trust boundary.
type stubAPI struct{ ingested int }

func (s *stubAPI) Ingest(contract.ActorID, contract.ObservationEnvelope) (int64, bool, error) {
	s.ingested++
	return 1, false, nil
}
func (s *stubAPI) PullPresentationView(contract.ActorID, contract.Subscription) (view.View, error) {
	return view.View{}, nil
}

// The authorizer is the only layer holding the binding, so it is where an observation's named
// ResourceRefs are clamped to the binding scope — a memory-scoped principal cannot observe a write
// naming a skill resource (mirrors the pull-scope clamp).
func TestIngestRejectsOutOfScopeResourceRef(t *testing.T) {
	memRef := contract.ResourceRef{Kind: "memory", ID: "project"}
	binding := HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{memRef})
	binding.AllowedObservedTypes = []string{"memory.write_candidate.observed"}
	bs, err := NewBindingSet(binding)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	inner := &stubAPI{}
	api := NewAuthorizedAPI(inner, bs)

	_, _, err = api.Ingest("codex@project", contract.ObservationEnvelope{
		Event: contract.Event{
			Type:         "memory.write_candidate.observed",
			ResourceRefs: []contract.ResourceRef{{Kind: "skill", ID: "project"}},
			Payload:      map[string]any{"content": "x"},
		},
	})
	if err == nil {
		t.Fatal("an out-of-scope resource ref must be rejected at intake")
	}
	if inner.ingested != 0 {
		t.Fatalf("a rejected observation must not reach the inner API; reached %d times", inner.ingested)
	}

	if _, _, err := api.Ingest("codex@project", contract.ObservationEnvelope{
		Event: contract.Event{
			Type:         "memory.write_candidate.observed",
			ResourceRefs: []contract.ResourceRef{memRef},
			Payload:      map[string]any{"content": "x"},
		},
	}); err != nil {
		t.Fatalf("an in-scope observation must pass: %v", err)
	}
	if inner.ingested != 1 {
		t.Fatalf("the in-scope observation must reach the inner API exactly once; reached %d", inner.ingested)
	}
}

// ClampRefs 语义对齐:空 scope binding 显式命名 refs 必须被拒(fail-closed)——
// 此前 len(scope)==0 时整个检查被跳过。唯一例外:未命名 refs 的观察不受约束。
func TestIngestEmptyScopeRejectsExplicitRefs(t *testing.T) {
	binding := HostAgentBinding("codex@project", "http://127.0.0.1:8787", nil) // 空 scope
	binding.AllowedObservedTypes = []string{"memory.write_candidate.observed"}
	bs, err := NewBindingSet(binding)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	inner := &stubAPI{}
	api := NewAuthorizedAPI(inner, bs)

	if _, _, err := api.Ingest("codex@project", contract.ObservationEnvelope{
		Event: contract.Event{
			Type:         "memory.write_candidate.observed",
			ResourceRefs: []contract.ResourceRef{{Kind: "memory", ID: "project"}},
			Payload:      map[string]any{"content": "x"},
		},
	}); err == nil {
		t.Fatal("an empty-scope binding must reject every explicitly named ref")
	}
	if inner.ingested != 0 {
		t.Fatal("rejected observation must not cross the trust boundary")
	}

	// 例外不变:同一 binding,未命名 refs → 不受约束,放行。
	if _, _, err := api.Ingest("codex@project", contract.ObservationEnvelope{
		Event: contract.Event{
			Type:    "memory.write_candidate.observed",
			Payload: map[string]any{"content": "x"},
		},
	}); err != nil {
		t.Fatalf("an observation naming no refs must stay unconstrained: %v", err)
	}
	if inner.ingested != 1 {
		t.Fatal("the unconstrained observation must reach the inner API")
	}
}
