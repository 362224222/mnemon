package runtime

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// TestEmptyRefPullClampedToBindingScope closes the P2 adversarial finding: when the engine
// subscription is BROADER than the binding's SubscriptionScope, an empty-ref pull (and Status) must
// still be clamped to the binding scope — the binding is the auditable narrowing ceiling, including
// on the default request shape, not just on explicit out-of-scope refs.
func TestEmptyRefPullClampedToBindingScope(t *testing.T) {
	m1 := contract.ResourceRef{Kind: "memory", ID: "m1"}
	secret := contract.ResourceRef{Kind: "memory", ID: "secret"}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{
		// engine scope is BROADER than the binding scope.
		Subs: map[contract.ActorID]contract.Subscription{"codex": {Actor: "codex", Refs: []contract.ResourceRef{m1, secret}}},
		Bindings: []access.ChannelBinding{{
			Principal: "codex", ActorKind: contract.KindHostAgent,
			AllowedVerbs: []access.Verb{access.VerbPull, access.VerbStatus}, SubscriptionScope: []contract.ResourceRef{m1},
		}},
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	proj, err := rt.API().PullPresentationView("codex", contract.Subscription{Actor: "codex"})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if len(proj.Resources) != 1 || proj.Resources[0].Ref != m1 {
		t.Fatalf("empty-ref pull must clamp to the binding scope {m1}, not widen to cfg.Subs; got %+v", proj.Resources)
	}
	st, err := rt.Status("codex")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Resources != 1 {
		t.Fatalf("status must reflect the binding scope (1 ref); got %d", st.Resources)
	}
}
