package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func TestSchemaGuardRejectsMissingField(t *testing.T) {
	g := SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}})
	if g.Validate("memory", map[string]any{}) == nil {
		t.Fatal("expected missing-content rejection")
	}
	if g.Validate("memory", map[string]any{"content": "x"}) != nil {
		t.Fatal("valid memory rejected")
	}
}
func TestAuthzStrictRejectsUnknownActor(t *testing.T) {
	r := AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"user": {"memory"}}}
	if r.Enforce("user", "memory") != nil {
		t.Fatal("user/memory should pass")
	}
	if r.Enforce("codex@x", "memory") == nil {
		t.Fatal("unknown actor should fail")
	}
}
