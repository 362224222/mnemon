package access

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// ClampRefs is the ONE scope-clamp for the binding ceiling (pull / sync / status previously carried
// hand-rolled copies that had already diverged on empty-scope handling). Semantics: empty requested
// defaults to the full scope; an explicit ref outside the scope is an error; an EMPTY scope denies
// every explicit ref (fail closed). The ingest path keeps its documented exception (refs optional)
// at its own call site.
func TestClampRefsSemantics(t *testing.T) {
	mem := contract.ResourceRef{Kind: "memory", ID: "project"}
	skill := contract.ResourceRef{Kind: "skill", ID: "project"}
	b := HostAgentBinding("a@p", "http://x", []contract.ResourceRef{mem, skill})

	// empty requested -> full scope copy
	got, err := b.ClampRefs(nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("empty requested must default to the scope: %v err=%v", got, err)
	}
	// narrowing stays
	got, err = b.ClampRefs([]contract.ResourceRef{mem})
	if err != nil || len(got) != 1 || got[0] != mem {
		t.Fatalf("narrowing must pass through: %v err=%v", got, err)
	}
	// out-of-scope explicit ref denied
	if _, err := b.ClampRefs([]contract.ResourceRef{{Kind: "note", ID: "project"}}); err == nil {
		t.Fatal("an out-of-scope explicit ref must be denied")
	}
	// empty scope: explicit refs denied (fail closed), empty requested yields empty
	unscoped := HostAgentBinding("a@p", "http://x", nil)
	if _, err := unscoped.ClampRefs([]contract.ResourceRef{mem}); err == nil {
		t.Fatal("an empty scope must deny every explicit ref")
	}
	if got, err := unscoped.ClampRefs(nil); err != nil || len(got) != 0 {
		t.Fatalf("empty scope + empty requested must clamp to nothing: %v err=%v", got, err)
	}
}
