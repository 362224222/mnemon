package contract

import "testing"

// contract.ClampRefs is THE one scope-clamp implementation (sync-abi-v1 §2): channel bindings and
// the mnemonhub hub both delegate here. Pin its semantics at the source: empty requested defaults
// to the full scope; narrowing passes; an out-of-scope explicit ref errors; an empty scope denies
// every explicit ref (fail closed) while empty+empty clamps to nothing.
func TestContractClampRefsSemantics(t *testing.T) {
	mem := ResourceRef{Kind: "memory", ID: "project"}
	skill := ResourceRef{Kind: "skill", ID: "project"}
	scope := []ResourceRef{mem, skill}

	got, err := ClampRefs("a@p", scope, nil)
	if err != nil || len(got) != 2 {
		t.Fatalf("empty requested must default to the scope: %v err=%v", got, err)
	}
	got, err = ClampRefs("a@p", scope, []ResourceRef{mem})
	if err != nil || len(got) != 1 || got[0] != mem {
		t.Fatalf("narrowing must pass through: %v err=%v", got, err)
	}
	if _, err := ClampRefs("a@p", scope, []ResourceRef{{Kind: "note", ID: "project"}}); err == nil {
		t.Fatal("an out-of-scope explicit ref must be denied")
	}
	if _, err := ClampRefs("a@p", nil, []ResourceRef{mem}); err == nil {
		t.Fatal("an empty scope must deny every explicit ref")
	}
	if got, err := ClampRefs("a@p", nil, nil); err != nil || len(got) != 0 {
		t.Fatalf("empty scope + empty requested must clamp to nothing: %v err=%v", got, err)
	}
}
