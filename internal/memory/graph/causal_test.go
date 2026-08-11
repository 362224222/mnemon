package graph

import (
	"testing"
)

func TestHasCausalSignal_English(t *testing.T) {
	cases := []string{
		"chose SQLite because of simplicity",
		"therefore we switched to Go",
		"this was due to performance issues",
		"decided to use WAL mode",
		"as a result the latency dropped",
	}
	for _, text := range cases {
		if !HasCausalSignal(text) {
			t.Errorf("HasCausalSignal(%q) = false, want true", text)
		}
	}
}

func TestHasCausalSignal_Chinese(t *testing.T) {
	cases := []string{
		"因为性能问题",
		"所以选择了Go",
		"由于内存限制",
		"导致延迟增加",
		"因此改用SQLite",
		"决定使用WAL模式",
	}
	for _, text := range cases {
		if !HasCausalSignal(text) {
			t.Errorf("HasCausalSignal(%q) = false, want true", text)
		}
	}
}

func TestHasCausalSignal_NoSignal(t *testing.T) {
	cases := []string{
		"Go uses SQLite for storage",
		"the database is fast",
		"graph traversal algorithm",
		"数据库很快",
	}
	for _, text := range cases {
		if HasCausalSignal(text) {
			t.Errorf("HasCausalSignal(%q) = true, want false", text)
		}
	}
}

func TestSuggestSubType_Causes(t *testing.T) {
	got := suggestSubType("this happened because of that")
	if got != "causes" {
		t.Errorf("want 'causes', got %q", got)
	}
}

func TestSuggestSubType_Enables(t *testing.T) {
	got := suggestSubType("we did this so that it would work")
	if got != "enables" {
		t.Errorf("want 'enables', got %q", got)
	}
}

func TestSuggestSubType_Prevents(t *testing.T) {
	got := suggestSubType("this prevented the crash from happening")
	if got != "prevents" {
		t.Errorf("want 'prevents', got %q", got)
	}
}

func TestSuggestSubType_DefaultCauses(t *testing.T) {
	got := suggestSubType("no causal keywords here")
	if got != "causes" {
		t.Errorf("default: want 'causes', got %q", got)
	}
}

func TestSuggestSubType_PreventsOverridesCauses(t *testing.T) {
	// "prevents" should take priority over "because"
	got := suggestSubType("prevented the crash because of the guard")
	if got != "prevents" {
		t.Errorf("prevents should override causes: got %q", got)
	}
}

func TestTokenOverlap_Basic(t *testing.T) {
	a := map[string]bool{"go": true, "sqlite": true, "memory": true}
	b := map[string]bool{"go": true, "sqlite": true, "python": true, "flask": true}
	// intersection = 2, max(3, 4) = 4
	got := tokenOverlap(a, b)
	want := 2.0 / 4.0
	if got != want {
		t.Errorf("want %f, got %f", want, got)
	}
}

func TestTokenOverlap_NoOverlap(t *testing.T) {
	a := map[string]bool{"go": true}
	b := map[string]bool{"python": true}
	got := tokenOverlap(a, b)
	if got != 0 {
		t.Errorf("no overlap: want 0, got %f", got)
	}
}

func TestTokenOverlap_EmptySets(t *testing.T) {
	if got := tokenOverlap(nil, map[string]bool{"a": true}); got != 0 {
		t.Errorf("nil a: want 0, got %f", got)
	}
	if got := tokenOverlap(map[string]bool{"a": true}, nil); got != 0 {
		t.Errorf("nil b: want 0, got %f", got)
	}
	if got := tokenOverlap(map[string]bool{}, map[string]bool{}); got != 0 {
		t.Errorf("both empty: want 0, got %f", got)
	}
}

func TestTokenOverlap_Identical(t *testing.T) {
	a := map[string]bool{"go": true, "sqlite": true}
	got := tokenOverlap(a, a)
	if got != 1.0 {
		t.Errorf("identical sets: want 1.0, got %f", got)
	}
}

func TestFindCausalSignal_ReturnsMatch(t *testing.T) {
	got := findCausalSignal("chose X because of performance")
	if got == "" {
		t.Error("want causal signal match, got empty string")
	}
}

func TestFindCausalSignal_NoMatch(t *testing.T) {
	got := findCausalSignal("Go is a programming language")
	if got != "" {
		t.Errorf("want empty string, got %q", got)
	}
}
