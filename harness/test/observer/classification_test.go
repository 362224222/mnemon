package observer

import (
	"sort"
	"strings"
	"testing"
)

func validFactClassification(fact factRecord) bool {
	expected, exists := factClassifications[fact.Kind]
	if !exists || fact.Source.Class != expected.source || fact.Truth != expected.truth {
		return false
	}
	return !strings.HasPrefix(fact.Kind, "r8.") || fact.Refs.Selection != ""
}

func knownFactKinds() []string {
	result := make([]string, 0, len(factClassifications))
	for kind := range factClassifications {
		result = append(result, kind)
	}
	sort.Strings(result)
	return result
}

func factClassificationRows() []string {
	result := make([]string, 0, len(factClassifications))
	for kind, classification := range factClassifications {
		result = append(result, kind+"|"+classification.source+"|"+classification.truth)
	}
	sort.Strings(result)
	return result
}

func TestFactClassificationMatchesBrowser(t *testing.T) {
	html := string(readFile(t, "index.html"))
	assertSameStrings(t, "browser fact classifications",
		javascriptStringArray(t, html, "const FACT_CLASSIFICATION_ROWS"),
		factClassificationRows())
}

func TestFactClassificationFailsClosed(t *testing.T) {
	for kind, expected := range factClassifications {
		fact := factRecord{Kind: kind, Source: sourceWire{Class: expected.source}, Truth: expected.truth}
		if strings.HasPrefix(kind, "r8.") {
			fact.Refs.Selection = "sha256:" + strings.Repeat("a", 64)
		}
		if !validFactClassification(fact) {
			t.Fatalf("valid classification for %q was rejected", kind)
		}
		fact.Source.Class = "runner"
		if fact.Source.Class == expected.source {
			fact.Source.Class = "runtime"
		}
		if validFactClassification(fact) {
			t.Fatalf("wrong source for %q was accepted", kind)
		}
		fact.Source.Class = expected.source
		fact.Truth = "assertion"
		if fact.Truth == expected.truth {
			fact.Truth = "observation"
		}
		if validFactClassification(fact) {
			t.Fatalf("wrong truth for %q was accepted", kind)
		}
	}
	r8 := factRecord{Kind: "r8.selection.seeded", Source: sourceWire{Class: "r8_selector"}, Truth: "local_preference"}
	if validFactClassification(r8) {
		t.Fatal("R8 fact without SelectionID was accepted")
	}
}
