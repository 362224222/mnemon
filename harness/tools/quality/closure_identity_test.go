package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestClosureIdentityDoesNotChurnWhenEarlierClosureIsInserted(t *testing.T) {
	original := closureSymbolsByMarker(t, `package harness
func Run() {
	use(func() { laterMarker() })
}
`)
	withInsertion := closureSymbolsByMarker(t, `package harness
func Run() {
	use(func() { insertedMarker() })
	use(func() { laterMarker() })
}
`)
	if original["laterMarker"] == "" || original["laterMarker"] != withInsertion["laterMarker"] {
		t.Fatalf("later closure identity churned: %q != %q", original["laterMarker"], withInsertion["laterMarker"])
	}
}

func TestNamedClosureIdentitySurvivesUnrelatedSiblingInsertion(t *testing.T) {
	original := closureSymbolsByMarker(t, `package harness
func Run() {
	later := func() { laterMarker() }
	_ = later
}
`)
	withInsertion := closureSymbolsByMarker(t, `package harness
func Run() {
	earlier := func() { insertedMarker() }
	later := func() { laterMarker() }
	_, _ = earlier, later
}
`)
	if original["laterMarker"] != withInsertion["laterMarker"] {
		t.Fatalf("named closure identity churned: %q != %q", original["laterMarker"], withInsertion["laterMarker"])
	}
}

func TestEquivalentClosureGroupGrowsWithoutIdentityChurn(t *testing.T) {
	original := closureSymbolsForMarker(t, `package harness
func Run() {
	use(func() { repeatedMarker() })
	use(func() { repeatedMarker() })
}
`, "repeatedMarker")
	withInsertion := closureSymbolsForMarker(t, `package harness
func Run() {
	use(func() { repeatedMarker() })
	use(func() { repeatedMarker() })
	use(func() { repeatedMarker() })
}
`, "repeatedMarker")
	if len(original) != 2 || len(withInsertion) != 3 {
		t.Fatalf("equivalent symbols = %#v / %#v", original, withInsertion)
	}
	for _, symbol := range original {
		if !containsString(withInsertion, symbol) {
			t.Fatalf("existing equivalent identity %q churned: %#v", symbol, withInsertion)
		}
	}
}

func TestPackageClosuresAcrossDeclarationsHaveDistinctIdentities(t *testing.T) {
	symbols := closureSymbolsForMarker(t, `package harness
var first = use(func() { packageMarker() })
var second = use(func() { packageMarker() })
`, "packageMarker")
	if len(symbols) != 2 || symbols[0] == symbols[1] {
		t.Fatalf("package closure symbols = %#v", symbols)
	}
}

func TestNonEquivalentNestedClosureTreesFailClosed(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", `package harness
func Run() {
	use(func() { use(func() { nestedInsertedMarker() }) })
	use(func() { use(func() { nestedLaterMarker() }) })
}
`)
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := measureFunctions(files); err == nil || !strings.Contains(err.Error(), "closure identity collision") {
		t.Fatalf("non-equivalent nested trees error = %v", err)
	}
}

func TestCompositeSiblingLabelStabilizesNestedClosureTrees(t *testing.T) {
	original := closureSymbolsForMarker(t, `package harness
func Run() {
	tests := []struct { name string; configure func() }{
		{name: "later", configure: func() { use(func() { laterNestedMarker() }) }},
	}
	_ = tests
}
`, "laterNestedMarker")
	withInsertion := closureSymbolsForMarker(t, `package harness
func Run() {
	tests := []struct { name string; configure func() }{
		{name: "earlier", configure: func() { use(func() { insertedNestedMarker() }) }},
		{name: "later", configure: func() { use(func() { laterNestedMarker() }) }},
	}
	_ = tests
}
`, "laterNestedMarker")
	if len(original) != 1 || len(withInsertion) != 1 || original[0] != withInsertion[0] {
		t.Fatalf("labelled composite identity churned: %#v != %#v", original, withInsertion)
	}
}

func TestClosureRatchetSameNameNestedScopeInsertion(t *testing.T) {
	body := ratchetClosureBody("laterMarker")
	assertClosureRatchetStable(t, `package harness
func Run() {
	{ x := func() { `+body+` }; _ = x }
}
`, `package harness
func Run() {
	{ x := func() { insertedMarker() }; _ = x }
	{ x := func() { `+body+` }; _ = x }
}
`, "laterMarker")
}

func TestClosureRatchetUnlabelledOuterNestedInsertion(t *testing.T) {
	body := ratchetClosureBody("outerMarker")
	assertClosureRatchetStable(t, `package harness
func Run() {
	use(func() { `+body+` })
}
`, `package harness
func Run() {
	use(func() { use(func() { nestedMarker() }); `+body+` })
}
`, "outerMarker")
}

func TestClosureRatchetNestedChildSiblingInsertion(t *testing.T) {
	body := ratchetClosureBody("nestedLaterMarker")
	assertClosureRatchetStable(t, `package harness
func Run() {
	use(func() { use(func() { `+body+` }) })
}
`, `package harness
func Run() {
	use(func() {
		use(func() { insertedNestedMarker() })
		use(func() { `+body+` })
	})
}
`, "nestedLaterMarker")
}

func TestClosureRatchetPackagePrefixInsertion(t *testing.T) {
	body := ratchetClosureBody("packageLaterMarker")
	assertClosureRatchetStable(t, `package harness
var later = use(func() { `+body+` })
`, `package harness
var earlier = use(func() { insertedMarker() })
var later = use(func() { `+body+` })
`, "packageLaterMarker")
}

func TestClosureRatchetExactEquivalentPrefixInsertion(t *testing.T) {
	body := ratchetClosureBody("equivalentMarker")
	original := `package harness
func Run() {
	use(func() { ` + body + ` })
}
`
	modified := `package harness
func Run() {
	use(func() { ` + body + ` })
	use(func() { ` + body + ` })
}
`
	assertClosureRatchetStable(t, original, modified, "equivalentMarker")
}

func closureSymbolsByMarker(t *testing.T, source string) map[string]string {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", source)
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string)
	functions, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, function := range functions {
		for _, token := range function.Tokens {
			if token == "laterMarker" || token == "insertedMarker" {
				result[token] = function.Symbol
			}
		}
	}
	return result
}

func closureSymbolsForMarker(t *testing.T, source, marker string) []string {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", source)
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	var symbols []string
	functions, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, function := range functions {
		if containsString(function.Tokens, marker) {
			symbols = append(symbols, function.Symbol)
		}
	}
	return symbols
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func assertClosureRatchetStable(t *testing.T, original, modified, marker string) {
	t.Helper()
	before := closureDebtsForMarker(t, original, marker)
	after := closureDebtsForMarker(t, modified, marker)
	if len(before) != 1 {
		t.Fatalf("original debts for %s = %#v", marker, before)
	}
	var current *baselineEntry
	for index := range after {
		if after[index].Identity == before[0].Identity {
			current = &after[index]
			break
		}
	}
	if current == nil || current.Ceiling != before[0].Ceiling {
		t.Fatalf("ratcheted closure debt churned: before=%#v after=%#v", before, after)
	}
	baseline := validBaselineManifest()
	baseline.Entries = before
	prior := contractBundle{
		baseline: baseline, exceptions: exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}},
		architecture: architectureManifest{SchemaVersion: 1, SourceCommit: baseline.SourceCommit, Entries: []architectureEntry{}},
	}
	candidate := prior
	candidate.baseline.Entries = []baselineEntry{}
	candidate.exceptions.Entries = []exceptionEntry{ratchetException(*current, current.Ceiling)}
	if err := compareContractBundles(prior, candidate); err == nil || !strings.Contains(err.Error(), "historical baseline identity") {
		t.Fatalf("closure baseline laundering error = %v", err)
	}
}

func closureDebtsForMarker(t *testing.T, source, marker string) []baselineEntry {
	t.Helper()
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", source)
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	functions, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	var debts []baselineEntry
	for _, function := range functions {
		if !containsString(function.Tokens, marker) || function.Cognitive <= 25 {
			continue
		}
		debts = append(debts, baselineEntry{
			Rule: ruleCognitive, Identity: functionIdentity(ruleCognitive, function.Path, function.Symbol),
			Path: function.Path, Symbol: function.Symbol, Ceiling: function.Cognitive,
		})
	}
	return debts
}

func ratchetClosureBody(marker string) string {
	var body strings.Builder
	fmt.Fprintf(&body, "ok := true; %s(); ", marker)
	for index := 0; index < 26; index++ {
		fmt.Fprintf(&body, "if ok { ok = false }; ")
	}
	return body.String()
}
