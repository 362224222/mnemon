package main

import (
	"strings"
	"testing"
)

func TestMeasureFunctionsCountsControlFlowAndNestedLiterals(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/flow.go", `package harness
func flow(v int) int {
	if v > 0 && v < 10 {
		for i := 0; i < v; i++ {
			if i == 3 { break }
		}
	} else { v = 2 }
	f := func(ok bool) bool { if ok { return true }; return false }
	_ = f
	return v
}
`)
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	measured, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(measured) != 2 {
		t.Fatalf("function count = %d, want 2: %#v", len(measured), measured)
	}
	outer := measured[0]
	if outer.Symbol != "flow" || outer.Cyclomatic != 5 {
		t.Fatalf("outer metrics = %#v", outer)
	}
	if outer.Nesting < 3 || outer.Cognitive < 7 || outer.Statements < 8 {
		t.Fatalf("outer structural metrics = %#v", outer)
	}
	if !strings.HasPrefix(measured[1].Symbol, "flow.$func-") || measured[1].Cyclomatic != 2 {
		t.Fatalf("literal metrics = %#v", measured[1])
	}
}

func TestClosureIdentityIsScopedToItsParent(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", `package harness
func First() { _ = func() {} }
func Second() { _ = func() { _ = func() {} } }
`)
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	measured, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	wantParents := map[string]bool{"First.$func-": false, "Second.$func-": false}
	for _, function := range measured {
		for prefix := range wantParents {
			if strings.HasPrefix(function.Symbol, prefix) {
				wantParents[prefix] = true
			}
		}
	}
	for prefix, found := range wantParents {
		if !found {
			t.Fatalf("missing parent-scoped closure identity %s: %#v", prefix, measured)
		}
	}
}

func TestNormalizedTokensIgnoreLiteralValuesAndFormatting(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", "package harness\nfunc a() string { return \"one\" }\nfunc b() string {\nreturn \"two\"\n}\n")
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	measured, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	if len(measured) != 2 {
		t.Fatalf("functions = %#v", measured)
	}
	if measured[0].Tokens[len(measured[0].Tokens)-2] != "$string" || measured[1].Tokens[len(measured[1].Tokens)-2] != "$string" {
		t.Fatalf("tokens were not normalized: %#v / %#v", measured[0].Tokens, measured[1].Tokens)
	}
}

func TestLogicalLinesTrackEnclosingSpanAndNestedClosureIndependently(t *testing.T) {
	outer, child := measuredLogicalLines(t, `package harness
func outer() {
	before()
	use(func() {
		nestedOne()
		nestedTwo()
	})
	after()
}
`)
	if outer != 6 || child != 3 {
		t.Fatalf("logical lines outer=%d child=%d, want 6/3", outer, child)
	}
	outer, child = measuredLogicalLines(t, `package harness
func outer() {
	before()
	use(func() {
		nestedOne()
		nestedTwo()
		nestedThree()
		nestedFour()
	})
	after()
}
`)
	if outer != 8 || child != 5 {
		t.Fatalf("grown nested body logical lines outer=%d child=%d, want 8/5", outer, child)
	}
}

func measuredLogicalLines(t *testing.T, source string) (int, int) {
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
	if len(functions) != 2 || functions[0].Symbol != "outer" || !strings.HasPrefix(functions[1].Symbol, "outer.$func-") {
		t.Fatalf("measured functions = %#v", functions)
	}
	return functions[0].LogicalLines, functions[1].LogicalLines
}
