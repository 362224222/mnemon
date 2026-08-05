package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestMeasureDuplicatesIsDeterministic(t *testing.T) {
	root := t.TempDir()
	body := duplicateFixtureBody(55)
	writeTestFile(t, root, "harness/b.go", "package harness\nfunc second(seed int) int {\n"+body+"return seed\n}\n")
	writeTestFile(t, root, "harness/a.go", "package harness\nfunc first(seed int) int {\n"+body+"return seed\n}\n")
	files, err := loadHarnessSources(root)
	if err != nil {
		t.Fatal(err)
	}
	functions, err := measureFunctions(files)
	if err != nil {
		t.Fatal(err)
	}
	first, err := measureDuplicates(functions)
	if err != nil {
		t.Fatal(err)
	}
	second, err := measureDuplicates(functions)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%#v", first) != fmt.Sprintf("%#v", second) {
		t.Fatalf("duplicate result changed: %#v != %#v", first, second)
	}
	if len(first) != 1 || first[0].DebtID != "dup-0001" || first[0].Tokens < duplicateTokenMinimum {
		t.Fatalf("duplicates = %#v", first)
	}
	wantOwners := []string{"harness/a.go::first", "harness/b.go::second"}
	if strings.Join(first[0].Owners, "|") != strings.Join(wantOwners, "|") {
		t.Fatalf("owners = %#v", first[0].Owners)
	}
}

func TestMeasureDuplicatesIgnoresShortBlocks(t *testing.T) {
	functions := []functionMeasurement{
		{Path: "harness/a.go", Symbol: "a", Tokens: repeatedTokens(duplicateTokenMinimum - 1)},
		{Path: "harness/b.go", Symbol: "b", Tokens: repeatedTokens(duplicateTokenMinimum - 1)},
	}
	duplicates, err := measureDuplicates(functions)
	if err != nil {
		t.Fatal(err)
	}
	if len(duplicates) != 0 {
		t.Fatalf("duplicates = %#v", duplicates)
	}
}

func duplicateFixtureBody(statements int) string {
	var body strings.Builder
	for index := 0; index < statements; index++ {
		fmt.Fprintf(&body, "value%d := seed + %d\nseed = value%d\n", index, index, index)
	}
	return body.String()
}

func repeatedTokens(count int) []string {
	tokens := make([]string, count)
	for index := range tokens {
		tokens[index] = fmt.Sprintf("token-%d", index)
	}
	return tokens
}
