package main

import (
	"strings"
	"testing"
)

func TestReadExactJSONRejectsUnknownAndNoncanonicalInput(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "unknown.json", `{"schema_version":1,"unknown":true}`+"\n")
	if _, err := readExactJSON[exceptionManifest](root + "/unknown.json"); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
	writeTestFile(t, root, "compact.json", `{"schema_version":1,"entries":[]}`+"\n")
	if _, err := readExactJSON[exceptionManifest](root + "/compact.json"); err == nil || !strings.Contains(err.Error(), "not canonical") {
		t.Fatalf("canonical error = %v", err)
	}
}

func TestCanonicalJSONHasStableIndentAndNewline(t *testing.T) {
	data, err := canonicalJSON(exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}})
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"schema_version\": 1,\n  \"entries\": []\n}\n"
	if string(data) != want {
		t.Fatalf("canonical JSON = %q, want %q", data, want)
	}
}

func TestValidateFullCommitAndSortedUnique(t *testing.T) {
	if err := validateFullCommit(strings.Repeat("a", 40), "commit"); err != nil {
		t.Fatal(err)
	}
	if err := validateFullCommit("abcd", "commit"); err == nil {
		t.Fatal("short commit was accepted")
	}
	if err := validateSortedUnique([]string{"b", "a"}, "values", true); err == nil {
		t.Fatal("unsorted values were accepted")
	}
}

func TestSymbolWildcardValidationAllowsPointerReceiverOnly(t *testing.T) {
	if err := rejectSymbolWildcard("function_logical_lines:harness/a.go::(*Runner).Run", "identity"); err != nil {
		t.Fatalf("pointer receiver rejected: %v", err)
	}
	for _, value := range []string{"(*Runner*).Run", "Runner.*", "Runner.?", "Runner[all]"} {
		if err := rejectSymbolWildcard(value, "symbol"); err == nil {
			t.Errorf("wildcard symbol %q was accepted", value)
		}
	}
}
