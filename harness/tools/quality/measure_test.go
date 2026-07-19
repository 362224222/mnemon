package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMeasureTreeFindsFileAndFunctionDebt(t *testing.T) {
	root := t.TempDir()
	var source strings.Builder
	source.WriteString("package harness\nfunc long(ok bool) {\n")
	for index := 0; index < 90; index++ {
		source.WriteString("if ok { ok = false }\n")
	}
	source.WriteString("}\n")
	for source.Len() < 5000 {
		source.WriteString("\n")
	}
	writeTestFile(t, root, "harness/long.go", source.String())
	manifest, err := measureTree(root, strings.Repeat("a", 40))
	if err != nil {
		t.Fatal(err)
	}
	rules := make(map[string]bool)
	for _, entry := range manifest.Entries {
		rules[entry.Rule] = true
	}
	for _, rule := range []string{ruleCognitive, ruleCyclomatic, ruleFunctionLines, ruleStatements, ruleProductionFile} {
		if !rules[rule] {
			t.Errorf("missing measured rule %s", rule)
		}
	}
}

func TestWriteCanonicalJSONReplacesOutput(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "nested", "baseline.json")
	manifest := validBaselineManifest()
	if err := writeCanonicalJSON(path, manifest); err != nil {
		t.Fatal(err)
	}
	loaded, err := readExactJSON[baselineManifest](path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SourceCommit != manifest.SourceCommit {
		t.Fatalf("source commit = %q", loaded.SourceCommit)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
}
