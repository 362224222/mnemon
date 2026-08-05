package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecuteMeasureWritesBaseline(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", "package harness\nfunc Run() {}\n")
	output := filepath.Join(root, "out", "baseline.json")
	var stdout bytes.Buffer
	err := execute([]string{
		"measure", "--root", root, "--source-commit", strings.Repeat("a", 40), "--output", output,
	}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), qualityToolVersion) {
		t.Fatalf("stdout = %q", stdout.String())
	}
	manifest, err := readExactJSON[baselineManifest](output)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.SchemaVersion != 1 || manifest.Entries == nil {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestExecuteRejectsUnknownAndIncompleteCommands(t *testing.T) {
	for _, arguments := range [][]string{{}, {"unknown"}, {"measure", "--root", t.TempDir()}, {"check"}} {
		if err := execute(arguments, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
			t.Fatalf("arguments %#v were accepted", arguments)
		}
	}
}
