package main

import (
	"strings"
	"testing"
)

func TestValidateArchitectureRequiresStableEvidence(t *testing.T) {
	entry := architectureEntry{
		Rule: "dependency_direction", Identity: "dependency_direction:harness/a.go::legacy",
		Path: "harness/a.go", Component: "legacy", Risk: "high", Evidence: "harness/a.go::Run",
		Owner: "harness", RemovalCheckpoint: "7R",
	}
	manifest := architectureManifest{SchemaVersion: 1, SourceCommit: strings.Repeat("a", 40), Entries: []architectureEntry{entry}}
	if err := validateArchitectureManifest(manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Entries[0].Evidence = "free form evidence"
	if err := validateArchitectureManifest(manifest); err == nil {
		t.Fatal("free-form evidence was accepted")
	}
}
