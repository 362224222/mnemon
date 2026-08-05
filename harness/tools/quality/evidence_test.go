package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

func TestValidateArchitectureEvidenceRequiresTrackedFindingAndLiveSymbol(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", "package harness\nfunc Run() {}\n")
	finding := newArchitectureFinding("dependency_direction", "harness/a.go", "legacy")
	entry := architectureEntry{
		Rule: finding.Rule, Identity: finding.Identity, Path: finding.Path, Component: finding.Component,
		Risk: "high", Evidence: finding.Evidence, Owner: "team", RemovalCheckpoint: "7R",
	}
	if err := validateArchitectureEvidence(root, architectureManifest{Entries: []architectureEntry{entry}}, []architectureFinding{finding}); err != nil {
		t.Fatal(err)
	}
	entry.Evidence = "harness/a.go::Missing"
	if err := validateArchitectureEvidence(root, architectureManifest{Entries: []architectureEntry{entry}}, nil); err == nil || !strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("missing symbol error = %v", err)
	}
	if err := validateArchitectureEvidence(root, architectureManifest{Entries: []architectureEntry{}}, []architectureFinding{finding}); err == nil || !strings.Contains(err.Error(), "untracked") {
		t.Fatalf("untracked finding error = %v", err)
	}
}

func TestValidateArchitectureEvidenceRejectsStaleAutoFindingButAllowsManualDebt(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/a.go", "package harness\nfunc Run() {}\n")
	automatic := newArchitectureFinding("dependency_direction", "harness/a.go", "legacy")
	automaticEntry := architectureEntry{
		Rule: automatic.Rule, Identity: automatic.Identity, Path: automatic.Path, Component: automatic.Component,
		Risk: "medium", Evidence: automatic.Evidence, Owner: "team", RemovalCheckpoint: "7R",
	}
	if err := validateArchitectureEvidence(root, architectureManifest{Entries: []architectureEntry{automaticEntry}}, nil); err == nil || !strings.Contains(err.Error(), "stale auto-detected") {
		t.Fatalf("stale automatic debt error = %v", err)
	}
	manual := architectureEntry{
		Rule: "goroutine_ownership", Identity: "goroutine_ownership:run", Path: "harness/a.go", Symbol: "Run",
		Risk: "medium", Evidence: "harness/a.go::Run", Owner: "team", RemovalCheckpoint: "7R",
	}
	if err := validateArchitectureEvidence(root, architectureManifest{Entries: []architectureEntry{manual}}, nil); err != nil {
		t.Fatalf("manual evidence-only debt: %v", err)
	}
}

func TestValidateRequirementEvidenceMatchesInvariantIDsAndCurrentTests(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := corecontract.Load(root)
	if err != nil {
		t.Fatal(err)
	}
	requirements, err := corecontract.LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRequirementEvidence(root, contract, requirements); err != nil {
		t.Fatal(err)
	}
	requirements.Invariants[0].ID = "P-99"
	if err := validateRequirementEvidence(root, contract, requirements); err == nil ||
		!strings.Contains(err.Error(), "invariant IDs") {
		t.Fatalf("unknown invariant error = %v", err)
	}
}
