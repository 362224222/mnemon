package main

import (
	"sort"
	"strings"
	"testing"
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

func TestValidateRequirementEvidenceMatchesIDsAndTestSymbols(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "harness/a_test.go", "package harness\nfunc TestProof() {}\n")
	commit := commitTestRepository(t, root, "add proof")
	contract := testCoreContract()
	requirements := requirementsManifest{SchemaVersion: 2, Requirements: []requirementRecord{{
		ID:              "SC-01",
		AcceptedCommits: []string{commit}, TestSymbols: []string{"harness/a_test.go::TestProof"},
		ScenarioKeys: []string{},
	}}}
	if err := validateRequirementEvidence(root, contract, requirements); err != nil {
		t.Fatal(err)
	}
	requirements.Requirements[0].ID = "SC-99"
	if err := validateRequirementEvidence(root, contract, requirements); err == nil ||
		!strings.Contains(err.Error(), "unknown requirement") {
		t.Fatalf("unknown requirement error = %v", err)
	}
}

func TestValidateRequirementEvidenceBindsTestToAcceptedCommitTree(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "harness/a_test.go", "package harness\nfunc TestEarlier() {}\nvar TestProof = func() {}\n")
	earlier := commitTestRepository(t, root, "add earlier test")
	writeTestFile(t, root, "harness/a_test.go", "package harness\nfunc TestEarlier() {}\nfunc TestProof() {}\n")

	requirement := requirementRecord{
		ID:              "SC-01",
		AcceptedCommits: []string{earlier}, TestSymbols: []string{"harness/a_test.go::TestProof"},
		ScenarioKeys: []string{},
	}
	registry := requirementsManifest{
		SchemaVersion: 2, Requirements: []requirementRecord{requirement},
	}
	if err := validateRequirementEvidence(root, testCoreContract(), registry); err == nil ||
		!strings.Contains(err.Error(), "absent from every accepted commit") {
		t.Fatalf("working-tree-only evidence error = %v", err)
	}

	writeTestFile(t, root, "harness/a_test.go", "package harness\nfunc TestProof() {}\n")
	proof := commitTestRepository(t, root, "add accepted proof")
	writeTestFile(t, root, "harness/a_test.go", "package harness\nfunc TestEarlier() {}\nfunc TestProof() {}\n")
	requirement.AcceptedCommits = []string{earlier, proof}
	sort.Strings(requirement.AcceptedCommits)
	requirement.TestSymbols = []string{
		"harness/a_test.go::TestEarlier",
		"harness/a_test.go::TestProof",
	}
	registry.Requirements[0] = requirement
	if err := validateRequirementEvidence(root, testCoreContract(), registry); err != nil {
		t.Fatalf("evidence distributed across accepted commits: %v", err)
	}

	requirement.AcceptedCommits = []string{}
	registry.Requirements[0] = requirement
	if err := validateRequirementEvidence(root, testCoreContract(), registry); err == nil ||
		!strings.Contains(err.Error(), "ungrounded behavioral evidence") {
		t.Fatalf("test evidence without accepted commit error = %v", err)
	}
}
