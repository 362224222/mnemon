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

func TestValidateRequirementsRequiresVerifiedEvidence(t *testing.T) {
	record := requirementRecord{
		ID: "EQ-02", Status: "verified", OwnerPackages: []string{"harness/internal/model"},
		AcceptedCommits: []string{}, TestSymbols: []string{}, ScenarioKeys: []string{}, EvidenceGates: []string{},
	}
	if err := validateRequirementRecord(record); err == nil || !strings.Contains(err.Error(), "must not be empty") {
		t.Fatalf("verified evidence error = %v", err)
	}
	record.Status = "pending"
	if err := validateRequirementRecord(record); err != nil {
		t.Fatalf("pending evidence: %v", err)
	}
}

func TestExpectedRequirementsRejectUnknownOrderAndLevel(t *testing.T) {
	manifest := expectedManifest{SchemaVersion: 1, Requirements: []expectedRequirement{
		{ID: "PR-02", Level: "MUST"},
		{ID: "PR-01", Level: "MUST"},
	}}
	if err := validateExpectedManifest(manifest); err == nil || !strings.Contains(err.Error(), "sorted") {
		t.Fatalf("order error = %v", err)
	}
	manifest.Requirements = []expectedRequirement{{ID: "PR-01", Level: "OPTIONAL"}}
	if err := validateExpectedManifest(manifest); err == nil || !strings.Contains(err.Error(), "level") {
		t.Fatalf("level error = %v", err)
	}
}
