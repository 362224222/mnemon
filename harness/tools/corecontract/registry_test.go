package corecontract

import (
	"strings"
	"testing"
)

func TestRegistryDerivesUnresolvedAndRejectsManualStatus(t *testing.T) {
	contract := oneRequirementContract("G-PROCESS")
	registry := oneRequirementRegistry()
	if err := ValidateRegistry(contract, registry); err != nil {
		t.Fatal(err)
	}
	if unresolved := UnresolvedMust(contract, registry); len(unresolved) != 1 ||
		unresolved[0] != "SC-01" {
		t.Fatalf("unresolved = %v, want [SC-01]", unresolved)
	}
	document := []byte(`{"schema_version":2,"requirements":[{"id":"SC-01","status":"verified",` +
		`"accepted_commits":[],"test_symbols":[],"scenario_keys":[]}]}`)
	if _, err := DecodeRegistry(document); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("manual status error = %v", err)
	}
}

func TestRegistryRejectsUngroundedEvidenceAndUnknownID(t *testing.T) {
	contract := oneRequirementContract("G-PROCESS")
	registry := oneRequirementRegistry()
	registry.Requirements[0].AcceptedCommits = []string{strings.Repeat("a", 40)}
	if err := ValidateRegistry(contract, registry); err == nil ||
		!strings.Contains(err.Error(), "ungrounded behavioral evidence") {
		t.Fatalf("ungrounded evidence error = %v", err)
	}
	registry = oneRequirementRegistry()
	registry.Requirements[0].ID = "SC-99"
	if err := ValidateRegistry(contract, registry); err == nil ||
		!strings.Contains(err.Error(), "unknown requirement") {
		t.Fatalf("unknown requirement error = %v", err)
	}
}

func TestParseScenarioKeyRejectsUnknownAnchorKind(t *testing.T) {
	if _, err := ParseScenarioKey("core-flow/marker/complete"); err == nil ||
		!strings.Contains(err.Error(), "unknown scenario anchor kind") {
		t.Fatalf("unknown scenario anchor kind error = %v", err)
	}
	if _, err := ParseScenarioKey("invented-key"); err == nil ||
		!strings.Contains(err.Error(), "scenario/(fault|system|task|experience)/anchor") {
		t.Fatalf("unbound scenario key error = %v", err)
	}
}

func oneRequirementContract(gate string) Contract {
	return Contract{
		Requirements: []Requirement{{
			ID: "SC-01", Level: "MUST", Clause: "proof", Owner: ".", PrimaryGate: gate,
		}},
		Gates: []Gate{{ID: gate, Closure: "proof"}},
	}
}

func oneRequirementRegistry() Registry {
	return Registry{
		SchemaVersion: RegistrySchemaVersion,
		Requirements: []EvidenceRecord{{
			ID: "SC-01", AcceptedCommits: []string{}, TestSymbols: []string{}, ScenarioKeys: []string{},
		}},
	}
}
