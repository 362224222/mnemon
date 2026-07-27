package corecontract

import (
	"strings"
	"testing"
)

func TestRegistryIsMappingOnlyAndRejectsManualStatus(t *testing.T) {
	contract := oneRequirementContract("G-PROCESS")
	registry := oneRequirementRegistry()
	if err := ValidateRegistry(contract, registry); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"schema_version":3,"requirements":[{"id":"SC-01","status":"verified",` +
		`"test_symbols":[],"scenario_keys":[],"live_scenario_keys":[]}]}`)
	if _, err := DecodeRegistry(document); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("manual status error = %v", err)
	}
	document = []byte(`{"schema_version":3,"requirements":[{"id":"SC-01",` +
		`"accepted_commits":[],"test_symbols":[],"scenario_keys":[],"live_scenario_keys":[]}]}`)
	if _, err := DecodeRegistry(document); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("accepted commit error = %v", err)
	}
}

func TestRegistryRejectsIncompleteScenarioMappingAndUnknownID(t *testing.T) {
	contract := oneRequirementContract("G-DOCKER")
	registry := oneRequirementRegistry()
	registry.Requirements[0].TestSymbols = []string{"proof_test.go::TestProof"}
	if err := ValidateRegistry(contract, registry); err == nil ||
		!strings.Contains(err.Error(), "requires a Hermetic scenario key") {
		t.Fatalf("incomplete scenario mapping error = %v", err)
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
			ID: "SC-01", TestSymbols: []string{}, ScenarioKeys: []string{},
			LiveScenarioKeys: []string{},
		}},
	}
}
