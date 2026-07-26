package corecontract

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestTrackedContractLoadsCanonicalAuthority(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(contract.Requirements) != RequirementCount {
		t.Fatalf("requirements = %d, want %d", len(contract.Requirements), RequirementCount)
	}
	wantGates := []string{
		"G-CONTRACT", "G-DOCKER", "G-EVIDENCE", "G-LIVE", "G-PROCESS", "G-ROOT", "G-UNIT",
	}
	if got := contract.GateIDs(); !slices.Equal(got, wantGates) {
		t.Fatalf("gates = %v, want %v", got, wantGates)
	}
	if err := ValidateOwnerDirectories(root, contract); err != nil {
		t.Fatal(err)
	}
}
