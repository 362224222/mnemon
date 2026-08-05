package corecontract

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestTrackedR7RegistryIsCanonicalCompleteAndGrounded(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBindings(root, contract, registry); err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRejectsNullListsUnknownKindsAndDivergentSharedSteps(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	registry.Invariants = nil
	if err := ValidateBindings(root, contract, registry); err == nil || !strings.Contains(err.Error(), "non-null") {
		t.Fatalf("null list error = %v", err)
	}
	registry, _ = LoadRegistry(root)
	registry.Gates[0].Steps[0].Kind = "manual"
	if err := ValidateBindings(root, contract, registry); err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("unknown kind error = %v", err)
	}
	registry, _ = LoadRegistry(root)
	for index := range registry.Gates {
		if registry.Gates[index].ID == "G-R7-ROOT-ISOLATION" {
			registry.Gates[index].Steps[0].Oracles = []string{"test:./test/contracts::TestReleaseBoundary"}
		}
	}
	if err := ValidateBindings(root, contract, registry); err == nil || !strings.Contains(err.Error(), "shared step") {
		t.Fatalf("shared step error = %v", err)
	}
}

func TestRegistryRejectsInvariantWithoutExecutableGateOracle(t *testing.T) {
	root := filepath.Clean("../../..")
	contract, err := Load(root)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := LoadRegistry(root)
	if err != nil {
		t.Fatal(err)
	}
	missing := registry.Invariants[0].Oracles[0].Test
	for gateIndex := range registry.Gates {
		for stepIndex := range registry.Gates[gateIndex].Steps {
			step := &registry.Gates[gateIndex].Steps[stepIndex]
			kept := step.Oracles[:0]
			for _, oracle := range step.Oracles {
				if oracle != "test:"+missing {
					kept = append(kept, oracle)
				}
			}
			step.Oracles = kept
		}
	}
	if err := ValidateBindings(root, contract, registry); err == nil ||
		!strings.Contains(err.Error(), "not bound to any gate step") {
		t.Fatalf("missing executable oracle error = %v", err)
	}
}
