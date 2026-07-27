package contracts_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

func TestCoreReleaseClosureRequiresRuntimeReport(t *testing.T) {
	root, contract, registry := loadCoreRequirements(t)
	err := coreReleaseClosureError(root, contract, registry, nil)
	if err == nil || !strings.Contains(err.Error(), "requires a runtime gate report") {
		t.Fatalf("release closure missing-report error = %v", err)
	}
}

func TestTrackedCoreContractRejectsUnknownGate(t *testing.T) {
	contents := readCoreContract(t)
	changed := strings.Replace(string(contents),
		"| `G-ROOT` static + process |", "| `G-UNKNOWN` static + process |", 1)
	if changed == string(contents) {
		t.Fatal("unknown-gate fixture did not change the contract")
	}
	if _, err := corecontract.Parse([]byte(changed)); err == nil ||
		!strings.Contains(err.Error(), "unknown primary gate") {
		t.Fatalf("unknown gate error = %v", err)
	}
}

func TestTrackedCoreContractRejectsDuplicateIDAndMalformedOwner(t *testing.T) {
	contents := string(readCoreContract(t))
	duplicate := strings.Replace(contents, "| SC-02 | MUST |", "| SC-01 | MUST |", 1)
	if _, err := corecontract.Parse([]byte(duplicate)); err == nil ||
		!strings.Contains(err.Error(), "repeats requirement SC-01") {
		t.Fatalf("duplicate requirement error = %v", err)
	}
	malformedOwner := strings.Replace(contents,
		"`harness/test/contracts` | `G-ROOT`", "`../escape` | `G-ROOT`", 1)
	if _, err := corecontract.Parse([]byte(malformedOwner)); err == nil ||
		!strings.Contains(err.Error(), "repository-relative directory") {
		t.Fatalf("malformed owner error = %v", err)
	}
}

func TestCoreRegistryRejectsUnknownIDAndUngroundedEvidence(t *testing.T) {
	_, contract, registry := loadCoreRequirements(t)
	registry.Requirements[0].ID = "ZZ-99"
	if err := corecontract.ValidateRegistry(contract, registry); err == nil ||
		!strings.Contains(err.Error(), "unknown requirement") {
		t.Fatalf("unknown requirement error = %v", err)
	}

	_, _, registry = loadCoreRequirements(t)
	registry.Requirements[0].TestSymbols = []string{
		"harness/test/contracts/release_boundary_test.go::TestReleaseBoundary",
	}
	if err := corecontract.ValidateRegistry(contract, registry); err == nil ||
		!strings.Contains(err.Error(), "requires a Hermetic scenario key") {
		t.Fatalf("incomplete evidence mapping error = %v", err)
	}
}

func TestCoreRegistryRejectsManualVerifiedClaim(t *testing.T) {
	document := []byte(`{"schema_version":3,"requirements":[{"id":"SC-01",` +
		`"status":"verified","test_symbols":[],"scenario_keys":[],"live_scenario_keys":[]}]}`)
	if _, err := corecontract.DecodeRegistry(document); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("manual verified claim error = %v", err)
	}
}

func readCoreContract(t *testing.T) []byte {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(repositoryRoot(t),
		filepath.FromSlash(corecontract.DocumentPath)))
	if err != nil {
		t.Fatal(err)
	}
	return contents
}
