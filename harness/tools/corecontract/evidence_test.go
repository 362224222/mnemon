package corecontract

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBindingsRequireCurrentTopLevelTestDeclaration(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "proof_test.go", `package proof
import "testing"
func TestProof(t *testing.T) {
	if got := 2 + 2; got != 4 { t.Fatal(got) }
}
`)
	contract := oneRequirementContract("G-PROCESS")
	registry := oneRequirementRegistry()
	registry.Requirements[0].TestSymbols = []string{"proof_test.go::TestProof"}
	if err := ValidateBindings(root, contract, registry); err != nil {
		t.Fatal(err)
	}
	registry.Requirements[0].TestSymbols = []string{"proof_test.go::TestMissing"}
	if err := ValidateBindings(root, contract, registry); err == nil ||
		!strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("missing test error = %v", err)
	}
}

func TestBindingsRequireCurrentScenarioAnchor(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, root, "harness/test/e2e/scenarios/core-flow/manifest.json", `{
  "schema_version": 1,
  "name": "core-flow",
  "faults": [{"id": "receipt-loss"}],
  "oracles": {"system": [], "task": [], "experience": []}
}`)
	contract := oneRequirementContract("G-DOCKER")
	registry := oneRequirementRegistry()
	registry.Requirements[0].ScenarioKeys = []string{"core-flow/fault/receipt-loss"}
	if err := ValidateBindings(root, contract, registry); err != nil {
		t.Fatal(err)
	}
	registry.Requirements[0].ScenarioKeys = []string{"core-flow/fault/invented-fault"}
	if err := ValidateBindings(root, contract, registry); err == nil ||
		!strings.Contains(err.Error(), "does not declare fault anchor") {
		t.Fatalf("invented scenario anchor error = %v", err)
	}
}

func runTestGit(t *testing.T, root string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", arguments, err, output)
	}
	return strings.TrimSpace(string(output))
}

func writeTestFile(t *testing.T, root, relative, contents string) {
	t.Helper()
	filename := filepath.Join(root, filepath.FromSlash(relative))
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
