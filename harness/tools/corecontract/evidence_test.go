package corecontract

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBehavioralEvidenceBindsTestToAcceptedCommit(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "--quiet")
	runTestGit(t, root, "config", "user.email", "core-contract@example.invalid")
	runTestGit(t, root, "config", "user.name", "Core Contract Test")
	writeTestFile(t, root, "proof_test.go", "package proof\nfunc TestProof() {}\n")
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "--quiet", "-m", "proof")
	commit := runTestGit(t, root, "rev-parse", "HEAD")

	contract := oneRequirementContract("G-PROCESS")
	registry := oneRequirementRegistry()
	registry.Requirements[0].AcceptedCommits = []string{commit}
	registry.Requirements[0].TestSymbols = []string{"proof_test.go::TestProof"}
	if err := ValidateBehavioralEvidence(root, contract, registry); err != nil {
		t.Fatal(err)
	}
	if unresolved := UnresolvedMust(contract, registry); len(unresolved) != 0 {
		t.Fatalf("grounded requirement remains unresolved: %v", unresolved)
	}
	registry.Requirements[0].TestSymbols = []string{"proof_test.go::TestMissing"}
	if err := ValidateBehavioralEvidence(root, contract, registry); err == nil ||
		!strings.Contains(err.Error(), "does not declare") {
		t.Fatalf("missing test error = %v", err)
	}
}

func TestBehavioralEvidenceBindsScenarioAnchorToAcceptedCommit(t *testing.T) {
	root := t.TempDir()
	runTestGit(t, root, "init", "--quiet")
	runTestGit(t, root, "config", "user.email", "core-contract@example.invalid")
	runTestGit(t, root, "config", "user.name", "Core Contract Test")
	writeTestFile(t, root, "proof_test.go", "package proof\nfunc TestProof() {}\n")
	writeTestFile(t, root, "harness/test/e2e/scenarios/core-flow/manifest.json", `{
  "schema_version": 1,
  "name": "core-flow",
  "faults": [{"id": "receipt-loss"}],
  "oracles": {"system": [], "task": [], "experience": []}
}`)
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "--quiet", "-m", "proof")
	commit := runTestGit(t, root, "rev-parse", "HEAD")

	contract := oneRequirementContract("G-DOCKER")
	registry := oneRequirementRegistry()
	registry.Requirements[0].AcceptedCommits = []string{commit}
	registry.Requirements[0].TestSymbols = []string{"proof_test.go::TestProof"}
	registry.Requirements[0].ScenarioKeys = []string{"core-flow/fault/receipt-loss"}
	if err := ValidateBehavioralEvidence(root, contract, registry); err != nil {
		t.Fatal(err)
	}

	writeTestFile(t, root, "harness/test/e2e/scenarios/core-flow/manifest.json", `{
  "schema_version": 1,
  "name": "core-flow",
  "faults": [{"id": "receipt-loss"}, {"id": "uncommitted-fault"}],
  "oracles": {"system": [], "task": [], "experience": []}
}`)
	registry.Requirements[0].ScenarioKeys = []string{"core-flow/fault/uncommitted-fault"}
	if err := ValidateBehavioralEvidence(root, contract, registry); err == nil ||
		!strings.Contains(err.Error(), "absent from every accepted commit") {
		t.Fatalf("uncommitted scenario anchor error = %v", err)
	}

	registry.Requirements[0].ScenarioKeys = []string{"core-flow/fault/invented-fault"}
	if err := ValidateBehavioralEvidence(root, contract, registry); err == nil ||
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
