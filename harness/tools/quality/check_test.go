package main

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
)

func TestValidateAllManifestsRejectsMalformedManifest(t *testing.T) {
	baseline := validBaselineManifest()
	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}}
	architecture := architectureManifest{SchemaVersion: 1, SourceCommit: baseline.SourceCommit, Entries: []architectureEntry{}}
	contract := testCoreContract()
	requirements := testCoreRegistry()
	if err := validateAllManifests(baseline, exceptions, architecture, contract, requirements); err != nil {
		t.Fatalf("valid manifests: %v", err)
	}
	requirements.Requirements[0].ID = "SC-99"
	if err := validateAllManifests(baseline, exceptions, architecture, contract, requirements); err == nil ||
		!strings.Contains(err.Error(), "unknown requirement") {
		t.Fatalf("unknown requirement error = %v", err)
	}
	baseline.ToolVersion = "latest"
	if err := validateAllManifests(baseline, exceptions, architecture, contract,
		testCoreRegistry()); err == nil || !strings.Contains(err.Error(), "tool_version") {
		t.Fatalf("tool version error = %v", err)
	}
}

func testCoreContract() corecontract.Contract {
	return corecontract.Contract{
		Requirements: []corecontract.Requirement{{
			ID: "SC-01", Level: "MUST", Clause: "proof", Owner: ".", PrimaryGate: "G-PROCESS",
		}},
		Gates: []corecontract.Gate{{ID: "G-PROCESS", Closure: "proof"}},
	}
}

func testCoreRegistry() requirementsManifest {
	return requirementsManifest{
		SchemaVersion: corecontract.RegistrySchemaVersion,
		Requirements: []requirementRecord{{
			ID: "SC-01", TestSymbols: []string{}, ScenarioKeys: []string{},
			LiveScenarioKeys: []string{},
		}},
	}
}

func TestCommittedManifestChainFindsBootstrapOnSecondMergeParent(t *testing.T) {
	root := initTestRepository(t)
	writeTestFile(t, root, "README.md", "base\n")
	base := commitTestRepository(t, root, "base")
	branchBytes, err := runGit(root, "branch", "--show-current")
	if err != nil {
		t.Fatal(err)
	}
	mainBranch := strings.TrimSpace(string(branchBytes))
	if _, err := runGit(root, "checkout", "-b", "quality-branch"); err != nil {
		t.Fatal(err)
	}
	manifest := validBaselineManifest()
	manifest.SourceCommit = base
	writeRatchetBundle(t, root, manifest)
	commitTestRepository(t, root, "quality bootstrap")
	if _, err := runGit(root, "checkout", mainBranch); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, "README.md", "main branch\n")
	commitTestRepository(t, root, "main work")
	if _, err := runGit(root, "merge", "--no-ff", "quality-branch", "-m", "merge quality"); err != nil {
		t.Fatal(err)
	}
	if err := validateCommittedManifestChain(root, base); err != nil {
		t.Fatalf("merge history chain: %v", err)
	}
}

func initTestRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if _, err := runGit(root, "init"); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(root, "config", "user.email", "quality@example.invalid"); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(root, "config", "user.name", "Quality Test"); err != nil {
		t.Fatal(err)
	}
	return root
}

func commitTestRepository(t *testing.T, root, message string) string {
	t.Helper()
	if _, err := runGit(root, "add", "."); err != nil {
		t.Fatal(err)
	}
	if _, err := runGit(root, "commit", "-m", message); err != nil {
		t.Fatal(err)
	}
	commit, err := currentCommit(root)
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func writeCanonicalTestFile(t *testing.T, root, relative string, value any) {
	t.Helper()
	if err := writeCanonicalJSON(root+"/"+relative, value); err != nil {
		t.Fatal(err)
	}
}
