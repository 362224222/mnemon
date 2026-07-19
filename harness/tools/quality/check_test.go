package main

import (
	"strings"
	"testing"
)

func TestValidateAllManifestsRejectsMalformedManifest(t *testing.T) {
	baseline := validBaselineManifest()
	exceptions := exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{}}
	architecture := architectureManifest{SchemaVersion: 1, SourceCommit: baseline.SourceCommit, Entries: []architectureEntry{}}
	expected := expectedManifest{SchemaVersion: 1, Requirements: []expectedRequirement{{ID: "EQ-02", Level: "MUST"}}}
	requirements := requirementsManifest{SchemaVersion: 1, Requirements: []requirementRecord{{
		ID: "EQ-03", Status: "pending", OwnerPackages: []string{"."}, AcceptedCommits: []string{},
		TestSymbols: []string{}, ScenarioKeys: []string{}, EvidenceGates: []string{},
	}}}
	if err := validateAllManifests(baseline, exceptions, architecture, expected, requirements); err != nil {
		t.Fatalf("individual schemas should be valid before cross-check: %v", err)
	}
	if err := validateRequirementEvidence(t.TempDir(), expected, requirements); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("cross-manifest mismatch error = %v", err)
	}
	baseline.ToolVersion = "latest"
	if err := validateAllManifests(baseline, exceptions, architecture, expected, requirements); err == nil || !strings.Contains(err.Error(), "tool_version") {
		t.Fatalf("tool version error = %v", err)
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
