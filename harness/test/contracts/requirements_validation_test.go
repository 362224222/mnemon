package contracts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateAcceptedCommitRejectsNonAncestor(t *testing.T) {
	t.Parallel()
	repository := t.TempDir()
	runTestGit(t, repository, "init", "--quiet", "--initial-branch=main")
	runTestGit(t, repository, "config", "user.name", "Mnemon Contract Test")
	runTestGit(t, repository, "config", "user.email", "contracts@example.invalid")
	if err := os.WriteFile(filepath.Join(repository, "main.txt"), []byte("main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repository, "add", "main.txt")
	runTestGit(t, repository, "commit", "--quiet", "-m", "main")
	nonAncestor := runTestGit(t, repository, "rev-parse", "HEAD")

	runTestGit(t, repository, "switch", "--quiet", "--orphan", "side")
	if err := os.WriteFile(filepath.Join(repository, "side.txt"), []byte("side\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runTestGit(t, repository, "add", "--all")
	runTestGit(t, repository, "commit", "--quiet", "-m", "side")
	head := runTestGit(t, repository, "rev-parse", "HEAD")
	if err := validateAcceptedCommit(repository, head); err != nil {
		t.Fatalf("current HEAD rejected: %v", err)
	}
	if err := validateAcceptedCommit(repository, nonAncestor); err == nil ||
		!strings.Contains(err.Error(), "not an ancestor") {
		t.Fatalf("non-ancestor error = %v, want ancestry rejection", err)
	}
}

func TestValidateScenarioBindingsFailsClosed(t *testing.T) {
	t.Parallel()
	requirements := []requirementEvidence{{
		ID:           "ND-17",
		ScenarioKeys: []string{"nd-17-attachment-filesystem-db-crash-gap"},
	}}
	manifest := canonicalScenarioManifest{SchemaVersion: 1, Name: "parallel-hardening"}
	manifest.Oracles.System = []string{"ND-17"}
	load := func(string) (canonicalScenarioManifest, error) { return manifest, nil }

	t.Run("orphan registry key", func(t *testing.T) {
		err := validateScenarioBindings(requirements, nil, load)
		assertErrorContains(t, err, "has no canonical definition")
	})
	t.Run("missing concrete anchor", func(t *testing.T) {
		definitions := []requirementScenarioDefinition{{
			Key:           "nd-17-attachment-filesystem-db-crash-gap",
			RequirementID: "ND-17",
			Case:          "parallel-hardening",
			AnchorKind:    "fault",
			Anchor:        "preclaim-attachment-rename-crash",
		}}
		err := validateScenarioBindings(requirements, definitions, load)
		assertErrorContains(t, err, "does not define fault anchor")
	})
}

func TestValidateClauseBindingsRejectsNormativeDrift(t *testing.T) {
	t.Parallel()
	expected := []expectedRequirement{{ID: "AR-01", Level: "MUST"}}
	clauses := []requirementClause{{
		ID:           "AR-01",
		Level:        "MUST",
		ClauseDigest: "sha256:" + strings.Repeat("a", 64),
	}}
	bindingDigest := digestRequirementClauses(clauses)
	clauses[0].ClauseDigest = "sha256:" + strings.Repeat("b", 64)
	err := validateClauseBindings(expected, clauses, bindingDigest)
	assertErrorContains(t, err, "binding digest")
}

func runTestGit(t *testing.T, repository string, arguments ...string) string {
	t.Helper()
	command := exec.Command("git", arguments...)
	command.Dir = repository
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(arguments, " "), err, output)
	}
	return strings.TrimSpace(string(output))
}

func assertErrorContains(t *testing.T, err error, fragment string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), fragment) {
		t.Fatalf("error = %v, want fragment %q", err, fragment)
	}
}
