package contracts_test

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

type expectedRequirementRegistry struct {
	SchemaVersion int                   `json:"schema_version"`
	Requirements  []expectedRequirement `json:"requirements"`
}

type expectedRequirement struct {
	ID    string `json:"id"`
	Level string `json:"level"`
}

type requirementRegistry struct {
	SchemaVersion int                   `json:"schema_version"`
	Requirements  []requirementEvidence `json:"requirements"`
}

type requirementEvidence struct {
	ID              string   `json:"id"`
	Status          string   `json:"status"`
	OwnerPackages   []string `json:"owner_packages"`
	AcceptedCommits []string `json:"accepted_commits"`
	TestSymbols     []string `json:"test_symbols"`
	ScenarioKeys    []string `json:"scenario_keys"`
	EvidenceGates   []string `json:"evidence_gates"`
}

var requirementIDPattern = regexp.MustCompile(`^[A-Z]{2,3}-[0-9]{2}$`)
var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)

func TestRequirementsRegistryIsClosedAndEvidenceBacked(t *testing.T) {
	t.Parallel()
	root := repositoryRoot(t)
	contracts := filepath.Join(root, "harness", "test", "contracts")
	expected := decodeContractJSON[expectedRequirementRegistry](t,
		filepath.Join(contracts, "expected_requirements.json"))
	registry := decodeContractJSON[requirementRegistry](t,
		filepath.Join(contracts, "requirements.json"))
	if expected.SchemaVersion != 1 || registry.SchemaVersion != 1 {
		t.Fatalf("registry schema versions = (%d,%d), want (1,1)",
			expected.SchemaVersion, registry.SchemaVersion)
	}
	if len(expected.Requirements) != 132 {
		t.Fatalf("expected requirement count = %d, want 132", len(expected.Requirements))
	}
	if len(registry.Requirements) != len(expected.Requirements) {
		t.Fatalf("requirement evidence count = %d, want %d", len(registry.Requirements),
			len(expected.Requirements))
	}

	wantIDs := make([]string, len(expected.Requirements))
	for index, requirement := range expected.Requirements {
		if !requirementIDPattern.MatchString(requirement.ID) {
			t.Errorf("invalid expected requirement ID %q", requirement.ID)
		}
		if requirement.Level != "MUST" && requirement.Level != "SHOULD" {
			t.Errorf("requirement %s has invalid level %q", requirement.ID, requirement.Level)
		}
		wantIDs[index] = requirement.ID
	}
	assertSortedUnique(t, "expected requirement IDs", wantIDs)

	gotIDs := make([]string, len(registry.Requirements))
	for index, requirement := range registry.Requirements {
		gotIDs[index] = requirement.ID
		validateRequirementEvidence(t, root, requirement)
	}
	assertSortedUnique(t, "requirement evidence IDs", gotIDs)
	if !slices.Equal(gotIDs, wantIDs) {
		t.Fatalf("requirement evidence IDs differ from expected set\ngot:  %v\nwant: %v",
			gotIDs, wantIDs)
	}
	requireScenarioKeys(t, registry.Requirements)
}

func validateRequirementEvidence(t *testing.T, root string, requirement requirementEvidence) {
	t.Helper()
	if requirement.Status != "pending" && requirement.Status != "verified" {
		t.Errorf("requirement %s has invalid status %q", requirement.ID, requirement.Status)
	}
	if len(requirement.OwnerPackages) == 0 || len(requirement.EvidenceGates) == 0 {
		t.Errorf("requirement %s lacks owner packages or evidence gates", requirement.ID)
	}
	assertSortedUnique(t, requirement.ID+" owner packages", requirement.OwnerPackages)
	assertSortedUnique(t, requirement.ID+" accepted commits", requirement.AcceptedCommits)
	assertSortedUnique(t, requirement.ID+" test symbols", requirement.TestSymbols)
	assertSortedUnique(t, requirement.ID+" scenario keys", requirement.ScenarioKeys)
	assertSortedUnique(t, requirement.ID+" evidence gates", requirement.EvidenceGates)
	for _, owner := range requirement.OwnerPackages {
		if filepath.IsAbs(owner) || strings.Contains(owner, "..") {
			t.Errorf("requirement %s has unsafe owner path %q", requirement.ID, owner)
			continue
		}
		if info, err := os.Stat(filepath.Join(root, filepath.FromSlash(owner))); err != nil || !info.IsDir() {
			t.Errorf("requirement %s owner path %q is not a directory", requirement.ID, owner)
		}
	}
	for _, commit := range requirement.AcceptedCommits {
		if !commitPattern.MatchString(commit) {
			t.Errorf("requirement %s has invalid accepted commit %q", requirement.ID, commit)
			continue
		}
		command := exec.Command("git", "cat-file", "-e", commit+"^{commit}")
		command.Dir = root
		if output, err := command.CombinedOutput(); err != nil {
			t.Errorf("requirement %s accepted commit %q: %v: %s",
				requirement.ID, commit, err, output)
		}
	}
	for _, symbol := range requirement.TestSymbols {
		validateTestSymbol(t, root, requirement.ID, symbol, requirement.AcceptedCommits)
	}
	if requirement.Status == "verified" && (len(requirement.AcceptedCommits) == 0 ||
		len(requirement.TestSymbols) == 0 || len(requirement.ScenarioKeys) == 0 ||
		len(requirement.EvidenceGates) == 0) {
		t.Errorf("verified requirement %s lacks complete evidence", requirement.ID)
	}
}

func validateTestSymbol(t *testing.T, root, requirementID, evidence string, acceptedCommits []string) {
	t.Helper()
	path, symbol, ok := strings.Cut(evidence, "::")
	if !ok || path == "" || symbol == "" || !strings.HasSuffix(path, "_test.go") ||
		filepath.IsAbs(path) || strings.Contains(path, "..") {
		t.Errorf("requirement %s has invalid test symbol %q", requirementID, evidence)
		return
	}
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, filepath.FromSlash(path)),
		nil, parser.SkipObjectResolution)
	if err != nil {
		t.Errorf("requirement %s test symbol %q: %v", requirementID, evidence, err)
		return
	}
	if !declaresTopLevelTestFunction(file, symbol) {
		t.Errorf("requirement %s test symbol %q does not exist", requirementID, evidence)
		return
	}
	var firstReadError error
	var firstReadErrorCommit string
	for _, commit := range acceptedCommits {
		found, err := commitDeclaresTestSymbol(root, commit, path, symbol)
		if err != nil {
			if firstReadError == nil {
				firstReadError = err
				firstReadErrorCommit = commit
			}
			continue
		}
		if found {
			return
		}
	}
	if firstReadError != nil {
		t.Errorf("requirement %s test symbol %q at accepted commit %s: %v",
			requirementID, evidence, firstReadErrorCommit, firstReadError)
		return
	}
	t.Errorf("requirement %s test symbol %q does not exist in any accepted commit",
		requirementID, evidence)
}

func commitDeclaresTestSymbol(root, commit, path, symbol string) (bool, error) {
	command := exec.Command("git", "show", "--no-ext-diff", commit+":"+path)
	command.Dir = root
	data, err := command.Output()
	if err != nil {
		return false, nil
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.SkipObjectResolution)
	if err != nil {
		return false, err
	}
	return declaresTopLevelTestFunction(file, symbol), nil
}

func declaresTopLevelTestFunction(file *ast.File, symbol string) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == symbol {
			return true
		}
	}
	return false
}

func requireScenarioKeys(t *testing.T, requirements []requirementEvidence) {
	t.Helper()
	required := map[string]string{
		"ND-17":  "nd-17-attachment-filesystem-db-crash-gap",
		"ND-18":  "nd-18-shared-registration-subentry-ownership",
		"ND-20":  "nd-20-response-loss-operation-journal",
		"ND-21":  "nd-21-one-action-team-expansion",
		"ND-22":  "nd-22-unique-default-profile-adapter",
		"CH-15":  "ch-15-terminal-peer-replay",
		"AR-01":  "ar-01-local-replica-producer-provenance",
		"AR-10":  "ar-10-readonly-view-crash-lifecycle",
		"TW-10":  "tw-10-deadline-restart-race",
		"NET-07": "net-07-frozen-hermetic-resource-profile",
		"PI-05":  "pi-05-revision-tombstone-conflict",
		"PI-07":  "pi-07-closed-channel-frames",
		"PI-10":  "pi-10-eligible-auto-team-invariance",
		"PI-11":  "pi-11-prompt-injection-boundary",
		"EQ-02":  "eq-02-closed-registry-completeness-determinism",
		"EQ-03":  "eq-03-goroutine-owner-cancel-bound-wait",
		"EQ-07":  "eq-07-durable-transition-io-fencing",
		"EQ-08":  "eq-08-supervisor-lifecycle",
		"EQ-09":  "eq-09-baseline-delta-hygiene",
		"EQ-10":  "eq-10-race-fuzz-oracle-preservation",
	}
	byID := make(map[string]requirementEvidence, len(requirements))
	for _, requirement := range requirements {
		byID[requirement.ID] = requirement
	}
	for id, scenario := range required {
		if !slices.Contains(byID[id].ScenarioKeys, scenario) {
			t.Errorf("requirement %s lacks independent scenario key %q", id, scenario)
		}
	}
	for _, id := range []string{"PI-01", "PI-02", "PI-03", "PI-04", "PI-05", "PI-06",
		"PI-07", "PI-08", "PI-09", "PI-10", "PI-11", "PI-12"} {
		if byID[id].Status != "pending" {
			t.Errorf("7Q Profile requirement %s status = %q, want pending", id, byID[id].Status)
		}
	}
}

func decodeContractJSON[T any](t *testing.T, path string) T {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var value T
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("decode %s trailing data: %v", path, err)
	}
	return value
}

func assertSortedUnique(t *testing.T, name string, values []string) {
	t.Helper()
	if !slices.IsSorted(values) {
		t.Errorf("%s are not sorted: %v", name, values)
	}
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			t.Errorf("%s contain duplicate %q", name, values[index])
		}
	}
}
