package contracts_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
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

type requirementClauseRegistry struct {
	SchemaVersion  int                 `json:"schema_version"`
	SourceDocument string              `json:"source_document"`
	DigestScheme   string              `json:"digest_scheme"`
	BindingDigest  string              `json:"binding_digest"`
	Requirements   []requirementClause `json:"requirements"`
}

type requirementClause struct {
	ID           string `json:"id"`
	Level        string `json:"level"`
	ClauseDigest string `json:"clause_digest"`
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

type requirementScenarioRegistry struct {
	SchemaVersion int                             `json:"schema_version"`
	Scenarios     []requirementScenarioDefinition `json:"scenarios"`
}

type requirementScenarioDefinition struct {
	Key           string `json:"key"`
	RequirementID string `json:"requirement_id"`
	Case          string `json:"case"`
	AnchorKind    string `json:"anchor_kind"`
	Anchor        string `json:"anchor"`
}

type canonicalScenarioManifest struct {
	SchemaVersion int    `json:"schema_version"`
	Name          string `json:"name"`
	Faults        []struct {
		ID string `json:"id"`
	} `json:"faults"`
	Oracles struct {
		System     []string `json:"system"`
		Experience []string `json:"experience"`
	} `json:"oracles"`
}

// The source path is provenance only: the R5 blueprint forbids tracked CI gates from
// depending on the ignored design tree, so the reviewed digest catalog is authoritative.
const normativeRequirementsDocument = ".mnemon-dev/architecture/r5/requirements-and-gates.md"
const normativeRequirementDigestScheme = "sha256:mnemon-r5-requirement-clause-v1(id,level,clause)"

var requirementIDPattern = regexp.MustCompile(`^[A-Z]{2,3}-[0-9]{2}$`)
var commitPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var clauseDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var scenarioKeyPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,95}$`)
var scenarioNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,63}$`)

func TestRequirementsRegistryIsClosedAndEvidenceBacked(t *testing.T) {
	t.Parallel()
	root := repositoryRoot(t)
	contracts := filepath.Join(root, "harness", "test", "contracts")
	expected := decodeContractJSON[expectedRequirementRegistry](t,
		filepath.Join(contracts, "expected_requirements.json"))
	registry := decodeContractJSON[requirementRegistry](t,
		filepath.Join(contracts, "requirements.json"))
	clauses := decodeContractJSON[requirementClauseRegistry](t,
		filepath.Join(contracts, "requirement_clauses.json"))
	scenarios := decodeContractJSON[requirementScenarioRegistry](t,
		filepath.Join(contracts, "requirement_scenarios.json"))
	if expected.SchemaVersion != 1 || registry.SchemaVersion != 1 || clauses.SchemaVersion != 1 ||
		scenarios.SchemaVersion != 1 {
		t.Fatalf("registry schema versions = (%d,%d,%d,%d), want (1,1,1,1)",
			expected.SchemaVersion, registry.SchemaVersion, clauses.SchemaVersion,
			scenarios.SchemaVersion)
	}
	if clauses.SourceDocument != normativeRequirementsDocument {
		t.Fatalf("normative source document = %q, want %q", clauses.SourceDocument,
			normativeRequirementsDocument)
	}
	if clauses.DigestScheme != normativeRequirementDigestScheme {
		t.Fatalf("normative digest scheme = %q, want %q", clauses.DigestScheme,
			normativeRequirementDigestScheme)
	}
	if len(expected.Requirements) != 103 {
		t.Fatalf("expected requirement count = %d, want 103", len(expected.Requirements))
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
	if err := validateClauseBindings(expected.Requirements, clauses.Requirements,
		clauses.BindingDigest); err != nil {
		t.Error(err)
	}
	requireScenarioKeys(t, registry.Requirements)
	validateRequirementScenarios(t, root, registry.Requirements, scenarios.Scenarios)
}

func validateClauseBindings(expected []expectedRequirement, clauses []requirementClause,
	bindingDigest string,
) error {
	var validationErrors []error
	expectedByID := make(map[string]expectedRequirement, len(expected))
	for _, requirement := range expected {
		expectedByID[requirement.ID] = requirement
	}
	clauseIDs := make([]string, len(clauses))
	for index, clause := range clauses {
		clauseIDs[index] = clause.ID
		expectedRequirement, exists := expectedByID[clause.ID]
		if !exists {
			validationErrors = append(validationErrors,
				fmt.Errorf("normative clause references unknown requirement %s", clause.ID))
			continue
		}
		if clause.Level != expectedRequirement.Level {
			validationErrors = append(validationErrors,
				fmt.Errorf("requirement %s normative level = %q, want %q", clause.ID,
					clause.Level, expectedRequirement.Level))
		}
		if !clauseDigestPattern.MatchString(clause.ClauseDigest) {
			validationErrors = append(validationErrors,
				fmt.Errorf("requirement %s has invalid normative clause digest %q", clause.ID,
					clause.ClauseDigest))
		}
	}
	wantIDs := make([]string, len(expected))
	for index, requirement := range expected {
		wantIDs[index] = requirement.ID
	}
	if !slices.Equal(clauseIDs, wantIDs) {
		validationErrors = append(validationErrors,
			fmt.Errorf("normative clause IDs differ from expected set"))
	}
	if !clauseDigestPattern.MatchString(bindingDigest) {
		validationErrors = append(validationErrors,
			fmt.Errorf("invalid normative clause binding digest %q", bindingDigest))
	} else if observed := digestRequirementClauses(clauses); observed != bindingDigest {
		validationErrors = append(validationErrors,
			fmt.Errorf("normative clause binding digest = %q, want %q", observed,
				bindingDigest))
	}
	return errors.Join(validationErrors...)
}

func digestRequirementClauses(clauses []requirementClause) string {
	var binding strings.Builder
	binding.WriteString("mnemon-r5-requirement-registry-v1\n")
	for _, clause := range clauses {
		fmt.Fprintf(&binding, "%s\n%s\n%s\n", clause.ID, clause.Level, clause.ClauseDigest)
	}
	digest := sha256.Sum256([]byte(binding.String()))
	return fmt.Sprintf("sha256:%x", digest)
}

func validateRequirementEvidence(t *testing.T, root string, requirement requirementEvidence) {
	t.Helper()
	if !requirementIDPattern.MatchString(requirement.ID) {
		t.Errorf("invalid requirement evidence ID %q", requirement.ID)
	}
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
		if err := validateAcceptedCommit(root, commit); err != nil {
			t.Errorf("requirement %s accepted commit %q: %v", requirement.ID, commit, err)
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

func validateAcceptedCommit(root, commit string) error {
	if !commitPattern.MatchString(commit) {
		return errors.New("is not a canonical 40-character Git object ID")
	}
	command := exec.Command("git", "cat-file", "-e", commit+"^{commit}")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("does not resolve to a commit: %w: %s", err,
			strings.TrimSpace(string(output)))
	}
	command = exec.Command("git", "merge-base", "--is-ancestor", commit, "HEAD")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err == nil {
		return nil
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) && exitError.ExitCode() == 1 {
		return errors.New("is not an ancestor of HEAD")
	}
	return fmt.Errorf("cannot check ancestry against HEAD: %w: %s", err,
		strings.TrimSpace(string(output)))
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
		"ND-17": "nd-17-attachment-filesystem-db-crash-gap",
		"ND-18": "nd-18-shared-registration-subentry-ownership",
		"ND-20": "nd-20-response-loss-operation-journal",
		"ND-21": "nd-21-one-action-team-expansion",
		"CH-15": "ch-15-terminal-peer-replay",
		"AR-01": "ar-01-local-replica-producer-provenance",
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
}

func validateRequirementScenarios(t *testing.T, root string, requirements []requirementEvidence,
	definitions []requirementScenarioDefinition,
) {
	t.Helper()
	loader := func(name string) (canonicalScenarioManifest, error) {
		if !scenarioNamePattern.MatchString(name) {
			return canonicalScenarioManifest{}, fmt.Errorf("invalid canonical case name %q", name)
		}
		path := filepath.Join(root, "harness", "test", "e2e", "scenarios", name,
			"manifest.json")
		contents, err := os.ReadFile(path)
		if err != nil {
			return canonicalScenarioManifest{}, err
		}
		var manifest canonicalScenarioManifest
		if err := json.Unmarshal(contents, &manifest); err != nil {
			return canonicalScenarioManifest{}, err
		}
		return manifest, nil
	}
	if err := validateScenarioBindings(requirements, definitions, loader); err != nil {
		t.Error(err)
	}
}

type canonicalScenarioLoader func(string) (canonicalScenarioManifest, error)

func validateScenarioBindings(requirements []requirementEvidence,
	definitions []requirementScenarioDefinition, load canonicalScenarioLoader,
) error {
	var validationErrors []error
	requirementByID := make(map[string]requirementEvidence, len(requirements))
	referenceByKey := make(map[string]string)
	for _, requirement := range requirements {
		requirementByID[requirement.ID] = requirement
		for _, key := range requirement.ScenarioKeys {
			if !scenarioKeyPattern.MatchString(key) {
				validationErrors = append(validationErrors,
					fmt.Errorf("requirement %s has invalid scenario key %q", requirement.ID, key))
			}
			if previous, exists := referenceByKey[key]; exists {
				validationErrors = append(validationErrors,
					fmt.Errorf("scenario key %q is referenced by both %s and %s", key, previous,
						requirement.ID))
				continue
			}
			referenceByKey[key] = requirement.ID
		}
	}

	definitionKeys := make([]string, len(definitions))
	definitionByKey := make(map[string]requirementScenarioDefinition, len(definitions))
	loaded := make(map[string]canonicalScenarioManifest)
	for index, definition := range definitions {
		definitionKeys[index] = definition.Key
		if previous, exists := definitionByKey[definition.Key]; exists {
			validationErrors = append(validationErrors,
				fmt.Errorf("canonical scenario key %q is defined more than once (%s and %s)",
					definition.Key, previous.RequirementID, definition.RequirementID))
			continue
		}
		definitionByKey[definition.Key] = definition
		if !scenarioKeyPattern.MatchString(definition.Key) {
			validationErrors = append(validationErrors,
				fmt.Errorf("canonical scenario has invalid key %q", definition.Key))
		}
		if !requirementIDPattern.MatchString(definition.RequirementID) {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q has invalid requirement ID %q", definition.Key,
					definition.RequirementID))
			continue
		}
		if _, exists := requirementByID[definition.RequirementID]; !exists {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q references unknown requirement %s", definition.Key,
					definition.RequirementID))
		}
		referencedBy, exists := referenceByKey[definition.Key]
		if !exists {
			validationErrors = append(validationErrors,
				fmt.Errorf("canonical scenario %q is not referenced by the requirement registry",
					definition.Key))
		} else if referencedBy != definition.RequirementID {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q belongs to %s but is referenced by %s", definition.Key,
					definition.RequirementID, referencedBy))
		}
		manifest, exists := loaded[definition.Case]
		if !exists {
			var err error
			manifest, err = load(definition.Case)
			if err != nil {
				validationErrors = append(validationErrors,
					fmt.Errorf("scenario %q canonical case %q: %w", definition.Key,
						definition.Case, err))
				continue
			}
			loaded[definition.Case] = manifest
		}
		if manifest.SchemaVersion != 1 || manifest.Name != definition.Case {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q canonical case identity = (%d,%q), want (1,%q)",
					definition.Key, manifest.SchemaVersion, manifest.Name, definition.Case))
		}
		if !slices.Contains(manifest.Oracles.System, definition.RequirementID) {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q canonical case %q lacks requirement oracle %s",
					definition.Key, definition.Case, definition.RequirementID))
		}
		if definition.Anchor == "" || definition.Anchor == definition.RequirementID {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q lacks an independent canonical anchor", definition.Key))
			continue
		}
		if !scenarioDefinesAnchor(manifest, definition.AnchorKind, definition.Anchor) {
			validationErrors = append(validationErrors,
				fmt.Errorf("scenario %q canonical case %q does not define %s anchor %q",
					definition.Key, definition.Case, definition.AnchorKind, definition.Anchor))
		}
	}
	if !slices.IsSorted(definitionKeys) {
		validationErrors = append(validationErrors,
			fmt.Errorf("canonical scenario keys are not sorted: %v", definitionKeys))
	}
	for key, requirementID := range referenceByKey {
		if _, exists := definitionByKey[key]; !exists {
			validationErrors = append(validationErrors,
				fmt.Errorf("requirement %s scenario key %q has no canonical definition",
					requirementID, key))
		}
	}
	return errors.Join(validationErrors...)
}

func scenarioDefinesAnchor(manifest canonicalScenarioManifest, kind, anchor string) bool {
	switch kind {
	case "fault":
		for _, fault := range manifest.Faults {
			if fault.ID == anchor {
				return true
			}
		}
	case "system":
		return slices.Contains(manifest.Oracles.System, anchor)
	case "experience":
		return slices.Contains(manifest.Oracles.Experience, anchor)
	}
	return false
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
