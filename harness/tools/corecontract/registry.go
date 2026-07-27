package corecontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

const (
	RegistryPath          = "harness/test/contracts/requirements.json"
	RegistrySchemaVersion = 3
)

var (
	scenarioNamePattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,63}$`)
	scenarioAnchorPattern = regexp.MustCompile(
		`^[A-Za-z0-9][A-Za-z0-9-]{2,95}$`,
	)
	testSymbolPattern = regexp.MustCompile(`^(Test|Fuzz)[A-Za-z0-9_]+$`)
)

type Registry struct {
	SchemaVersion int              `json:"schema_version"`
	Requirements  []EvidenceRecord `json:"requirements"`
}

type EvidenceRecord struct {
	ID               string   `json:"id"`
	TestSymbols      []string `json:"test_symbols"`
	ScenarioKeys     []string `json:"scenario_keys"`
	LiveScenarioKeys []string `json:"live_scenario_keys"`
}

type ScenarioKey struct {
	Scenario string
	Kind     string
	Anchor   string
}

const scenarioDirectory = "harness/test/e2e/scenarios"

type scenarioManifest struct {
	SchemaVersion int    `json:"schema_version"`
	Name          string `json:"name"`
	Faults        []struct {
		ID string `json:"id"`
	} `json:"faults"`
	Oracles struct {
		System []string `json:"system"`
		Task   []struct {
			ID string `json:"id"`
		} `json:"task"`
		Experience []string `json:"experience"`
	} `json:"oracles"`
}

// ValidateBindings validates tracked requirement mappings without treating
// those declarations as runtime completion.
func ValidateBindings(root string, contract Contract, registry Registry) error {
	if err := ValidateRegistry(contract, registry); err != nil {
		return err
	}
	if err := ValidateOwnerDirectories(root, contract); err != nil {
		return err
	}
	for _, record := range registry.Requirements {
		for _, reference := range record.TestSymbols {
			pathValue, symbol, _ := ParseTestSymbol(reference)
			if err := validateCurrentTestSymbol(root, record.ID, pathValue, symbol); err != nil {
				return err
			}
		}
		for _, values := range [][]string{record.ScenarioKeys, record.LiveScenarioKeys} {
			for _, value := range values {
				key, _ := ParseScenarioKey(value)
				if err := validateScenarioBinding(root, record.ID, key); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateCurrentTestSymbol(root, requirementID, relative, symbol string) error {
	filename := filepath.Join(root, filepath.FromSlash(relative))
	parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("requirement %s test evidence %s::%s: %w",
			requirementID, relative, symbol, err)
	}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == symbol {
			if function.Body == nil || len(function.Body.List) == 0 {
				return fmt.Errorf("requirement %s test evidence %s has an empty body",
					requirementID, symbol)
			}
			return nil
		}
	}
	return fmt.Errorf("requirement %s test evidence %s does not declare %s",
		requirementID, relative, symbol)
}

func validateScenarioBinding(root, requirementID string, key ScenarioKey) error {
	relative := filepath.ToSlash(filepath.Join(
		scenarioDirectory, key.Scenario, "manifest.json"))
	current, err := readScenarioManifest(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return fmt.Errorf("requirement %s scenario evidence %s: %w", requirementID,
			strings.Join([]string{key.Scenario, key.Kind, key.Anchor}, "/"), err)
	}
	if err := validateScenarioAnchor(current, key); err != nil {
		return fmt.Errorf("requirement %s scenario evidence %s: %w", requirementID,
			strings.Join([]string{key.Scenario, key.Kind, key.Anchor}, "/"), err)
	}
	return nil
}

func readScenarioManifest(filename string) (scenarioManifest, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return scenarioManifest{}, fmt.Errorf("read canonical scenario manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var manifest scenarioManifest
	if err := decoder.Decode(&manifest); err != nil {
		return scenarioManifest{}, fmt.Errorf("decode canonical scenario manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return scenarioManifest{}, fmt.Errorf(
			"decode canonical scenario manifest trailing data: %v", err)
	}
	if manifest.SchemaVersion != 1 {
		return scenarioManifest{}, fmt.Errorf("canonical scenario schema_version = %d, want 1",
			manifest.SchemaVersion)
	}
	if !scenarioNamePattern.MatchString(manifest.Name) {
		return scenarioManifest{}, fmt.Errorf("canonical scenario name %q is malformed", manifest.Name)
	}
	return manifest, nil
}

func validateScenarioAnchor(manifest scenarioManifest, key ScenarioKey) error {
	if manifest.Name != key.Scenario {
		return fmt.Errorf("canonical scenario name = %q, want %q", manifest.Name, key.Scenario)
	}
	var anchors []string
	switch key.Kind {
	case "fault":
		for _, fault := range manifest.Faults {
			anchors = append(anchors, fault.ID)
		}
	case "system":
		anchors = manifest.Oracles.System
	case "task":
		for _, task := range manifest.Oracles.Task {
			anchors = append(anchors, task.ID)
		}
	case "experience":
		anchors = manifest.Oracles.Experience
	}
	count := 0
	for _, anchor := range anchors {
		if anchor == key.Anchor {
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("canonical scenario does not declare %s anchor %q",
			key.Kind, key.Anchor)
	}
	if count > 1 {
		return fmt.Errorf("canonical scenario repeats %s anchor %q", key.Kind, key.Anchor)
	}
	return nil
}

func LoadRegistry(filename string) (Registry, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return Registry{}, fmt.Errorf("read Core evidence registry: %w", err)
	}
	return DecodeRegistry(contents)
}

func DecodeRegistry(contents []byte) (Registry, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var registry Registry
	if err := decoder.Decode(&registry); err != nil {
		return Registry{}, fmt.Errorf("decode Core evidence registry: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Registry{}, fmt.Errorf("decode Core evidence registry trailing data: %v", err)
	}
	return registry, nil
}

func ValidateRegistry(contract Contract, registry Registry) error {
	if registry.SchemaVersion != RegistrySchemaVersion {
		return fmt.Errorf("Core evidence registry schema_version = %d, want %d",
			registry.SchemaVersion, RegistrySchemaVersion)
	}
	if registry.Requirements == nil {
		return fmt.Errorf("Core evidence requirements must be an array, not null")
	}
	authority := contract.RequirementByID()
	if len(registry.Requirements) != len(authority) {
		return fmt.Errorf("Core evidence registry has %d IDs, want %d",
			len(registry.Requirements), len(authority))
	}
	seen := make(map[string]struct{}, len(registry.Requirements))
	previous := ""
	for index, record := range registry.Requirements {
		requirement, exists := authority[record.ID]
		if !exists {
			return fmt.Errorf("Core evidence registry contains unknown requirement %q", record.ID)
		}
		if _, duplicate := seen[record.ID]; duplicate {
			return fmt.Errorf("Core evidence registry repeats requirement %s", record.ID)
		}
		if index > 0 && record.ID <= previous {
			return fmt.Errorf("Core evidence registry IDs are not sorted at %s", record.ID)
		}
		seen[record.ID] = struct{}{}
		previous = record.ID
		if err := validateEvidenceRecord(requirement, record); err != nil {
			return fmt.Errorf("requirement %s: %w", record.ID, err)
		}
	}
	for id := range authority {
		if _, exists := seen[id]; !exists {
			return fmt.Errorf("Core evidence registry is missing requirement %s", id)
		}
	}
	return nil
}

func ParseTestSymbol(reference string) (string, string, error) {
	pathValue, symbol, found := strings.Cut(reference, "::")
	if !found || pathValue == "" || symbol == "" || strings.Contains(symbol, "::") {
		return "", "", fmt.Errorf("%q is not path::test-symbol", reference)
	}
	if err := validateEvidencePath(pathValue); err != nil {
		return "", "", err
	}
	if !strings.HasSuffix(pathValue, "_test.go") {
		return "", "", fmt.Errorf("%q does not reference a _test.go file", reference)
	}
	if !testSymbolPattern.MatchString(symbol) {
		return "", "", fmt.Errorf("%q does not name a top-level Test or Fuzz function", symbol)
	}
	return pathValue, symbol, nil
}

// ParseScenarioKey parses a direct binding to one canonical scenario manifest
// anchor: <scenario>/<fault|system|task|experience>/<anchor>.
func ParseScenarioKey(value string) (ScenarioKey, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 3 || !scenarioNamePattern.MatchString(parts[0]) ||
		!scenarioAnchorPattern.MatchString(parts[2]) {
		return ScenarioKey{}, fmt.Errorf(
			"%q is not scenario/(fault|system|task|experience)/anchor", value)
	}
	switch parts[1] {
	case "fault", "system", "task", "experience":
	default:
		return ScenarioKey{}, fmt.Errorf("%q has unknown scenario anchor kind %q",
			value, parts[1])
	}
	return ScenarioKey{Scenario: parts[0], Kind: parts[1], Anchor: parts[2]}, nil
}

func validateEvidenceRecord(requirement Requirement, record EvidenceRecord) error {
	arrays := []struct {
		name   string
		values []string
	}{
		{name: "test_symbols", values: record.TestSymbols},
		{name: "scenario_keys", values: record.ScenarioKeys},
		{name: "live_scenario_keys", values: record.LiveScenarioKeys},
	}
	for _, array := range arrays {
		if array.values == nil {
			return fmt.Errorf("%s must be an array, not null", array.name)
		}
		if !slices.IsSorted(array.values) {
			return fmt.Errorf("%s must be sorted", array.name)
		}
		for index := 1; index < len(array.values); index++ {
			if array.values[index] == array.values[index-1] {
				return fmt.Errorf("%s repeats %q", array.name, array.values[index])
			}
		}
	}
	for _, reference := range record.TestSymbols {
		if _, _, err := ParseTestSymbol(reference); err != nil {
			return err
		}
	}
	for _, key := range record.ScenarioKeys {
		if _, err := ParseScenarioKey(key); err != nil {
			return err
		}
	}
	for _, key := range record.LiveScenarioKeys {
		if _, err := ParseScenarioKey(key); err != nil {
			return err
		}
	}
	anyEvidence := len(record.TestSymbols)+len(record.ScenarioKeys)+
		len(record.LiveScenarioKeys) > 0
	if !anyEvidence {
		return nil
	}
	if (requirement.PrimaryGate == "G-DOCKER" || requirement.PrimaryGate == "G-EVIDENCE") &&
		len(record.ScenarioKeys) == 0 {
		return fmt.Errorf("primary gate %s requires a Hermetic scenario key",
			requirement.PrimaryGate)
	}
	if requirement.PrimaryGate == "G-LIVE" && len(record.LiveScenarioKeys) == 0 {
		return fmt.Errorf("primary gate %s requires a Live scenario key",
			requirement.PrimaryGate)
	}
	return nil
}

func validateEvidencePath(value string) error {
	if value == "" || value == "." || strings.Contains(value, "\\") || path.IsAbs(value) ||
		path.Clean(value) != value || strings.HasPrefix(value, "../") {
		return fmt.Errorf("%q is not a clean repository-relative evidence path", value)
	}
	return nil
}

func LoadGateReport(filename string) (GateReport, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return GateReport{}, fmt.Errorf("read Core gate report: %w", err)
	}
	return DecodeGateReport(data)
}

func DecodeGateReport(data []byte) (GateReport, error) {
	var report GateReport
	if err := decodeStrictJSON(data, &report); err != nil {
		return GateReport{}, fmt.Errorf("decode Core gate report: %w", err)
	}
	return report, nil
}

func decodeStrictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("trailing data: %v", err)
	}
	return nil
}

func gateStepRule(id string) (stepRule, bool) {
	for _, rule := range gateStepRules {
		if rule.id == id {
			return rule, true
		}
	}
	return stepRule{}, false
}

func expectedStepArgv(rule stepRule, report GateReport) ([]string, error) {
	if rule.kind != "evidence" {
		return rule.argv, nil
	}
	runtimeName := "scripted"
	if rule.id == "evidence-live" {
		runtimeName = "codex"
	}
	for _, bundle := range report.Bundles {
		if bundle.Runtime == runtimeName {
			return []string{"harness/test/e2e/runner/validate_evidence.sh",
				"--run", bundle.RunID}, nil
		}
	}
	return nil, fmt.Errorf("%s bundle is required by %s", runtimeName, rule.id)
}
