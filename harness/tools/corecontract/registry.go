package corecontract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
)

const RegistrySchemaVersion = 2

var (
	fullCommitPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
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
	ID              string   `json:"id"`
	AcceptedCommits []string `json:"accepted_commits"`
	TestSymbols     []string `json:"test_symbols"`
	ScenarioKeys    []string `json:"scenario_keys"`
}

type ScenarioKey struct {
	Scenario string
	Kind     string
	Anchor   string
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

func UnresolvedMust(contract Contract, registry Registry) []string {
	records := make(map[string]EvidenceRecord, len(registry.Requirements))
	for _, record := range registry.Requirements {
		records[record.ID] = record
	}
	var unresolved []string
	for _, requirement := range contract.Requirements {
		if requirement.Level == "MUST" && !recordGrounded(requirement, records[requirement.ID]) {
			unresolved = append(unresolved, requirement.ID)
		}
	}
	slices.Sort(unresolved)
	return unresolved
}

func UnresolvedGates(contract Contract, registry Registry) []string {
	unresolvedRequirements := make(map[string]struct{})
	for _, id := range UnresolvedMust(contract, registry) {
		unresolvedRequirements[id] = struct{}{}
	}
	gates := make(map[string]struct{})
	for _, requirement := range contract.Requirements {
		if _, unresolved := unresolvedRequirements[requirement.ID]; unresolved {
			gates[requirement.PrimaryGate] = struct{}{}
		}
	}
	result := make([]string, 0, len(gates))
	for gate := range gates {
		result = append(result, gate)
	}
	slices.Sort(result)
	return result
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
		{name: "accepted_commits", values: record.AcceptedCommits},
		{name: "test_symbols", values: record.TestSymbols},
		{name: "scenario_keys", values: record.ScenarioKeys},
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
	for _, commit := range record.AcceptedCommits {
		if !fullCommitPattern.MatchString(commit) {
			return fmt.Errorf("accepted commit %q is not a full Git object ID", commit)
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
	anyEvidence := len(record.AcceptedCommits)+len(record.TestSymbols)+len(record.ScenarioKeys) > 0
	if !anyEvidence {
		return nil
	}
	if len(record.AcceptedCommits) == 0 || len(record.TestSymbols) == 0 {
		return fmt.Errorf("ungrounded behavioral evidence requires accepted commits and test symbols")
	}
	if gateNeedsScenario(requirement.PrimaryGate) && len(record.ScenarioKeys) == 0 {
		return fmt.Errorf("primary gate %s requires a grounded scenario key", requirement.PrimaryGate)
	}
	return nil
}

func recordGrounded(requirement Requirement, record EvidenceRecord) bool {
	if len(record.AcceptedCommits) == 0 || len(record.TestSymbols) == 0 {
		return false
	}
	return !gateNeedsScenario(requirement.PrimaryGate) || len(record.ScenarioKeys) > 0
}

func gateNeedsScenario(gate string) bool {
	return gate == "G-DOCKER" || gate == "G-EVIDENCE" || gate == "G-LIVE"
}

func validateEvidencePath(value string) error {
	if value == "" || value == "." || strings.Contains(value, "\\") || path.IsAbs(value) ||
		path.Clean(value) != value || strings.HasPrefix(value, "../") {
		return fmt.Errorf("%q is not a clean repository-relative evidence path", value)
	}
	return nil
}
