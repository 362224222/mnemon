package main

import (
	"fmt"
	"regexp"
	"strings"
)

var requirementIDPattern = regexp.MustCompile(`^(PR|ND|EV|CH|PX|AR|PI|TW|NET|CUT|EQ)-[0-9]{2}$`)

func validateArchitectureManifest(manifest architectureManifest) error {
	if err := validateSchema(manifest.SchemaVersion, "architecture debt"); err != nil {
		return err
	}
	if err := validateFullCommit(manifest.SourceCommit, "architecture debt source_commit"); err != nil {
		return err
	}
	if manifest.Entries == nil {
		return fmt.Errorf("architecture debt entries must be a JSON array, not null")
	}
	previous := ""
	for index, entry := range manifest.Entries {
		key := entry.Rule + "\x00" + entry.Identity
		if index > 0 && key <= previous {
			return fmt.Errorf("architecture debt entries must be uniquely sorted by rule and identity")
		}
		previous = key
		if err := validateArchitectureEntry(entry); err != nil {
			return fmt.Errorf("architecture debt %q: %w", entry.Identity, err)
		}
	}
	return nil
}

func validateArchitectureEntry(entry architectureEntry) error {
	for field, value := range map[string]string{
		"rule": entry.Rule, "identity": entry.Identity, "path": entry.Path,
		"risk": entry.Risk, "evidence": entry.Evidence, "owner": entry.Owner,
		"removal_checkpoint": entry.RemovalCheckpoint,
	} {
		if err := requireText(value, field); err != nil {
			return err
		}
	}
	for field, value := range map[string]string{
		"rule": entry.Rule, "path": entry.Path, "component": entry.Component,
	} {
		if err := rejectWildcard(value, field); err != nil {
			return err
		}
	}
	if err := rejectSymbolWildcard(entry.Identity, "identity"); err != nil {
		return err
	}
	if err := rejectSymbolWildcard(entry.Symbol, "symbol"); err != nil {
		return err
	}
	if err := validateRepoPath(entry.Path, "path"); err != nil {
		return err
	}
	if entry.Symbol != "" && entry.Component != "" {
		return fmt.Errorf("symbol and component are mutually exclusive")
	}
	if entry.Risk != "critical" && entry.Risk != "high" && entry.Risk != "medium" {
		return fmt.Errorf("unsupported risk %q", entry.Risk)
	}
	evidencePath, evidenceSymbol, err := parseEvidence(entry.Evidence)
	if err != nil {
		return err
	}
	if err := validateRepoPath(evidencePath, "evidence path"); err != nil {
		return err
	}
	if evidenceSymbol != "" {
		if err := requireText(evidenceSymbol, "evidence symbol"); err != nil {
			return err
		}
		if err := rejectSymbolWildcard(evidenceSymbol, "evidence symbol"); err != nil {
			return err
		}
	}
	return nil
}

func validateExpectedManifest(manifest expectedManifest) error {
	if err := validateSchema(manifest.SchemaVersion, "expected requirements"); err != nil {
		return err
	}
	if manifest.Requirements == nil {
		return fmt.Errorf("expected requirements must be a JSON array, not null")
	}
	previous := ""
	for index, requirement := range manifest.Requirements {
		if !requirementIDPattern.MatchString(requirement.ID) {
			return fmt.Errorf("expected requirement[%d] has invalid id %q", index, requirement.ID)
		}
		if index > 0 && requirement.ID <= previous {
			return fmt.Errorf("expected requirements must be uniquely sorted by id")
		}
		previous = requirement.ID
		if requirement.Level != "MUST" && requirement.Level != "SHOULD" {
			return fmt.Errorf("expected requirement %s has invalid level %q", requirement.ID, requirement.Level)
		}
	}
	return nil
}

func validateRequirementsManifest(manifest requirementsManifest) error {
	if err := validateSchema(manifest.SchemaVersion, "requirements evidence"); err != nil {
		return err
	}
	if manifest.Requirements == nil {
		return fmt.Errorf("requirements evidence must be a JSON array, not null")
	}
	previous := ""
	for index, requirement := range manifest.Requirements {
		if !requirementIDPattern.MatchString(requirement.ID) {
			return fmt.Errorf("requirement[%d] has invalid id %q", index, requirement.ID)
		}
		if index > 0 && requirement.ID <= previous {
			return fmt.Errorf("requirements evidence must be uniquely sorted by id")
		}
		previous = requirement.ID
		if err := validateRequirementRecord(requirement); err != nil {
			return fmt.Errorf("requirement %s: %w", requirement.ID, err)
		}
	}
	return nil
}

func validateRequirementRecord(requirement requirementRecord) error {
	if requirement.Status != "pending" && requirement.Status != "verified" {
		return fmt.Errorf("invalid status %q", requirement.Status)
	}
	if err := validateSortedUnique(requirement.OwnerPackages, "owner_packages", true); err != nil {
		return err
	}
	if err := validateOwnerPackagePaths(requirement.OwnerPackages); err != nil {
		return err
	}
	if err := validateRequirementArrays(requirement); err != nil {
		return err
	}
	return validateTestSymbolReferences(requirement.TestSymbols)
}

func validateOwnerPackagePaths(packages []string) error {
	for index, packagePath := range packages {
		if packagePath == "." {
			continue
		}
		if err := validateRepoPath(packagePath, fmt.Sprintf("owner_packages[%d]", index)); err != nil {
			return err
		}
	}
	return nil
}

func validateRequirementArrays(requirement requirementRecord) error {
	arrays := []struct {
		name   string
		values []string
	}{
		{name: "accepted_commits", values: requirement.AcceptedCommits},
		{name: "test_symbols", values: requirement.TestSymbols},
		{name: "scenario_keys", values: requirement.ScenarioKeys},
		{name: "evidence_gates", values: requirement.EvidenceGates},
	}
	for _, array := range arrays {
		if err := validateSortedUnique(array.values, array.name, requirement.Status == "verified"); err != nil {
			return err
		}
	}
	for index, commit := range requirement.AcceptedCommits {
		if err := validateFullCommit(commit, fmt.Sprintf("accepted_commits[%d]", index)); err != nil {
			return err
		}
	}
	return nil
}

func validateTestSymbolReferences(references []string) error {
	for index, reference := range references {
		path, symbol, err := parsePathSymbol(reference)
		if err != nil {
			return fmt.Errorf("test_symbols[%d]: %w", index, err)
		}
		if !strings.HasSuffix(path, "_test.go") {
			return fmt.Errorf("test_symbols[%d] does not reference a _test.go file", index)
		}
		if err := validateRepoPath(path, fmt.Sprintf("test_symbols[%d] path", index)); err != nil {
			return err
		}
		if !strings.HasPrefix(symbol, "Test") && !strings.HasPrefix(symbol, "Fuzz") {
			return fmt.Errorf("test_symbols[%d] symbol %q is not a Test or Fuzz function", index, symbol)
		}
		if err := rejectSymbolWildcard(symbol, fmt.Sprintf("test_symbols[%d] symbol", index)); err != nil {
			return err
		}
	}
	return nil
}

func parseEvidence(value string) (string, string, error) {
	if !strings.Contains(value, "::") {
		return value, "", nil
	}
	return parsePathSymbol(value)
}
