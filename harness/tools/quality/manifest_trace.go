package main

import (
	"fmt"
	"strings"
)

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

func parseEvidence(value string) (string, string, error) {
	if !strings.Contains(value, "::") {
		return value, "", nil
	}
	return parsePathSymbol(value)
}
