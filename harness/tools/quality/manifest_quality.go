package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var debtIDPattern = regexp.MustCompile(`^dup-[a-z0-9][a-z0-9-]*$`)

func validateBaseline(manifest baselineManifest) error {
	if err := validateSchema(manifest.SchemaVersion, "quality baseline"); err != nil {
		return err
	}
	if manifest.ToolVersion != qualityToolVersion {
		return fmt.Errorf("quality baseline tool_version = %q, want %q", manifest.ToolVersion, qualityToolVersion)
	}
	if err := validateFullCommit(manifest.SourceCommit, "quality baseline source_commit"); err != nil {
		return err
	}
	if len(manifest.Thresholds) != len(qualityThresholds) {
		return fmt.Errorf("quality baseline has %d thresholds, want %d", len(manifest.Thresholds), len(qualityThresholds))
	}
	for index := range qualityThresholds {
		if manifest.Thresholds[index] != qualityThresholds[index] {
			return fmt.Errorf("quality baseline threshold[%d] = %#v, want %#v", index, manifest.Thresholds[index], qualityThresholds[index])
		}
	}
	if manifest.Entries == nil {
		return fmt.Errorf("quality baseline entries must be a JSON array, not null")
	}
	previous := ""
	identities := make(map[string]struct{})
	debtIDs := make(map[string]struct{})
	for index, entry := range manifest.Entries {
		key := entry.Rule + "\x00" + entry.Identity
		if index > 0 && key <= previous {
			return fmt.Errorf("quality baseline entries must be uniquely sorted by rule and identity")
		}
		previous = key
		if _, exists := identities[entry.Identity]; exists {
			return fmt.Errorf("quality baseline repeats identity %q", entry.Identity)
		}
		identities[entry.Identity] = struct{}{}
		if err := validateBaselineEntry(entry); err != nil {
			return fmt.Errorf("quality baseline entry %q: %w", entry.Identity, err)
		}
		if entry.DebtID != "" {
			if _, exists := debtIDs[entry.DebtID]; exists {
				return fmt.Errorf("quality baseline repeats debt_id %q", entry.DebtID)
			}
			debtIDs[entry.DebtID] = struct{}{}
		}
	}
	return nil
}

func validateBaselineEntry(entry baselineEntry) error {
	limit, ok := thresholdLimit(entry.Rule)
	if !ok {
		return fmt.Errorf("unknown rule %q", entry.Rule)
	}
	if err := validateHarnessPath(entry.Path, "path"); err != nil {
		return err
	}
	if entry.Ceiling <= limit {
		return fmt.Errorf("ceiling %d does not exceed rule limit %d", entry.Ceiling, limit)
	}
	if entry.Rule == ruleDuplicate {
		return validateDuplicateBaselineEntry(entry)
	}
	if entry.DebtID != "" || entry.Owners != nil || entry.Fingerprint != "" {
		return fmt.Errorf("non-duplicate entry carries duplicate matching fields")
	}
	if isFunctionRule(entry.Rule) {
		if err := requireText(entry.Symbol, "symbol"); err != nil {
			return err
		}
		if err := rejectSymbolWildcard(entry.Symbol, "symbol"); err != nil {
			return err
		}
		if entry.Identity != functionIdentity(entry.Rule, entry.Path, entry.Symbol) {
			return fmt.Errorf("identity does not match rule/path/symbol")
		}
		return nil
	}
	if entry.Symbol != "" || entry.Identity != fileIdentity(entry.Rule, entry.Path) {
		return fmt.Errorf("file identity does not match rule/path")
	}
	return nil
}

func validateDuplicateBaselineEntry(entry baselineEntry) error {
	if !debtIDPattern.MatchString(entry.DebtID) {
		return fmt.Errorf("invalid duplicate debt_id %q", entry.DebtID)
	}
	if entry.Identity != ruleDuplicate+":"+entry.DebtID {
		return fmt.Errorf("duplicate identity must be rule:debt_id")
	}
	if entry.Symbol != "" {
		return fmt.Errorf("duplicate entry must use owners rather than symbol")
	}
	if err := validateSortedUnique(entry.Owners, "owners", true); err != nil {
		return err
	}
	if len(entry.Owners) < 2 {
		return fmt.Errorf("duplicate entry must have at least two owners")
	}
	for index, owner := range entry.Owners {
		path, symbol, err := parsePathSymbol(owner)
		if err != nil {
			return fmt.Errorf("owners[%d]: %w", index, err)
		}
		if err := validateHarnessPath(path, fmt.Sprintf("owners[%d] path", index)); err != nil {
			return err
		}
		if err := requireText(symbol, fmt.Sprintf("owners[%d] symbol", index)); err != nil {
			return err
		}
		if err := rejectSymbolWildcard(symbol, fmt.Sprintf("owners[%d] symbol", index)); err != nil {
			return err
		}
	}
	firstPath, _, _ := parsePathSymbol(entry.Owners[0])
	if entry.Path != firstPath {
		return fmt.Errorf("duplicate path must equal the first sorted owner path")
	}
	if len(entry.Fingerprint) != 64 || !isLowerHex(entry.Fingerprint) {
		return fmt.Errorf("fingerprint must be a lowercase SHA-256 digest")
	}
	return nil
}

func validateExceptions(manifest exceptionManifest) error {
	if err := validateSchema(manifest.SchemaVersion, "quality exceptions"); err != nil {
		return err
	}
	if manifest.Entries == nil {
		return fmt.Errorf("quality exception entries must be a JSON array, not null")
	}
	previous := ""
	for index, entry := range manifest.Entries {
		key := entry.Rule + "\x00" + entry.Identity
		if index > 0 && key <= previous {
			return fmt.Errorf("quality exception entries must be uniquely sorted by rule and identity")
		}
		previous = key
		if err := validateExceptionEntry(entry); err != nil {
			return fmt.Errorf("quality exception %q: %w", entry.Identity, err)
		}
	}
	return nil
}

func validateExceptionEntry(entry exceptionEntry) error {
	limit, ok := thresholdLimit(entry.Rule)
	if !ok {
		return fmt.Errorf("rule %q is not waivable quality debt", entry.Rule)
	}
	if entry.Rule == ruleDuplicate {
		return fmt.Errorf("normalized duplicate exceptions require stable owner evidence unavailable in v1")
	}
	if entry.Ceiling <= limit {
		return fmt.Errorf("ceiling %d does not exceed rule limit %d", entry.Ceiling, limit)
	}
	if entry.Rule == ruleCyclomatic && entry.Ceiling > 30 {
		return fmt.Errorf("cyclomatic ceiling %d cannot exceed 30", entry.Ceiling)
	}
	if err := validateHarnessPath(entry.Path, "path"); err != nil {
		return err
	}
	for field, value := range map[string]string{"path": entry.Path, "component": entry.Component} {
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
	if err := validateExceptionScope(entry); err != nil {
		return err
	}
	return validateExceptionMetadata(entry)
}

func validateExceptionScope(entry exceptionEntry) error {
	if isFunctionRule(entry.Rule) {
		if entry.Identity != functionIdentity(entry.Rule, entry.Path, entry.Symbol) || entry.Symbol == "" {
			return fmt.Errorf("function exception identity does not match rule/path/symbol")
		}
	} else if entry.Identity != fileIdentity(entry.Rule, entry.Path) || entry.Symbol != "" {
		return fmt.Errorf("file exception identity does not match rule/path")
	}
	if entry.Component != "" {
		return fmt.Errorf("metric exceptions do not use component")
	}
	return nil
}

func validateExceptionMetadata(entry exceptionEntry) error {
	for field, value := range map[string]string{"reason": entry.Reason, "risk": entry.Risk, "owner": entry.Owner, "removal_checkpoint": entry.RemovalCheckpoint} {
		if err := requireText(value, field); err != nil {
			return err
		}
	}
	if !validExceptionRisk(entry.Risk) {
		return fmt.Errorf("unsupported risk %q", entry.Risk)
	}
	return nil
}

func thresholdLimit(rule string) (int, bool) {
	index := sort.Search(len(qualityThresholds), func(index int) bool { return qualityThresholds[index].Rule >= rule })
	if index == len(qualityThresholds) || qualityThresholds[index].Rule != rule {
		return 0, false
	}
	return qualityThresholds[index].Limit, true
}

func isFunctionRule(rule string) bool {
	return rule == ruleCyclomatic || rule == ruleCognitive || rule == ruleFunctionLines || rule == ruleStatements || rule == ruleNesting
}

func validExceptionRisk(risk string) bool {
	return risk == "critical" || risk == "high" || risk == "medium" || risk == "low"
}

func isLowerHex(value string) bool {
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
