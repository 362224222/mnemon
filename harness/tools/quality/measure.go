package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func measureTree(root, sourceCommit string) (baselineManifest, error) {
	if err := validateFullCommit(sourceCommit, "source_commit"); err != nil {
		return baselineManifest{}, err
	}
	files, err := loadHarnessSources(root)
	if err != nil {
		return baselineManifest{}, err
	}
	metricFiles := metricEligibleSources(files)
	functions, err := measureFunctions(metricFiles)
	if err != nil {
		return baselineManifest{}, err
	}
	entries := make([]baselineEntry, 0)
	for _, function := range functions {
		metrics := []struct {
			rule  string
			value int
		}{
			{rule: ruleCognitive, value: function.Cognitive},
			{rule: ruleCyclomatic, value: function.Cyclomatic},
			{rule: ruleNesting, value: function.Nesting},
			{rule: ruleFunctionLines, value: function.LogicalLines},
			{rule: ruleStatements, value: function.Statements},
		}
		for _, metric := range metrics {
			limit, _ := thresholdLimit(metric.rule)
			if metric.value > limit {
				entries = append(entries, baselineEntry{
					Rule: metric.rule, Identity: functionIdentity(metric.rule, function.Path, function.Symbol),
					Path: function.Path, Symbol: function.Symbol, Ceiling: metric.value,
				})
			}
		}
	}
	for _, file := range metricFiles {
		rule := ruleProductionFile
		if file.IsTest {
			rule = rulePairedTestFile
		}
		limit, _ := thresholdLimit(rule)
		if file.LineCount > limit {
			entries = append(entries, baselineEntry{
				Rule: rule, Identity: fileIdentity(rule, file.Path), Path: file.Path, Ceiling: file.LineCount,
			})
		}
	}
	duplicates, err := measureDuplicates(functions)
	if err != nil {
		return baselineManifest{}, err
	}
	for _, duplicate := range duplicates {
		path, _, splitErr := parsePathSymbol(duplicate.Owners[0])
		if splitErr != nil {
			return baselineManifest{}, fmt.Errorf("internal duplicate owner: %w", splitErr)
		}
		entries = append(entries, baselineEntry{
			Rule: ruleDuplicate, Identity: ruleDuplicate + ":" + duplicate.DebtID,
			Path: path, DebtID: duplicate.DebtID, Owners: append([]string(nil), duplicate.Owners...),
			Fingerprint: duplicate.Fingerprint, Ceiling: duplicate.Tokens,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Rule != entries[j].Rule {
			return entries[i].Rule < entries[j].Rule
		}
		return entries[i].Identity < entries[j].Identity
	})
	manifest := baselineManifest{
		SchemaVersion: manifestSchemaVersion,
		ToolVersion:   qualityToolVersion,
		SourceCommit:  sourceCommit,
		Thresholds:    append([]threshold(nil), qualityThresholds...),
		Entries:       entries,
	}
	if err := validateBaseline(manifest); err != nil {
		return baselineManifest{}, fmt.Errorf("measured baseline is invalid: %w", err)
	}
	return manifest, nil
}

func writeCanonicalJSON(path string, value any) error {
	data, err := canonicalJSON(value)
	if err != nil {
		return fmt.Errorf("encode output: %w", err)
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".quality-*.json")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set output mode: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write output: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace output: %w", err)
	}
	removeTemporary = false
	return nil
}

func currentCommit(root string) (string, error) {
	output, err := runGit(root, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	commit := strings.TrimSpace(string(output))
	if err := validateFullCommit(commit, "current commit"); err != nil {
		return "", err
	}
	return commit, nil
}
