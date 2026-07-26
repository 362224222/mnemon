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
	"os/exec"
	"path/filepath"
	"strings"
)

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

func ValidateBehavioralEvidence(root string, contract Contract, registry Registry) error {
	if err := ValidateRegistry(contract, registry); err != nil {
		return err
	}
	if err := ValidateOwnerDirectories(root, contract); err != nil {
		return err
	}
	for _, record := range registry.Requirements {
		if len(record.AcceptedCommits) == 0 {
			continue
		}
		for _, commit := range record.AcceptedCommits {
			if err := validateAcceptedCommit(root, record.ID, commit); err != nil {
				return err
			}
		}
		for _, reference := range record.TestSymbols {
			pathValue, symbol, _ := ParseTestSymbol(reference)
			if err := validateCurrentTestSymbol(root, record.ID, pathValue, symbol); err != nil {
				return err
			}
			if err := validateCommittedTestSymbol(root, record, pathValue, symbol); err != nil {
				return err
			}
		}
		for _, value := range record.ScenarioKeys {
			key, _ := ParseScenarioKey(value)
			if err := validateScenarioBinding(root, record, key); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateAcceptedCommit(root, requirementID, commit string) error {
	if _, err := runGit(root, "cat-file", "-e", commit+"^{commit}"); err != nil {
		return fmt.Errorf("requirement %s accepted commit %s does not exist: %w",
			requirementID, commit, err)
	}
	if _, err := runGit(root, "merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
		return fmt.Errorf("requirement %s accepted commit %s is not an ancestor of HEAD",
			requirementID, commit)
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
	if !declaresTopLevelTest(parsed, symbol) {
		return fmt.Errorf("requirement %s test evidence %s does not declare %s",
			requirementID, relative, symbol)
	}
	return nil
}

func validateCommittedTestSymbol(root string, record EvidenceRecord, relative, symbol string) error {
	for _, commit := range record.AcceptedCommits {
		data, err := runGit(root, "show", commit+":"+relative)
		if err != nil {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), relative, data, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("requirement %s parse accepted test %s at %s: %w",
				record.ID, relative, commit, err)
		}
		if declaresTopLevelTest(parsed, symbol) {
			return nil
		}
	}
	return fmt.Errorf("requirement %s test evidence %s::%s is absent from every accepted commit",
		record.ID, relative, symbol)
}

func declaresTopLevelTest(file *ast.File, symbol string) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == symbol {
			return true
		}
	}
	return false
}

func validateScenarioBinding(root string, record EvidenceRecord, key ScenarioKey) error {
	relative := filepath.ToSlash(filepath.Join(
		scenarioDirectory, key.Scenario, "manifest.json"))
	current, err := readScenarioManifest(filepath.Join(root, filepath.FromSlash(relative)))
	if err != nil {
		return fmt.Errorf("requirement %s scenario evidence %s: %w", record.ID,
			formatScenarioKey(key), err)
	}
	if err := validateScenarioAnchor(current, key); err != nil {
		return fmt.Errorf("requirement %s scenario evidence %s: %w", record.ID,
			formatScenarioKey(key), err)
	}
	for _, commit := range record.AcceptedCommits {
		data, err := runGit(root, "show", commit+":"+relative)
		if err != nil {
			continue
		}
		manifest, err := decodeScenarioManifest(data)
		if err == nil && validateScenarioAnchor(manifest, key) == nil {
			return nil
		}
	}
	return fmt.Errorf("requirement %s scenario evidence %s is absent from every accepted commit",
		record.ID, formatScenarioKey(key))
}

func readScenarioManifest(filename string) (scenarioManifest, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return scenarioManifest{}, fmt.Errorf("read canonical scenario manifest: %w", err)
	}
	return decodeScenarioManifest(data)
}

func decodeScenarioManifest(data []byte) (scenarioManifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var manifest scenarioManifest
	if err := decoder.Decode(&manifest); err != nil {
		return scenarioManifest{}, fmt.Errorf("decode canonical scenario manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return scenarioManifest{}, fmt.Errorf("decode canonical scenario manifest trailing data: %v", err)
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
		anchors = make([]string, len(manifest.Faults))
		for index, fault := range manifest.Faults {
			anchors[index] = fault.ID
		}
	case "system":
		anchors = manifest.Oracles.System
	case "task":
		anchors = make([]string, len(manifest.Oracles.Task))
		for index, task := range manifest.Oracles.Task {
			anchors[index] = task.ID
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

func formatScenarioKey(key ScenarioKey) string {
	return strings.Join([]string{key.Scenario, key.Kind, key.Anchor}, "/")
}

func runGit(root string, arguments ...string) ([]byte, error) {
	command := exec.Command("git", arguments...)
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(arguments, " "), err,
			strings.TrimSpace(string(output)))
	}
	return output, nil
}
