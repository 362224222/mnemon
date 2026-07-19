package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
)

func validateArchitectureEvidence(root string, manifest architectureManifest, findings []architectureFinding) error {
	entries := make(map[string]architectureEntry, len(manifest.Entries))
	for _, entry := range manifest.Entries {
		if err := pathEvidenceExists(root, entry.Path, entry.Symbol); err != nil {
			return fmt.Errorf("architecture debt %s path evidence: %w", entry.Identity, err)
		}
		evidencePath, evidenceSymbol, _ := parseEvidence(entry.Evidence)
		if err := pathEvidenceExists(root, evidencePath, evidenceSymbol); err != nil {
			return fmt.Errorf("architecture debt %s evidence: %w", entry.Identity, err)
		}
		entries[entry.Identity] = entry
	}
	staticFindings := make(map[string]struct{}, len(findings))
	for _, finding := range findings {
		staticFindings[finding.Identity] = struct{}{}
		entry, exists := entries[finding.Identity]
		if !exists {
			return fmt.Errorf("untracked architecture violation %s", finding.Identity)
		}
		if entry.Rule != finding.Rule || entry.Path != finding.Path || entry.Component != finding.Component || entry.Evidence != finding.Evidence {
			return fmt.Errorf("architecture debt %s does not exactly describe its static finding", finding.Identity)
		}
	}
	for _, entry := range manifest.Entries {
		if !automaticArchitectureRule(entry.Rule) {
			continue
		}
		if _, exists := staticFindings[entry.Identity]; !exists {
			return fmt.Errorf("stale auto-detected architecture debt %s has no current static finding", entry.Identity)
		}
	}
	return nil
}

func automaticArchitectureRule(rule string) bool {
	switch rule {
	case "dependency_direction", "unexpected_harness_package", "root_harness_dependency", "harness_legacy_dependency", "deprecated_libp2p_core":
		return true
	default:
		return false
	}
}

func validateRequirementEvidence(root string, expected expectedManifest, requirements requirementsManifest) error {
	if len(expected.Requirements) != len(requirements.Requirements) {
		return fmt.Errorf("requirements evidence has %d IDs, expected %d", len(requirements.Requirements), len(expected.Requirements))
	}
	for index := range expected.Requirements {
		if expected.Requirements[index].ID != requirements.Requirements[index].ID {
			return fmt.Errorf("requirements ID mismatch at index %d: got %s, want %s", index, requirements.Requirements[index].ID, expected.Requirements[index].ID)
		}
	}
	for _, requirement := range requirements.Requirements {
		if err := validateRequirementRecordEvidence(root, requirement); err != nil {
			return err
		}
	}
	return nil
}

func validateRequirementRecordEvidence(root string, requirement requirementRecord) error {
	if err := validateOwnerPackages(root, requirement); err != nil {
		return err
	}
	if err := validateAcceptedCommits(root, requirement); err != nil {
		return err
	}
	for _, reference := range requirement.TestSymbols {
		path, symbol, _ := parsePathSymbol(reference)
		if err := testSymbolEvidenceExists(root, path, symbol); err != nil {
			return fmt.Errorf("requirement %s test evidence %s: %w", requirement.ID, reference, err)
		}
		if err := validateTestSymbolCommitBinding(root, requirement, path, symbol); err != nil {
			return err
		}
	}
	return nil
}

func validateTestSymbolCommitBinding(root string, requirement requirementRecord, path, symbol string) error {
	var firstReadError error
	for _, commit := range requirement.AcceptedCommits {
		found, err := commitDeclaresTestSymbol(root, commit, path, symbol)
		if err != nil {
			if firstReadError == nil {
				firstReadError = fmt.Errorf("requirement %s test evidence %s::%s at accepted commit %s: %w",
					requirement.ID, path, symbol, commit, err)
			}
			continue
		}
		if found {
			return nil
		}
	}
	if firstReadError != nil {
		return firstReadError
	}
	return fmt.Errorf("requirement %s test evidence %s::%s is not declared in any accepted commit",
		requirement.ID, path, symbol)
}

func commitDeclaresTestSymbol(root, commit, path, symbol string) (bool, error) {
	reference := commit + ":" + path
	if _, err := runGit(root, "cat-file", "-e", reference); err != nil {
		return false, nil
	}
	data, err := runGit(root, "show", reference)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", path, err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, data, parser.SkipObjectResolution)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", path, err)
	}
	return fileDeclaresTestSymbol(parsed, symbol), nil
}

func testSymbolEvidenceExists(root, relative, symbol string) error {
	absolute := filepath.Join(root, filepath.FromSlash(relative))
	info, err := os.Stat(absolute)
	if err != nil {
		return fmt.Errorf("%s does not exist", relative)
	}
	if info.IsDir() || filepath.Ext(absolute) != ".go" {
		return fmt.Errorf("%s cannot provide Go test symbol %s", relative, symbol)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), absolute, nil, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("parse %s: %w", relative, err)
	}
	if fileDeclaresTestSymbol(parsed, symbol) {
		return nil
	}
	return fmt.Errorf("%s does not declare top-level test function %s", relative, symbol)
}

func fileDeclaresTestSymbol(file *ast.File, symbol string) bool {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Recv == nil && function.Name.Name == symbol {
			return true
		}
	}
	return false
}

func validateOwnerPackages(root string, requirement requirementRecord) error {
	for _, packagePath := range requirement.OwnerPackages {
		path := root
		if packagePath != "." {
			path = filepath.Join(root, filepath.FromSlash(packagePath))
		}
		info, err := os.Stat(path)
		if err != nil || !info.IsDir() {
			return fmt.Errorf("requirement %s owner package %s does not exist", requirement.ID, packagePath)
		}
	}
	return nil
}

func validateAcceptedCommits(root string, requirement requirementRecord) error {
	for _, commit := range requirement.AcceptedCommits {
		if _, err := runGit(root, "cat-file", "-e", commit+"^{commit}"); err != nil {
			return fmt.Errorf("requirement %s accepted commit %s does not exist: %w", requirement.ID, commit, err)
		}
		if _, err := runGit(root, "merge-base", "--is-ancestor", commit, "HEAD"); err != nil {
			return fmt.Errorf("requirement %s accepted commit %s is not an ancestor of HEAD", requirement.ID, commit)
		}
	}
	return nil
}

func pathEvidenceExists(root, relative, symbol string) error {
	absolute := filepath.Join(root, filepath.FromSlash(relative))
	info, err := os.Stat(absolute)
	if err != nil {
		return fmt.Errorf("%s does not exist", relative)
	}
	if symbol == "" {
		return nil
	}
	if info.IsDir() || filepath.Ext(absolute) != ".go" {
		return fmt.Errorf("%s cannot provide Go symbol %s", relative, symbol)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), absolute, nil, parser.SkipObjectResolution)
	if err != nil {
		return fmt.Errorf("parse %s: %w", relative, err)
	}
	if fileDeclaresSymbol(parsed, symbol) {
		return nil
	}
	return fmt.Errorf("%s does not declare symbol %s", relative, symbol)
}

func fileDeclaresSymbol(file *ast.File, symbol string) bool {
	for _, declaration := range file.Decls {
		if declarationDeclaresSymbol(declaration, symbol) {
			return true
		}
	}
	return false
}

func declarationDeclaresSymbol(declaration ast.Decl, symbol string) bool {
	if function, ok := declaration.(*ast.FuncDecl); ok {
		return functionSymbol(function) == symbol
	}
	general, ok := declaration.(*ast.GenDecl)
	if !ok {
		return false
	}
	for _, specification := range general.Specs {
		if specificationDeclaresSymbol(specification, symbol) {
			return true
		}
	}
	return false
}

func specificationDeclaresSymbol(specification ast.Spec, symbol string) bool {
	if value, ok := specification.(*ast.TypeSpec); ok {
		return value.Name.Name == symbol
	}
	value, ok := specification.(*ast.ValueSpec)
	if !ok {
		return false
	}
	for _, name := range value.Names {
		if name.Name == symbol {
			return true
		}
	}
	return false
}
