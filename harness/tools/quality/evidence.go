package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/tools/corecontract"
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

func validateRequirementEvidence(root string, contract corecontract.Contract,
	requirements requirementsManifest,
) error {
	return corecontract.ValidateBehavioralEvidence(root, contract, requirements)
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
