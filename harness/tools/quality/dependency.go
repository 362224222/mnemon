package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type dependencyCollector struct {
	root     string
	scope    repositoryGoScope
	findings map[string]architectureFinding
}

type repositoryGoScope struct {
	paths       map[string]struct{}
	directories map[string]struct{}
	gitScoped   bool
}

type dependencySource struct {
	relative     string
	packagePath  string
	layer        string
	inHarness    bool
	isProduction bool
}

func dependencyFindings(root string) ([]architectureFinding, error) {
	collector := &dependencyCollector{root: root, scope: loadRepositoryGoScope(root), findings: make(map[string]architectureFinding)}
	err := filepath.WalkDir(root, collector.inspectPath)
	findings := make([]architectureFinding, 0, len(collector.findings))
	for _, finding := range collector.findings {
		findings = append(findings, finding)
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Identity < findings[j].Identity })
	return findings, err
}

func (collector *dependencyCollector) inspectPath(path string, entry fs.DirEntry, walkErr error) error {
	if walkErr != nil {
		return walkErr
	}
	if entry.IsDir() {
		if path != collector.root && skipRepositoryDirectory(collector.root, path, entry.Name(), collector.scope) {
			return filepath.SkipDir
		}
		return nil
	}
	if entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(entry.Name(), ".go") {
		return nil
	}
	source, included, err := collector.describeSource(path)
	if err != nil || !included {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, data, parser.ImportsOnly)
	if err != nil {
		return fmt.Errorf("parse imports in %s: %w", source.relative, err)
	}
	collector.recordUnexpectedLayer(source)
	for _, imported := range parsed.Imports {
		if err := collector.recordImport(source, imported); err != nil {
			return err
		}
	}
	return nil
}

func (collector *dependencyCollector) describeSource(path string) (dependencySource, bool, error) {
	relative, err := filepath.Rel(collector.root, path)
	if err != nil {
		return dependencySource{}, false, err
	}
	relative = filepath.ToSlash(relative)
	if collector.scope.gitScoped {
		if _, included := collector.scope.paths[relative]; !included {
			return dependencySource{}, false, nil
		}
	}
	inHarness := strings.HasPrefix(relative, "harness/")
	return dependencySource{
		relative: relative, packagePath: sourcePackagePath(relative), layer: harnessPackage(relative),
		inHarness: inHarness, isProduction: !strings.HasSuffix(relative, "_test.go"),
	}, true, nil
}

func (collector *dependencyCollector) recordUnexpectedLayer(source dependencySource) {
	if source.inHarness && source.isProduction && source.layer != "" && !knownHarnessPackage(source.layer) {
		collector.add("unexpected_harness_package", source.packagePath, source.layer, source.relative)
	}
}

func (collector *dependencyCollector) recordImport(source dependencySource, imported *ast.ImportSpec) error {
	importPath, err := strconv.Unquote(imported.Path.Value)
	if err != nil {
		return fmt.Errorf("decode import in %s: %w", source.relative, err)
	}
	if !source.inHarness && isHarnessImport(importPath) {
		collector.add("root_harness_dependency", source.packagePath, importPath, source.relative)
	}
	if source.inHarness && strings.HasPrefix(importPath, modulePath+"/internal/") {
		collector.add("harness_legacy_dependency", source.packagePath, importPath, source.relative)
	}
	if strings.HasPrefix(importPath, "github.com/libp2p/go-libp2p-core/") {
		collector.add("deprecated_libp2p_core", source.packagePath, importPath, source.relative)
	}
	collector.recordLayerEdge(source, importPath)
	return nil
}

func (collector *dependencyCollector) recordLayerEdge(source dependencySource, importPath string) {
	if !source.inHarness || !source.isProduction || !knownHarnessPackage(source.layer) {
		return
	}
	target := importedHarnessPackage(importPath)
	if target != "" && !allowedHarnessImport(source.layer, target) {
		collector.add("dependency_direction", source.packagePath, target, source.relative)
	}
}

func (collector *dependencyCollector) add(rule, path, component, evidence string) {
	identity := rule + ":" + path + "::" + component
	finding := architectureFinding{Rule: rule, Identity: identity, Path: path, Component: component, Evidence: evidence}
	prior, exists := collector.findings[identity]
	if !exists || evidence < prior.Evidence {
		collector.findings[identity] = finding
	}
}

func loadRepositoryGoScope(root string) repositoryGoScope {
	data, err := runGit(root, "ls-files", "--cached", "--others", "--exclude-standard", "-z")
	if err != nil {
		return repositoryGoScope{}
	}
	paths := make(map[string]struct{})
	directories := map[string]struct{}{".": {}}
	for _, raw := range bytes.Split(data, []byte{0}) {
		path := filepath.ToSlash(string(raw))
		if path != "" && strings.HasSuffix(path, ".go") {
			paths[path] = struct{}{}
			for directory := filepath.ToSlash(filepath.Dir(path)); directory != "."; directory = filepath.ToSlash(filepath.Dir(directory)) {
				directories[directory] = struct{}{}
			}
		}
	}
	return repositoryGoScope{paths: paths, directories: directories, gitScoped: true}
}

func skipRepositoryDirectory(root, absolute, name string, scope repositoryGoScope) bool {
	if name == ".git" || name == "vendor" {
		return true
	}
	if !scope.gitScoped {
		return strings.HasPrefix(name, ".")
	}
	relative, err := filepath.Rel(root, absolute)
	if err != nil {
		return true
	}
	_, included := scope.directories[filepath.ToSlash(relative)]
	return !included
}

func newArchitectureFinding(rule, path, component string) architectureFinding {
	return architectureFinding{Rule: rule, Identity: rule + ":" + path + "::" + component, Path: path, Component: component, Evidence: path}
}

func sourcePackagePath(relative string) string {
	directory := filepath.ToSlash(filepath.Dir(relative))
	if directory == "." {
		return relative
	}
	return directory
}

func harnessPackage(path string) string {
	parts := strings.Split(filepath.ToSlash(path), "/")
	if len(parts) >= 3 && parts[0] == "harness" && parts[1] == "cmd" {
		return "cmd"
	}
	if len(parts) >= 3 && parts[0] == "harness" && parts[1] == "internal" {
		return parts[2]
	}
	return ""
}

func importedHarnessPackage(importPath string) string {
	prefix := modulePath + "/harness/internal/"
	if !strings.HasPrefix(importPath, prefix) {
		return ""
	}
	return strings.Split(strings.TrimPrefix(importPath, prefix), "/")[0]
}

func isHarnessImport(importPath string) bool {
	return importPath == modulePath+"/harness" || strings.HasPrefix(importPath, modulePath+"/harness/")
}

func allowedHarnessImport(source, target string) bool {
	allowed := map[string]map[string]bool{
		"attach":   {"agency": true},
		"cmd":      {"attach": true, "cli": true, "daemon": true},
		"cli":      {"agency": true},
		"cas":      {"agency": true},
		"daemon":   {"agency": true, "authority": true, "cas": true, "peerlink": true},
		"peerlink": {"agency": true, "cas": true},
		"agency":   {}, "authority": {"agency": true}, "selector": {"agency": true},
	}
	return allowed[source][target]
}

func knownHarnessPackage(name string) bool {
	switch name {
	case "cmd", "agency", "attach", "authority", "cas", "cli", "daemon", "peerlink", "selector":
		return true
	default:
		return false
	}
}
