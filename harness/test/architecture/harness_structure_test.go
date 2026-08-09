package architecture_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestHarnessArchitecture(t *testing.T) {
	root := harnessModuleRoot(t)
	t.Run("package dependencies match the frozen graph", func(t *testing.T) {
		assertPackageGraph(t, root)
	})
	t.Run("collaboration cases stay out of Core", func(t *testing.T) {
		assertNoCaseKindsInProduction(t, root)
	})
	t.Run("attachments have one interactive issuer", func(t *testing.T) {
		assertInteractiveAttachmentOnly(t, root)
	})
	t.Run("selector is not a Core dependency", func(t *testing.T) {
		assertCoreDoesNotImportSelector(t, root)
	})
	t.Run("case semantics stay in fixtures", func(t *testing.T) {
		assertCaseFixturesAreDataOnly(t, root)
	})
	t.Run("Core does not depend on fixture paths", func(t *testing.T) {
		assertNoFixturePathsInProduction(t, root)
	})
}

func TestExactlyOneActiveHarnessContract(t *testing.T) {
	directory := filepath.Join(repositoryRoot(t), "docs", "harness")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var active []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "-core-contract.md") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.SplitN(string(raw), "\n", 8)
		for _, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "Status: **ACTIVE**") {
				active = append(active, entry.Name())
				break
			}
		}
	}
	if len(active) != 1 || active[0] != "r7-core-contract.md" {
		t.Fatalf("active Harness contracts = %v, want [r7-core-contract.md]", active)
	}
}

func assertNoCaseKindsInProduction(t *testing.T, root string) {
	t.Helper()
	forEachProductionGoFile(t, root, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Errorf("%s: unquote string: %v", path, err)
				return true
			}
			for _, forbidden := range []string{
				"review.", "contract-net.", "blackboard.",
				"memory.wiki.", "teamwork.", "channel.",
			} {
				if strings.Contains(strings.ToLower(value), forbidden) {
					t.Errorf("%s contains case-specific production literal %q", path, value)
				}
			}
			return true
		})
	})
}

func assertPackageGraph(t *testing.T, root string) {
	t.Helper()
	want := map[string][]string{
		"internal/agency":    {},
		"internal/attach":    {},
		"internal/authority": {"internal/agency"},
		"internal/cas":       {"internal/agency"},
		"internal/cli":       {"internal/agency"},
		"internal/daemon":    {"internal/agency", "internal/authority", "internal/cas", "internal/peerlink"},
		"internal/peerlink":  {"internal/agency", "internal/cas"},
		"internal/selector":  {"internal/agency"},
		"cmd/mnemon-harness": {"internal/attach", "internal/cli", "internal/daemon"},
		"cmd/mnemond":        {"internal/daemon"},
	}
	got := make(map[string]map[string]struct{}, len(want))
	for component := range want {
		got[component] = map[string]struct{}{}
	}
	forEachProductionGoFile(t, root, func(path string, file *ast.File) {
		component := harnessComponent(t, root, path)
		if _, ok := want[component]; !ok {
			t.Errorf("unexpected production component %q", component)
			return
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Errorf("%s: unquote import: %v", path, err)
				continue
			}
			if importPath == "github.com/libp2p/go-libp2p-core" ||
				strings.HasPrefix(importPath, "github.com/libp2p/go-libp2p-core/") {
				t.Errorf("%s imports retired libp2p Core path %q", path, importPath)
			}
			const prefix = modulePath + "/harness/"
			if !strings.HasPrefix(importPath, prefix) {
				continue
			}
			dependency := harnessImportComponent(strings.TrimPrefix(importPath, prefix))
			if dependency != component {
				got[component][dependency] = struct{}{}
			}
		}
	})
	for component, expected := range want {
		actual := make([]string, 0, len(got[component]))
		for dependency := range got[component] {
			actual = append(actual, dependency)
		}
		slices.Sort(actual)
		slices.Sort(expected)
		if !slices.Equal(actual, expected) {
			t.Errorf("%s dependencies = %v, want %v", component, actual, expected)
		}
	}
}

func assertInteractiveAttachmentOnly(t *testing.T, root string) {
	t.Helper()
	var declarations, calls []string
	forEachProductionGoFile(t, root, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.FuncDecl:
				if strings.HasPrefix(value.Name.Name, "Issue") &&
					strings.HasSuffix(value.Name.Name, "Attachment") {
					declarations = append(declarations, filepath.ToSlash(path)+"::"+value.Name.Name)
				}
			case *ast.CallExpr:
				selector, ok := value.Fun.(*ast.SelectorExpr)
				if ok && strings.HasPrefix(selector.Sel.Name, "Issue") &&
					strings.HasSuffix(selector.Sel.Name, "Attachment") {
					calls = append(calls, filepath.ToSlash(path)+"::"+selector.Sel.Name)
				}
			}
			return true
		})
	})
	assertSingleArchitectureMatch(t, declarations, "/internal/authority/", "IssueInteractiveAttachment",
		"attachment issuer declaration")
	assertSingleArchitectureMatch(t, calls, "/internal/daemon/", "IssueInteractiveAttachment",
		"attachment issuer call")
}

func assertCoreDoesNotImportSelector(t *testing.T, root string) {
	t.Helper()
	selectorImport := modulePath + "/harness/internal/selector"
	forEachProductionGoFile(t, root, func(path string, file *ast.File) {
		if strings.Contains(filepath.ToSlash(path), "/internal/selector/") {
			return
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Errorf("%s: unquote import: %v", path, err)
				continue
			}
			if importPath == selectorImport || strings.HasPrefix(importPath, selectorImport+"/") {
				t.Errorf("%s imports optional selector package %q", path, importPath)
			}
		}
	})
}

func assertCaseFixturesAreDataOnly(t *testing.T, root string) {
	t.Helper()
	casesRoot := filepath.Join(root, "testdata", "r7", "cases")
	entries, err := os.ReadDir(casesRoot)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		names = append(names, name)
		for _, file := range []string{"nodes.txt", "playbook.md", "oracle.sh"} {
			info, err := os.Stat(filepath.Join(casesRoot, name, file))
			if err != nil || info.Size() == 0 {
				t.Errorf("case fixture %s/%s is missing or empty", name, file)
			}
			if file == "oracle.sh" && err == nil && info.Mode()&0o111 == 0 {
				t.Errorf("case fixture %s/%s is not executable", name, file)
			}
		}
	}
	if len(names) == 0 {
		t.Fatal("no R7 collaboration case fixtures")
	}

	for _, runnerName := range []string{"lib.sh", "run_cases.sh"} {
		runner, err := os.ReadFile(filepath.Join(root, "test", "r7", "runner", runnerName))
		if err != nil {
			t.Fatal(err)
		}
		text := strings.ToLower(string(runner))
		for _, forbidden := range append(slices.Clone(names), "examples/") {
			if strings.Contains(text, strings.ToLower(forbidden)) {
				t.Errorf("generic runner %s contains case-specific token %q", runnerName, forbidden)
			}
		}
	}

	examplesRoot := filepath.Join(root, "testdata", "r7", "examples")
	err = filepath.WalkDir(examplesRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Mode()&0o111 != 0 {
				t.Errorf("example is executable: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertNoFixturePathsInProduction(t *testing.T, root string) {
	t.Helper()
	forEachProductionGoFile(t, root, func(path string, file *ast.File) {
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			for _, forbidden := range []string{
				"testdata/r7/examples", "testdata/r7/cases", "testdata/r7/domain-ops",
			} {
				if strings.Contains(filepath.ToSlash(value), forbidden) {
					t.Errorf("%s refers to fixture path %q", path, value)
				}
			}
			return true
		})
	})
}

func forEachProductionGoFile(t *testing.T, root string, visit func(string, *ast.File)) {
	t.Helper()
	for _, base := range []string{filepath.Join(root, "internal"), filepath.Join(root, "cmd")} {
		err := filepath.WalkDir(base, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			visit(path, file)
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", base, err)
		}
	}
}

func harnessComponent(t *testing.T, root, path string) string {
	t.Helper()
	relative, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	if len(parts) < 3 || parts[0] != "internal" && parts[0] != "cmd" {
		t.Fatalf("production source has no component: %s", relative)
	}
	return parts[0] + "/" + parts[1]
}

func harnessImportComponent(importPath string) string {
	parts := strings.Split(importPath, "/")
	if len(parts) < 2 {
		return importPath
	}
	return parts[0] + "/" + parts[1]
}

func assertSingleArchitectureMatch(t *testing.T, matches []string, directory, want, label string) {
	t.Helper()
	if len(matches) != 1 || !strings.Contains(matches[0], directory) ||
		!strings.HasSuffix(matches[0], "::"+want) {
		t.Fatalf("%s = %v, want one %s in %s", label, matches, want, directory)
	}
}
