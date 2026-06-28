package coreguard

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAcceptanceCommandIsIsolatedFromProductCommands(t *testing.T) {
	productCmdDir := filepath.Join("..", "..", "cmd", "mnemon-harness")
	entries, err := os.ReadDir(productCmdDir)
	if err != nil {
		t.Fatalf("read product command dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "acceptance") && strings.HasSuffix(name, ".go") {
			t.Fatalf("test-only acceptance source %q must not live under mnemon-harness", name)
		}
	}
	if !hasNonTestGoFiles(filepath.Join("..", "..", "cmd", "mnemon-acceptance")) {
		t.Fatalf("mnemon-acceptance command must own acceptance scenarios")
	}
}

func TestProductCodeDoesNotImportAcceptanceCommand(t *testing.T) {
	root := filepath.Join("..", "..")
	for _, dir := range []string{
		filepath.Join(root, "internal"),
		filepath.Join(root, "cmd", "mnemon-harness"),
		filepath.Join(root, "cmd", "mnemond"),
		filepath.Join(root, "cmd", "mnemon-hub"),
	} {
		assertNoAcceptanceCommandImports(t, dir)
	}
}

func assertNoAcceptanceCommandImports(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(importPath, "harness/cmd/mnemon-acceptance") {
				t.Errorf("%s imports test-only acceptance command %q", path, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
}
