package coreguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var multicaRuntimeProjectionForbiddenImports = []string{
	"harness/internal/app",
	"harness/internal/hostagent",
	"harness/internal/mnemond/admission",
	"harness/internal/mnemond/state",
	"harness/internal/mnemonhub",
	"harness/internal/productconfig",
	"harness/internal/runtime",
}

func TestMulticaRuntimeProjectionWriterDoesNotOwnIngestOrDrive(t *testing.T) {
	path := filepath.Join("..", "..", "cmd", "mnemon-multica-runtime", "hub_writer.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, imp := range file.Imports {
		importPath := strings.Trim(imp.Path.Value, `"`)
		if roleImportForbidden(importPath, multicaRuntimeProjectionForbiddenImports) {
			t.Errorf("Multica hub projection writer imports forbidden package %q; projection may read views and write Multica artifacts, but must not own local ingest, drive, product config, or hub exchange", importPath)
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			switch n.Sel.Name {
			case "IngestObserve", "IngestObservedEnvelope", "Observe", "Wake":
				t.Errorf("Multica hub projection writer calls %s at %s; projection must not ingest governed events or drive managed turns", n.Sel.Name, fset.Position(n.Pos()))
			}
		case *ast.CompositeLit:
			if selectorName(n.Type) == "ManagedAgentDriver" {
				t.Errorf("Multica hub projection writer constructs ManagedAgentDriver at %s; managed drive belongs outside projection", fset.Position(n.Pos()))
			}
		}
		return true
	})
}

func TestMulticaRuntimeRoleBoundaryGuardLogicIsNotVacuous(t *testing.T) {
	if !roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/mnemonhub", multicaRuntimeProjectionForbiddenImports) {
		t.Fatal("Multica projection writer guard must flag mnemonhub imports")
	}
	if roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/mnemond/access", multicaRuntimeProjectionForbiddenImports) {
		t.Fatal("Multica projection writer guard must allow read-only mnemond access")
	}
	if selectorName(&ast.SelectorExpr{Sel: ast.NewIdent("ManagedAgentDriver")}) != "ManagedAgentDriver" {
		t.Fatal("selectorName helper must identify selector names")
	}
}

func TestMulticaRuntimeDoesNotAdvertiseRootMnemonMulticaCommands(t *testing.T) {
	root := filepath.Join("..", "..", "cmd", "mnemon-multica-runtime")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(lit.Value)
			if err != nil {
				t.Errorf("unquote string literal in %s: %v", path, err)
				return true
			}
			for _, forbidden := range []string{"mnemon multica", "mnemon-harness multica"} {
				if strings.Contains(value, forbidden) {
					t.Errorf("%s advertises forbidden root/harness Multica command shape %q at %s; runtime progress should name the adapter process", path, forbidden, fset.Position(lit.Pos()))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk Multica runtime command: %v", err)
	}
}

func selectorName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		return e.Sel.Name
	default:
		return ""
	}
}
