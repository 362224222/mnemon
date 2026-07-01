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

var multicaRuntimeSurfaceForbiddenImports = []string{
	"harness/internal/app",
	"harness/internal/hostagent",
	"harness/internal/mnemond/admission",
	"harness/internal/mnemond/state",
	"harness/internal/mnemonhub",
	"harness/internal/productconfig",
	"harness/internal/runtime",
}

func TestMulticaRuntimeDoesNotOwnManagedWakeOrDisplayWriteback(t *testing.T) {
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
		for _, imp := range file.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if roleImportForbidden(importPath, multicaRuntimeSurfaceForbiddenImports) {
				t.Errorf("Multica runtime imports forbidden package %q; runtime may import surface input and call mnemond access, but must not own product config, state, runtime core, or hub exchange", importPath)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.SelectorExpr:
				switch n.Sel.Name {
				case "Wake", "SetIssueStatus", "AssignIssue", "AddIssueComment", "SetIssueMetadata", "SetIssueMetadataMap":
					t.Errorf("Multica runtime calls %s at %s; R3 runtime input import must not own managed wake or display writeback", n.Sel.Name, fset.Position(n.Pos()))
				}
			case *ast.CompositeLit:
				if selectorName(n.Type) == "ManagedAgentDriver" {
					t.Errorf("Multica runtime constructs ManagedAgentDriver at %s; managed wake belongs to mnemond-managed source", fset.Position(n.Pos()))
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

func TestMulticaRuntimeRoleBoundaryGuardLogicIsNotVacuous(t *testing.T) {
	if !roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/mnemonhub", multicaRuntimeSurfaceForbiddenImports) {
		t.Fatal("Multica surface adapter guard must flag mnemonhub imports")
	}
	if roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/mnemond/access", multicaRuntimeSurfaceForbiddenImports) {
		t.Fatal("Multica runtime guard must allow mnemond access")
	}
	if selectorName(&ast.SelectorExpr{Sel: ast.NewIdent("ManagedAgentDriver")}) != "ManagedAgentDriver" {
		t.Fatal("selectorName helper must identify selector names")
	}
}

func TestMnemondAndMnemonHubDoNotDependOnMulticaSurface(t *testing.T) {
	for _, pkg := range []string{"mnemond/access", "mnemond/admission", "mnemond/state", "mnemond/presentation", "mnemonhub"} {
		_, files := packageFiles(t, pkg)
		for _, file := range files {
			for _, imp := range file.Imports {
				importPath := strings.Trim(imp.Path.Value, `"`)
				for _, forbidden := range []string{"harness/internal/surface/multica", "harness/internal/driver"} {
					if strings.Contains(importPath, forbidden) {
						t.Errorf("package %q imports Multica-facing package %q; mnemond/MnemonHub must stay product-surface agnostic", pkg, importPath)
					}
				}
			}
		}
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
