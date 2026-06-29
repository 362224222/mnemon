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

var rootCLIForbiddenCommands = map[string]bool{
	"acceptance":               true,
	"daemon":                   true,
	"harness":                  true,
	"hub":                      true,
	"mnemond":                  true,
	"mnemon-acceptance":        true,
	"mnemon-harness":           true,
	"mnemon-hub":               true,
	"mnemon-multica-runtime":   true,
	"mnemon-runtime-multica":   true,
	"mnemonhub":                true,
	"multica":                  true,
	"multica-runtime-prod-sim": true,
}

func TestRootMnemonCLIDoesNotImportHarnessCluster(t *testing.T) {
	for _, file := range rootCLIGoFiles(t) {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse imports %s: %v", file, err)
		}
		for _, imp := range parsed.Imports {
			importPath := strings.Trim(imp.Path.Value, `"`)
			if strings.Contains(importPath, "github.com/mnemon-dev/mnemon/harness/") {
				t.Errorf("root mnemon CLI imports R2 harness cluster package %q in %s", importPath, file)
			}
		}
	}
}

func TestRootMnemonCLIDoesNotExposeR2ClusterCommands(t *testing.T) {
	for _, file := range rootCLIGoFiles(t) {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse file %s: %v", file, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			kv, ok := node.(*ast.KeyValueExpr)
			if !ok {
				return true
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok || key.Name != "Use" {
				return true
			}
			value, ok := kv.Value.(*ast.BasicLit)
			if !ok || value.Kind != token.STRING {
				return true
			}
			use, err := strconv.Unquote(value.Value)
			if err != nil {
				t.Errorf("unquote Use value in %s: %v", file, err)
				return true
			}
			command := firstCommandToken(use)
			if rootCLIForbiddenCommands[command] {
				t.Errorf("root mnemon CLI exposes R2 cluster command %q in %s; keep it under harness/cmd", command, file)
			}
			return true
		})
	}
}

func TestRootCLIBoundaryGuardLogicIsNotVacuous(t *testing.T) {
	for _, command := range []string{"mnemond", "mnemon-hub", "multica", "acceptance"} {
		if !rootCLIForbiddenCommands[command] {
			t.Fatalf("root CLI boundary guard must forbid %q", command)
		}
	}
	for _, command := range []string{"remember", "recall", "search", "setup"} {
		if rootCLIForbiddenCommands[command] {
			t.Fatalf("root CLI boundary guard must allow existing memory command %q", command)
		}
	}
}

func rootCLIGoFiles(t *testing.T) []string {
	t.Helper()
	root := filepath.Join("..", "..", "..", "cmd")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root cmd dir: %v", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			files = append(files, filepath.Join(root, name))
		}
	}
	if len(files) == 0 {
		t.Fatalf("root cmd dir has no non-test Go files")
	}
	return files
}

func firstCommandToken(use string) string {
	use = strings.TrimSpace(use)
	if use == "" {
		return ""
	}
	return strings.Fields(use)[0]
}
