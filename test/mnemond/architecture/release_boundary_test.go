package architecture_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/mnemon-dev/mnemon"

func TestReleaseBoundary(t *testing.T) {
	root := repositoryRoot(t)
	t.Run("all retained Go packages belong to the root module", func(t *testing.T) {
		assertSingleModuleImportLaw(t, root)
	})
	t.Run("formal command set is mnemon and mnemond", func(t *testing.T) {
		assertFormalCommands(t, root)
	})
	t.Run("retired Harness topology is absent", func(t *testing.T) {
		assertRetiredHarnessAbsent(t, root)
	})
	t.Run("command help preserves product separation", func(t *testing.T) {
		assertCommandHelpSeparation(t, root)
	})
}

func assertSingleModuleImportLaw(t *testing.T, root string) {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(contents, []byte("module "+modulePath+"\n")) {
		t.Fatalf("root go.mod does not declare %s", modulePath)
	}
	assertOnlyRootGoModule(t, root)

	command := exec.Command("go", "list", "-f", "{{.ImportPath}}", "./...")
	command.Dir = root
	command.Env = withoutGoWork(os.Environ())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list root packages: %v\n%s", err, output)
	}
	for _, packagePath := range strings.Fields(string(output)) {
		if packagePath != modulePath && !strings.HasPrefix(packagePath, modulePath+"/") {
			t.Errorf("root package has foreign import path %q", packagePath)
		}
		if packagePath == modulePath+"/harness" || strings.HasPrefix(packagePath, modulePath+"/harness/") {
			t.Errorf("retained package still belongs to the Harness module: %q", packagePath)
		}
	}

	for _, base := range []string{"cmd", "internal", "test", "testdata"} {
		assertImportsUseRootModule(t, filepath.Join(root, base))
	}
}

func assertOnlyRootGoModule(t *testing.T, root string) {
	t.Helper()
	var modules []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "dist") {
			return filepath.SkipDir
		}
		if !entry.IsDir() && entry.Name() == "go.mod" {
			relative, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			modules = append(modules, filepath.ToSlash(relative))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan module manifests: %v", err)
	}
	if !slices.Equal(modules, []string{"go.mod"}) {
		t.Fatalf("Go module manifests = %v, want only root go.mod", modules)
	}
}

func assertImportsUseRootModule(t *testing.T, base string) {
	t.Helper()
	err := filepath.WalkDir(base, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				return err
			}
			if importPath == modulePath+"/harness" || strings.HasPrefix(importPath, modulePath+"/harness/") {
				t.Errorf("%s imports retired Harness path %q", path, importPath)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan imports under %s: %v", base, err)
	}
}

func assertFormalCommands(t *testing.T, root string) {
	t.Helper()
	assertDirectoryNames(t, filepath.Join(root, "cmd"), []string{"mnemon", "mnemond"})
	assertRootCompatibilityWrapper(t, root)
	for _, name := range []string{"mnemon", "mnemond"} {
		command := exec.Command("go", "list", "-f", "{{.Name}}", "./cmd/"+name)
		command.Dir = root
		command.Env = withoutGoWork(os.Environ())
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("list cmd/%s: %v\n%s", name, err, output)
		}
		if strings.TrimSpace(string(output)) != "main" {
			t.Errorf("cmd/%s package = %q, want main", name, strings.TrimSpace(string(output)))
		}
	}
}

func assertRootCompatibilityWrapper(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "main.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse root compatibility wrapper: %v", err)
	}
	if len(file.Imports) != 1 {
		t.Fatalf("root compatibility wrapper imports = %d, want one", len(file.Imports))
	}
	importPath, err := strconv.Unquote(file.Imports[0].Path.Value)
	if err != nil || importPath != modulePath+"/internal/mnemoncli" {
		t.Fatalf("root compatibility wrapper import = %q, want internal/mnemoncli", importPath)
	}
	var mainFunction *ast.FuncDecl
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "main" {
			mainFunction = function
		}
	}
	if mainFunction == nil || mainFunction.Body == nil || len(mainFunction.Body.List) != 1 {
		t.Fatal("root compatibility wrapper must contain one main statement")
	}
	statement, ok := mainFunction.Body.List[0].(*ast.ExprStmt)
	if !ok {
		t.Fatal("root compatibility wrapper main statement must be a call")
	}
	call, callOK := statement.X.(*ast.CallExpr)
	if !callOK {
		t.Fatal("root compatibility wrapper main statement must be a call")
	}
	selector, selectorOK := call.Fun.(*ast.SelectorExpr)
	if !selectorOK {
		t.Fatal("root compatibility wrapper must call mnemoncli.Execute()")
	}
	identifier, identifierOK := selector.X.(*ast.Ident)
	if !identifierOK || identifier.Name != "mnemoncli" ||
		selector.Sel.Name != "Execute" || len(call.Args) != 0 {
		t.Fatal("root compatibility wrapper must only call mnemoncli.Execute()")
	}
}

func assertRetiredHarnessAbsent(t *testing.T, root string) {
	t.Helper()
	for _, path := range []string{"harness", "cmd/mnemon-harness"} {
		if _, err := os.Lstat(filepath.Join(root, filepath.FromSlash(path))); !os.IsNotExist(err) {
			t.Errorf("retired path still exists: %s", path)
		}
	}
}

func assertCommandHelpSeparation(t *testing.T, root string) {
	t.Helper()
	mnemon := commandHelp(t, root, "mnemon")
	wantMnemon := []string{
		"completion", "embed", "forget", "gc", "help", "import", "link", "log",
		"recall", "receipt", "related", "remember", "search", "setup", "status",
		"store", "viz",
	}
	if got := cobraTopLevelCommands(mnemon); !slices.Equal(got, wantMnemon) {
		t.Errorf("mnemon top-level commands = %v, want %v", got, wantMnemon)
	}

	mnemond := commandHelp(t, root, "mnemond")
	if got, want := mnemondUsageCommands(mnemond), []string{"peer", "serve", "setup"}; !slices.Equal(got, want) {
		t.Errorf("mnemond top-level commands = %v, want %v", got, want)
	}
}

func cobraTopLevelCommands(help []byte) []string {
	lines := strings.Split(string(help), "\n")
	inCommands := false
	var commands []string
	for _, line := range lines {
		switch strings.TrimSpace(line) {
		case "Available Commands:":
			inCommands = true
			continue
		case "Flags:":
			inCommands = false
		}
		if !inCommands || !strings.HasPrefix(line, "  ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 0 {
			commands = append(commands, fields[0])
		}
	}
	return commands
}

func mnemondUsageCommands(help []byte) []string {
	lines := strings.Split(string(help), "\n")
	var commands []string
	for _, line := range lines {
		if !strings.HasPrefix(line, "  mnemond ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "mnemond" || strings.HasPrefix(fields[1], "-") {
			continue
		}
		commands = append(commands, fields[1])
	}
	slices.Sort(commands)
	return slices.Compact(commands)
}

func commandHelp(t *testing.T, root, name string) []byte {
	t.Helper()
	command := exec.Command("go", "run", "./cmd/"+name, "--help")
	command.Dir = root
	command.Env = withoutGoWork(os.Environ())
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s help: %v\n%s", name, err, output)
	}
	return output
}

func withoutGoWork(environment []string) []string {
	result := slices.DeleteFunc(slices.Clone(environment),
		func(value string) bool { return strings.HasPrefix(value, "GOWORK=") })
	return append(result, "GOWORK=off")
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source")
	}
	for dir := filepath.Dir(source); ; dir = filepath.Dir(dir) {
		contents, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil && bytes.Contains(contents, []byte("module "+modulePath+"\n")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repository root not found from %s", source)
		}
	}
}

func assertDirectoryNames(t *testing.T, path string, want []string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var got []string
	for _, entry := range entries {
		if entry.IsDir() {
			got = append(got, entry.Name())
		}
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("directories under %s = %v, want %v", path, got, want)
	}
}
