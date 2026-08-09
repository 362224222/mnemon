package architecture_test

import (
	"bytes"
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
	t.Parallel()
	root := repositoryRoot(t)
	t.Run("Go modules are independent", func(t *testing.T) { assertModuleIsolation(t, root) })
	t.Run("root production has no Harness dependency", func(t *testing.T) { assertRootImportsNoHarness(t, root) })
	t.Run("product command set is closed", func(t *testing.T) { assertHarnessCommands(t, root) })
	t.Run("internal package set is closed", func(t *testing.T) { assertHarnessPackages(t, root) })
	t.Run("retired topology is absent", func(t *testing.T) { assertRetiredTopologyAbsent(t, root) })
	t.Run("root command exposes no Harness product surface", func(t *testing.T) { assertRootHelpIsReleaseOnly(t, root) })
}

func assertModuleIsolation(t *testing.T, root string) {
	t.Helper()
	rootModule := inspectModuleBoundary(t, root)
	harnessModule := inspectModuleBoundary(t, filepath.Join(root, "harness"))
	if rootModule.path != modulePath || harnessModule.path != modulePath+"/harness" {
		t.Fatalf("module paths = (%q, %q)", rootModule.path, harnessModule.path)
	}
	if rootModule.goVersion == "" || rootModule.goVersion != harnessModule.goVersion {
		t.Fatalf("module Go versions = (%q, %q)", rootModule.goVersion, harnessModule.goVersion)
	}
	for _, name := range []string{"go.work", "go.work.sum"} {
		if _, err := os.Lstat(filepath.Join(root, name)); !os.IsNotExist(err) {
			t.Fatalf("repository workspace file %s exists: %v", name, err)
		}
	}
	for _, packagePath := range rootModule.packages {
		if packagePath == modulePath+"/harness" || strings.HasPrefix(packagePath, modulePath+"/harness/") {
			t.Fatalf("root module depends on Harness package %q", packagePath)
		}
	}
	for _, packagePath := range harnessModule.packages {
		if packagePath == modulePath || strings.HasPrefix(packagePath, modulePath+"/") &&
			packagePath != modulePath+"/harness" && !strings.HasPrefix(packagePath, modulePath+"/harness/") {
			t.Fatalf("Harness module depends on root package %q", packagePath)
		}
	}
}

func assertRootImportsNoHarness(t *testing.T, root string) {
	t.Helper()
	for _, path := range []string{filepath.Join(root, "main.go"), filepath.Join(root, "cmd"), filepath.Join(root, "internal")} {
		err := filepath.WalkDir(path, func(name string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, spec := range file.Imports {
				importPath, err := strconv.Unquote(spec.Path.Value)
				if err != nil {
					return err
				}
				if importPath == modulePath+"/harness" || strings.HasPrefix(importPath, modulePath+"/harness/") {
					t.Errorf("%s imports experimental Harness package %q", name, importPath)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("scan %s: %v", path, err)
		}
	}
}

func assertHarnessCommands(t *testing.T, root string) {
	t.Helper()
	assertDirectoryNames(t, filepath.Join(root, "harness", "cmd"), []string{"mnemon-harness", "mnemond"})
}

func assertHarnessPackages(t *testing.T, root string) {
	t.Helper()
	assertHarnessPackageDirectory(t, filepath.Join(root, "harness", "internal"))
}

func TestHarnessInternalPackageSet(t *testing.T) {
	assertHarnessPackageDirectory(t, filepath.Join(harnessModuleRoot(t), "internal"))
}

func assertHarnessPackageDirectory(t *testing.T, path string) {
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
	core := []string{"agency", "attach", "authority", "cas", "cli", "daemon", "peerlink"}
	if !slices.Equal(got, core) {
		t.Fatalf("directories under %s = %v, want R7 Core %v", path, got, core)
	}
}

func assertRetiredTopologyAbsent(t *testing.T, root string) {
	t.Helper()
	for _, path := range []string{
		"harness/cloudflare", "harness/cmd/mnemon-acceptance", "harness/cmd/mnemon-hub",
		"harness/cmd/mnemon-multica-runtime", "harness/internal/agencycli", "harness/internal/agent",
		"harness/internal/artifact", "harness/internal/assets", "harness/internal/event",
		"harness/internal/integration", "harness/internal/localapi", "harness/internal/model",
		"harness/internal/mnemonhub", "harness/internal/node", "harness/internal/peer",
		"harness/internal/productconfig", "harness/internal/session", "harness/internal/store",
		"harness/internal/teamwork", "harness/internal/testkit", "harness/internal/ui",
		"harness/test/e2e", "harness/test/process",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); !os.IsNotExist(err) {
			t.Errorf("retired path still exists: %s", path)
		}
	}
}

func assertRootHelpIsReleaseOnly(t *testing.T, root string) {
	t.Helper()
	command := exec.Command("go", "run", ".", "--help")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("root help: %v\n%s", err, output)
	}
	for _, commandName := range []string{"channel", "peer", "teamwork", "mnemond"} {
		if bytes.Contains(output, []byte("\n  "+commandName+" ")) {
			t.Errorf("root command exposes Harness command %q", commandName)
		}
	}
	if !bytes.Contains(output, []byte("\n  setup ")) || !bytes.Contains(output, []byte("\n  remember ")) {
		t.Errorf("root help lost release commands:\n%s", output)
	}
}

type moduleBoundary struct {
	path, goVersion string
	packages        []string
}

func inspectModuleBoundary(t *testing.T, root string) moduleBoundary {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	var boundary moduleBoundary
	for _, line := range strings.Split(string(contents), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		switch fields[0] {
		case "module":
			boundary.path = fields[1]
		case "go":
			boundary.goVersion = fields[1]
		}
	}
	command := exec.Command("go", "list", "-deps", "-test", "./...")
	command.Dir = root
	command.Env = slices.DeleteFunc(os.Environ(),
		func(value string) bool { return strings.HasPrefix(value, "GOWORK=") })
	command.Env = append(command.Env, "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("list packages in %s: %v\n%s", root, err, output)
	}
	boundary.packages = strings.Fields(string(output))
	return boundary
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

func harnessModuleRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source")
	}
	for dir := filepath.Dir(source); ; dir = filepath.Dir(dir) {
		contents, err := os.ReadFile(filepath.Join(dir, "go.mod"))
		if err == nil && bytes.Contains(contents, []byte("module "+modulePath+"/harness\n")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("Harness module root not found from %s", source)
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
