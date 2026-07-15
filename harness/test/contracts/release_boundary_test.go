package contracts_test

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

	t.Run("root production has no Harness dependency", func(t *testing.T) {
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
	})

	t.Run("product command set is closed", func(t *testing.T) {
		assertDirectoryNames(t, filepath.Join(root, "harness", "cmd"), []string{"mnemon-harness", "mnemond"})
	})

	t.Run("internal package set is closed", func(t *testing.T) {
		assertDirectoryNames(t, filepath.Join(root, "harness", "internal"), []string{
			"agent", "artifact", "assets", "event", "integration", "localapi",
			"model", "node", "peer", "store", "teamwork", "testkit",
		})
	})

	t.Run("retired topology is absent", func(t *testing.T) {
		for _, path := range []string{
			"harness/cloudflare",
			"harness/cmd/mnemon-acceptance",
			"harness/cmd/mnemon-hub",
			"harness/cmd/mnemon-multica-runtime",
			"harness/internal/mnemonhub",
			"harness/internal/productconfig",
			"harness/internal/session",
			"harness/internal/ui",
		} {
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); !os.IsNotExist(err) {
				t.Errorf("retired path still exists: %s", path)
			}
		}
	})

	t.Run("root command exposes no R5 product surface", func(t *testing.T) {
		command := exec.Command("go", "run", ".", "--help")
		command.Dir = root
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("root help: %v\n%s", err, output)
		}
		for _, commandName := range []string{"channel", "peer", "teamwork", "mnemond"} {
			needle := []byte("\n  " + commandName + " ")
			if bytes.Contains(output, needle) {
				t.Errorf("root command exposes R5 command %q", commandName)
			}
		}
		if !bytes.Contains(output, []byte("\n  setup ")) || !bytes.Contains(output, []byte("\n  remember ")) {
			t.Errorf("root help lost release commands:\n%s", output)
		}
	})
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
