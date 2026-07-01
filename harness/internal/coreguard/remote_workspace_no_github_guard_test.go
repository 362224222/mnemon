package coreguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var forbiddenFirstPartyGitHubTerms = []string{
	"RemoteBackendGitHub",
	"ConnectionGitHub",
	"GitHubConnection",
	"runConnectGitHub",
	"syncGitHub",
	"GitHubMesh",
	"GitHub Mesh",
	"github mesh",
	"GitHub Remote Workspace",
	"GitHub-backed",
	"github-repo",
	"github-token-file",
	"github-branch",
	"backend/github",
}

func TestNoFirstPartyGitHubRemoteWorkspaceBackendPackage(t *testing.T) {
	root := filepath.Clean("..")
	backendRoot := filepath.Join(root, "mnemonhub", "exchange", "backend")
	if _, err := os.Stat(backendRoot); os.IsNotExist(err) {
		return
	}
	err := filepath.WalkDir(backendRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		for _, segment := range strings.Split(filepath.ToSlash(path), "/") {
			if strings.EqualFold(segment, "github") {
				t.Fatalf("GitHub must not be implemented as a first-party Remote Workspace backend: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk Remote Workspace backends: %v", err)
	}
}

func TestNoFirstPartyGitHubConnectionOrBackendSymbols(t *testing.T) {
	for _, root := range []string{filepath.Clean(".."), filepath.Clean("../../cmd")} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if strings.HasPrefix(entry.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				return nil
			}
			assertNoFirstPartyGitHubTerms(t, path)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
}

func assertNoFirstPartyGitHubTerms(t *testing.T, path string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.Ident:
			if term := firstForbiddenGitHubTerm(n.Name); term != "" {
				t.Errorf("%s uses removed first-party GitHub symbol %q", fset.Position(n.Pos()), term)
			}
		case *ast.BasicLit:
			if n.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(n.Value)
			if err != nil {
				value = strings.Trim(n.Value, "`\"")
			}
			if isAllowedModuleImport(value) {
				return true
			}
			if term := firstForbiddenGitHubTerm(value); term != "" {
				t.Errorf("%s uses removed first-party GitHub literal %q", fset.Position(n.Pos()), term)
			}
		case *ast.Comment:
			if term := firstForbiddenGitHubTerm(n.Text); term != "" {
				t.Errorf("%s documents removed first-party GitHub behavior %q", fset.Position(n.Pos()), term)
			}
		}
		return true
	})
}

func firstForbiddenGitHubTerm(text string) string {
	normalized := strings.ToLower(text)
	for _, term := range forbiddenFirstPartyGitHubTerms {
		if strings.Contains(normalized, strings.ToLower(term)) {
			return term
		}
	}
	return ""
}

func isAllowedModuleImport(value string) bool {
	return strings.HasPrefix(value, "github.com/mnemon-dev/mnemon/")
}
