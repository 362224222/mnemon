package coreguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

var githubRemoteWorkspaceCorePackages = []string{
	"runtime",
	"mnemond/state",
	"mnemond/admission",
	"mnemond/presentation",
	"hostagent",
}

var forbiddenGitHubBackendTerms = []string{
	"github issue",
	"github issues",
	"github pr",
	"github pull request",
	"github action",
	"github actions",
	"issue-based",
	"pr-based",
	"action-based",
	"p2p discovery",
	"peer discovery",
	"node discovery",
	"gossip",
	"dht",
	"routing table",
	"nat traversal",
	"overlay network",
}

func TestGitHubRemoteWorkspaceGuardLogicIsNotVacuous(t *testing.T) {
	if !isGitHubRemoteBackendImportPath("github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange/backend/github") {
		t.Fatal("GitHub backend import matcher must flag the planned exchange/backend/github package")
	}
	if isGitHubRemoteBackendImportPath("github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange") {
		t.Fatal("plain exchange package must not be treated as GitHub backend")
	}
	if !isAllowedGitHubBackendDir(filepath.FromSlash("mnemonhub/exchange/backend/github")) {
		t.Fatal("planned GitHub backend directory should be allowed under mnemonhub/exchange/backend")
	}
	if isAllowedGitHubBackendDir(filepath.FromSlash("runtime/github")) {
		t.Fatal("GitHub backend outside mnemonhub/exchange/backend must not be allowed")
	}
	if !hasForbiddenGitHubBackendTerm("use GitHub Issues as assignments") {
		t.Fatal("forbidden GitHub teamwork semantic matcher should flag Issue/PR/Actions concepts")
	}
	if hasForbiddenGitHubBackendTerm("publication branch enumeration") {
		t.Fatal("publication terminology should remain allowed")
	}
}

func TestGitHubRemoteWorkspaceBackendDoesNotLeakIntoCore(t *testing.T) {
	for _, pkg := range githubRemoteWorkspaceCorePackages {
		_, files := packageFiles(t, pkg)
		for _, file := range files {
			for _, imp := range file.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				if isGitHubRemoteBackendImportPath(path) {
					t.Errorf("core package %q imports GitHub Remote Workspace backend %q; GitHub must stay below mnemonhub/exchange/backend", pkg, path)
				}
			}
		}
	}
}

func TestGitHubRemoteWorkspaceBackendLocationAndNaming(t *testing.T) {
	root := filepath.Clean("..")
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
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		relFile, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relDir := filepath.Dir(relFile)
		if pathMentionsGitHubBackend(relDir) && !isAllowedGitHubBackendDir(relDir) {
			t.Errorf("GitHub backend source %s is outside mnemonhub/exchange/backend", relFile)
		}
		if isAllowedGitHubBackendDir(relDir) {
			assertGitHubBackendFileUsesPublicationVocabulary(t, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk harness/internal: %v", err)
	}
}

func isGitHubRemoteBackendImportPath(path string) bool {
	rel := strings.TrimPrefix(path, "github.com/mnemon-dev/mnemon/")
	if !strings.HasPrefix(rel, "harness/internal/mnemonhub/exchange/") {
		return false
	}
	tail := strings.TrimPrefix(rel, "harness/internal/mnemonhub/exchange/")
	for _, segment := range strings.Split(tail, "/") {
		if strings.Contains(strings.ToLower(segment), "github") {
			return true
		}
	}
	return false
}

func isAllowedGitHubBackendDir(relDir string) bool {
	clean := filepath.ToSlash(filepath.Clean(relDir))
	return clean == "mnemonhub/exchange/backend" ||
		strings.HasPrefix(clean, "mnemonhub/exchange/backend/")
}

func pathMentionsGitHubBackend(relDir string) bool {
	for _, segment := range strings.Split(filepath.ToSlash(relDir), "/") {
		if strings.Contains(strings.ToLower(segment), "github") {
			return true
		}
	}
	return false
}

func assertGitHubBackendFileUsesPublicationVocabulary(t *testing.T, path string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.Ident:
			if hasForbiddenGitHubBackendTerm(splitIdentifierWords(n.Name)) {
				t.Errorf("%s uses forbidden GitHub/P2P backend term %q; use repo-mediated publication vocabulary", fset.Position(n.Pos()), n.Name)
			}
		case *ast.BasicLit:
			if n.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(n.Value)
			if err != nil {
				value = strings.Trim(n.Value, "`\"")
			}
			if hasForbiddenGitHubBackendTerm(value) {
				t.Errorf("%s uses forbidden GitHub/P2P backend literal %q; use repo-mediated publication vocabulary", fset.Position(n.Pos()), value)
			}
		}
		return true
	})
}

func splitIdentifierWords(name string) string {
	var b strings.Builder
	var prevLower bool
	for _, r := range name {
		if r == '_' || r == '-' {
			b.WriteByte(' ')
			prevLower = false
			continue
		}
		if r >= 'A' && r <= 'Z' {
			if prevLower {
				b.WriteByte(' ')
			}
			b.WriteRune(r + ('a' - 'A'))
			prevLower = false
			continue
		}
		b.WriteRune(r)
		prevLower = r >= 'a' && r <= 'z'
	}
	return b.String()
}

func hasForbiddenGitHubBackendTerm(text string) bool {
	normalized := strings.ToLower(strings.Join(strings.Fields(strings.ReplaceAll(text, "_", " ")), " "))
	for _, term := range forbiddenGitHubBackendTerms {
		if strings.Contains(normalized, term) {
			return true
		}
	}
	return false
}
