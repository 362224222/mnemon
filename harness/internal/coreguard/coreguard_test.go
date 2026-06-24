package coreguard

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// corePackages are the collaboration-channel core: the generic governed-event mechanism. The
// human-readable invariant is "the core only contains channel-related content, and stays generic."
var corePackages = []string{
	"contract", "channel", "kernel", "store", "eventview", "rule", "reconcile", "runtime",
}

// forbiddenImports are the outer rings the core must never depend on: application vocabulary
// (capability), host integration (hostsurface), hot content rendering (render),
// wiring/consumers (app, assembler, driver, ui), the OPTIONAL autopilot, the codex adapter, and
// the cmd binaries. Dependencies flow inward only.
var forbiddenImports = []string{
	"harness/internal/capability",
	"harness/internal/hostsurface",
	"harness/internal/render",
	"harness/internal/app",
	"harness/internal/assembler",
	"harness/internal/driver",
	"harness/internal/ui",
	"harness/internal/autopilot",
	"harness/internal/codexapp",
	"harness/cmd/",
}

// businessKinds are application/coordination vocabulary that must NOT appear as a string literal in
// the core. The kernel's governance kinds (lease/budget/receipt/coordination) are deliberately
// EXCLUDED — they are control-plane state the kernel owns. (coordination is the one borderline case:
// it is registered governance, not active control-plane logic; kept for now, revisit if it proves to
// be pure app vocabulary.) User kinds are injected at assembly time, never hardcoded in the core.
var businessKinds = []string{
	"memory", "skill", "codex", "claude", "tower",
	"agent_profile", "teamwork_signal", "assignment", "progress_digest", "project_intent",
	"assignment_status", "assignment_expired",
	"poc_claim", "poc_decision", "poc_role", "ic_role", "goal", "approval",
}

type importBoundaryRule struct {
	pkg       string
	forbids   []string
	rationale string
}

var outerRingImportBoundaries = []importBoundaryRule{
	{
		pkg:       "capability",
		forbids:   []string{"harness/internal/hostsurface"},
		rationale: "capability semantics must not know host hook/settings mechanics",
	},
	{
		pkg:       "hostsurface",
		forbids:   []string{"harness/internal/kernel", "harness/internal/store", "harness/internal/runtime"},
		rationale: "hostsurface is static integration and must not reach into governed state owners",
	},
	{
		pkg:       "render",
		forbids:   []string{"harness/internal/hostsurface"},
		rationale: "render produces hot content/cues and must not depend on host settings writers",
	},
}

// TestGuardLogicIsNotVacuous proves the matchers actually fire. A guard that can never flag
// anything would pass forever while silently allowing the leak it claims to prevent.
func TestGuardLogicIsNotVacuous(t *testing.T) {
	forbidden := map[string]bool{}
	for _, k := range businessKinds {
		forbidden[k] = true
	}
	if !forbidden["memory"] {
		t.Fatal(`"memory" must be treated as a business kind`)
	}
	if forbidden[":memory:"] {
		t.Fatal(`the sqlite ":memory:" DSN must NOT be flagged (exact-literal match)`)
	}
	if forbidden["lease"] || forbidden["coordination"] {
		t.Fatal("governance kinds (lease/coordination) must be allowed in the core")
	}
	hit := false
	for _, forbid := range forbiddenImports {
		if strings.Contains("github.com/mnemon-dev/mnemon/harness/internal/app", forbid) {
			hit = true
		}
	}
	if !hit {
		t.Fatal("import guard should flag a forbidden internal/app import")
	}
}

func packageFiles(t *testing.T, pkg string) (*token.FileSet, []*ast.File) {
	t.Helper()
	dir := filepath.Join("..", pkg)
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	var files []*ast.File
	for _, p := range pkgs {
		for _, f := range p.Files {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		t.Fatalf("no non-test source found for package %q (looked in %s) — guard package list out of date?", pkg, dir)
	}
	return fset, files
}

// TestCoreImportsNoOuterRing enforces that no core package imports an outer ring, so the core stays
// a generic protocol mechanism with the add-ons deletable around it (deps flow inward only).
func TestCoreImportsNoOuterRing(t *testing.T) {
	for _, pkg := range corePackages {
		_, files := packageFiles(t, pkg)
		for _, f := range files {
			for _, imp := range f.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				for _, forbidden := range forbiddenImports {
					if strings.Contains(path, forbidden) {
						t.Errorf("core package %q imports outer ring %q — the collaboration-channel core must stay generic (deps flow inward only)", pkg, path)
					}
				}
			}
		}
	}
}

// TestOuterRingImportBoundaries pins the R1 package topology around the hook/skill event pipeline.
// These packages are outside the core, but their dependency direction still matters: host integration,
// hot cue rendering, and capability semantics must remain independently replaceable.
func TestOuterRingImportBoundaries(t *testing.T) {
	for _, rule := range outerRingImportBoundaries {
		_, files := packageFiles(t, rule.pkg)
		for _, f := range files {
			for _, imp := range f.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				for _, forbidden := range rule.forbids {
					if strings.Contains(path, forbidden) {
						t.Errorf("package %q imports forbidden package %q — %s", rule.pkg, path, rule.rationale)
					}
				}
			}
		}
	}
}

// TestCoreHasNoBusinessKindLiterals enforces that no core package hardcodes an application kind as a
// string literal — business vocabulary (memory/skill/codex/…) is injected at assembly, never
// baked into the kernel. Comments are not literals, so a doc that mentions a kind is fine; only real
// string literals are checked (so the sqlite ":memory:" DSN, for example, never trips this).
func TestCoreHasNoBusinessKindLiterals(t *testing.T) {
	forbidden := make(map[string]bool, len(businessKinds))
	for _, k := range businessKinds {
		forbidden[k] = true
	}
	for _, pkg := range corePackages {
		fset, files := packageFiles(t, pkg)
		for _, f := range files {
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				val := strings.Trim(lit.Value, "`\"")
				if forbidden[val] {
					t.Errorf("core package %q hardcodes business kind %q at %s — keep the core generic; user kinds are injected at assembly, not baked into the channel core",
						pkg, val, fset.Position(lit.Pos()))
				}
				return true
			})
		}
	}
}
