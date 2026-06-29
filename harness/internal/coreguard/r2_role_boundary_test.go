package coreguard

import (
	"strings"
	"testing"
)

type roleImportBoundary struct {
	pkg       string
	forbids   []string
	rationale string
}

var projectionSurfaceImportBoundaries = []roleImportBoundary{
	{
		pkg: "projection",
		forbids: []string{
			"harness/internal/mnemond/access",
			"harness/internal/mnemond/admission",
			"harness/internal/mnemond/state",
			"harness/internal/runtime",
			"harness/internal/eventstore",
			"harness/internal/app",
			"harness/internal/assembler",
		},
		rationale: "projection packages render accepted state into external artifacts and must not ingest governed events",
	},
	{
		pkg: "surface/multica",
		forbids: []string{
			"harness/internal/mnemond/access",
			"harness/internal/mnemond/admission",
			"harness/internal/mnemond/state",
			"harness/internal/runtime",
			"harness/internal/eventstore",
			"harness/internal/app",
			"harness/internal/assembler",
		},
		rationale: "the Multica surface is a projection/adapter boundary, not a local mnemond write path",
	},
}

var activationTraceImportBoundary = roleImportBoundary{
	pkg: "activationtrace",
	forbids: []string{
		"harness/internal/contract",
		"harness/internal/event",
		"harness/internal/eventstore",
		"harness/internal/mnemond/access",
		"harness/internal/mnemond/admission",
		"harness/internal/mnemond/state",
		"harness/internal/runtime",
	},
	rationale: "activation trace is non-canonical run display material and must not become EventEnvelope material",
}

func TestR2RoleBoundaryGuardLogicIsNotVacuous(t *testing.T) {
	if !roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/mnemond/access", projectionSurfaceImportBoundaries[0].forbids) {
		t.Fatal("projection boundary guard must flag mnemond access imports")
	}
	if roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view", projectionSurfaceImportBoundaries[0].forbids) {
		t.Fatal("projection boundary guard must allow read-only presentation view imports")
	}
	if !roleImportForbidden("github.com/mnemon-dev/mnemon/harness/internal/event", activationTraceImportBoundary.forbids) {
		t.Fatal("activation trace boundary guard must flag event model imports")
	}
}

func TestProjectionSurfacesDoNotIngestGovernedEvents(t *testing.T) {
	for _, boundary := range projectionSurfaceImportBoundaries {
		assertPackageAvoidsForbiddenImports(t, boundary)
	}
}

func TestActivationTraceNeverWritesEventMaterial(t *testing.T) {
	assertPackageAvoidsForbiddenImports(t, activationTraceImportBoundary)
}

func assertPackageAvoidsForbiddenImports(t *testing.T, boundary roleImportBoundary) {
	t.Helper()
	_, files := packageFiles(t, boundary.pkg)
	for _, f := range files {
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			if roleImportForbidden(path, boundary.forbids) {
				t.Errorf("package %q imports forbidden package %q: %s", boundary.pkg, path, boundary.rationale)
			}
		}
	}
}

func roleImportForbidden(path string, forbids []string) bool {
	for _, forbidden := range forbids {
		if strings.Contains(path, forbidden) {
			return true
		}
	}
	return false
}
