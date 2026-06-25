package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// #2: the trusted mode catalogs must advertise only modes the kernel actually delivers.
// permissive/audit_only/dry_run are not implemented (Apply enforces rules unconditionally = strict),
// so they must NOT be in the catalog — a future config seam selects modes by catalog lookup
// (define≠select, Invariant #12), and a catalog promising behavior it cannot deliver would let
// `dry_run` still commit. (Reserved like `serializable` until they have real, distinct teeth.)
//
// The catalogs are asserted directly: the prior ResolveModes vehicle was deleted with the superseded
// NewFromConfig boot front door (assembler.Assemble is the sole config front door).
func TestUnimplementedModesNotInCatalogs(t *testing.T) {
	for _, bad := range []string{contract.AuthzPermissive, contract.AuthzAuditOnly, contract.AuthzDryRun} {
		if contract.AuthzCatalog[bad] {
			t.Fatalf("authz mode %q must NOT be in the catalog until implemented", bad)
		}
	}
	if !contract.AuthzCatalog[contract.AuthzStrict] {
		t.Fatal("strict must remain in the authz catalog")
	}
	// define≠select: a script path can never be a catalog member, so a lookup-only selector can
	// never execute one.
	for _, catalog := range []map[string]bool{contract.ConflictCatalog, contract.IsolationCatalog, contract.AuthzCatalog} {
		if catalog["./evil.sh"] {
			t.Fatal("non-catalog script string present — SAFETY BREACH")
		}
	}
}
