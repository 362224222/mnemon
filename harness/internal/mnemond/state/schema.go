package state

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

type SchemaGuard struct {
	Required map[contract.ResourceKind][]string
}

func DefaultSchemaGuard() SchemaGuard {
	// DefaultSchemaGuard carries ONLY the GOVERNANCE kinds — kernel-internal control-plane state whose
	// writes the kernel produces (D3). Event data kinds are NOT compiled here: the assembler registers
	// each enabled capability's kind + required header onto this base (PD2), so the live guard is
	// governance ∪ enabled caps.
	return SchemaGuard{Required: map[contract.ResourceKind][]string{
		// lease/budget/receipt are versioned control-plane state whose required fields back fencing,
		// budget accounting, and the durable record of an external effect. Lockstep with
		// contract.KindCatalog == GovernanceKinds (kind_catalog_test).
		"lease":   {"job_id", "owner", "fence_until"},
		"budget":  {"limit_usd", "spent_usd"},
		"receipt": {"job_id", "effect_id", "outcome"},
		// coordination records a governed teamwork-topology op (P2.2 route 3/3); operation is the
		// minimal required field. Lockstep with contract.KindCatalog (kind_catalog_test).
		"coordination": {"operation"},
	}}
}

// SchemaGuardWith returns the governance DefaultSchemaGuard extended with the given user-kind
// required fields. The live runtime gets these from the assembler (each enabled capability's
// declared required header); SchemaGuardWith is for callers that build a kernel WITHOUT assembling a
// catalog (tests using a canonical user kind, and any hand-built config) — they name the kinds they
// need rather than relying on a privileged default.
func SchemaGuardWith(extra map[contract.ResourceKind][]string) SchemaGuard {
	g := DefaultSchemaGuard()
	for k, v := range extra {
		g.Required[k] = v
	}
	return g
}

func (g SchemaGuard) Validate(kind contract.ResourceKind, fields map[string]any) error {
	required, known := g.Required[kind]
	if !known {
		return fmt.Errorf("%w: unknown resource kind %q", errSchema, kind)
	}
	for _, f := range required {
		if _, ok := fields[f]; !ok {
			return fmt.Errorf("%w: %s requires %q", errSchema, kind, f)
		}
	}
	return nil
}
