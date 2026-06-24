package app

import (
	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

// budgetShapeEventView returns a copy of proj whose per-resource Content is shaped to the subscriber's
// context-budget tier (P4b). It is a LOCAL presentation transform on render/pull context (I11: budget
// acts on derived presentation + pull results, and the LOCAL side decides; the hub is never tier-aware).
// Each resource's fields pass through the owning capability's ShapeByBudget, which keeps the most-recent
// K items and re-renders the header over them. A kind with no catalogued capability passes through
// unchanged (no silent drop). Resources and Digest are left attesting the FULL authoritative scope:
// budget bounds CONTEXT, not authority (the grant scope is the security boundary), and render output
// reads from Content. The input proj is never mutated (a fresh Content slice + fresh shaped maps), so
// the same projection can also be served unbudgeted elsewhere.
func budgetShapeEventView(proj eventview.EventView, catalog map[string]capability.Capability, tier contract.BudgetTier) eventview.EventView {
	if resolved, err := contract.ResolveBudgetTier(tier); err != nil || resolved == contract.BudgetHot {
		return proj // hot / full / unknown: no shaping, exact passthrough
	}
	shaped := make([]eventview.ResourceContent, len(proj.Content))
	for i, rc := range proj.Content {
		shaped[i] = rc
		cap, ok := catalog[string(rc.Ref.Kind)]
		if !ok {
			continue
		}
		shaped[i].Fields = capability.ShapeByBudget(cap, rc.Fields, tier)
	}
	out := proj
	out.Content = shaped
	return out
}
