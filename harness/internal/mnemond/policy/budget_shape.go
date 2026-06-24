package policy

import "github.com/mnemon-dev/mnemon/harness/internal/contract"

// Budget item caps per tier (P4b). REDUCER-FREE by construction: a tier bounds the COUNT of items the
// local render/pull context presents (most-recent-first), never a model summary (which would be a reducer — out of
// scope / B1, the no-remote-reducer entry decision). "digest-only" is therefore the minimal
// recent-context tier (the single latest item), "warm" a bounded recent window, "hot" the full set. A
// true semantic-summary digest is a sync-abi-v2 / reducer concern, deliberately deferred.
const (
	BudgetWarmItems   = 8
	BudgetDigestItems = 1
)

// ShapeByBudget returns the resource fields shaped for a context-budget tier: it keeps only the
// most-recent K items (K per tier; hot = all) and RE-RENDERS the capability's header over the kept
// subset, so a content-rendered surface that reads the rendered `content` field, not the raw item list,
// actually shrinks. "Most-recent" = the tail of the item list, whose
// order is the local append/import sequence (replica-deterministic, so an offline replay reshapes
// identically — B6). Non-item kinds, an unknown tier, and an already-within-budget set are returned
// UNCHANGED (exact passthrough preserves updated_by and any header the writer set; unknown fails open
// to hot — never silently drops data, the closed-set guard lives at config time in ResolveBudgetTier).
//
// This is a pure LOCAL presentation transform: it never reduces on the hub and never alters authority
// (the grant scope is the security boundary — budget bounds CONTEXT only; B2 remote settles, local decides).
func ShapeByBudget(cap Capability, fields map[string]any, tier contract.BudgetTier) map[string]any {
	resolved, err := contract.ResolveBudgetTier(tier)
	if err != nil || resolved == contract.BudgetHot {
		return fields
	}
	limit := BudgetWarmItems
	if resolved == contract.BudgetDigestOnly {
		limit = BudgetDigestItems
	}
	if cap.ItemsField == "" {
		return fields
	}
	items := itemsFromFields(fields, cap.ItemsField)
	if len(items) <= limit {
		return fields
	}
	kept := items[len(items)-limit:] // tail = most-recent K (append order = local import seq)
	// Store the kept items back in the CANONICAL []any shape: every reader (itemsFromFields, the
	// projection JSON round-trip) expects []any of map[string]any — storing []Item would read back empty.
	keptAny := make([]any, len(kept))
	for i, it := range kept {
		keptAny[i] = map[string]any(it)
	}
	shaped := make(map[string]any, len(fields))
	for k, v := range fields {
		shaped[k] = v
	}
	shaped[cap.ItemsField] = keptAny
	for k, v := range cap.Header(kept) { // re-render content/header over the kept subset
		shaped[k] = v
	}
	return shaped
}
