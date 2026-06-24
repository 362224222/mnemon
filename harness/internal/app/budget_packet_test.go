package app

import (
	"fmt"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
)

func projItems(n int) []any {
	out := make([]any, n)
	for i := 0; i < n; i++ {
		out[i] = map[string]any{"id": fmt.Sprintf("a%d", i), "scope": fmt.Sprintf("task-%d", i)}
	}
	return out
}

// P4b: budgetShapeEventView shapes a DERIVED-MIRROR projection's Content to the subscriber's tier —
// digest-only/warm shrink the rendered packet; hot is exact passthrough; the input is never mutated;
// the integrity Digest is left attesting the full authoritative scope (budget bounds context, not authority).
func TestBudgetShapeProjection(t *testing.T) {
	catalog := policy.EmbeddedCatalog()
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	proj := eventview.EventView{
		Digest: "full-scope-digest",
		Content: []eventview.ResourceContent{
			{Ref: ref, Version: 12, Fields: map[string]any{"items": projItems(12), "updated_by": "x"}},
		},
	}

	digest := budgetShapeEventView(proj, catalog, contract.BudgetDigestOnly)
	if n := len(digest.Content[0].Fields["items"].([]any)); n != policy.BudgetDigestItems {
		t.Fatalf("digest-only must shrink to %d item, got %d", policy.BudgetDigestItems, n)
	}
	if digest.Digest != "full-scope-digest" {
		t.Fatalf("budget must NOT alter the integrity digest (it attests the full scope), got %q", digest.Digest)
	}

	warm := budgetShapeEventView(proj, catalog, contract.BudgetWarm)
	if n := len(warm.Content[0].Fields["items"].([]any)); n != policy.BudgetWarmItems {
		t.Fatalf("warm must shrink to %d items, got %d", policy.BudgetWarmItems, n)
	}

	hot := budgetShapeEventView(proj, catalog, contract.BudgetHot)
	if n := len(hot.Content[0].Fields["items"].([]any)); n != 12 {
		t.Fatalf("hot must keep all 12 items, got %d", n)
	}

	// the ORIGINAL projection must be untouched — the same scope can still be served unbudgeted
	if n := len(proj.Content[0].Fields["items"].([]any)); n != 12 {
		t.Fatalf("budgetShapeEventView must not mutate its input, original now has %d items", n)
	}
}

// An uncatalogued kind passes through unchanged (no silent drop) even under a shrinking tier.
func TestBudgetShapeProjectionUnknownKindPassthrough(t *testing.T) {
	ref := contract.ResourceRef{Kind: "mystery", ID: "x"}
	proj := eventview.EventView{Content: []eventview.ResourceContent{
		{Ref: ref, Version: 1, Fields: map[string]any{"items": projItems(20)}},
	}}
	out := budgetShapeEventView(proj, policy.EmbeddedCatalog(), contract.BudgetDigestOnly)
	if n := len(out.Content[0].Fields["items"].([]any)); n != 20 {
		t.Fatalf("uncatalogued kind must pass through unshaped, got %d items", n)
	}
}
