package policy

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// makeItems builds n id-bearing items (id-bearing because itemsFromFields requires a non-empty id),
// each with a summary field the progress_digest render folds into the rendered `content`.
func makeBudgetItems(n int) []any {
	out := make([]any, n)
	for i := 0; i < n; i++ {
		out[i] = map[string]any{"id": "e" + string(rune('a'+i)), "summary": "item-" + string(rune('a'+i))}
	}
	return out
}

// P4b: ShapeByBudget caps the item COUNT per tier and RE-RENDERS the header over the kept tail, so a
// content-rendered surface actually shrinks. hot = full; warm = recent 8; digest-only = recent 1.
func TestShapeByBudgetCapsItemsAndRerenders(t *testing.T) {
	cap := EmbeddedCatalog()["progress_digest"] // items_field=items, content render = bullet-list of summary
	if cap.ItemsField != "items" {
		t.Fatalf("fixture: progress_digest must have items_field=items, got %q", cap.ItemsField)
	}
	full := makeBudgetItems(12)
	fields := map[string]any{"items": full, "updated_by": "codex@x"}
	// seed the rendered header the way an admitted write would, so "shaping re-renders content" is real
	for k, v := range cap.Header(itemsFromFields(fields, "items")) {
		fields[k] = v
	}

	cases := []struct {
		tier      contract.BudgetTier
		wantItems int
	}{
		{contract.BudgetHot, 12},
		{contract.BudgetWarm, BudgetWarmItems},
		{contract.BudgetDigestOnly, BudgetDigestItems},
	}
	for _, c := range cases {
		shaped := ShapeByBudget(cap, fields, c.tier)
		got := itemsFromFields(shaped, "items")
		if len(got) != c.wantItems {
			t.Fatalf("tier %s: kept %d items, want %d", c.tier, len(got), c.wantItems)
		}
		// re-rendered content must reflect the kept tail, not the full set
		content, _ := shaped["content"].(string)
		if c.tier != contract.BudgetHot {
			if strings.Contains(content, "item-a") {
				t.Fatalf("tier %s: content must drop the oldest item (item-a), got %q", c.tier, content)
			}
			if !strings.Contains(content, "item-l") { // the newest (12th, index 11 = 'l') is always kept
				t.Fatalf("tier %s: content must keep the newest item (item-l), got %q", c.tier, content)
			}
		}
		// updated_by (a non-item, non-header field) is preserved across shaping
		if ub, _ := shaped["updated_by"].(string); ub != "codex@x" {
			t.Fatalf("tier %s: shaping must preserve updated_by, got %q", c.tier, ub)
		}
	}
}

// hot is an exact passthrough — the SAME map, so an unbudgeted surface is byte-identical to today.
func TestShapeByBudgetHotIsIdentity(t *testing.T) {
	cap := EmbeddedCatalog()["assignment"]
	fields := map[string]any{"items": makeBudgetItems(20), "updated_by": "x"}
	if got := ShapeByBudget(cap, fields, contract.BudgetHot); len(itemsFromFields(got, "items")) != 20 {
		t.Fatalf("hot must keep all 20 items, got %d", len(itemsFromFields(got, "items")))
	}
	// empty tier resolves to hot (full) — never a silent downgrade
	if got := ShapeByBudget(cap, fields, ""); len(itemsFromFields(got, "items")) != 20 {
		t.Fatalf("empty tier must resolve to hot/full, kept %d", len(itemsFromFields(got, "items")))
	}
}

// Already-within-budget and unknown-tier are exact passthroughs (no reshape, no data loss).
func TestShapeByBudgetWithinBudgetAndUnknownPassthrough(t *testing.T) {
	cap := EmbeddedCatalog()["assignment"]
	small := map[string]any{"items": makeBudgetItems(3)} // 3 <= warm cap 8
	if got := ShapeByBudget(cap, small, contract.BudgetWarm); len(itemsFromFields(got, "items")) != 3 {
		t.Fatalf("within-budget warm must keep all 3, got %d", len(itemsFromFields(got, "items")))
	}
	big := map[string]any{"items": makeBudgetItems(20)}
	if got := ShapeByBudget(cap, big, contract.BudgetTier("cold")); len(itemsFromFields(got, "items")) != 20 {
		t.Fatalf("unknown tier must fail open to full (never drop), kept %d", len(itemsFromFields(got, "items")))
	}
}
