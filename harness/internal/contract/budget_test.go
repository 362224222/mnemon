package contract

import "testing"

// P4a: BudgetTier is a CLOSED set resolved at one site. Empty resolves to hot (migration-safe full
// delivery — budget is not a security axis, so empty must preserve existing behavior, mirroring
// ClampRefs's empty-requested=full-scope). Any non-catalogued value is rejected, never silently widened.
func TestResolveBudgetTier(t *testing.T) {
	cases := []struct {
		in      BudgetTier
		want    BudgetTier
		wantErr bool
	}{
		{"", BudgetHot, false},          // empty => hot (full), not digest-only — no silent downgrade
		{BudgetHot, BudgetHot, false},   // catalogued passes through
		{BudgetWarm, BudgetWarm, false}, //
		{BudgetDigestOnly, BudgetDigestOnly, false},
		{"cold", "", true}, // unknown => fail-loud, never widened
		{"HOT", "", true},  // case-sensitive closed set
		{"hot ", "", true}, // no trimming/normalization beyond the empty default
	}
	for _, c := range cases {
		got, err := ResolveBudgetTier(c.in)
		if c.wantErr {
			if err == nil {
				t.Fatalf("ResolveBudgetTier(%q): want error, got %q", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("ResolveBudgetTier(%q): unexpected error %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("ResolveBudgetTier(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// The closed set is exactly three tiers — a guard so adding a tier is a deliberate edit, and so the
// smallest-context-first ordering the local render/pull derivation relies on stays a fixed catalog.
func TestBudgetTierCatalogIsClosed(t *testing.T) {
	if len(budgetTiers) != 3 {
		t.Fatalf("budget tier catalog must be exactly {hot,warm,digest-only}, got %d entries", len(budgetTiers))
	}
	for _, tier := range []BudgetTier{BudgetHot, BudgetWarm, BudgetDigestOnly} {
		if !budgetTiers[tier] {
			t.Fatalf("catalog missing %q", tier)
		}
	}
}
