package participant

import (
	"strings"
	"testing"
)

type record struct {
	Principal   string
	DisplayName string
}

func TestRequirePrincipalTrimsAndRejectsEmpty(t *testing.T) {
	principal, err := RequirePrincipal("participant", " planner@team ")
	if err != nil {
		t.Fatal(err)
	}
	if principal != "planner@team" {
		t.Fatalf("principal = %q", principal)
	}
	if _, err := RequirePrincipal("multica registry participant", " "); err == nil || !strings.Contains(err.Error(), "multica registry participant principal is required") {
		t.Fatalf("expected principal required error, got %v", err)
	}
}

func TestValidateUniquePrincipals(t *testing.T) {
	items := []record{{Principal: "planner@team"}, {Principal: " reviewer@team "}}
	if err := ValidateUniquePrincipals(items, "participant", func(item record) string { return item.Principal }); err != nil {
		t.Fatal(err)
	}
	items = append(items, record{Principal: "planner@team"})
	if err := ValidateUniquePrincipals(items, "participant", func(item record) string { return item.Principal }); err == nil || !strings.Contains(err.Error(), "duplicate participant principal") {
		t.Fatalf("expected duplicate participant error, got %v", err)
	}
}

func TestUpsertByPrincipalCanReplaceOrMerge(t *testing.T) {
	items := []record{{Principal: "planner@team", DisplayName: "planner"}}
	items = UpsertByPrincipal(items, record{Principal: " planner@team ", DisplayName: "lead"}, func(item record) string {
		return item.Principal
	}, nil)
	if len(items) != 1 || items[0].DisplayName != "lead" {
		t.Fatalf("replace upsert mismatch: %+v", items)
	}
	items = UpsertByPrincipal(items, record{Principal: "reviewer@team", DisplayName: "reviewer"}, func(item record) string {
		return item.Principal
	}, func(existing, next record) record {
		if existing.DisplayName == "" {
			existing.DisplayName = next.DisplayName
		}
		return existing
	})
	if len(items) != 2 {
		t.Fatalf("append upsert mismatch: %+v", items)
	}
}
