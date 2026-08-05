package main

import (
	"strings"
	"testing"
)

func TestCompareMeasurementRejectsIncreaseAndUnloweredImprovement(t *testing.T) {
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	baseline := validBaselineManifest()
	baseline.Entries = []baselineEntry{entry}
	measured := baseline
	measured.Entries = append([]baselineEntry(nil), baseline.Entries...)
	measured.Entries[0].Ceiling = 91
	if err := compareMeasurement(baseline, exceptionManifest{Entries: []exceptionEntry{}}, measured); err == nil || !strings.Contains(err.Error(), "increased") {
		t.Fatalf("increase error = %v", err)
	}
	measured.Entries[0].Ceiling = 89
	if err := compareMeasurement(baseline, exceptionManifest{Entries: []exceptionEntry{}}, measured); err == nil || !strings.Contains(err.Error(), "lower its baseline") {
		t.Fatalf("improvement error = %v", err)
	}
}

func TestCompareMeasurementAcceptsExactException(t *testing.T) {
	measuredEntry := baselineEntry{Rule: ruleCyclomatic, Identity: functionIdentity(ruleCyclomatic, "harness/new.go", "Run"), Path: "harness/new.go", Symbol: "Run", Ceiling: 24}
	exception := exceptionEntry{Rule: measuredEntry.Rule, Identity: measuredEntry.Identity, Path: measuredEntry.Path, Symbol: measuredEntry.Symbol, Ceiling: 24}
	if err := compareMeasurement(
		baselineManifest{Entries: []baselineEntry{}},
		exceptionManifest{Entries: []exceptionEntry{exception}},
		baselineManifest{Entries: []baselineEntry{measuredEntry}},
	); err != nil {
		t.Fatal(err)
	}
	measuredEntry.Ceiling = 31
	exception.Ceiling = 30
	if err := compareMeasurement(
		baselineManifest{Entries: []baselineEntry{}},
		exceptionManifest{Entries: []exceptionEntry{exception}},
		baselineManifest{Entries: []baselineEntry{measuredEntry}},
	); err == nil || !strings.Contains(err.Error(), "cannot be waived") {
		t.Fatalf("absolute cyclomatic error = %v", err)
	}
}

func TestCompareMeasurementRatchetsExceptionCeilingExactly(t *testing.T) {
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/new.go", "Run"), Path: "harness/new.go", Symbol: "Run", Ceiling: 91}
	exception := exceptionEntry{Rule: entry.Rule, Identity: entry.Identity, Path: entry.Path, Symbol: entry.Symbol, Ceiling: 90}
	compare := func() error {
		return compareMeasurement(
			baselineManifest{Entries: []baselineEntry{}},
			exceptionManifest{Entries: []exceptionEntry{exception}},
			baselineManifest{Entries: []baselineEntry{entry}},
		)
	}
	if err := compare(); err == nil || !strings.Contains(err.Error(), "increased") {
		t.Fatalf("exception increase error = %v", err)
	}
	entry.Ceiling = 89
	if err := compare(); err == nil || !strings.Contains(err.Error(), "lower its ceiling") {
		t.Fatalf("exception improvement error = %v", err)
	}
}

func TestMatchDuplicateBaselineRequiresCurrentExactEvidence(t *testing.T) {
	owners := []string{"harness/a.go::A", "harness/b.go::B"}
	baseline := baselineEntry{Rule: ruleDuplicate, Identity: ruleDuplicate + ":dup-reviewed", DebtID: "dup-reviewed", Owners: owners, Fingerprint: strings.Repeat("a", 64), Ceiling: 160}
	current := baseline
	current.Identity = ruleDuplicate + ":dup-0001"
	current.DebtID = "dup-0001"
	current.Fingerprint = strings.Repeat("b", 64)
	matched, err := matchDuplicateBaseline(current, []baselineEntry{baseline}, map[string]struct{}{})
	if err != nil || matched != nil {
		t.Fatalf("match = %#v, error = %v", matched, err)
	}
	current.Fingerprint = baseline.Fingerprint
	matched, err = matchDuplicateBaseline(current, []baselineEntry{baseline}, map[string]struct{}{})
	if err != nil || matched == nil || matched.DebtID != "dup-reviewed" {
		t.Fatalf("exact match = %#v, error = %v", matched, err)
	}
}

func TestMatchDuplicateBaselineRejectsAmbiguousExactEvidence(t *testing.T) {
	owners := []string{"harness/a.go::A", "harness/b.go::B"}
	fingerprint := strings.Repeat("a", 64)
	first := baselineEntry{Rule: ruleDuplicate, Identity: ruleDuplicate + ":dup-first", DebtID: "dup-first", Owners: owners, Fingerprint: fingerprint, Ceiling: 160}
	second := first
	second.Identity = ruleDuplicate + ":dup-second"
	second.DebtID = "dup-second"
	current := first
	current.Identity = ruleDuplicate + ":dup-0001"
	current.DebtID = "dup-0001"
	matched, err := matchDuplicateBaseline(current, []baselineEntry{first, second}, map[string]struct{}{})
	if err == nil || !strings.Contains(err.Error(), "ambiguous exact") || matched != nil {
		t.Fatalf("ambiguous match = %#v, error = %v", matched, err)
	}
}
