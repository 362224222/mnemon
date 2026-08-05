package main

import (
	"strings"
	"testing"
)

func TestCompareBaseBaselineRejectsAdditionAndIncrease(t *testing.T) {
	entry := baselineEntry{Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"), Path: "harness/a.go", Symbol: "Run", Ceiling: 90}
	base := validBaselineManifest()
	base.Entries = []baselineEntry{entry}
	candidate := base
	candidate.Entries = append([]baselineEntry(nil), base.Entries...)
	candidate.Entries[0].Ceiling = 91
	if err := compareBaseBaseline(base, candidate); err == nil || !strings.Contains(err.Error(), "raises") {
		t.Fatalf("increase error = %v", err)
	}
	candidate.Entries[0] = entry
	candidate.Entries = append(candidate.Entries, baselineEntry{Rule: ruleProductionFile, Identity: fileIdentity(ruleProductionFile, "harness/b.go"), Path: "harness/b.go", Ceiling: 401})
	if err := compareBaseBaseline(base, candidate); err == nil || !strings.Contains(err.Error(), "adds") {
		t.Fatalf("addition error = %v", err)
	}
}

func TestCompareBaseManifestsRejectSourceCommitRebinding(t *testing.T) {
	base := validBaselineManifest()
	candidate := base
	candidate.SourceCommit = strings.Repeat("b", 40)
	if err := compareBaseBaseline(base, candidate); err == nil || !strings.Contains(err.Error(), "source_commit") {
		t.Fatalf("baseline source rebind error = %v", err)
	}
	baseArchitecture := architectureManifest{SourceCommit: base.SourceCommit}
	candidateArchitecture := architectureManifest{SourceCommit: candidate.SourceCommit}
	if err := compareBaseArchitecture(baseArchitecture, candidateArchitecture); err == nil || !strings.Contains(err.Error(), "source_commit") {
		t.Fatalf("architecture source rebind error = %v", err)
	}
}

func TestCompareBaseArchitectureAndExceptionsRejectBroaderDebt(t *testing.T) {
	architecture := architectureEntry{Rule: "dependency_direction", Identity: "dep:a", Path: "harness/a.go", Component: "legacy", Risk: "medium", Evidence: "harness/a.go", Owner: "team", RemovalCheckpoint: "7R"}
	baseArchitecture := architectureManifest{Entries: []architectureEntry{architecture}}
	candidateArchitecture := baseArchitecture
	candidateArchitecture.Entries = append([]architectureEntry(nil), baseArchitecture.Entries...)
	candidateArchitecture.Entries[0].Risk = "high"
	if err := compareBaseArchitecture(baseArchitecture, candidateArchitecture); err == nil {
		t.Fatal("architecture risk upgrade was accepted")
	}
	exception := exceptionEntry{Rule: ruleFunctionLines, Identity: "id", Path: "harness/a.go", Symbol: "Run"}
	if err := compareBaseExceptions(exceptionManifest{}, exceptionManifest{Entries: []exceptionEntry{exception}}); err != nil {
		t.Fatalf("exact new exception was rejected: %v", err)
	}
	changed := exception
	changed.Path = "harness/b.go"
	if err := compareBaseExceptions(exceptionManifest{Entries: []exceptionEntry{exception}}, exceptionManifest{Entries: []exceptionEntry{changed}}); err == nil {
		t.Fatal("rebound exception was accepted")
	}
}

func TestDuplicateEvolutionAllowsOneDimensionalCleanupOnly(t *testing.T) {
	prior := baselineEntry{
		Rule: ruleDuplicate, Identity: ruleDuplicate + ":dup-reviewed", DebtID: "dup-reviewed",
		Path:        "harness/a.go",
		Owners:      []string{"harness/a.go::A", "harness/b.go::B", "harness/c.go::C"},
		Fingerprint: strings.Repeat("a", 64),
	}
	ownerCleanup := prior
	ownerCleanup.Owners = ownerCleanup.Owners[:2]
	if err := validateDuplicateEvolution(prior, ownerCleanup); err != nil {
		t.Fatalf("owner cleanup: %v", err)
	}
	fingerprintRebind := prior
	fingerprintRebind.Fingerprint = strings.Repeat("b", 64)
	if err := validateDuplicateEvolution(prior, fingerprintRebind); err == nil || !strings.Contains(err.Error(), "cannot rebind fingerprint") {
		t.Fatalf("fingerprint rebind error = %v", err)
	}
	rebound := ownerCleanup
	rebound.Fingerprint = fingerprintRebind.Fingerprint
	if err := validateDuplicateEvolution(prior, rebound); err == nil {
		t.Fatal("simultaneous owner and fingerprint rebind was accepted")
	}
	added := prior
	added.Owners = append(append([]string(nil), prior.Owners...), "harness/d.go::D")
	if err := validateDuplicateEvolution(prior, added); err == nil {
		t.Fatal("duplicate owner addition was accepted")
	}
}

func TestCompareBaseBaselineAllowsDuplicateDerivedPathToFollowOwnerCleanup(t *testing.T) {
	entry := baselineEntry{
		Rule: ruleDuplicate, Identity: ruleDuplicate + ":dup-reviewed", DebtID: "dup-reviewed",
		Path: "harness/a.go", Owners: []string{"harness/a.go::A", "harness/b.go::B", "harness/c.go::C"},
		Fingerprint: strings.Repeat("a", 64), Ceiling: 160,
	}
	base := validBaselineManifest()
	base.Entries = []baselineEntry{entry}
	candidate := base
	candidate.Entries = append([]baselineEntry(nil), base.Entries...)
	candidate.Entries[0].Owners = []string{"harness/b.go::B", "harness/c.go::C"}
	candidate.Entries[0].Path = "harness/b.go"
	if err := compareBaseBaseline(base, candidate); err != nil {
		t.Fatalf("derived path cleanup: %v", err)
	}
	candidate.Entries[0].Path = "harness/c.go"
	if err := compareBaseBaseline(base, candidate); err == nil || !strings.Contains(err.Error(), "first remaining owner") {
		t.Fatalf("misbound derived path error = %v", err)
	}
}
