package main

import (
	"strings"
	"testing"
)

func TestValidateBaselineRejectsStaleThresholdAndMalformedDuplicate(t *testing.T) {
	manifest := validBaselineManifest()
	manifest.Thresholds[0].Limit++
	if err := validateBaseline(manifest); err == nil || !strings.Contains(err.Error(), "threshold") {
		t.Fatalf("threshold error = %v", err)
	}
	manifest = validBaselineManifest()
	manifest.Entries = []baselineEntry{{
		Rule: ruleDuplicate, Identity: ruleDuplicate + ":dup-0001", Path: "harness/a.go",
		DebtID: "dup-0001", Owners: []string{"harness/b.go::B", "harness/a.go::A"},
		Fingerprint: strings.Repeat("a", 64), Ceiling: 160,
	}}
	if err := validateBaseline(manifest); err == nil || !strings.Contains(err.Error(), "sorted") {
		t.Fatalf("owner sorting error = %v", err)
	}
}

func TestValidateExceptionsRejectsWildcardAndForbiddenRule(t *testing.T) {
	entry := exceptionEntry{
		Rule: ruleFunctionLines, Identity: functionIdentity(ruleFunctionLines, "harness/a.go", "Run"),
		Path: "harness/a.go", Symbol: "Run", Reason: "legacy transaction shell", Risk: "medium",
		Owner: "harness", RemovalCheckpoint: "7R", Ceiling: 90,
	}
	if err := validateExceptions(exceptionManifest{SchemaVersion: 1, Entries: []exceptionEntry{entry}}); err != nil {
		t.Fatal(err)
	}
	entry.Path = "harness/*"
	if err := validateExceptionEntry(entry); err == nil {
		t.Fatal("wildcard exception was accepted")
	}
	entry.Rule = "unowned_goroutine"
	if err := validateExceptionEntry(entry); err == nil {
		t.Fatal("forbidden exception rule was accepted")
	}
	entry.Rule = ruleDuplicate
	entry.Identity = ruleDuplicate + ":dup-0001"
	entry.Symbol = ""
	entry.Ceiling = 160
	if err := validateExceptionEntry(entry); err == nil || !strings.Contains(err.Error(), "unavailable in v1") {
		t.Fatalf("duplicate exception error = %v", err)
	}
}

func validBaselineManifest() baselineManifest {
	thresholds := append([]threshold(nil), qualityThresholds...)
	return baselineManifest{
		SchemaVersion: 1,
		ToolVersion:   qualityToolVersion,
		SourceCommit:  strings.Repeat("a", 40),
		Thresholds:    thresholds,
		Entries:       []baselineEntry{},
	}
}
