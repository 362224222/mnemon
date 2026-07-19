package main

import "testing"

func TestQualityThresholdsAreCanonical(t *testing.T) {
	want := []threshold{
		{Rule: ruleCognitive, Limit: 25},
		{Rule: ruleNesting, Limit: 4},
		{Rule: ruleCyclomatic, Limit: 20},
		{Rule: ruleFunctionLines, Limit: 80},
		{Rule: ruleStatements, Limit: 50},
		{Rule: ruleDuplicate, Limit: 149},
		{Rule: rulePairedTestFile, Limit: 800},
		{Rule: ruleProductionFile, Limit: 400},
	}
	if len(qualityThresholds) != len(want) {
		t.Fatalf("threshold count = %d, want %d", len(qualityThresholds), len(want))
	}
	for i := range want {
		if qualityThresholds[i] != want[i] {
			t.Fatalf("threshold[%d] = %#v, want %#v", i, qualityThresholds[i], want[i])
		}
	}
}

func TestStableIdentitiesDoNotUseLines(t *testing.T) {
	if got := functionIdentity(ruleCyclomatic, "harness/a.go", "(*T).Run"); got != "cyclomatic_complexity:harness/a.go::(*T).Run" {
		t.Fatalf("function identity = %q", got)
	}
	if got := fileIdentity(ruleProductionFile, "harness/a.go"); got != "production_file_lines:harness/a.go" {
		t.Fatalf("file identity = %q", got)
	}
}
