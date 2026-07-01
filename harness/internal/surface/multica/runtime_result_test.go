package multica

import (
	"strings"
	"testing"
)

func TestFormatRuntimeFinalAnswerSummarizesRuntimeOutcome(t *testing.T) {
	got := FormatRuntimeFinalAnswer(RuntimeResultSummary{
		IssueID:    "iss-1",
		Identifier: "TEA-1",
		Title:      "Runtime adapter cleanup",
		Principal:  "planner@team",
		Status:     "recorded",
	})
	for _, want := range []string{
		"Mnemon Multica runtime handled issue TEA-1 (Runtime adapter cleanup).",
		"Principal: planner@team.",
		"Multica surface input: observed.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("final answer missing %q:\n%s", want, got)
		}
	}
	for _, old := range []string{"assignment mailbox", "Managed wake", "Multica projection", "Multica updates"} {
		if strings.Contains(got, old) {
			t.Fatalf("final answer contains old R2 wording %q:\n%s", old, got)
		}
	}
}

func TestRuntimeFinalAnswerCarriesFailures(t *testing.T) {
	got := FormatRuntimeFinalAnswer(RuntimeResultSummary{
		IssueID: "iss-2",
		Status:  "failed",
		Err:     "ingest refused",
	})
	for _, want := range []string{
		"Multica surface input: failed (ingest refused).",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("failure answer missing %q:\n%s", want, got)
		}
	}
}

func TestRuntimeFinalAnswerSummarizesSkippedSurfaceInput(t *testing.T) {
	got := FormatRuntimeFinalAnswer(RuntimeResultSummary{
		IssueID: "iss-3",
		Status:  "skipped",
		Err:     "MNEMON_CONTROL_ADDR is not set",
	})
	if !strings.Contains(got, "Multica surface input: skipped (MNEMON_CONTROL_ADDR is not set).") {
		t.Fatalf("skipped answer mismatch:\n%s", got)
	}
}

func TestRuntimeLabels(t *testing.T) {
	if got := RuntimePrincipalLabel(""); got != "the resolved principal" {
		t.Fatalf("empty principal label = %q", got)
	}
	if got := RuntimeIssueLabel("iss-1", "TEA-1", "Issue title"); got != "TEA-1 (Issue title)" {
		t.Fatalf("issue label = %q", got)
	}
	if got := RuntimeIssueLabel("iss-1", "", ""); got != "iss-1" {
		t.Fatalf("fallback issue label = %q", got)
	}
}
