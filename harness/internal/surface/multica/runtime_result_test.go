package multica

import (
	"strings"
	"testing"
)

func TestFormatRuntimeFinalAnswerSummarizesRuntimeOutcome(t *testing.T) {
	got := FormatRuntimeFinalAnswer(RuntimeResultSummary{
		IssueID:             "iss-1",
		Identifier:          "TEA-1",
		Title:               "Runtime adapter cleanup",
		Principal:           "planner@team",
		Status:              "recorded",
		ProjectionStatus:    "commented",
		WakeStatus:          "completed",
		HubWriteStatus:      "updated",
		HubChildIssues:      1,
		HubFeedbackComments: 2,
	})
	for _, want := range []string{
		"Mnemon Multica runtime handled issue TEA-1 (Runtime adapter cleanup).",
		"Principal: planner@team.",
		"Mnemon ingest: recorded.",
		"Multica projection: comment posted.",
		"Managed wake: completed.",
		"Multica updates: 1 assignment mailbox and 2 feedback comments synced.",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("final answer missing %q:\n%s", want, got)
		}
	}
}

func TestRuntimeFinalAnswerCarriesFailures(t *testing.T) {
	got := FormatRuntimeFinalAnswer(RuntimeResultSummary{
		IssueID:          "iss-2",
		Status:           "failed",
		Err:              "ingest refused",
		ProjectionStatus: "failed",
		ProjectionErr:    "comment rejected",
		WakeStatus:       "failed",
		WakeErr:          "render unavailable",
		HubWriteStatus:   "failed",
		HubWriteErr:      "metadata timeout",
	})
	for _, want := range []string{
		"Mnemon ingest: failed (ingest refused).",
		"Multica projection: failed (comment rejected).",
		"Managed wake: failed (render unavailable).",
		"Multica updates: failed (metadata timeout).",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("failure answer missing %q:\n%s", want, got)
		}
	}
}

func TestRuntimeProgressSummaries(t *testing.T) {
	if got := RuntimeWakeProgress(RuntimeResultSummary{WakeStatus: "completed", WakeTurnID: "turn-1"}); got != "Managed wake completed: turn=turn-1." {
		t.Fatalf("wake progress = %q", got)
	}
	if got := RuntimeHubWriteProgress(RuntimeResultSummary{HubWriteStatus: "commented", HubFeedbackComments: 1}); got != "Multica updates: 1 feedback comment posted." {
		t.Fatalf("hub progress = %q", got)
	}
	if got := RuntimeProjectionProgress(RuntimeResultSummary{ProjectionStatus: "commented", ProjectionCommentID: "comment-1"}); got != "Multica comment projection completed: comment=comment-1." {
		t.Fatalf("projection progress = %q", got)
	}
	if got := RuntimeAssignmentCorrelationProgress(); got != "Mnemon assignment mailbox: correlated." {
		t.Fatalf("assignment correlation progress = %q", got)
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
