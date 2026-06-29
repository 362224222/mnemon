package multica

import (
	"strings"
	"testing"
)

func TestAssignmentMailboxMaterial(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:               "asg-1",
		SessionID:        "session-1",
		Scope:            "runtime/readiness",
		Assignee:         "researcher@team",
		AssigneeDisplay:  "mnemon-researcher",
		RootIssueID:      "root-1",
		RootIssueLabel:   "TEA-1",
		RootIssueTitle:   "Validate runtime",
		ExpectedWork:     "Check runtime output.",
		ExpectedFeedback: "Brief result.",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-1: runtime/readiness" {
		t.Fatalf("title = %q", got)
	}
	body := AssignmentMailboxDescription(item)
	for _, want := range []string{
		"## Assignment",
		"Root issue: [TEA-1](mention://issue/root-1) - Validate runtime",
		"Assignee: `researcher@team` (mnemon-researcher)",
		"Session: `session-1`",
		"Assignment: `asg-1`",
		"Post a `progress_digest`",
		"metadata under `mnemon.*`",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("description missing %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, "mnemon.session_id") {
		t.Fatalf("visible description must not copy machine metadata keys:\n%s", body)
	}
}

func TestAssignmentMailboxTitleStripsRootLabelFromScope(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:             "asg-1",
		Scope:          "TEA-14 Mnemon R2 Multica hub final validation",
		RootIssueLabel: "TEA-14",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-14: Mnemon R2 Multica hub final validation" {
		t.Fatalf("title = %q", got)
	}
}

func TestProgressFeedbackMaterial(t *testing.T) {
	item := ProgressFeedbackMaterial{
		AssignmentRef: "asg-1",
		FeedbackKind:  "result",
		Summary:       "Done",
		Result:        "Validated",
		ArtifactRefs:  []string{"run-1"},
		EvidenceRefs:  []string{"comment-1"},
	}
	body := ProgressCommentBody(item)
	for _, want := range []string{
		"## Assignment Feedback",
		"Assignment: `asg-1`",
		"Feedback: `result`",
		"## Artifacts",
		"- run-1",
		"## Evidence",
		"- comment-1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("progress body missing %q:\n%s", want, body)
		}
	}
	if got := ProgressIssueStatus(item); got != "done" {
		t.Fatalf("progress status = %q", got)
	}
	if !ProgressCompletesAssignment(item) {
		t.Fatal("result progress should complete assignment")
	}
	if !IssueStatusDone("completed") {
		t.Fatal("completed should be terminal")
	}
}
