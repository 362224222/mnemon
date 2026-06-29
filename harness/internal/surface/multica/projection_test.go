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
		ExpectedWork:     "Check runtime output.",
		ExpectedFeedback: "Brief result.",
	}
	if got := AssignmentMailboxTitle(item); got != "Mnemon assignment asg-1: runtime/readiness" {
		t.Fatalf("title = %q", got)
	}
	body := AssignmentMailboxDescription(item)
	for _, want := range []string{"Assignment: asg-1", "Session: session-1", "Assignee: researcher@team"} {
		if !strings.Contains(body, want) {
			t.Fatalf("description missing %q:\n%s", want, body)
		}
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
	for _, want := range []string{"Assignment: asg-1", "Feedback: result", "Artifacts: run-1", "Evidence: comment-1"} {
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
