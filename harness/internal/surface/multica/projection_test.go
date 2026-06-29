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
		"Scope: runtime/readiness",
		"## Feedback",
		"Expected feedback: Brief result.",
		"Progress path: Mnemon runtime progress, result, or blocker feedback",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("description missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{"Session:", "Assignment: `asg-1`", "progress_digest", "assignment_ref", "mnemon."} {
		if strings.Contains(body, blocked) {
			t.Fatalf("visible description must not expose machine field %q:\n%s", blocked, body)
		}
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

func TestAssignmentMailboxTitleUsesAssignmentIDForBroadDrillScope(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:             "assignment-tea66-routing-isolation",
		Scope:          "TEA-66 Mnemon R2 hub-flow readiness drill",
		RootIssueLabel: "TEA-66",
		ExpectedWork:   "Validate assignment child issue routing and stale or cross-session isolation.",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-66: routing isolation" {
		t.Fatalf("title = %q", got)
	}
}

func TestAssignmentMailboxTitleUsesPathScopeTopicBeforeBroadIDFallback(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:             "assignment-tea88-root-runtime",
		Scope:          "multica-hub-readiness-drill/TEA-88/stage1/root-metadata-run-visibility",
		RootIssueLabel: "TEA-88",
		ExpectedWork:   "Validate root session metadata and run visibility.",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-88: root metadata run visibility" {
		t.Fatalf("title = %q", got)
	}
}

func TestAssignmentMailboxTitleUsesAssignmentIDForReadinessScope(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:             "r2-drill-child-routing-isolation",
		Scope:          "mnemon-r2-multica-hub-flow-readiness",
		RootIssueLabel: "TEA-95",
		ExpectedWork:   "Validate child routing isolation.",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-95: child routing isolation" {
		t.Fatalf("title = %q", got)
	}
}

func TestAssignmentMailboxTitleDropsRootIdentifierTokensFromAssignmentID(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:             "tea-104-root-visibility",
		Scope:          "mnemon-r2-multica-hub-flow-readiness",
		RootIssueLabel: "TEA-104",
		ExpectedWork:   "Validate root session visibility.",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-104: root visibility" {
		t.Fatalf("title = %q", got)
	}
}

func TestAssignmentMailboxTitleUsesAssignmentIDForMachineReferenceScope(t *testing.T) {
	item := AssignmentMailboxMaterial{
		ID:             "assignment-tea84-root-runtime",
		Scope:          "multica:issue:582a0697-df9b-4e81-84f9-da72b0a685e5",
		RootIssueLabel: "TEA-84",
		ExpectedWork:   "Validate root session metadata and run visibility.",
	}
	if got := AssignmentMailboxTitle(item); got != "TEA-84: root runtime" {
		t.Fatalf("title = %q", got)
	}
}

func TestAssignmentMailboxDescriptionNormalizesProtocolFeedback(t *testing.T) {
	body := AssignmentMailboxDescription(AssignmentMailboxMaterial{
		ID:               "asg-1",
		Scope:            "release validation",
		ExpectedFeedback: "progress_digest with result or blocker",
	})
	if !strings.Contains(body, "Expected feedback: result or blocker") {
		t.Fatalf("description should expose human feedback wording:\n%s", body)
	}
	if strings.Contains(body, "progress_digest") {
		t.Fatalf("description should keep protocol event type out of visible text:\n%s", body)
	}
}

func TestAssignmentMailboxDescriptionNormalizesFeedbackKindProtocolWording(t *testing.T) {
	body := AssignmentMailboxDescription(AssignmentMailboxMaterial{
		ID:               "asg-1",
		Scope:            "release validation",
		ExpectedFeedback: "progress_digest feedback_kind=result or blocker with exact Multica evidence",
	})
	for _, blocked := range []string{"progress_digest", "feedback_kind"} {
		if strings.Contains(body, blocked) {
			t.Fatalf("description should keep protocol word %q out of visible text:\n%s", blocked, body)
		}
	}
	if !strings.Contains(body, "runtime feedback status=result or blocker") {
		t.Fatalf("description missing readable feedback wording:\n%s", body)
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
		"## Feedback",
		"Status: result",
		"## Summary",
		"Done",
		"## Result",
		"Validated",
		"## Artifacts",
		"- run-1",
		"## Evidence",
		"- comment-1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("progress body missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{"Assignment: `asg-1`", "Feedback: `result`", "progress_digest", "assignment_ref", "mnemon."} {
		if strings.Contains(body, blocked) {
			t.Fatalf("progress body must keep machine field %q in metadata/markers, not visible text:\n%s", blocked, body)
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

func TestRuntimeProjectionCommentBodyForIntake(t *testing.T) {
	body := RuntimeProjectionCommentBody(RuntimeProjectionMaterial{
		Status:             "recorded",
		IssueID:            "issue-1",
		IssueLabel:         "TEA-1",
		Principal:          "planner@team",
		TaskID:             "task-1",
		HubBackend:         "multica",
		SessionID:          "multica:session:issue-1",
		HasIngestReceipt:   true,
		IngestSeq:          42,
		IngestDuplicate:    false,
		IngestTicked:       true,
		WakeStatus:         "completed",
		WakeTurnID:         "turn-1",
		HubWriteStatus:     "created",
		HubChildIssues:     2,
		HubFeedbackComment: 1,
	})
	for _, want := range []string{
		"## Mnemon Runtime",
		"Issue: [TEA-1](mention://issue/issue-1)",
		"Principal: `planner@team`",
		"Mnemond ingest: seq=42 duplicate=false ticked=true",
		"Managed wake: `completed`",
		"Managed turn: `turn-1`",
		"Projection status: updates created",
		"Assignments created: 2",
		"Feedback comments added: 1",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("runtime projection body missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{"Hub backend:", "Multica hub write", "child_issues", "feedback_comments"} {
		if strings.Contains(body, blocked) {
			t.Fatalf("runtime projection body must not expose machine field %q:\n%s", blocked, body)
		}
	}
}

func TestRuntimeProjectionCommentBodyForAssignmentMailbox(t *testing.T) {
	body := RuntimeProjectionCommentBody(RuntimeProjectionMaterial{
		AssignmentMailbox: true,
		Status:            "correlated",
		RootIssueID:       "root-1",
		RootIssueLabel:    "TEA-1",
		AssignmentID:      "asg-1",
		SessionID:         "multica:session:root-1",
		WakeStatus:        "completed",
	})
	for _, want := range []string{
		"## Mnemon Runtime",
		"Status: correlated",
		"Root issue: [TEA-1](mention://issue/root-1)",
		"Managed wake: `completed`",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("assignment runtime projection body missing %q:\n%s", want, body)
		}
	}
	for _, blocked := range []string{"Assignment: `asg-1`", "Session: `multica:session:root-1`"} {
		if strings.Contains(body, blocked) {
			t.Fatalf("assignment runtime projection should keep %q in metadata, not comment text:\n%s", blocked, body)
		}
	}
}

func TestCanonicalIssueStatus(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{in: "backlog", want: StatusBacklog},
		{in: "waiting", want: StatusTodo},
		{in: "in progress", want: StatusInProgress},
		{in: "review", want: StatusInReview},
		{in: "completed", want: StatusDone},
		{in: "blocked", want: StatusBlocked},
		{in: "canceled", want: StatusCancelled},
		{in: "cancelled", want: StatusCancelled},
		{in: "unknown", want: ""},
	} {
		if got := CanonicalIssueStatus(tc.in); got != tc.want {
			t.Fatalf("CanonicalIssueStatus(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestProgressIssueStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		name string
		item ProgressFeedbackMaterial
		want string
	}{
		{name: "waiting", item: ProgressFeedbackMaterial{FeedbackKind: "waiting"}, want: StatusTodo},
		{name: "in progress", item: ProgressFeedbackMaterial{FeedbackKind: "progress"}, want: StatusInProgress},
		{name: "review", item: ProgressFeedbackMaterial{FeedbackKind: "review"}, want: StatusInReview},
		{name: "done", item: ProgressFeedbackMaterial{FeedbackKind: "result"}, want: StatusDone},
		{name: "blocked", item: ProgressFeedbackMaterial{FeedbackKind: "blocker"}, want: StatusBlocked},
		{name: "canceled", item: ProgressFeedbackMaterial{FeedbackKind: "canceled"}, want: StatusCancelled},
		{name: "blocked fallback", item: ProgressFeedbackMaterial{Blocker: "waiting on access"}, want: StatusBlocked},
		{name: "done fallback", item: ProgressFeedbackMaterial{Result: "validated"}, want: StatusDone},
		{name: "unknown", item: ProgressFeedbackMaterial{Summary: "no lifecycle signal"}, want: ""},
	} {
		if got := ProgressIssueStatus(tc.item); got != tc.want {
			t.Fatalf("%s: ProgressIssueStatus = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestProgressRootIssueStatusMapping(t *testing.T) {
	if got := ProgressRootIssueStatus(ProgressFeedbackMaterial{FeedbackKind: "progress"}, false); got != StatusInProgress {
		t.Fatalf("progress root status = %q", got)
	}
	if got := ProgressRootIssueStatus(ProgressFeedbackMaterial{FeedbackKind: "blocker"}, false); got != StatusBlocked {
		t.Fatalf("blocked root status = %q", got)
	}
	if got := ProgressRootIssueStatus(ProgressFeedbackMaterial{FeedbackKind: "result"}, false); got != StatusInReview {
		t.Fatalf("partial result root status = %q", got)
	}
	if got := ProgressRootIssueStatus(ProgressFeedbackMaterial{FeedbackKind: "result"}, true); got != StatusDone {
		t.Fatalf("complete result root status = %q", got)
	}
}
