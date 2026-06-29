package multica

import (
	"strings"
)

type AssignmentMailboxMaterial struct {
	ID               string
	SessionID        string
	Scope            string
	Assignee         string
	ExpectedWork     string
	ExpectedFeedback string
	Rationale        string
}

type ProgressFeedbackMaterial struct {
	AssignmentRef string
	FeedbackKind  string
	Summary       string
	Result        string
	Blocker       string
	ArtifactRefs  []string
	EvidenceRefs  []string
}

func AssignmentMailboxTitle(item AssignmentMailboxMaterial) string {
	scope := strings.TrimSpace(item.Scope)
	if scope == "" {
		scope = strings.TrimSpace(item.ID)
	}
	if scope == "" {
		scope = "assignment"
	}
	return "Mnemon assignment " + item.ID + ": " + scope
}

func AssignmentMailboxDescription(item AssignmentMailboxMaterial) string {
	var b strings.Builder
	b.WriteString("Mnemon assignment mailbox\n\n")
	writeLine := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		b.WriteString(label)
		b.WriteString(": ")
		b.WriteString(value)
		b.WriteString("\n")
	}
	writeLine("Assignment", item.ID)
	writeLine("Session", item.SessionID)
	writeLine("Scope", item.Scope)
	writeLine("Assignee", item.Assignee)
	writeLine("Expected work", item.ExpectedWork)
	writeLine("Expected feedback", item.ExpectedFeedback)
	writeLine("Rationale", item.Rationale)
	return strings.TrimSpace(b.String())
}

func ProgressCommentBody(item ProgressFeedbackMaterial) string {
	var b strings.Builder
	writeLine := func(label, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		b.WriteString(label)
		b.WriteString(": ")
		b.WriteString(value)
		b.WriteString("\n")
	}
	writeLine("Assignment", item.AssignmentRef)
	writeLine("Feedback", item.FeedbackKind)
	writeLine("Summary", item.Summary)
	writeLine("Result", item.Result)
	writeLine("Blocker", item.Blocker)
	if len(item.ArtifactRefs) > 0 {
		writeLine("Artifacts", strings.Join(item.ArtifactRefs, ", "))
	}
	if len(item.EvidenceRefs) > 0 {
		writeLine("Evidence", strings.Join(item.EvidenceRefs, ", "))
	}
	return strings.TrimSpace(b.String())
}

func ProgressIssueStatus(item ProgressFeedbackMaterial) string {
	switch strings.ToLower(strings.TrimSpace(item.FeedbackKind)) {
	case "blocker":
		return "blocked"
	case "result":
		return "done"
	case "progress":
		return "in_progress"
	}
	if strings.TrimSpace(item.Blocker) != "" {
		return "blocked"
	}
	if strings.TrimSpace(item.Result) != "" {
		return "done"
	}
	return ""
}

func ProgressCompletesAssignment(item ProgressFeedbackMaterial) bool {
	if strings.EqualFold(strings.TrimSpace(item.FeedbackKind), "result") {
		return true
	}
	return strings.TrimSpace(item.Result) != ""
}

func IssueStatusDone(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "done", "completed", "complete":
		return true
	default:
		return false
	}
}
