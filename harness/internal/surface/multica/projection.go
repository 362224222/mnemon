package multica

import (
	"strings"
)

type AssignmentMailboxMaterial struct {
	ID               string
	SessionID        string
	Scope            string
	Assignee         string
	AssigneeDisplay  string
	RootIssueID      string
	RootIssueLabel   string
	RootIssueTitle   string
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
	topic := assignmentTitleTopic(item)
	root := strings.TrimSpace(item.RootIssueLabel)
	if root != "" && topic != "" {
		return trimTitle(root + ": " + topic)
	}
	if topic == "" {
		topic = "assignment"
	}
	return trimTitle("Assignment: " + topic)
}

func AssignmentMailboxDescription(item AssignmentMailboxMaterial) string {
	var b strings.Builder
	b.WriteString("## Assignment\n\n")
	if work := strings.TrimSpace(item.ExpectedWork); work != "" {
		b.WriteString(work)
		b.WriteString("\n\n")
	} else if scope := strings.TrimSpace(item.Scope); scope != "" {
		b.WriteString(scope)
		b.WriteString("\n\n")
	}
	b.WriteString("## Context\n\n")
	writeBullet(&b, "Root issue", rootIssueReference(item))
	writeBullet(&b, "Assignee", assigneeReference(item))
	writeBullet(&b, "Scope", item.Scope)
	writeBullet(&b, "Session", codeSpan(item.SessionID))
	writeBullet(&b, "Assignment", codeSpan(item.ID))
	if rationale := strings.TrimSpace(item.Rationale); rationale != "" {
		b.WriteString("\n## Rationale\n\n")
		b.WriteString(rationale)
		b.WriteString("\n")
	}
	b.WriteString("\n## Expected Feedback\n\n")
	b.WriteString("Post a `progress_digest` that includes:\n")
	writeBullet(&b, "`assignment_ref`", codeSpan(item.ID))
	if feedback := strings.TrimSpace(item.ExpectedFeedback); feedback != "" {
		writeBullet(&b, "Requested content", feedback)
	}
	b.WriteString("\nRouting and dedupe fields live in Multica issue metadata under `mnemon.*`; this description is for human review.\n")
	return strings.TrimSpace(b.String())
}

func ProgressCommentBody(item ProgressFeedbackMaterial) string {
	var b strings.Builder
	b.WriteString("## Assignment Feedback\n\n")
	writeBullet(&b, "Assignment", codeSpan(item.AssignmentRef))
	writeBullet(&b, "Feedback", codeSpan(item.FeedbackKind))
	if summary := strings.TrimSpace(item.Summary); summary != "" {
		b.WriteString("\n## Summary\n\n")
		b.WriteString(summary)
		b.WriteString("\n")
	}
	if result := strings.TrimSpace(item.Result); result != "" {
		b.WriteString("\n## Result\n\n")
		b.WriteString(result)
		b.WriteString("\n")
	}
	if blocker := strings.TrimSpace(item.Blocker); blocker != "" {
		b.WriteString("\n## Blocker\n\n")
		b.WriteString(blocker)
		b.WriteString("\n")
	}
	if len(item.ArtifactRefs) > 0 {
		b.WriteString("\n## Artifacts\n\n")
		writeList(&b, item.ArtifactRefs)
	}
	if len(item.EvidenceRefs) > 0 {
		b.WriteString("\n## Evidence\n\n")
		writeList(&b, item.EvidenceRefs)
	}
	return strings.TrimSpace(b.String())
}

func IssueMention(label, issueID string) string {
	issueID = strings.TrimSpace(issueID)
	label = strings.TrimSpace(label)
	if label == "" {
		label = issueID
	}
	if issueID == "" {
		return label
	}
	return "[" + escapeMarkdownLinkLabel(label) + "](mention://issue/" + issueID + ")"
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

func assignmentTitleTopic(item AssignmentMailboxMaterial) string {
	for _, candidate := range []string{firstSentence(item.Scope), firstSentence(item.ExpectedWork), item.ID} {
		candidate = stripTitleRootLabel(candidate, item.RootIssueLabel)
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func stripTitleRootLabel(value, label string) string {
	value = strings.TrimSpace(value)
	label = strings.TrimSpace(label)
	if value == "" || label == "" {
		return value
	}
	if strings.EqualFold(value, label) {
		return ""
	}
	lower := strings.ToLower(value)
	for _, separator := range []string{": ", " - ", " -- ", " "} {
		prefix := label + separator
		if strings.HasPrefix(lower, strings.ToLower(prefix)) {
			return strings.TrimSpace(value[len(prefix):])
		}
	}
	return value
}

func rootIssueReference(item AssignmentMailboxMaterial) string {
	label := firstNonEmptyString(item.RootIssueLabel, item.RootIssueID)
	ref := IssueMention(label, item.RootIssueID)
	if title := strings.TrimSpace(item.RootIssueTitle); title != "" {
		if ref != "" {
			return ref + " - " + title
		}
		return title
	}
	return ref
}

func assigneeReference(item AssignmentMailboxMaterial) string {
	principal := codeSpan(item.Assignee)
	display := strings.TrimSpace(item.AssigneeDisplay)
	if display == "" {
		return principal
	}
	if principal == "" {
		return display
	}
	return principal + " (" + display + ")"
}

func writeBullet(b *strings.Builder, label, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	b.WriteString("- ")
	b.WriteString(label)
	b.WriteString(": ")
	b.WriteString(value)
	b.WriteString("\n")
}

func writeList(b *strings.Builder, values []string) {
	for _, value := range cleanStrings(values) {
		b.WriteString("- ")
		b.WriteString(value)
		b.WriteString("\n")
	}
}

func codeSpan(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return "`" + strings.ReplaceAll(value, "`", "'") + "`"
}

func firstSentence(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	for _, sep := range []string{". ", "\n"} {
		if before, _, ok := strings.Cut(value, sep); ok {
			return strings.TrimSpace(before)
		}
	}
	return value
}

func trimTitle(value string) string {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	const max = 96
	if len(value) <= max {
		return value
	}
	return strings.TrimSpace(value[:max-1]) + "..."
}

func escapeMarkdownLinkLabel(value string) string {
	value = strings.ReplaceAll(value, "[", `\[`)
	value = strings.ReplaceAll(value, "]", `\]`)
	return value
}

func cleanStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
