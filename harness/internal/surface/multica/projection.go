package multica

import (
	"fmt"
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

type RuntimeProjectionMaterial struct {
	AssignmentMailbox  bool
	Status             string
	IssueID            string
	IssueLabel         string
	Principal          string
	TaskID             string
	HubBackend         string
	SessionID          string
	RootIssueID        string
	RootIssueLabel     string
	AssignmentID       string
	HasIngestReceipt   bool
	IngestSeq          int64
	IngestDuplicate    bool
	IngestTicked       bool
	WakeStatus         string
	WakeTurnID         string
	HubWriteStatus     string
	HubChildIssues     int
	HubFeedbackComment int
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
	if rationale := strings.TrimSpace(item.Rationale); rationale != "" {
		b.WriteString("\n## Rationale\n\n")
		b.WriteString(rationale)
		b.WriteString("\n")
	}
	b.WriteString("\n## Feedback\n\n")
	if feedback := visibleExpectedFeedback(item.ExpectedFeedback); feedback != "" {
		writeBullet(&b, "Expected feedback", feedback)
	}
	if strings.TrimSpace(item.ExpectedFeedback) == "" {
		b.WriteString("Report progress, results, or blockers through the Mnemon runtime path.\n")
	} else {
		writeBullet(&b, "Progress path", "Mnemon runtime progress, result, or blocker feedback")
	}
	return strings.TrimSpace(b.String())
}

func ProgressCommentBody(item ProgressFeedbackMaterial) string {
	var b strings.Builder
	b.WriteString("## Feedback\n\n")
	if status := visibleFeedbackStatus(item.FeedbackKind); status != "" {
		writeBullet(&b, "Status", status)
	}
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

func RuntimeProjectionCommentBody(item RuntimeProjectionMaterial) string {
	var b strings.Builder
	if item.AssignmentMailbox {
		b.WriteString("## Mnemon Runtime\n\n")
		writeBullet(&b, "Status", firstNonEmptyString(item.Status, "correlated"))
		writeBullet(&b, "Root issue", IssueMention(firstNonEmptyString(item.RootIssueLabel, item.RootIssueID), item.RootIssueID))
	} else {
		b.WriteString("## Mnemon Runtime\n\n")
		writeBullet(&b, "Status", item.Status)
		writeBullet(&b, "Issue", IssueMention(firstNonEmptyString(item.IssueLabel, item.IssueID), item.IssueID))
		writeBullet(&b, "Principal", codeSpan(item.Principal))
		if item.HasIngestReceipt {
			writeBullet(&b, "Mnemond ingest", fmt.Sprintf("seq=%d duplicate=%v ticked=%v", item.IngestSeq, item.IngestDuplicate, item.IngestTicked))
		}
	}
	if item.WakeStatus != "" || item.HubWriteStatus != "" {
		b.WriteString("\n## Effects\n\n")
		writeBullet(&b, "Managed wake", codeSpan(item.WakeStatus))
		writeBullet(&b, "Managed turn", codeSpan(item.WakeTurnID))
		if item.HubWriteStatus != "" {
			writeBullet(&b, "Projection status", visibleProjectionStatus(item.HubWriteStatus))
			if item.HubChildIssues > 0 {
				writeBullet(&b, "Assignments created", fmt.Sprintf("%d", item.HubChildIssues))
			}
			if item.HubFeedbackComment > 0 {
				writeBullet(&b, "Feedback comments added", fmt.Sprintf("%d", item.HubFeedbackComment))
			}
		}
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
	switch canonicalProgressStatus(item.FeedbackKind) {
	case "blocker":
		return StatusBlocked
	case "result":
		return StatusDone
	case "progress":
		return StatusInProgress
	case "review":
		return StatusInReview
	case "waiting":
		return StatusTodo
	case "cancelled":
		return StatusCancelled
	}
	if strings.TrimSpace(item.Blocker) != "" {
		return StatusBlocked
	}
	if strings.TrimSpace(item.Result) != "" {
		return StatusDone
	}
	return ""
}

func ProgressRootIssueStatus(item ProgressFeedbackMaterial, allAssignmentsDone bool) string {
	switch ProgressIssueStatus(item) {
	case StatusBlocked:
		return StatusBlocked
	case StatusCancelled:
		return StatusCancelled
	case StatusDone:
		if allAssignmentsDone {
			return StatusDone
		}
		return StatusInReview
	case StatusInReview:
		return StatusInReview
	case StatusInProgress:
		return StatusInProgress
	case StatusTodo:
		return StatusTodo
	case StatusBacklog:
		return StatusBacklog
	default:
		return ""
	}
}

func ProgressCompletesAssignment(item ProgressFeedbackMaterial) bool {
	return ProgressIssueStatus(item) == StatusDone
}

func IssueStatusDone(status string) bool {
	return CanonicalIssueStatus(status) == StatusDone
}

func assignmentTitleTopic(item AssignmentMailboxMaterial) string {
	scope := firstSentence(item.Scope)
	if topic := scopePathTitleTopic(scope, item.RootIssueLabel); topic != "" {
		return topic
	}
	idTopic := assignmentIDTitleTopic(item.ID, item.RootIssueLabel)
	if idTopic != "" && (broadAssignmentScope(scope) || machineReferenceScope(scope)) {
		return idTopic
	}
	for _, candidate := range []string{scope, firstSentence(item.ExpectedWork), idTopic, item.ID} {
		candidate = stripTitleRootLabel(candidate, item.RootIssueLabel)
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

func scopePathTitleTopic(scope, rootLabel string) string {
	scope = strings.TrimSpace(scope)
	if scope == "" || !strings.Contains(scope, "/") || strings.Contains(scope, "://") {
		return ""
	}
	parts := strings.Split(scope, "/")
	if len(parts) < 3 {
		return ""
	}
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			continue
		}
		part = stripTitleRootLabel(part, rootLabel)
		part = strings.Trim(strings.ReplaceAll(strings.ReplaceAll(part, "_", " "), "-", " "), " ")
		if part == "" || stageLikeTitlePart(part) {
			continue
		}
		return part
	}
	return ""
}

func stageLikeTitlePart(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || value == "stage" || value == "phase" {
		return true
	}
	if strings.HasPrefix(value, "stage") && len(value) > len("stage") {
		for _, r := range value[len("stage"):] {
			if r < '0' || r > '9' {
				return false
			}
		}
		return true
	}
	if strings.HasPrefix(value, "phase") && len(value) > len("phase") {
		for _, r := range value[len("phase"):] {
			if r < '0' || r > '9' {
				return false
			}
		}
		return true
	}
	return false
}

func broadAssignmentScope(scope string) bool {
	lower := strings.ToLower(strings.TrimSpace(scope))
	return strings.Contains(lower, "drill") ||
		strings.Contains(lower, "validation") ||
		strings.Contains(lower, "readiness") ||
		strings.Contains(lower, "hub-flow")
}

func machineReferenceScope(scope string) bool {
	scope = strings.TrimSpace(strings.ToLower(scope))
	if scope == "" {
		return false
	}
	return strings.HasPrefix(scope, "multica:") || strings.HasPrefix(scope, "github:") || strings.Contains(scope, "://")
}

func assignmentIDTitleTopic(id, rootLabel string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	root := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(rootLabel), "-", ""))
	rootParts := titleRootParts(rootLabel)
	parts := strings.FieldsFunc(strings.ToLower(id), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	var out []string
	for i := 0; i < len(parts); i++ {
		part := parts[i]
		if len(rootParts) > 0 && titlePartsMatch(parts[i:], rootParts) {
			i += len(rootParts) - 1
			continue
		}
		switch part {
		case "", "assignment", "asg", "r1", "r2", "drill", "readiness", root:
			continue
		}
		if root != "" && strings.TrimPrefix(part, root) == "" {
			continue
		}
		out = append(out, part)
	}
	joined := strings.Join(out, " ")
	if strings.IndexFunc(joined, func(r rune) bool { return r >= 'a' && r <= 'z' }) < 0 {
		return ""
	}
	return joined
}

func titleRootParts(rootLabel string) []string {
	return strings.FieldsFunc(strings.ToLower(strings.TrimSpace(rootLabel)), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
}

func titlePartsMatch(parts, want []string) bool {
	if len(want) == 0 || len(parts) < len(want) {
		return false
	}
	for i, part := range want {
		if parts[i] != part {
			return false
		}
	}
	return true
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

func visibleExpectedFeedback(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = strings.ReplaceAll(value, "progress_digest", "runtime feedback")
	value = strings.ReplaceAll(value, "progress digest", "runtime feedback")
	value = strings.ReplaceAll(value, "feedback_kind=", "status=")
	value = strings.ReplaceAll(value, "feedback_kind", "status")
	lower := strings.ToLower(value)
	for _, prefix := range []string{"runtime feedback with "} {
		if strings.HasPrefix(lower, prefix) {
			return strings.TrimSpace(value[len(prefix):])
		}
	}
	if lower == "runtime feedback" {
		return "progress, result, or blocker"
	}
	return value
}

func visibleFeedbackStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "progress":
		return "progress update"
	case "result":
		return "result"
	case "blocker":
		return "blocked"
	case "review":
		return "ready for review"
	case "waiting":
		return "waiting"
	case "cancelled", "canceled":
		return "cancelled"
	default:
		return strings.ReplaceAll(strings.TrimSpace(value), "_", " ")
	}
}

func visibleProjectionStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return ""
	case "created":
		return "updates created"
	case "updated":
		return "updates synced"
	case "commented":
		return "feedback posted"
	case "skipped":
		return "no visible updates needed"
	case "failed":
		return "update failed"
	default:
		return strings.ReplaceAll(strings.TrimSpace(value), "_", " ")
	}
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
