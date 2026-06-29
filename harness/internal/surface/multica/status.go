package multica

import "strings"

const (
	StatusBacklog    = "backlog"
	StatusTodo       = "todo"
	StatusInProgress = "in_progress"
	StatusInReview   = "in_review"
	StatusDone       = "done"
	StatusBlocked    = "blocked"
	StatusCancelled  = "cancelled"
)

func CanonicalIssueStatus(status string) string {
	status = normalizeStatusToken(status)
	switch status {
	case StatusBacklog, StatusTodo, StatusInProgress, StatusInReview, StatusDone, StatusBlocked, StatusCancelled:
		return status
	case "pending", "queued":
		return StatusBacklog
	case "open", "wait", "waiting", "to_do":
		return StatusTodo
	case "active", "started", "working", "progress":
		return StatusInProgress
	case "review", "reviewing", "ready_for_review":
		return StatusInReview
	case "complete", "completed", "closed", "resolved":
		return StatusDone
	case "blocker":
		return StatusBlocked
	case "abort", "aborted", "cancel", "canceled":
		return StatusCancelled
	default:
		return ""
	}
}

func canonicalProgressStatus(kind string) string {
	switch normalizeStatusToken(kind) {
	case "blocker", "blocked":
		return "blocker"
	case "result", "done", "complete", "completed":
		return "result"
	case "progress", "in_progress", "working", "started":
		return "progress"
	case "review", "in_review", "reviewing", "ready_for_review":
		return "review"
	case "wait", "waiting", "todo", "to_do":
		return "waiting"
	case "cancel", "canceled", "cancelled", "aborted":
		return "cancelled"
	default:
		return ""
	}
}

func normalizeStatusToken(status string) string {
	status = strings.ToLower(strings.TrimSpace(status))
	status = strings.ReplaceAll(status, "-", "_")
	status = strings.ReplaceAll(status, " ", "_")
	for strings.Contains(status, "__") {
		status = strings.ReplaceAll(status, "__", "_")
	}
	return status
}
