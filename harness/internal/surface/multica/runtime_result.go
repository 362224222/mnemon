package multica

import (
	"fmt"
	"strings"
)

type RuntimeResultSummary struct {
	IssueID             string
	Identifier          string
	Title               string
	Principal           string
	Status              string
	Err                 string
	ProjectionStatus    string
	ProjectionCommentID string
	ProjectionErr       string
	WakeStatus          string
	WakeTurnID          string
	WakeErr             string
	HubWriteStatus      string
	HubChildIssues      int
	HubFeedbackComments int
	HubWriteErr         string
}

func FormatRuntimeFinalAnswer(result RuntimeResultSummary) string {
	var b strings.Builder
	if result.IssueID == "" {
		b.WriteString("Mnemon Multica runtime did not receive a Multica issue id.")
	} else {
		label := strings.TrimSpace(result.Identifier)
		if label == "" {
			label = result.IssueID
		}
		b.WriteString("Mnemon Multica runtime handled issue ")
		b.WriteString(label)
		if title := strings.TrimSpace(result.Title); title != "" {
			b.WriteString(" (")
			b.WriteString(title)
			b.WriteString(")")
		}
		b.WriteString(".")
	}
	if principal := strings.TrimSpace(result.Principal); principal != "" {
		b.WriteString(" Principal: ")
		b.WriteString(principal)
		b.WriteString(".")
	}
	switch result.Status {
	case "recorded":
		b.WriteString(" Mnemon ingest: recorded.")
	case "correlated":
		b.WriteString(" Mnemon assignment mailbox: correlated.")
	case "skipped":
		b.WriteString(" Mnemon ingest: skipped")
		if result.Err != "" {
			b.WriteString(" (")
			b.WriteString(result.Err)
			b.WriteString(")")
		} else {
			b.WriteString(" because MNEMON_CONTROL_ADDR is not set")
		}
		b.WriteString(".")
	case "failed":
		b.WriteString(" Mnemon ingest: failed")
		if result.Err != "" {
			b.WriteString(" (")
			b.WriteString(result.Err)
			b.WriteString(")")
		}
		b.WriteString(".")
	default:
		if result.Err != "" {
			b.WriteString(" Mnemon ingest: failed (")
			b.WriteString(result.Err)
			b.WriteString(").")
		}
	}
	switch result.ProjectionStatus {
	case "commented":
		b.WriteString(" Multica projection: comment posted.")
	case "skipped":
		b.WriteString(" Multica projection: skipped.")
	case "failed":
		b.WriteString(" Multica projection: failed")
		if result.ProjectionErr != "" {
			b.WriteString(" (")
			b.WriteString(result.ProjectionErr)
			b.WriteString(")")
		}
		b.WriteString(".")
	}
	switch result.WakeStatus {
	case "completed":
		b.WriteString(" Managed wake: completed.")
	case "skipped":
		b.WriteString(" Managed wake: skipped")
		if result.WakeErr != "" {
			b.WriteString(" (")
			b.WriteString(result.WakeErr)
			b.WriteString(")")
		}
		b.WriteString(".")
	case "failed":
		b.WriteString(" Managed wake: failed")
		if result.WakeErr != "" {
			b.WriteString(" (")
			b.WriteString(result.WakeErr)
			b.WriteString(")")
		}
		b.WriteString(".")
	default:
		if result.WakeStatus != "" {
			b.WriteString(" Managed wake: ")
			b.WriteString(result.WakeStatus)
			b.WriteString(".")
		}
	}
	switch result.HubWriteStatus {
	case "created", "commented", "updated", "noop":
		b.WriteString(" Multica updates: ")
		b.WriteString(RuntimeHubWriteSummary(result))
	case "skipped":
		b.WriteString(" Multica updates: skipped.")
	case "failed":
		b.WriteString(" Multica updates: failed")
		if result.HubWriteErr != "" {
			b.WriteString(" (")
			b.WriteString(result.HubWriteErr)
			b.WriteString(")")
		}
		b.WriteString(".")
	}
	return strings.TrimSpace(b.String())
}

func RuntimePrincipalLabel(principal string) string {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return "the resolved principal"
	}
	return principal
}

func RuntimeIssueLabel(id, identifier, title string) string {
	if strings.TrimSpace(identifier) != "" && strings.TrimSpace(title) != "" {
		return strings.TrimSpace(identifier) + " (" + strings.TrimSpace(title) + ")"
	}
	if strings.TrimSpace(identifier) != "" {
		return strings.TrimSpace(identifier)
	}
	if strings.TrimSpace(id) != "" {
		return strings.TrimSpace(id)
	}
	return "the Multica issue"
}

func RuntimeAssignmentCorrelationProgress() string {
	return "Mnemon assignment mailbox: correlated."
}

func RuntimeWakeProgress(result RuntimeResultSummary) string {
	switch strings.TrimSpace(result.WakeStatus) {
	case "":
		if result.WakeErr != "" {
			return "Managed wake failed: " + result.WakeErr
		}
		return "Managed wake did not run."
	case "completed":
		if strings.TrimSpace(result.WakeTurnID) != "" {
			return "Managed wake completed: turn=" + result.WakeTurnID + "."
		}
		return "Managed wake completed."
	case "skipped":
		if result.WakeErr != "" {
			return "Managed wake skipped: " + result.WakeErr
		}
		return "Managed wake skipped."
	case "failed":
		if result.WakeErr != "" {
			return "Managed wake failed: " + result.WakeErr
		}
		return "Managed wake failed."
	default:
		return "Managed wake status: " + result.WakeStatus + "."
	}
}

func RuntimeHubWriteProgress(result RuntimeResultSummary) string {
	switch strings.TrimSpace(result.HubWriteStatus) {
	case "":
		return ""
	case "failed":
		if result.HubWriteErr != "" {
			return "Multica updates failed: " + result.HubWriteErr
		}
		return "Multica updates failed."
	case "skipped":
		return "Multica updates skipped."
	default:
		return "Multica updates: " + RuntimeHubWriteSummary(result)
	}
}

func RuntimeHubWriteSummary(result RuntimeResultSummary) string {
	status := strings.TrimSpace(result.HubWriteStatus)
	var parts []string
	if result.HubChildIssues > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", result.HubChildIssues, runtimePluralize(result.HubChildIssues, "assignment mailbox", "assignment mailboxes")))
	}
	if result.HubFeedbackComments > 0 {
		parts = append(parts, fmt.Sprintf("%d %s", result.HubFeedbackComments, runtimePluralize(result.HubFeedbackComments, "feedback comment", "feedback comments")))
	}
	switch {
	case len(parts) > 0 && status == "created":
		return strings.Join(parts, " and ") + " created."
	case len(parts) > 0 && status == "commented":
		return strings.Join(parts, " and ") + " posted."
	case len(parts) > 0:
		return strings.Join(parts, " and ") + " synced."
	case status == "noop":
		return "no visible updates needed."
	case status != "":
		return strings.ReplaceAll(status, "_", " ") + "."
	default:
		return "no visible updates needed."
	}
}

func RuntimeProjectionProgress(result RuntimeResultSummary) string {
	switch strings.TrimSpace(result.ProjectionStatus) {
	case "":
		return ""
	case "failed":
		if result.ProjectionErr != "" {
			return "Multica comment projection failed: " + result.ProjectionErr
		}
		return "Multica comment projection failed."
	case "skipped":
		return "Multica comment projection skipped."
	default:
		if strings.TrimSpace(result.ProjectionCommentID) != "" {
			return "Multica comment projection completed: comment=" + result.ProjectionCommentID + "."
		}
		return "Multica comment projection completed."
	}
}

func runtimePluralize(n int, singular, plural string) string {
	if n == 1 {
		return singular
	}
	return plural
}
