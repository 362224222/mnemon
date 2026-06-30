package multica

import (
	"strings"
)

type RuntimeResultSummary struct {
	IssueID             string
	Identifier          string
	Title               string
	Principal           string
	Status              string
	Err                 string
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
		b.WriteString(" Multica surface input: observed.")
	case "skipped":
		b.WriteString(" Multica surface input: skipped")
		if result.Err != "" {
			b.WriteString(" (")
			b.WriteString(result.Err)
			b.WriteString(")")
		} else {
			b.WriteString(" because MNEMON_CONTROL_ADDR is not set")
		}
		b.WriteString(".")
	case "failed":
		b.WriteString(" Multica surface input: failed")
		if result.Err != "" {
			b.WriteString(" (")
			b.WriteString(result.Err)
			b.WriteString(")")
		}
		b.WriteString(".")
	default:
		if result.Err != "" {
			b.WriteString(" Multica surface input: failed (")
			b.WriteString(result.Err)
			b.WriteString(").")
		}
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
