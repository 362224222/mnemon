package projection

import (
	"strings"
)

type CommentMaterial struct {
	Title        string
	Body         string
	EventIDs     []string
	EventType    string
	SessionID    string
	AssignmentID string
}

func FormatComment(material CommentMaterial) string {
	title := strings.TrimSpace(material.Title)
	body := strings.TrimSpace(material.Body)
	var b strings.Builder
	if title != "" {
		b.WriteString("Mnemon update: ")
		b.WriteString(title)
		b.WriteString("\n\n")
	} else {
		b.WriteString("Mnemon update\n\n")
	}
	if body != "" {
		b.WriteString(body)
		b.WriteString("\n")
	}
	if ids := cleanStrings(material.EventIDs); len(ids) > 0 {
		b.WriteString("\n")
		for _, id := range ids {
			writeMarker(&b, "event", id)
		}
		writeMarker(&b, "type", material.EventType)
		writeMarker(&b, "session", material.SessionID)
		writeMarker(&b, "assignment", material.AssignmentID)
	}
	return strings.TrimSpace(b.String())
}

func writeMarker(b *strings.Builder, key, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return
	}
	b.WriteString("mnemon:")
	b.WriteString(key)
	b.WriteString("=")
	b.WriteString(value)
	b.WriteString("\n")
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
