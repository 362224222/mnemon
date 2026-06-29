package projection

import (
	"strings"
)

type CommentMaterial struct {
	Title    string
	Body     string
	EventIDs []string
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
			b.WriteString("mnemon:event=")
			b.WriteString(id)
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
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
