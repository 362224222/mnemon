package multica

import "strings"

type IssueSurfaceDescriptionMaterial struct {
	Request    string
	WorkMode   string
	Handoffs   []string
	Validation []string
	Completion string
}

func IssueSurfaceDescription(item IssueSurfaceDescriptionMaterial) string {
	var b strings.Builder
	writeDescriptionSection(&b, "Request", item.Request)
	writeDescriptionSection(&b, "Work mode", item.WorkMode)
	writeDescriptionList(&b, "Expected collaboration", item.Handoffs)
	writeDescriptionList(&b, "Acceptance", item.Validation)
	writeDescriptionSection(&b, "Completion", item.Completion)
	return strings.TrimSpace(b.String())
}

func writeDescriptionSection(b *strings.Builder, title, body string) {
	body = strings.TrimSpace(body)
	if body == "" {
		return
	}
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	b.WriteString(title)
	b.WriteString(":\n")
	b.WriteString(body)
}

func writeDescriptionList(b *strings.Builder, title string, values []string) {
	cleaned := cleanSurfaceStrings(values)
	if len(cleaned) == 0 {
		return
	}
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	b.WriteString(title)
	b.WriteString(":")
	for _, value := range cleaned {
		b.WriteString("\n- ")
		b.WriteString(value)
	}
}
