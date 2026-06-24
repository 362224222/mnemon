package presentation

import (
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

type contextPacketPresenter struct{}

func (contextPacketPresenter) Intent() string { return IntentContextPacket }

func (contextPacketPresenter) Present(req Request, proj view.View, _ time.Time) (PresentationBody, error) {
	return PresentationBody{Body: BuildContextPacket(req, proj)}, nil
}

func BuildContextPacket(_ Request, proj view.View) string {
	var lines []string
	lines = append(lines, "[mnemon:context]", fmt.Sprintf("View %s digest %s", proj.Ref, proj.Digest))
	for _, content := range proj.Content {
		kind := string(content.Ref.Kind)
		items := resourceItems(content)
		if len(items) == 0 {
			if summary := resourceSummary(content.Fields); summary != "" {
				lines = append(lines, fmt.Sprintf("- %s/%s: %s", kind, content.Ref.ID, summary))
			}
			continue
		}
		for _, item := range items {
			summary := firstNonEmpty(item,
				"content", "name", "summary", "statement", "scope", "expected_work", "focus", "skill_id", "status")
			if summary == "" {
				summary = contextItemID(item)
			}
			lines = append(lines, fmt.Sprintf("- %s/%s: %s", kind, contextItemID(item), summary))
		}
	}
	if len(lines) == 2 {
		return ""
	}
	return strings.Join(lines, "\n")
}

func contextItemID(item map[string]any) string {
	for _, key := range []string{"id", "skill_id", "assignment_id"} {
		if s := itemString(item, key); s != "" {
			return s
		}
	}
	return "unknown"
}

func resourceSummary(fields map[string]any) string {
	for _, key := range []string{"content", "name", "summary"} {
		if s, ok := fields[key].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}
