package presentation

import (
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

func resourceItems(content view.ResourceContent) []map[string]any {
	for _, field := range []string{"items", "entries", "declarations"} {
		if raw, ok := content.Fields[field]; ok {
			return anyItems(raw)
		}
	}
	return nil
}

func anyItems(raw any) []map[string]any {
	var out []map[string]any
	switch v := raw.(type) {
	case []any:
		for _, it := range v {
			if m, ok := it.(map[string]any); ok {
				out = append(out, m)
			}
		}
	case []map[string]any:
		out = append(out, v...)
	}
	return out
}

func itemString(item map[string]any, key string) string {
	if s, ok := item[key].(string); ok {
		return strings.TrimSpace(s)
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if s, ok := m[key].(string); ok {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func firstNonEmpty(item map[string]any, keys ...string) string {
	for _, key := range keys {
		if s := itemString(item, key); s != "" {
			return s
		}
	}
	return ""
}
