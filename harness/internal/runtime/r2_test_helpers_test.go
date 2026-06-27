package runtime

import eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"

func runtimeR2Progress(summary string) map[string]any {
	return eventmodel.BuildPayload(map[string]any{"feedback_kind": "progress"}, map[string]any{"summary": summary}, nil)
}

func runtimeR2ProgressWithContext(summary string, changedContext ...any) map[string]any {
	return eventmodel.BuildPayload(
		map[string]any{"feedback_kind": "progress"},
		map[string]any{"summary": summary, "changed_context": changedContext},
		nil,
	)
}

func runtimeItemString(item map[string]any, key string) string {
	if s, ok := item[key].(string); ok {
		return s
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if s, ok := m[key].(string); ok {
				return s
			}
		}
	}
	return ""
}
