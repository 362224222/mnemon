package main

import eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"

func cmdR2Progress(summary string) map[string]any {
	return eventmodel.BuildPayload(map[string]any{"feedback_kind": "progress"}, map[string]any{"summary": summary}, nil)
}

func cmdR2Assignment(scope, ttl, assignee, expectedWork, expectedFeedback string, evidenceRefs ...any) map[string]any {
	return eventmodel.BuildPayload(
		map[string]any{"scope": scope, "ttl": ttl, "assignee": assignee},
		map[string]any{"expected_work": expectedWork, "expected_feedback": expectedFeedback},
		map[string]any{"evidence_refs": evidenceRefs},
	)
}

func cmdItemString(item map[string]any, key string) string {
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
