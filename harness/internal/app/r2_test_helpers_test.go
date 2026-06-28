package app

import eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"

func r2AssignmentPayload(rule map[string]any, narrative map[string]any, refs map[string]any) map[string]any {
	return eventmodel.BuildPayload(rule, narrative, refs)
}

func r2Assignment(scope, ttl, assignee, expectedWork, expectedFeedback string, evidenceRefs ...any) map[string]any {
	return r2AssignmentPayload(
		map[string]any{"scope": scope, "ttl": ttl, "assignee": assignee},
		map[string]any{"expected_work": expectedWork, "expected_feedback": expectedFeedback},
		map[string]any{"evidence_refs": evidenceRefs},
	)
}

func r2Progress(summary string) map[string]any {
	return eventmodel.BuildPayload(map[string]any{"feedback_kind": "progress"}, map[string]any{"summary": summary}, nil)
}

func r2ProgressFor(assignmentRef, scope, summary string, evidenceRefs ...any) map[string]any {
	return eventmodel.BuildPayload(
		map[string]any{"assignment_ref": assignmentRef, "scope": scope, "feedback_kind": "progress"},
		map[string]any{"summary": summary},
		map[string]any{"evidence_refs": evidenceRefs},
	)
}

func r2ProjectIntent(statement string, evidenceRefs ...any) map[string]any {
	return eventmodel.BuildPayload(nil, map[string]any{"statement": statement}, map[string]any{"evidence_refs": evidenceRefs})
}

func r2AgentProfile(actor, focus, availability, ttl, summary string, contextAdvantages ...any) map[string]any {
	return eventmodel.BuildPayload(
		map[string]any{"actor": actor, "availability": availability, "ttl": ttl},
		map[string]any{"focus": focus, "context_advantages": contextAdvantages, "summary": summary},
		nil,
	)
}

func r2TeamworkSignal(scope, statement, whyTeamwork, ttl string, evidenceRefs ...any) map[string]any {
	return eventmodel.BuildPayload(
		map[string]any{"scope": scope, "ttl": ttl},
		map[string]any{"statement": statement, "why_teamwork": whyTeamwork},
		map[string]any{"evidence_refs": evidenceRefs},
	)
}

func r2Text(text string) map[string]any {
	return eventmodel.BuildPayload(nil, map[string]any{"text": text}, nil)
}

func r2NarrativeField(key, value string) map[string]any {
	return eventmodel.BuildPayload(nil, map[string]any{key: value}, nil)
}
