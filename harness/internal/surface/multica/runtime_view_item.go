package multica

import (
	"encoding/json"
	"strconv"
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

type RuntimeAssignmentItem struct {
	ID               string
	EventID          string
	IngestSeq        int64
	SessionID        string
	RootIssueID      string
	Actor            string
	Assignee         string
	Scope            string
	TTL              string
	SignalRef        string
	ExpectedWork     string
	ExpectedFeedback string
	Rationale        string
	ContextRefs      []string
	EvidenceRefs     []string
}

type RuntimeProgressItem struct {
	ID            string
	EventID       string
	IngestSeq     int64
	SessionID     string
	RootIssueID   string
	Actor         string
	AssignmentRef string
	Scope         string
	FeedbackKind  string
	Summary       string
	Result        string
	Blocker       string
	ContextRefs   []string
	ArtifactRefs  []string
	EvidenceRefs  []string
}

func RuntimeAssignmentViewItem(item map[string]any) RuntimeAssignmentItem {
	id := runtimeViewItemFirstString(item, "assignment_id", "id", "declaration_id")
	if id == "" {
		id = runtimeViewItemString(item, "event_id")
	}
	return RuntimeAssignmentItem{
		ID:               id,
		EventID:          runtimeViewItemFirstString(item, "event_id", "id", "declaration_id", "assignment_id"),
		IngestSeq:        runtimeViewItemInt64(item, "ingest_seq"),
		SessionID:        runtimeViewItemString(item, "session_id"),
		RootIssueID:      runtimeViewItemString(item, "root_issue_id"),
		Actor:            runtimeViewItemString(item, "actor"),
		Assignee:         runtimeViewItemString(item, "assignee"),
		Scope:            runtimeViewItemString(item, "scope"),
		TTL:              runtimeViewItemString(item, "ttl"),
		SignalRef:        runtimeViewItemString(item, "signal_ref"),
		ExpectedWork:     runtimeViewItemString(item, "expected_work"),
		ExpectedFeedback: runtimeViewItemString(item, "expected_feedback"),
		Rationale:        runtimeViewItemString(item, "rationale"),
		ContextRefs:      runtimeViewItemStringList(item, "context_refs"),
		EvidenceRefs:     runtimeViewItemStringList(item, "evidence_refs"),
	}
}

func RuntimeProgressViewItem(item map[string]any) RuntimeProgressItem {
	id := runtimeViewItemFirstString(item, "id", "declaration_id", "event_id")
	return RuntimeProgressItem{
		ID:            id,
		EventID:       runtimeViewItemFirstString(item, "event_id", "id", "declaration_id"),
		IngestSeq:     runtimeViewItemInt64(item, "ingest_seq"),
		SessionID:     runtimeViewItemString(item, "session_id"),
		RootIssueID:   runtimeViewItemString(item, "root_issue_id"),
		Actor:         runtimeViewItemString(item, "actor"),
		AssignmentRef: runtimeViewItemString(item, "assignment_ref"),
		Scope:         runtimeViewItemString(item, "scope"),
		FeedbackKind:  runtimeViewItemString(item, "feedback_kind"),
		Summary:       runtimeViewItemString(item, "summary"),
		Result:        runtimeViewItemString(item, "result"),
		Blocker:       runtimeViewItemString(item, "blocker"),
		ContextRefs:   runtimeViewItemStringList(item, "context_refs"),
		ArtifactRefs:  runtimeViewItemStringList(item, "artifact_refs"),
		EvidenceRefs:  runtimeViewItemStringList(item, "evidence_refs"),
	}
}

func RuntimeAssignmentMatchesScope(item RuntimeAssignmentItem, scope RuntimeScopeMaterial) bool {
	if !RuntimeExplicitScopeMatches(item.SessionID, item.RootIssueID, scope) {
		return false
	}
	refs := append([]string{}, item.ContextRefs...)
	refs = append(refs, item.EvidenceRefs...)
	return RuntimeRefsMatchScope(refs, scope)
}

func RuntimeProgressMatchesScope(item RuntimeProgressItem, scope RuntimeScopeMaterial, extraIssueIDs ...string) bool {
	if !RuntimeExplicitScopeMatches(item.SessionID, item.RootIssueID, scope) {
		return false
	}
	refs := append([]string{}, item.ContextRefs...)
	refs = append(refs, item.EvidenceRefs...)
	refs = append(refs, item.ArtifactRefs...)
	return RuntimeRefsMatchScope(refs, scope, extraIssueIDs...)
}

func RuntimeItemAfterRootIngest(ingestSeq, rootIngestSeq int64) bool {
	if rootIngestSeq <= 0 || ingestSeq <= 0 {
		return true
	}
	return ingestSeq > rootIngestSeq
}

func runtimeViewItemFirstString(item map[string]any, keys ...string) string {
	for _, key := range keys {
		if value := runtimeViewItemString(item, key); value != "" {
			return value
		}
	}
	return ""
}

func runtimeViewItemString(item map[string]any, key string) string {
	if value, ok := item[key].(string); ok {
		return strings.TrimSpace(value)
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if value, ok := m[key].(string); ok {
				return strings.TrimSpace(value)
			}
		}
	}
	return ""
}

func runtimeViewItemInt64(item map[string]any, key string) int64 {
	if value, ok := runtimeViewInt64(item[key]); ok {
		return value
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if value, ok := runtimeViewInt64(m[key]); ok {
				return value
			}
		}
	}
	return 0
}

func runtimeViewInt64(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case float64:
		return int64(v), true
	case float32:
		return int64(v), true
	case json.Number:
		n, err := v.Int64()
		return n, err == nil
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func runtimeViewItemStringList(item map[string]any, key string) []string {
	if out := runtimeViewStringList(item[key]); len(out) > 0 {
		return out
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if out := runtimeViewStringList(m[key]); len(out) > 0 {
				return out
			}
		}
	}
	return nil
}

func runtimeViewStringList(raw any) []string {
	seen := map[string]bool{}
	var out []string
	add := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return
		}
		seen[value] = true
		out = append(out, value)
	}
	switch v := raw.(type) {
	case []string:
		for _, item := range v {
			add(item)
		}
	case []any:
		for _, item := range v {
			if value, ok := item.(string); ok {
				add(value)
			}
		}
	case string:
		add(v)
	}
	return out
}
