package render

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
)

func BuildBody(req Request, proj projection.Projection, now time.Time) string {
	body, _ := BuildBodyAndAgentEvents(req, proj, now)
	return body
}

func BuildBodyAndAgentEvents(req Request, proj projection.Projection, now time.Time) (string, []AgentEvent) {
	switch req.RenderIntent {
	case IntentTeamworkCue:
		events := BuildAgentEvents(req, proj, now)
		return PresentAgentEvents(events), events
	case IntentProfileCue:
		events := BuildProfileEvents(req, proj)
		return PresentAgentEvents(events), events
	case IntentContextPacket:
		return BuildContextPacket(req, proj), nil
	case IntentPayloadContract:
		return BuildPayloadContract(), nil
	case IntentSkillBootstrap:
		return BuildSkillBootstrap(), nil
	default:
		return "", nil
	}
}

func BuildCue(req Request, proj projection.Projection, now time.Time) string {
	return PresentAgentEvents(BuildAgentEvents(req, proj, now))
}

func BuildProfileCue(req Request, proj projection.Projection) string {
	return PresentAgentEvents(BuildProfileEvents(req, proj))
}

func BuildContextPacket(_ Request, proj projection.Projection) string {
	var lines []string
	lines = append(lines, "[mnemon:context]", fmt.Sprintf("Projection %s digest %s", proj.Ref, proj.Digest))
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
				summary = itemID(item)
			}
			lines = append(lines, fmt.Sprintf("- %s/%s: %s", kind, itemID(item), summary))
		}
	}
	if len(lines) == 2 {
		return ""
	}
	return strings.Join(lines, "\n")
}

func BuildPayloadContract() string {
	return strings.Join([]string{
		"[mnemon:payload-contract]",
		"Emit governed events through mnemon observe; do not write canonical state directly.",
		"- agent_profile.write_candidate.observed requires actor, focus, context_advantages, availability, ttl, summary.",
		"- teamwork_signal.write_candidate.observed requires scope, statement, why_teamwork, ttl.",
		"- assignment.write_candidate.observed requires assignee, scope, expected_work, expected_feedback, ttl.",
		"- progress_digest.write_candidate.observed requires summary; include assignment_ref when reporting assignment feedback.",
	}, "\n")
}

func BuildSkillBootstrap() string {
	return strings.Join([]string{
		"[mnemon:skill-bootstrap]",
		"Use mnemon observe for durable profile, teamwork signal, assignment, and progress_digest events.",
		"Read current work through context.packet or teamwork.cue before emitting a governed event.",
	}, "\n")
}

func section(kind, body string) string {
	return fmt.Sprintf("[mnemon:%s]\n%s", kind, body)
}

func projectionItems(proj projection.Projection) map[string][]map[string]any {
	out := map[string][]map[string]any{}
	for _, c := range proj.Content {
		for _, item := range resourceItems(c) {
			out[string(c.Ref.Kind)] = append(out[string(c.Ref.Kind)], item)
		}
	}
	for k := range out {
		sort.SliceStable(out[k], func(i, j int) bool { return itemID(out[k][i]) < itemID(out[k][j]) })
	}
	return out
}

func resourceItems(content projection.ResourceContent) []map[string]any {
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

func profileStaleOrMissing(profiles []map[string]any, principal string) bool {
	for _, p := range profiles {
		if itemString(p, "actor") != principal {
			continue
		}
		return itemString(p, "freshness") == "stale"
	}
	return true
}

func assignmentExpired(item map[string]any, now time.Time) bool {
	created, err := time.Parse(time.RFC3339, itemString(item, "created_at"))
	if err != nil {
		return false
	}
	ttl, err := time.ParseDuration(itemString(item, "ttl"))
	if err != nil || ttl <= 0 {
		return false
	}
	return now.After(created.Add(ttl))
}

func summarizeProgress(items []map[string]any) string {
	var out []string
	for _, item := range items {
		if s := itemString(item, "summary"); s != "" {
			out = append(out, s)
		}
	}
	return strings.Join(out, "; ")
}

func itemID(item map[string]any) string {
	for _, key := range []string{"assignment_id", "id", "skill_id"} {
		if s := itemString(item, key); s != "" {
			return s
		}
	}
	return "unknown"
}

func itemString(item map[string]any, key string) string {
	if s, ok := item[key].(string); ok {
		return strings.TrimSpace(s)
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

func resourceSummary(fields map[string]any) string {
	for _, key := range []string{"content", "name", "summary"} {
		if s, ok := fields[key].(string); ok && strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func ref(kind, id string) contract.ResourceRef {
	return contract.ResourceRef{Kind: contract.ResourceKind(kind), ID: contract.ResourceID(id)}
}
