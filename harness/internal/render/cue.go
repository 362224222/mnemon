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
	switch req.RenderIntent {
	case IntentTeamworkCue:
		return BuildCue(req, proj, now)
	case IntentProfileCue:
		return BuildProfileCue(req, proj)
	case IntentContextPacket:
		return BuildContextPacket(req, proj)
	case IntentPayloadContract:
		return BuildPayloadContract()
	case IntentSkillBootstrap:
		return BuildSkillBootstrap()
	default:
		return ""
	}
}

func BuildCue(req Request, proj projection.Projection, now time.Time) string {
	principal := string(req.Principal)
	items := projectionItems(proj)
	var sections []string

	if profileStaleOrMissing(items["agent_profile"], principal) {
		sections = append(sections, section("profile", "Update your agent_profile if your focus, availability, or context advantages changed."))
	}

	for _, signal := range items["teamwork_signal"] {
		statement := itemString(signal, "statement")
		if statement == "" {
			continue
		}
		sections = append(sections, section("signal", fmt.Sprintf("Teamwork signal is open: %s. Decide whether to self-assign or assign a suited teammate.", statement)))
	}

	progressByAssignment := map[string][]map[string]any{}
	for _, progress := range items["progress_digest"] {
		if ref := itemString(progress, "assignment_ref"); ref != "" {
			progressByAssignment[ref] = append(progressByAssignment[ref], progress)
		}
	}

	for _, assignment := range items["assignment"] {
		id := itemID(assignment)
		assignee := itemString(assignment, "assignee")
		owner := itemString(assignment, "actor")
		scope := itemString(assignment, "scope")
		linked := progressByAssignment[id]
		expired := assignmentExpired(assignment, now) && len(linked) == 0

		switch {
		case owner == principal && expired:
			sections = append(sections, section("expired", fmt.Sprintf("Assignment %s expired without progress: %s. Start a new act: renew, reassign, split, close, or escalate.", id, scope)))
		case owner == principal && len(linked) > 0:
			sections = append(sections, section("integrate", fmt.Sprintf("Assignment %s has feedback: %s", id, summarizeProgress(linked))))
		case assignee == principal && !expired && len(linked) == 0:
			sections = append(sections, section("work", fmt.Sprintf("Assignment %s is yours: %s. Expected work: %s", id, scope, itemString(assignment, "expected_work"))))
			sections = append(sections, section("feedback", fmt.Sprintf("When you have progress or a blocker for assignment %s, emit progress_digest with assignment_ref=%s.", id, id)))
		}
	}

	if len(sections) == 0 {
		return ""
	}
	return strings.Join(sections, "\n\n")
}

func BuildProfileCue(req Request, proj projection.Projection) string {
	principal := string(req.Principal)
	items := projectionItems(proj)
	if !profileStaleOrMissing(items["agent_profile"], principal) {
		return ""
	}
	return section("profile", "Update your agent_profile if your focus, availability, or context advantages changed.")
}

func BuildContextPacket(_ Request, proj projection.Projection) string {
	items := projectionItems(proj)
	var lines []string
	lines = append(lines, "[mnemon:context]", fmt.Sprintf("Projection %s digest %s", proj.Ref, proj.Digest))
	for _, kind := range []string{"agent_profile", "teamwork_signal", "assignment", "progress_digest"} {
		for _, item := range items[kind] {
			summary := firstNonEmpty(item,
				"summary", "statement", "scope", "expected_work", "focus")
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
		raw, ok := c.Fields["items"]
		if !ok {
			continue
		}
		for _, item := range anyItems(raw) {
			out[string(c.Ref.Kind)] = append(out[string(c.Ref.Kind)], item)
		}
	}
	for k := range out {
		sort.SliceStable(out[k], func(i, j int) bool { return itemID(out[k][i]) < itemID(out[k][j]) })
	}
	return out
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
	for _, key := range []string{"assignment_id", "id"} {
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

func ref(kind, id string) contract.ResourceRef {
	return contract.ResourceRef{Kind: contract.ResourceKind(kind), ID: contract.ResourceID(id)}
}
