package presentation

import (
	"fmt"
	"sort"
	"strings"
	"time"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

const (
	DerivedEventProfileUpdateRequested   = "profile.update_requested"
	DerivedEventTeamworkSignalOpen       = "teamwork.signal_open"
	DerivedEventAssignmentExpired        = "assignment.expired"
	DerivedEventAssignmentProgressReady  = "assignment.progress_ready"
	DerivedEventAssignmentWorkAvailable  = "assignment.work_available"
	DerivedEventAssignmentFeedbackNeeded = "assignment.feedback_needed"
)

type teamworkEventsPresenter struct{}

func (teamworkEventsPresenter) Intent() string { return IntentTeamworkEvents }

func (teamworkEventsPresenter) Present(req Request, proj view.View, now time.Time) (PresentationBody, error) {
	events := DeriveEventEnvelopes(req, proj, now)
	return PresentationBody{Body: PresentEventEnvelopes(events), Events: events}, nil
}

type profileEventsPresenter struct{}

func (profileEventsPresenter) Intent() string { return IntentProfileEvents }

func (profileEventsPresenter) Present(req Request, proj view.View, _ time.Time) (PresentationBody, error) {
	events := DeriveProfileEventEnvelopes(req, proj)
	return PresentationBody{Body: PresentEventEnvelopes(events), Events: events}, nil
}

func DeriveEventEnvelopes(req Request, proj view.View, now time.Time) []eventmodel.EventEnvelope {
	principal := string(req.Principal)
	if principal == "" {
		return nil
	}
	derivedAt, expiresAt := derivedTimes(now)
	items := teamworkItems(proj)
	items = scopeTeamworkItemsForRequest(req, items)
	var events []eventmodel.EventEnvelope
	appendDerived := func(eventType, subject string, causedBy []string, body string, suggested []string) {
		model := eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            derivedEventID(eventType, subject, principal),
			Type:          eventType,
			Subject:       eventmodel.EventSubject(subject),
			Actor:         "mnemond@local",
			Audience:      principal,
			Payload:       eventmodel.BuildPayload(nil, map[string]any{"body": body}, nil),
			CausedBy:      append([]string(nil), causedBy...),
			CreatedAt:     derivedAt,
		}
		events = append(events, eventmodel.DerivedEnvelope(model, derivedAt, expiresAt, presentationHintForDerivedEventType(eventType), suggested))
	}

	if shouldRenderProfileCue(req, items, principal, now) {
		appendDerived(
			DerivedEventProfileUpdateRequested,
			"agent_profile/project",
			nil,
			"Your agent_profile is missing or stale. If your focus, availability, context advantages, or active scopes changed, you may record an agent_profile update.",
			[]string{"agent_profile.write_candidate.observed"},
		)
	}

	for _, signal := range items["teamwork_signal"] {
		statement := itemString(signal, "statement")
		if statement == "" {
			continue
		}
		id := teamworkItemID(signal)
		subject := "teamwork_signal/" + id
		body := fmt.Sprintf("Teamwork signal is open: %s. Assignment or self-assignment may be useful when you choose to act.", statement)
		if multicaHubRender(req) {
			body += " For Multica-hosted teamwork, create handoffs as Mnemon assignment events; Multica issues are OA and activation surfaces, not canonical assignment mailboxes."
		}
		if refs := signalContextRefs(signal); len(refs) > 0 {
			body = fmt.Sprintf("%s Context refs: %s.", body, strings.Join(refs, ", "))
		}
		appendDerived(
			DerivedEventTeamworkSignalOpen,
			subject,
			[]string{subject},
			body,
			[]string{"assignment.write_candidate.observed"},
		)
	}

	progressByAssignment := map[string][]map[string]any{}
	for _, progress := range items["progress_digest"] {
		if ref := itemString(progress, "assignment_ref"); ref != "" {
			progressByAssignment[ref] = append(progressByAssignment[ref], progress)
		}
	}

	for _, assignment := range items["assignment"] {
		id := teamworkItemID(assignment)
		assignee := itemString(assignment, "assignee")
		owner := itemString(assignment, "actor")
		scope := itemString(assignment, "scope")
		linked := progressByAssignment[id]
		expired := assignmentExpired(assignment, now) && len(linked) == 0
		subject := "assignment/" + id

		switch {
		case owner == principal && expired:
			appendDerived(
				DerivedEventAssignmentExpired,
				subject,
				[]string{subject},
				fmt.Sprintf("Assignment %s expired without progress: %s. Available follow-up options include renew, reassign, split, close, or escalate.", id, scope),
				[]string{"assignment.write_candidate.observed", "teamwork_signal.write_candidate.observed"},
			)
		case owner == principal && len(linked) > 0:
			appendDerived(
				DerivedEventAssignmentProgressReady,
				subject,
				append([]string{subject}, progressRefs(linked)...),
				fmt.Sprintf("Assignment %s has feedback: %s", id, summarizeProgress(linked)),
				[]string{"assignment.write_candidate.observed", "teamwork_signal.write_candidate.observed"},
			)
		case assignee == principal && !expired && len(linked) == 0:
			appendDerived(
				DerivedEventAssignmentWorkAvailable,
				subject,
				[]string{subject},
				fmt.Sprintf("Assignment %s is yours: %s. Expected work: %s", id, scope, itemString(assignment, "expected_work")),
				nil,
			)
			appendDerived(
				DerivedEventAssignmentFeedbackNeeded,
				subject,
				[]string{subject},
				fmt.Sprintf("Progress or blocker feedback for assignment %s can be recorded as progress_digest with assignment_ref=%s when useful.", id, id),
				[]string{"progress_digest.write_candidate.observed"},
			)
		}
	}

	return events
}

func multicaHubRender(req Request) bool {
	return strings.EqualFold(strings.TrimSpace(req.Host), "multica")
}

func scopeTeamworkItemsForRequest(req Request, items map[string][]map[string]any) map[string][]map[string]any {
	if !multicaHubRender(req) {
		return items
	}
	sessionID := strings.TrimSpace(req.SessionID)
	currentID := strings.TrimSpace(req.InputDigest)
	if sessionID == "" && currentID == "" {
		return items
	}
	out := map[string][]map[string]any{}
	for kind, list := range items {
		switch kind {
		case "agent_profile":
			out[kind] = append(out[kind], list...)
			continue
		}
		for _, item := range list {
			if multicaTeamworkItemInScope(kind, item, sessionID, currentID) {
				out[kind] = append(out[kind], item)
			}
		}
	}
	return out
}

func multicaTeamworkItemInScope(kind string, item map[string]any, sessionID, currentID string) bool {
	for _, key := range []string{"session_id", "root_issue_id", "source_issue_id", "assignment_id", "assignment_ref", "id", "declaration_id"} {
		value := itemString(item, key)
		if value != "" && (value == sessionID || value == currentID) {
			return true
		}
	}
	if kind == "assignment" && teamworkItemID(item) == currentID {
		return true
	}
	for _, key := range []string{"context_refs", "evidence_refs", "artifact_refs"} {
		for _, ref := range itemStringList(item, key) {
			if multicaRefMatchesScope(ref, sessionID, currentID) {
				return true
			}
		}
	}
	return false
}

func multicaRefMatchesScope(ref, sessionID, currentID string) bool {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return false
	}
	for _, want := range []string{sessionID, currentID} {
		if want != "" && strings.Contains(ref, want) {
			return true
		}
	}
	return false
}

func DeriveProfileEventEnvelopes(req Request, proj view.View) []eventmodel.EventEnvelope {
	events := DeriveEventEnvelopes(req, proj, time.Time{})
	var profile []eventmodel.EventEnvelope
	for _, event := range events {
		if event.Event.Type == DerivedEventProfileUpdateRequested {
			profile = append(profile, event)
		}
	}
	return profile
}

func PresentEventEnvelopes(events []eventmodel.EventEnvelope) string {
	var sections []string
	for _, event := range events {
		label := derivedPresentationHint(event)
		body := derivedBody(event)
		if label == "" || strings.TrimSpace(body) == "" {
			continue
		}
		sections = append(sections, section(label, body))
	}
	return strings.Join(sections, "\n\n")
}

func eventEnvelopeCounts(events []eventmodel.EventEnvelope) map[string]int {
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Event.Type]++
	}
	return counts
}

func teamworkItems(proj view.View) map[string][]map[string]any {
	out := map[string][]map[string]any{}
	for _, c := range proj.Content {
		for _, item := range resourceItems(c) {
			out[string(c.Ref.Kind)] = append(out[string(c.Ref.Kind)], item)
		}
	}
	for k := range out {
		sort.SliceStable(out[k], func(i, j int) bool { return teamworkItemID(out[k][i]) < teamworkItemID(out[k][j]) })
	}
	return out
}

func shouldRenderProfileCue(req Request, items map[string][]map[string]any, principal string, now time.Time) bool {
	if !profileStaleOrMissing(items["agent_profile"], principal, now) {
		return false
	}
	switch req.RenderIntent {
	case IntentProfileEvents:
		return true
	case IntentTeamworkEvents:
		switch strings.TrimSpace(req.Lifecycle) {
		case "prime", "compact":
			return true
		case "remind":
			return profileRelevantForCollaboration(items, principal, now)
		case "nudge":
			return profileRelevantForRecentChange(items, principal)
		default:
			return false
		}
	default:
		return false
	}
}

func profileStaleOrMissing(profiles []map[string]any, principal string, now time.Time) bool {
	for _, p := range profiles {
		if itemString(p, "actor") != principal {
			continue
		}
		return itemString(p, "freshness") == "stale" || profileTTLExpired(p, now)
	}
	return true
}

func profileTTLExpired(profile map[string]any, now time.Time) bool {
	if now.IsZero() {
		return false
	}
	created, err := time.Parse(time.RFC3339, itemString(profile, "created_at"))
	if err != nil {
		return false
	}
	ttl, err := time.ParseDuration(itemString(profile, "ttl"))
	if err != nil || ttl <= 0 {
		return false
	}
	return now.After(created.Add(ttl))
}

func profileRelevantForCollaboration(items map[string][]map[string]any, principal string, now time.Time) bool {
	for _, signal := range items["teamwork_signal"] {
		if itemString(signal, "statement") != "" {
			return true
		}
	}
	for _, assignment := range items["assignment"] {
		if itemString(assignment, "actor") != principal {
			continue
		}
		if assignmentExpired(assignment, now) {
			return true
		}
	}
	return false
}

func profileRelevantForRecentChange(items map[string][]map[string]any, principal string) bool {
	for _, progress := range items["progress_digest"] {
		if itemString(progress, "actor") != principal {
			continue
		}
		if itemString(progress, "blocker") != "" || len(itemStringList(progress, "changed_context")) > 0 {
			return true
		}
	}
	return false
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

func progressRefs(items []map[string]any) []string {
	var refs []string
	for _, item := range items {
		id := teamworkItemID(item)
		if id != "" && id != "unknown" {
			refs = append(refs, "progress_digest/"+id)
		}
	}
	return refs
}

func signalContextRefs(signal map[string]any) []string {
	refs := append([]string{}, itemStringList(signal, "context_refs")...)
	refs = append(refs, itemStringList(signal, "evidence_refs")...)
	seen := map[string]bool{}
	var out []string
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" || seen[ref] {
			continue
		}
		seen[ref] = true
		out = append(out, ref)
	}
	return out
}

func presentationHintForDerivedEventType(eventType string) string {
	switch eventType {
	case DerivedEventProfileUpdateRequested:
		return "profile"
	case DerivedEventTeamworkSignalOpen:
		return "signal"
	case DerivedEventAssignmentExpired:
		return "expired"
	case DerivedEventAssignmentProgressReady:
		return "integrate"
	case DerivedEventAssignmentWorkAvailable:
		return "work"
	case DerivedEventAssignmentFeedbackNeeded:
		return "feedback"
	default:
		return ""
	}
}

func derivedEventID(eventType, subject, audience string) string {
	id := eventType + ":" + subject + ":" + audience
	id = strings.ReplaceAll(id, " ", "_")
	if id == "::" {
		return "derived:event"
	}
	return "derived:" + id
}

func derivedTimes(now time.Time) (string, string) {
	if now.IsZero() {
		now = time.Unix(0, 0).UTC()
	}
	derivedAt := now.UTC().Format(time.RFC3339)
	return derivedAt, now.UTC().Add(5 * time.Minute).Format(time.RFC3339)
}

func derivedBody(env eventmodel.EventEnvelope) string {
	body, _ := eventmodel.PayloadNarrative(env.Event.Payload)["body"].(string)
	return body
}

func derivedPresentationHint(env eventmodel.EventEnvelope) string {
	hint, _ := env.Meta["presentation_hint"].(string)
	return hint
}

func teamworkItemID(item map[string]any) string {
	for _, key := range []string{"assignment_id", "id", "declaration_id"} {
		if s := itemString(item, key); s != "" {
			return s
		}
	}
	return "unknown"
}
