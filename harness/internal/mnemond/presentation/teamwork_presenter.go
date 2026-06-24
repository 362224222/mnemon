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
	var events []eventmodel.EventEnvelope
	appendDerived := func(eventType, subject string, causedBy []string, body string, suggested []string) {
		model := eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            derivedEventID(eventType, subject, principal),
			Type:          eventType,
			Subject:       eventmodel.EventSubject(subject),
			Actor:         "mnemond@local",
			Audience:      principal,
			Payload:       map[string]any{"body": body},
			CausedBy:      append([]string(nil), causedBy...),
			CreatedAt:     derivedAt,
		}
		events = append(events, eventmodel.DerivedEnvelope(model, derivedAt, expiresAt, presentationHintForDerivedEventType(eventType), suggested))
	}

	if profileStaleOrMissing(items["agent_profile"], principal) {
		appendDerived(
			DerivedEventProfileUpdateRequested,
			"agent_profile/project",
			nil,
			"Update your agent_profile if your focus, availability, or context advantages changed.",
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
		appendDerived(
			DerivedEventTeamworkSignalOpen,
			subject,
			[]string{subject},
			fmt.Sprintf("Teamwork signal is open: %s. Decide whether to self-assign or assign a suited teammate.", statement),
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
				fmt.Sprintf("Assignment %s expired without progress: %s. Start a new act: renew, reassign, split, close, or escalate.", id, scope),
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
				fmt.Sprintf("When you have progress or a blocker for assignment %s, emit progress_digest with assignment_ref=%s.", id, id),
				[]string{"progress_digest.write_candidate.observed"},
			)
		}
	}

	return events
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
	body, _ := env.Event.Payload["body"].(string)
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
