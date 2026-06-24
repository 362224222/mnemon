package render

import (
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
)

const (
	AgentEventProfileUpdateRequested   = "profile.update_requested"
	AgentEventTeamworkSignalOpen       = "teamwork.signal_open"
	AgentEventAssignmentExpired        = "assignment.expired"
	AgentEventAssignmentProgressReady  = "assignment.progress_ready"
	AgentEventAssignmentWorkAvailable  = "assignment.work_available"
	AgentEventAssignmentFeedbackNeeded = "assignment.feedback_needed"
)

// AgentEvent is the event-shaped read unit mnemond gives to a hostagent.
//
// It keeps the public mental model narrow: hostagents consume events and emit
// events. Text labels such as [mnemon:work] are presentation produced from this
// model, not durable protocol concepts.
type AgentEvent struct {
	Type                    string           `json:"type"`
	Audience                contract.ActorID `json:"audience"`
	Subject                 string           `json:"subject,omitempty"`
	CausedBy                []string         `json:"caused_by,omitempty"`
	Body                    string           `json:"body"`
	SuggestedObservedEvents []string         `json:"suggested_observed_events,omitempty"`
}

func BuildAgentEvents(req Request, proj projection.Projection, now time.Time) []AgentEvent {
	principal := string(req.Principal)
	if principal == "" {
		return nil
	}
	items := projectionItems(proj)
	var events []AgentEvent

	if profileStaleOrMissing(items["agent_profile"], principal) {
		events = append(events, AgentEvent{
			Type:                    AgentEventProfileUpdateRequested,
			Audience:                req.Principal,
			Subject:                 "agent_profile/project",
			Body:                    "Update your agent_profile if your focus, availability, or context advantages changed.",
			SuggestedObservedEvents: []string{"agent_profile.write_candidate.observed"},
		})
	}

	for _, signal := range items["teamwork_signal"] {
		statement := itemString(signal, "statement")
		if statement == "" {
			continue
		}
		id := itemID(signal)
		events = append(events, AgentEvent{
			Type:                    AgentEventTeamworkSignalOpen,
			Audience:                req.Principal,
			Subject:                 "teamwork_signal/" + id,
			CausedBy:                []string{"teamwork_signal/" + id},
			Body:                    fmt.Sprintf("Teamwork signal is open: %s. Decide whether to self-assign or assign a suited teammate.", statement),
			SuggestedObservedEvents: []string{"assignment.write_candidate.observed"},
		})
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
		subject := "assignment/" + id

		switch {
		case owner == principal && expired:
			events = append(events, AgentEvent{
				Type:                    AgentEventAssignmentExpired,
				Audience:                req.Principal,
				Subject:                 subject,
				CausedBy:                []string{subject},
				Body:                    fmt.Sprintf("Assignment %s expired without progress: %s. Start a new act: renew, reassign, split, close, or escalate.", id, scope),
				SuggestedObservedEvents: []string{"assignment.write_candidate.observed", "teamwork_signal.write_candidate.observed"},
			})
		case owner == principal && len(linked) > 0:
			events = append(events, AgentEvent{
				Type:                    AgentEventAssignmentProgressReady,
				Audience:                req.Principal,
				Subject:                 subject,
				CausedBy:                append([]string{subject}, progressRefs(linked)...),
				Body:                    fmt.Sprintf("Assignment %s has feedback: %s", id, summarizeProgress(linked)),
				SuggestedObservedEvents: []string{"assignment.write_candidate.observed", "teamwork_signal.write_candidate.observed"},
			})
		case assignee == principal && !expired && len(linked) == 0:
			events = append(events, AgentEvent{
				Type:     AgentEventAssignmentWorkAvailable,
				Audience: req.Principal,
				Subject:  subject,
				CausedBy: []string{subject},
				Body:     fmt.Sprintf("Assignment %s is yours: %s. Expected work: %s", id, scope, itemString(assignment, "expected_work")),
			})
			events = append(events, AgentEvent{
				Type:                    AgentEventAssignmentFeedbackNeeded,
				Audience:                req.Principal,
				Subject:                 subject,
				CausedBy:                []string{subject},
				Body:                    fmt.Sprintf("When you have progress or a blocker for assignment %s, emit progress_digest with assignment_ref=%s.", id, id),
				SuggestedObservedEvents: []string{"progress_digest.write_candidate.observed"},
			})
		}
	}

	return events
}

func BuildProfileEvents(req Request, proj projection.Projection) []AgentEvent {
	events := BuildAgentEvents(req, proj, time.Time{})
	var profile []AgentEvent
	for _, event := range events {
		if event.Type == AgentEventProfileUpdateRequested {
			profile = append(profile, event)
		}
	}
	return profile
}

func PresentAgentEvents(events []AgentEvent) string {
	var sections []string
	for _, event := range events {
		label := agentEventLabel(event.Type)
		if label == "" || strings.TrimSpace(event.Body) == "" {
			continue
		}
		sections = append(sections, section(label, event.Body))
	}
	return strings.Join(sections, "\n\n")
}

func agentEventCounts(events []AgentEvent) map[string]int {
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	return counts
}

func progressRefs(items []map[string]any) []string {
	var refs []string
	for _, item := range items {
		id := itemID(item)
		if id != "" && id != "unknown" {
			refs = append(refs, "progress_digest/"+id)
		}
	}
	return refs
}

func agentEventLabel(eventType string) string {
	switch eventType {
	case AgentEventProfileUpdateRequested:
		return "profile"
	case AgentEventTeamworkSignalOpen:
		return "signal"
	case AgentEventAssignmentExpired:
		return "expired"
	case AgentEventAssignmentProgressReady:
		return "integrate"
	case AgentEventAssignmentWorkAvailable:
		return "work"
	case AgentEventAssignmentFeedbackNeeded:
		return "feedback"
	default:
		return ""
	}
}
