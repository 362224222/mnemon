package drive

import (
	"strings"
	"testing"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

func TestManagedWakeCandidateForRenderMatchesStableIssueIdentity(t *testing.T) {
	resp := presentation.Response{
		AuditID:    "audit-1",
		BodyDigest: "sha256:render",
		Events: []eventmodel.EventEnvelope{
			renderedWakeEnvelope("derived:first", "assignment/asg-other", "Assignment ASG-OTHER is available."),
			renderedWakeEnvelope("derived:second", "assignment/asg-27", "Please handle TEA-27 for the Mnemon R2 hub-flow completion drill."),
		},
	}
	candidate, ok := ManagedWakeCandidateForRender("planner@team", resp, ManagedWakeMatchMaterial{
		IssueID:    "issue-123",
		Identifier: "TEA-27",
		Title:      "Mnemon R2 hub-flow completion drill",
		TaskID:     "task-123",
	})
	if !ok {
		t.Fatal("expected matching wake candidate")
	}
	if candidate.DerivedEventID != "derived:second" {
		t.Fatalf("selected candidate = %+v, want second event", candidate)
	}
	if candidate.RenderAuditID != resp.AuditID || candidate.RenderBodyDigest != resp.BodyDigest {
		t.Fatalf("candidate should carry render metadata: %+v", candidate)
	}
}

func TestManagedWakeMatchTermsPreferStableIssueIdentity(t *testing.T) {
	got := ManagedWakeMatchTerms(ManagedWakeMatchMaterial{
		IssueID:    "issue-123",
		Identifier: "TEA-27",
		Title:      "Mnemon R2 hub-flow completion drill",
		Statement:  "Run a small hub-flow readiness drill.",
		TaskID:     "task-123",
	})
	joined := strings.Join(got, "\n")
	for _, want := range []string{"issue-123", "TEA-27", "Mnemon R2 hub-flow completion drill", "task-123"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("match terms missing %q: %+v", want, got)
		}
	}
	if strings.Contains(joined, "Run a small hub-flow readiness drill.") {
		t.Fatalf("root issue matching must not prefer reusable statement text: %+v", got)
	}
}

func TestManagedWakeMatchTermsPreferAssignmentMailboxIdentity(t *testing.T) {
	got := ManagedWakeMatchTerms(ManagedWakeMatchMaterial{
		AssignmentID:          "assignment-final-routing-mailbox",
		AssignmentFingerprint: "fp-1",
		IssueID:               "child-issue",
		Identifier:            "TEA-16",
	})
	if strings.Join(got, "\n") != "assignment-final-routing-mailbox\nfp-1" {
		t.Fatalf("assignment terms = %+v", got)
	}
}

func renderedWakeEnvelope(id, subject, body string) eventmodel.EventEnvelope {
	return eventmodel.DerivedEnvelope(eventmodel.Event{
		SchemaVersion: eventmodel.SchemaVersion,
		ID:            id,
		Type:          "assignment.work_available",
		Subject:       eventmodel.EventSubject(subject),
		Actor:         "mnemond@local",
		Audience:      "planner@team",
		Payload:       eventmodel.BuildPayload(nil, map[string]any{"body": body}, nil),
	}, "2026-06-29T10:00:00Z", "2026-06-29T10:05:00Z", "work", nil)
}
