package activationtrace

import (
	"strings"
	"testing"
)

func TestEventsFromCodexNotificationsRedactsAndBounds(t *testing.T) {
	longOutput := strings.Repeat("x", TextLimit+20)
	events := EventsFromCodexNotifications("planner@team", []map[string]any{
		{
			"method": "item/completed",
			"params": map[string]any{
				"turnId": "inner-turn",
				"item": map[string]any{
					"type":             "commandExecution",
					"id":               "call-1",
					"command":          "TOKEN=raw-secret curl -H 'Authorization: Bearer ghp_secret' https://api.github.com",
					"cwd":              "/tmp/work",
					"status":           "completed",
					"exitCode":         0,
					"aggregatedOutput": "API_KEY=abc123 authorization: bearer mat_secret " + longOutput,
					"authToken":        "must-not-leak",
				},
			},
		},
	})
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one", events)
	}
	got := events[0]
	if got.Kind != "command_execution" || got.ItemType != "commandExecution" || got.Principal != "planner@team" {
		t.Fatalf("unexpected trace event: %+v", got)
	}
	joined := got.Command + "\n" + got.Output + "\n" + strings.TrimSpace(testString(got.Item["authToken"]))
	for _, forbidden := range []string{"raw-secret", "abc123", "must-not-leak", "ghp_secret", "mat_secret"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("trace leaked %q: %+v", forbidden, got)
		}
	}
	if !strings.Contains(got.Command, "TOKEN=[redacted]") || !strings.Contains(got.Command, "Bearer [redacted]") ||
		!strings.Contains(got.Output, "API_KEY=[redacted]") || !strings.Contains(got.Output, "bearer [redacted]") {
		t.Fatalf("trace did not redact command/output: command=%q output=%q", got.Command, got.Output[:80])
	}
	if !strings.Contains(got.Output, "[truncated]") {
		t.Fatalf("long output was not bounded: len=%d", len(got.Output))
	}
}

func TestEventsFromCodexNotificationsDoesNotTreatTraceAsCanonicalEvent(t *testing.T) {
	events := EventsFromCodexNotifications("planner@team", []map[string]any{
		{
			"method": "turn/completed",
			"params": map[string]any{"turnId": "turn-1"},
		},
	})
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one", events)
	}
	got := events[0]
	if got.Kind != "turn_completed" || got.Status != "completed" {
		t.Fatalf("unexpected trace event: %+v", got)
	}
	if got.Item != nil {
		t.Fatalf("turn trace should not carry canonical event payload: %+v", got.Item)
	}
}

func testString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
