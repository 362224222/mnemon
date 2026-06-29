package driver

import (
	"context"
	"strings"
	"testing"
)

func TestManagedTurnTraceEventsFromCodexNotificationsRedactsAndBounds(t *testing.T) {
	longOutput := strings.Repeat("x", managedTurnTraceTextLimit+20)
	events := ManagedTurnTraceEventsFromCodexNotifications("planner@team", []map[string]any{
		{
			"method": "item/completed",
			"params": map[string]any{
				"turnId": "inner-turn",
				"item": map[string]any{
					"type":             "commandExecution",
					"id":               "call-1",
					"command":          "TOKEN=raw-secret multica issue get TEA-1",
					"cwd":              "/tmp/work",
					"status":           "completed",
					"exitCode":         0,
					"aggregatedOutput": "API_KEY=abc123 " + longOutput,
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
	joined := got.Command + "\n" + got.Output + "\n" + strings.TrimSpace(traceTestString(got.Item["authToken"]))
	for _, forbidden := range []string{"raw-secret", "abc123", "must-not-leak"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("trace leaked %q: %+v", forbidden, got)
		}
	}
	if !strings.Contains(got.Command, "TOKEN=[redacted]") || !strings.Contains(got.Output, "API_KEY=[redacted]") {
		t.Fatalf("trace did not redact command/output: command=%q output=%q", got.Command, got.Output[:80])
	}
	if !strings.Contains(got.Output, "[truncated]") {
		t.Fatalf("long output was not bounded: len=%d", len(got.Output))
	}
}

func TestManagedAgentDriverRoutesTraceWithoutChangingWakeQuery(t *testing.T) {
	var gotQuery string
	var traces []ManagedTurnTraceEvent
	driver := &ManagedAgentDriver{
		Principal: "planner@team",
		Client: traceManagedTurnClientFunc(func(ctx context.Context, query string, sink ManagedTurnTraceSink) (ManagedTurnResult, error) {
			gotQuery = query
			sink.OnManagedTurnTrace(ManagedTurnTraceEvent{Kind: "agent_message", Text: "native trace"})
			return ManagedTurnResult{TurnID: "turn-1", Status: "completed", FinalAnswer: "done"}, nil
		}),
		Ledger: NewMemoryManagedWakeLedger(),
		TraceSink: ManagedTurnTraceSinkFunc(func(event ManagedTurnTraceEvent) {
			traces = append(traces, event)
		}),
	}
	record, err := driver.Wake(context.Background(), ManagedWakeCandidate{
		Principal:      "planner@team",
		DerivedEventID: "derived-1",
		BodyDigest:     "sha256:body",
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotQuery != ManagedWakeQuery {
		t.Fatalf("query = %q, want sentinel %q", gotQuery, ManagedWakeQuery)
	}
	if record.Status != "completed" || record.FinalAnswer != "done" {
		t.Fatalf("wake record = %+v", record)
	}
	if len(traces) != 1 || traces[0].Text != "native trace" {
		t.Fatalf("traces = %+v, want native trace", traces)
	}
}

type traceManagedTurnClientFunc func(context.Context, string, ManagedTurnTraceSink) (ManagedTurnResult, error)

func (f traceManagedTurnClientFunc) StartTurn(ctx context.Context, query string) (ManagedTurnResult, error) {
	return f(ctx, query, nil)
}

func (f traceManagedTurnClientFunc) StartTurnWithTrace(ctx context.Context, query string, sink ManagedTurnTraceSink) (ManagedTurnResult, error) {
	return f(ctx, query, sink)
}

func traceTestString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}
