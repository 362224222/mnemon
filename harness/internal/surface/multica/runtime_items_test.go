package multica

import (
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/activationtrace"
)

func TestRuntimeManagedTraceMessagesRemapItemIDs(t *testing.T) {
	now := time.Date(2026, 6, 29, 7, 30, 0, 0, time.UTC)
	started := activationtrace.Event{
		SourceRuntime: activationtrace.SourceCodexAppServer,
		Principal:     "planner@team",
		TurnID:        "inner-turn",
		ItemID:        "inner-msg",
		Method:        "item/started",
		Item: map[string]any{
			"type":  "agentMessage",
			"id":    "inner-msg",
			"text":  "",
			"phase": "commentary",
			"nested": map[string]any{
				"id": "nested-original",
			},
		},
	}
	delta := started
	delta.Method = "item/agentMessage/delta"
	delta.Text = "native progress"
	delta.Item = nil
	completed := started
	completed.Method = "item/completed"
	completed.Item = map[string]any{
		"type":  "agentMessage",
		"id":    "inner-msg",
		"text":  "native progress",
		"phase": "commentary",
	}

	var messages []RuntimeRPCMessage
	for _, event := range []activationtrace.Event{started, delta, completed} {
		messages = append(messages, RuntimeManagedTraceMessages("outer-thread", "outer-turn", event, now)...)
	}
	if len(messages) != 3 {
		t.Fatalf("messages = %+v, want 3", messages)
	}

	var itemID string
	for i, message := range messages {
		if message.Params["threadId"] != "outer-thread" || message.Params["turnId"] != "outer-turn" {
			t.Fatalf("message %d attached to wrong turn: %+v", i, message)
		}
		switch message.Method {
		case "item/started", "item/completed":
			item, _ := message.Params["item"].(map[string]any)
			if item["id"] == "inner-msg" {
				t.Fatalf("message %d leaked managed runtime item id: %+v", i, message)
			}
			if item["mnemonManagedTurnId"] != "inner-turn" || item["mnemonPrincipal"] != "planner@team" || item["mnemonSourceRuntime"] != activationtrace.SourceCodexAppServer {
				t.Fatalf("message %d missing managed trace metadata: %+v", i, item)
			}
			if itemID == "" {
				itemID, _ = item["id"].(string)
			} else if item["id"] != itemID {
				t.Fatalf("managed trace item id changed: first=%q got=%q", itemID, item["id"])
			}
		case "item/agentMessage/delta":
			if message.Params["itemId"] != itemID {
				t.Fatalf("delta item id = %q, want %q", message.Params["itemId"], itemID)
			}
			if message.Params["delta"] != "native progress" {
				t.Fatalf("delta params = %+v", message.Params)
			}
		default:
			t.Fatalf("unexpected method %q", message.Method)
		}
	}
	if started.Item["id"] != "inner-msg" {
		t.Fatalf("input item mutated: %+v", started.Item)
	}
}

func TestRuntimeCommandExecutionMessagesUseStableRuntimeShape(t *testing.T) {
	messages := RuntimeCommandExecutionMessages("thread-1", "turn-1", "", "/workspace", RuntimeCommandExecutionMaterial{
		Command:    "multica issue get iss-1",
		Output:     "Loaded TEA-1",
		ExitCode:   0,
		DurationMs: -1,
	}, time.Date(2026, 6, 29, 7, 31, 0, 0, time.UTC))
	if len(messages) != 2 {
		t.Fatalf("messages = %+v, want start+complete", messages)
	}
	started, _ := messages[0].Params["item"].(map[string]any)
	completed, _ := messages[1].Params["item"].(map[string]any)
	if messages[0].Method != "item/started" || started["status"] != "inProgress" || started["cwd"] != "/workspace" {
		t.Fatalf("unexpected started command item: %+v", messages[0])
	}
	if messages[1].Method != "item/completed" || completed["status"] != "completed" || completed["aggregatedOutput"] != "Loaded TEA-1" {
		t.Fatalf("unexpected completed command item: %+v", messages[1])
	}
	if completed["durationMs"] != int64(0) {
		t.Fatalf("durationMs = %+v, want 0", completed["durationMs"])
	}
	if id, _ := started["id"].(string); !strings.HasPrefix(id, "call_") || completed["id"] != id {
		t.Fatalf("command item id mismatch start=%+v completed=%+v", started["id"], completed["id"])
	}
}

func TestRuntimeAgentMessageMessagesUsePhaseAndDelta(t *testing.T) {
	messages := RuntimeAgentMessageMessages("thread-1", "turn-1", "msg-1", "done", "final_answer", time.Date(2026, 6, 29, 7, 32, 0, 0, time.UTC))
	if len(messages) != 3 {
		t.Fatalf("messages = %+v, want start+delta+complete", messages)
	}
	started, _ := messages[0].Params["item"].(map[string]any)
	completed, _ := messages[2].Params["item"].(map[string]any)
	if started["phase"] != "final_answer" || completed["phase"] != "final_answer" {
		t.Fatalf("phase mismatch start=%+v completed=%+v", started, completed)
	}
	if messages[1].Method != "item/agentMessage/delta" || messages[1].Params["delta"] != "done" || messages[1].Params["itemId"] != "msg-1" {
		t.Fatalf("unexpected delta message: %+v", messages[1])
	}
}

func TestRuntimeTextInputExtractsOnlyTextItems(t *testing.T) {
	got := RuntimeTextInput(map[string]any{
		"input": []any{
			map[string]any{"type": "text", "text": "Open [TEA-1](mention://issue/iss-1)."},
			map[string]any{"type": "image", "url": "ignored"},
			map[string]any{"type": "text", "text": "  "},
			map[string]any{"type": "text", "text": "Then summarize."},
			"ignored",
		},
	})
	want := "Open [TEA-1](mention://issue/iss-1).\nThen summarize."
	if got != want {
		t.Fatalf("RuntimeTextInput() = %q, want %q", got, want)
	}
	if got := RuntimeTextInput(map[string]any{"input": "not-list"}); got != "" {
		t.Fatalf("non-list input should be ignored, got %q", got)
	}
}

func TestRuntimeInputMaterialExtractsStructuredIssueIdentity(t *testing.T) {
	got := RuntimeInputMaterial(map[string]any{
		"input": []any{
			map[string]any{
				"type": "text",
				"id":   "item-1",
				"text": "Please review the linked issue.",
				"text_elements": []any{
					map[string]any{
						"type":        "mention",
						"target_type": "issue",
						"target_id":   "iss-49",
						"text":        "@TEA-49",
					},
				},
			},
		},
	})
	if got.Text != "Please review the linked issue." {
		t.Fatalf("visible text changed: %+v", got)
	}
	if got.IssueIdentity != "iss-49" {
		t.Fatalf("structured issue identity = %q, want iss-49", got.IssueIdentity)
	}
	if got.IssueIdentitySource != RuntimeIssueSourceInput {
		t.Fatalf("structured issue source = %q, want %q", got.IssueIdentitySource, RuntimeIssueSourceInput)
	}
}

func TestRuntimeInputMaterialFallsBackToVisibleIssueTag(t *testing.T) {
	got := RuntimeInputMaterial(map[string]any{
		"input": []any{
			map[string]any{"type": "text", "id": "item-1", "text": "Please handle @TEA-50 next."},
		},
	})
	if got.IssueIdentity != "TEA-50" {
		t.Fatalf("visible issue tag identity = %q, want TEA-50", got.IssueIdentity)
	}
	if got.IssueIdentitySource != RuntimeIssueSourceInputText {
		t.Fatalf("visible issue source = %q, want %q", got.IssueIdentitySource, RuntimeIssueSourceInputText)
	}
}

func TestRuntimeInputMaterialPrefersStructuredIssueOverVisibleTag(t *testing.T) {
	got := RuntimeInputMaterial(map[string]any{
		"input": []any{
			map[string]any{"type": "text", "text": "Ignore the stale copied tag @TEA-1."},
			map[string]any{
				"type": "text",
				"text": "Use the selected Multica issue tag.",
				"text_elements": []any{
					map[string]any{
						"type":        "mention",
						"target_type": "issue",
						"target_id":   "iss-selected",
						"text":        "@TEA-99",
					},
				},
			},
		},
	})
	if got.IssueIdentity != "iss-selected" {
		t.Fatalf("structured issue identity should win over visible tag, got %q", got.IssueIdentity)
	}
}

func TestRuntimeInputMaterialIgnoresNonIssueItemID(t *testing.T) {
	got := RuntimeInputMaterial(map[string]any{
		"input": []any{
			map[string]any{"type": "text", "id": "item-1", "text": "Coordinate with @team."},
		},
	})
	if got.IssueIdentity != "" {
		t.Fatalf("non-issue item id should be ignored, got %q", got.IssueIdentity)
	}
}

func TestRuntimeRefNormalizesMulticaRefs(t *testing.T) {
	if got := RuntimeRef(" issue ", " iss-1 "); got != "multica:issue:iss-1" {
		t.Fatalf("RuntimeRef() = %q", got)
	}
	if got := RuntimeRef("", "iss-1"); got != "" {
		t.Fatalf("empty kind should produce no ref, got %q", got)
	}
	if got := RuntimeRef("issue", ""); got != "" {
		t.Fatalf("empty id should produce no ref, got %q", got)
	}
}
