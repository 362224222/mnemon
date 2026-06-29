package multica

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/activationtrace"
)

type RuntimeRPCMessage struct {
	Method string
	Params map[string]any
}

type RuntimeCommandExecutionMaterial struct {
	Command    string
	CWD        string
	Output     string
	ExitCode   int
	DurationMs int64
}

func RuntimeManagedTraceMessages(threadID, turnID string, event activationtrace.Event, now time.Time) []RuntimeRPCMessage {
	switch event.Method {
	case "item/started":
		item := runtimeManagedTraceItem(event)
		if len(item) == 0 {
			return nil
		}
		return []RuntimeRPCMessage{{
			Method: "item/started",
			Params: RuntimeItemParams(threadID, turnID, item, "startedAtMs", now.UTC().UnixMilli()),
		}}
	case "item/completed":
		item := runtimeManagedTraceItem(event)
		if len(item) == 0 {
			return nil
		}
		return []RuntimeRPCMessage{{
			Method: "item/completed",
			Params: RuntimeItemParams(threadID, turnID, item, "completedAtMs", now.UTC().UnixMilli()),
		}}
	case "item/agentMessage/delta":
		text := strings.TrimSpace(event.Text)
		if text == "" {
			return nil
		}
		return []RuntimeRPCMessage{runtimeAgentDelta(threadID, turnID, runtimeManagedTraceItemID(event), text)}
	default:
		return nil
	}
}

func RuntimeCommandExecutionMessages(threadID, turnID, itemID, fallbackCWD string, event RuntimeCommandExecutionMaterial, now time.Time) []RuntimeRPCMessage {
	command := strings.TrimSpace(event.Command)
	if command == "" {
		return nil
	}
	if strings.TrimSpace(itemID) == "" {
		itemID = runtimeTextDigestID("call", command+"\n"+event.Output)
	}
	cwd := strings.TrimSpace(event.CWD)
	if cwd == "" {
		cwd = fallbackCWD
	}
	output := strings.TrimSpace(event.Output)
	durationMs := event.DurationMs
	if durationMs < 0 {
		durationMs = 0
	}
	nowMs := now.UTC().UnixMilli()
	started := runtimeCommandExecution(itemID, command, cwd, "inProgress", "", nil, nil)
	completed := runtimeCommandExecution(itemID, command, cwd, "completed", output, event.ExitCode, durationMs)
	return []RuntimeRPCMessage{
		{
			Method: "item/started",
			Params: RuntimeItemParams(threadID, turnID, started, "startedAtMs", nowMs),
		},
		{
			Method: "item/completed",
			Params: RuntimeItemParams(threadID, turnID, completed, "completedAtMs", nowMs),
		},
	}
}

func RuntimeAgentMessageMessages(threadID, turnID, itemID, text, phase string, now time.Time) []RuntimeRPCMessage {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	phase = strings.TrimSpace(phase)
	if phase == "" {
		phase = "commentary"
	}
	if strings.TrimSpace(itemID) == "" {
		itemID = runtimeTextDigestID("msg", text)
	}
	nowMs := now.UTC().UnixMilli()
	return []RuntimeRPCMessage{
		{
			Method: "item/started",
			Params: RuntimeItemParams(threadID, turnID, runtimeAgentMessage(itemID, "", phase), "startedAtMs", nowMs),
		},
		runtimeAgentDelta(threadID, turnID, itemID, text),
		{
			Method: "item/completed",
			Params: RuntimeItemParams(threadID, turnID, runtimeAgentMessage(itemID, text, phase), "completedAtMs", nowMs),
		},
	}
}

func RuntimeItemParams(threadID, turnID string, item map[string]any, timeKey string, timestampMs int64) map[string]any {
	params := map[string]any{
		"threadId": threadID,
		"turnId":   turnID,
		"item":     item,
	}
	if timeKey != "" && timestampMs > 0 {
		params[timeKey] = timestampMs
	}
	return params
}

func RuntimeUserMessage(text string) map[string]any {
	return map[string]any{
		"type":     "userMessage",
		"id":       runtimeTextDigestID("user", text),
		"clientId": nil,
		"content": []any{map[string]any{
			"type":          "text",
			"text":          text,
			"text_elements": []any{},
		}},
	}
}

func runtimeManagedTraceItem(event activationtrace.Event) map[string]any {
	if len(event.Item) == 0 {
		return nil
	}
	item, _ := cloneRuntimeAny(event.Item).(map[string]any)
	if item == nil {
		return nil
	}
	item["id"] = runtimeManagedTraceItemID(event)
	if _, ok := item["source"]; !ok {
		item["source"] = "managedCodexAppServer"
	}
	if strings.TrimSpace(event.SourceRuntime) != "" {
		item["mnemonSourceRuntime"] = event.SourceRuntime
	}
	if strings.TrimSpace(event.Principal) != "" {
		item["mnemonPrincipal"] = event.Principal
	}
	if strings.TrimSpace(event.TurnID) != "" {
		item["mnemonManagedTurnId"] = event.TurnID
	}
	return item
}

func runtimeManagedTraceItemID(event activationtrace.Event) string {
	key := firstNonEmptyString(event.ItemID, event.Command, event.Text, event.Kind, event.Method)
	return runtimeTextDigestID("managed", event.SourceRuntime+"\n"+event.TurnID+"\n"+key)
}

func cloneRuntimeAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for key, item := range typed {
			out[key] = cloneRuntimeAny(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, cloneRuntimeAny(item))
		}
		return out
	default:
		return typed
	}
}

func runtimeCommandExecution(id, command, cwd, status, output string, exitCode any, durationMs any) map[string]any {
	item := map[string]any{
		"type":             "commandExecution",
		"id":               id,
		"command":          command,
		"cwd":              cwd,
		"processId":        "mnemon-runtime",
		"source":           "mnemonRuntime",
		"status":           status,
		"commandActions":   []any{map[string]any{"type": "unknown", "command": command}},
		"aggregatedOutput": nil,
		"exitCode":         nil,
		"durationMs":       nil,
	}
	if status == "completed" {
		item["aggregatedOutput"] = output
		item["exitCode"] = exitCode
		item["durationMs"] = durationMs
	}
	return item
}

func runtimeAgentDelta(threadID, turnID, itemID, delta string) RuntimeRPCMessage {
	return RuntimeRPCMessage{
		Method: "item/agentMessage/delta",
		Params: map[string]any{
			"threadId": threadID,
			"turnId":   turnID,
			"itemId":   itemID,
			"delta":    delta,
		},
	}
}

func runtimeAgentMessage(id, text, phase string) map[string]any {
	id = strings.TrimSpace(id)
	if id == "" {
		id = runtimeTextDigestID("msg", text)
	}
	phase = strings.TrimSpace(phase)
	if phase == "" {
		phase = "commentary"
	}
	return map[string]any{
		"type":           "agentMessage",
		"id":             id,
		"text":           text,
		"phase":          phase,
		"memoryCitation": nil,
	}
}

func runtimeTextDigestID(prefix, text string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "item"
	}
	sum := sha256.Sum256([]byte(text))
	digest := fmt.Sprintf("%x", sum[:])[:24]
	return prefix + "_" + digest
}
