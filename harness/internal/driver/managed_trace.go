package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	ManagedTurnTraceSourceCodexAppServer = "codex-appserver"
	managedTurnTraceTextLimit            = 12000
)

type ManagedTurnTraceEvent struct {
	Kind          string         `json:"kind"`
	SourceRuntime string         `json:"source_runtime"`
	Principal     string         `json:"principal,omitempty"`
	TurnID        string         `json:"turn_id,omitempty"`
	ItemID        string         `json:"item_id,omitempty"`
	Method        string         `json:"method,omitempty"`
	ItemType      string         `json:"item_type,omitempty"`
	Phase         string         `json:"phase,omitempty"`
	Text          string         `json:"text,omitempty"`
	Command       string         `json:"command,omitempty"`
	CWD           string         `json:"cwd,omitempty"`
	Status        string         `json:"status,omitempty"`
	ExitCode      any            `json:"exit_code,omitempty"`
	Output        string         `json:"output,omitempty"`
	Item          map[string]any `json:"item,omitempty"`
}

type ManagedTurnTraceSink interface {
	OnManagedTurnTrace(ManagedTurnTraceEvent)
}

type ManagedTurnTraceSinkFunc func(ManagedTurnTraceEvent)

func (f ManagedTurnTraceSinkFunc) OnManagedTurnTrace(event ManagedTurnTraceEvent) {
	if f != nil {
		f(event)
	}
}

type ManagedTurnClientWithTrace interface {
	StartTurnWithTrace(ctx context.Context, query string, sink ManagedTurnTraceSink) (ManagedTurnResult, error)
}

func ManagedTurnTraceEventsFromCodexNotifications(principal string, notifications []map[string]any) []ManagedTurnTraceEvent {
	var out []ManagedTurnTraceEvent
	for _, notification := range notifications {
		event, ok := managedTurnTraceEventFromCodexNotification(principal, notification)
		if ok {
			out = append(out, event)
		}
	}
	return out
}

func managedTurnTraceEventFromCodexNotification(principal string, notification map[string]any) (ManagedTurnTraceEvent, bool) {
	method := stringFromMap(notification, "method")
	params, _ := notification["params"].(map[string]any)
	event := ManagedTurnTraceEvent{
		SourceRuntime: ManagedTurnTraceSourceCodexAppServer,
		Principal:     strings.TrimSpace(principal),
		Method:        method,
		TurnID:        stringFromMap(params, "turnId"),
	}
	switch method {
	case "item/started", "item/completed":
		rawItem, _ := params["item"].(map[string]any)
		if len(rawItem) == 0 {
			return ManagedTurnTraceEvent{}, false
		}
		item := sanitizeManagedTraceMap(rawItem)
		event.Item = item
		event.ItemID = firstNonEmptyString(stringFromMap(item, "id"), stringFromMap(params, "itemId"))
		event.ItemType = stringFromMap(item, "type")
		event.Phase = stringFromMap(item, "phase")
		event.Text = firstNonEmptyString(stringFromMap(item, "text"), stringFromMap(item, "aggregatedOutput"))
		event.Command = stringFromMap(item, "command")
		event.CWD = stringFromMap(item, "cwd")
		event.Status = stringFromMap(item, "status")
		event.ExitCode = item["exitCode"]
		event.Output = stringFromMap(item, "aggregatedOutput")
		event.Kind = managedTraceKindForItem(event.ItemType)
		return event, true
	case "item/agentMessage/delta":
		event.Kind = "agent_message_delta"
		event.ItemID = stringFromMap(params, "itemId")
		event.Text = sanitizeManagedTraceString("delta", stringFromMap(params, "delta"))
		event.ItemType = "agentMessage"
		return event, strings.TrimSpace(event.Text) != "" || strings.TrimSpace(event.ItemID) != ""
	case "turn/completed":
		event.Kind = "turn_completed"
		event.Status = "completed"
		return event, true
	case "turn/failed":
		event.Kind = "turn_failed"
		event.Status = "failed"
		event.Text = sanitizeManagedTraceAny("error", params["error"])
		return event, true
	default:
		return ManagedTurnTraceEvent{}, false
	}
}

func managedTraceKindForItem(itemType string) string {
	switch strings.TrimSpace(itemType) {
	case "agentMessage":
		return "agent_message"
	case "commandExecution":
		return "command_execution"
	case "fileChange", "patchApply", "patch_apply":
		return "file_change"
	default:
		if strings.TrimSpace(itemType) == "" {
			return "item"
		}
		return "item"
	}
}

func sanitizeManagedTraceMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = sanitizeManagedTraceValue(key, value)
	}
	return out
}

func sanitizeManagedTraceValue(key string, value any) any {
	if isSensitiveManagedTraceKey(key) {
		if value == nil {
			return nil
		}
		return "[redacted]"
	}
	switch typed := value.(type) {
	case map[string]any:
		return sanitizeManagedTraceMap(typed)
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, sanitizeManagedTraceValue(key, item))
		}
		return out
	case string:
		return sanitizeManagedTraceString(key, typed)
	default:
		return value
	}
}

func sanitizeManagedTraceAny(key string, value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return sanitizeManagedTraceString(key, text)
	}
	data, err := json.Marshal(sanitizeManagedTraceValue(key, value))
	if err != nil {
		return sanitizeManagedTraceString(key, fmt.Sprint(value))
	}
	return sanitizeManagedTraceString(key, string(data))
}

func sanitizeManagedTraceString(key, value string) string {
	value = redactManagedTraceSecrets(key, value)
	if len(value) <= managedTurnTraceTextLimit {
		return value
	}
	return value[:managedTurnTraceTextLimit] + "\n[truncated]"
}

func redactManagedTraceSecrets(key, value string) string {
	if isSensitiveManagedTraceKey(key) {
		if value == "" {
			return ""
		}
		return "[redacted]"
	}
	redacted := value
	for _, marker := range managedTraceSensitiveMarkers() {
		redacted = redactMarkerAssignments(redacted, marker)
	}
	return redacted
}

func redactMarkerAssignments(value, marker string) string {
	needle := strings.ToLower(marker)
	searchFrom := 0
	for {
		lower := strings.ToLower(value)
		if searchFrom >= len(value) {
			return value
		}
		relative := strings.Index(lower[searchFrom:], needle)
		if relative < 0 {
			return value
		}
		idx := searchFrom + relative
		if idx < 0 {
			return value
		}
		end := idx + len(marker)
		if end >= len(value) || (value[end] != '=' && value[end] != ':') {
			searchFrom = end
			continue
		}
		if strings.HasPrefix(strings.ToLower(value[end:]), "=[redacted]") || strings.HasPrefix(strings.ToLower(value[end:]), ":[redacted]") {
			searchFrom = end + len("=[redacted]")
			continue
		}
		separator := value[end]
		end++
		for end < len(value) {
			ch := value[end]
			if ch == ' ' || ch == '\n' || ch == '\t' || ch == ',' || ch == ';' {
				break
			}
			end++
		}
		value = value[:idx+len(marker)] + string(separator) + "[redacted]" + value[end:]
		searchFrom = idx + len(marker) + len("=[redacted]")
	}
}

func isSensitiveManagedTraceKey(key string) bool {
	key = strings.ToUpper(strings.TrimSpace(key))
	for _, marker := range managedTraceSensitiveMarkers() {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func managedTraceSensitiveMarkers() []string {
	return []string{"TOKEN", "SECRET", "PASSWORD", "API_KEY", "AUTH", "CREDENTIAL", "COOKIE"}
}

func stringFromMap(values map[string]any, key string) string {
	if values == nil {
		return ""
	}
	if value, ok := values[key].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}
