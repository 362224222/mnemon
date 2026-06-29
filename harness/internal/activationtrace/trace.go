package activationtrace

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	SourceCodexAppServer = "codex-appserver"
	TextLimit            = 12000
)

type Event struct {
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

type Sink interface {
	OnManagedTurnTrace(Event)
}

type SinkFunc func(Event)

func (f SinkFunc) OnManagedTurnTrace(event Event) {
	if f != nil {
		f(event)
	}
}

func EventsFromCodexNotifications(principal string, notifications []map[string]any) []Event {
	var out []Event
	for _, notification := range notifications {
		event, ok := eventFromCodexNotification(principal, notification)
		if ok {
			out = append(out, event)
		}
	}
	return out
}

func eventFromCodexNotification(principal string, notification map[string]any) (Event, bool) {
	method := stringFromMap(notification, "method")
	params, _ := notification["params"].(map[string]any)
	event := Event{
		SourceRuntime: SourceCodexAppServer,
		Principal:     strings.TrimSpace(principal),
		Method:        method,
		TurnID:        stringFromMap(params, "turnId"),
	}
	switch method {
	case "item/started", "item/completed":
		rawItem, _ := params["item"].(map[string]any)
		if len(rawItem) == 0 {
			return Event{}, false
		}
		item := sanitizeMap(rawItem)
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
		event.Kind = kindForItem(event.ItemType)
		return event, true
	case "item/agentMessage/delta":
		event.Kind = "agent_message_delta"
		event.ItemID = stringFromMap(params, "itemId")
		event.Text = sanitizeString("delta", stringFromMap(params, "delta"))
		event.ItemType = "agentMessage"
		return event, strings.TrimSpace(event.Text) != "" || strings.TrimSpace(event.ItemID) != ""
	case "turn/completed":
		event.Kind = "turn_completed"
		event.Status = "completed"
		return event, true
	case "turn/failed":
		event.Kind = "turn_failed"
		event.Status = "failed"
		event.Text = sanitizeAny("error", params["error"])
		return event, true
	default:
		return Event{}, false
	}
}

func kindForItem(itemType string) string {
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

func sanitizeMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for key, value := range in {
		out[key] = sanitizeValue(key, value)
	}
	return out
}

func sanitizeValue(key string, value any) any {
	if isSensitiveKey(key) {
		if value == nil {
			return nil
		}
		return "[redacted]"
	}
	switch typed := value.(type) {
	case map[string]any:
		return sanitizeMap(typed)
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, sanitizeValue(key, item))
		}
		return out
	case string:
		return sanitizeString(key, typed)
	default:
		return value
	}
}

func sanitizeAny(key string, value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return sanitizeString(key, text)
	}
	data, err := json.Marshal(sanitizeValue(key, value))
	if err != nil {
		return sanitizeString(key, fmt.Sprint(value))
	}
	return sanitizeString(key, string(data))
}

func sanitizeString(key, value string) string {
	value = redactSecrets(key, value)
	if len(value) <= TextLimit {
		return value
	}
	return value[:TextLimit] + "\n[truncated]"
}

func redactSecrets(key, value string) string {
	if isSensitiveKey(key) {
		if value == "" {
			return ""
		}
		return "[redacted]"
	}
	redacted := value
	for _, marker := range sensitiveMarkers() {
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

func isSensitiveKey(key string) bool {
	key = strings.ToUpper(strings.TrimSpace(key))
	for _, marker := range sensitiveMarkers() {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func sensitiveMarkers() []string {
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

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
