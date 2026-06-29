package interaction

import (
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

type EventMaterial struct {
	EventType  string         `json:"event_type"`
	ExternalID string         `json:"external_id"`
	Payload    map[string]any `json:"payload"`
}

func BuildPayload(rule, narrative, refs map[string]any) map[string]any {
	return eventmodel.BuildPayload(rule, narrative, refs)
}

func PutString(values map[string]any, key, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key != "" && value != "" {
		values[key] = value
	}
}

func CleanStrings(values []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
