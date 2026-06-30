package multica

import "strings"

func MergeIssueMetadata(base map[string]any, listed map[string]string) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range listed {
		if strings.TrimSpace(key) != "" {
			merged[key] = value
		}
	}
	return merged
}
