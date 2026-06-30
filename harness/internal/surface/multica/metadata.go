package multica

import "strings"

func NormalizeMulticaMetadata(raw any) map[string]string {
	out := map[string]string{}
	var visit func(any)
	visit = func(value any) {
		switch typed := value.(type) {
		case nil:
			return
		case map[string]string:
			for key, value := range typed {
				if strings.TrimSpace(key) != "" {
					out[strings.TrimSpace(key)] = strings.TrimSpace(value)
				}
			}
		case map[string]any:
			if key := strings.TrimSpace(anyString(typed["key"])); key != "" {
				out[key] = strings.TrimSpace(anyString(typed["value"]))
				return
			}
			if items, ok := typed["metadata"]; ok {
				visit(items)
				return
			}
			for key, value := range typed {
				if strings.TrimSpace(key) != "" {
					out[strings.TrimSpace(key)] = strings.TrimSpace(anyString(value))
				}
			}
		case []any:
			for _, item := range typed {
				visit(item)
			}
		}
	}
	visit(raw)
	return out
}

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
