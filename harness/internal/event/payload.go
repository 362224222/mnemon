package event

import "fmt"

const (
	PayloadRuleKey      = "rule"
	PayloadNarrativeKey = "narrative"
	PayloadRefsKey      = "refs"
)

var payloadSectionKeys = map[string]bool{
	PayloadRuleKey:      true,
	PayloadNarrativeKey: true,
	PayloadRefsKey:      true,
}

// BuildPayload constructs the R2 event payload shape. Nil sections are omitted.
func BuildPayload(rule, narrative, refs map[string]any) map[string]any {
	payload := map[string]any{}
	if rule != nil {
		payload[PayloadRuleKey] = copyPayloadMap(rule)
	}
	if narrative != nil {
		payload[PayloadNarrativeKey] = copyPayloadMap(narrative)
	}
	if refs != nil {
		payload[PayloadRefsKey] = copyPayloadMap(refs)
	}
	return payload
}

func PayloadRule(payload map[string]any) map[string]any {
	return payloadSection(payload, PayloadRuleKey)
}

func PayloadNarrative(payload map[string]any) map[string]any {
	return payloadSection(payload, PayloadNarrativeKey)
}

func PayloadRefs(payload map[string]any) map[string]any {
	return payloadSection(payload, PayloadRefsKey)
}

func payloadSection(payload map[string]any, key string) map[string]any {
	if payload == nil {
		return nil
	}
	section, _ := payload[key].(map[string]any)
	return section
}

func validatePayload(payload map[string]any) error {
	for key, value := range payload {
		if _, ok := phaseMetaKeys[key]; ok {
			return fmt.Errorf("event payload must not carry phase meta key %q", key)
		}
		if !payloadSectionKeys[key] {
			return fmt.Errorf("event payload must not carry business key %q outside rule/narrative/refs", key)
		}
		section, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("event payload section %q must be an object", key)
		}
		for sectionKey := range section {
			if _, ok := phaseMetaKeys[sectionKey]; ok {
				return fmt.Errorf("event payload section %q must not carry phase meta key %q", key, sectionKey)
			}
		}
	}
	return nil
}

func copyPayloadMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
