package interaction

import "testing"

func TestEventMaterialBuildPayloadSeparatesRuleNarrativeRefs(t *testing.T) {
	rule := map[string]any{"ttl": "30m"}
	narrative := map[string]any{"statement": "do the work"}
	refs := map[string]any{"evidence_refs": []string{"ev-1"}}
	material := EventMaterial{
		EventType:  "teamwork_signal.write_candidate.observed",
		ExternalID: "external-1",
		Payload:    BuildPayload(rule, narrative, refs),
	}
	if material.Payload["rule"].(map[string]any)["ttl"] != "30m" {
		t.Fatalf("rule payload missing: %+v", material.Payload)
	}
	if material.Payload["narrative"].(map[string]any)["statement"] != "do the work" {
		t.Fatalf("narrative payload missing: %+v", material.Payload)
	}
	if len(material.Payload["refs"].(map[string]any)["evidence_refs"].([]string)) != 1 {
		t.Fatalf("refs payload missing: %+v", material.Payload)
	}
}

func TestCleanStringsTrimsAndDedupes(t *testing.T) {
	got := CleanStrings([]string{" a ", "", "b", "a"})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("clean strings = %+v, want [a b]", got)
	}
}
