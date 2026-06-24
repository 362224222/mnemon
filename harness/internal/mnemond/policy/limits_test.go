package policy

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

func TestAppendItemRuleEnforcesMaxPayloadBytes(t *testing.T) {
	r := EmbeddedCatalog()["memory"].Rule("codex@project", contract.ResourceRef{Kind: "memory", ID: "project"},
		Limits{MaxPayloadBytes: 64})
	dec, err := r.Evaluate(rule.RuleInput{Event: contract.Event{
		Type:  MemoryWriteCandidateObserved,
		Actor: "codex@project",
		Payload: map[string]any{
			"content": strings.Repeat("x", 256), "source": "s", "confidence": "high",
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Verdict != contract.VerdictDeny {
		t.Fatalf("oversized payload must be denied, got %v", dec.Verdict)
	}
	if len(dec.Reasons) == 0 || !strings.Contains(dec.Reasons[0], "max_payload_bytes") {
		t.Fatalf("denial must name the limit, got %v", dec.Reasons)
	}
}

func TestAppendItemRuleZeroLimitMeansUnbounded(t *testing.T) {
	r := EmbeddedCatalog()["memory"].Rule("codex@project", contract.ResourceRef{Kind: "memory", ID: "project"}, Limits{})
	dec, err := r.Evaluate(rule.RuleInput{Event: contract.Event{
		Type:  MemoryWriteCandidateObserved,
		Actor: "codex@project",
		Payload: map[string]any{
			"content": strings.Repeat("x", 256), "source": "s", "confidence": "high",
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Verdict != contract.VerdictPropose {
		t.Fatalf("zero limit must not bound, got %v (reasons %v)", dec.Verdict, dec.Reasons)
	}
}
