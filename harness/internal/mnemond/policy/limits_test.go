package policy

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
)

func TestAppendItemRuleEnforcesMaxPayloadBytes(t *testing.T) {
	cap := EmbeddedCatalog()["progress_digest"]
	r := cap.Rule("codex@project", contract.ResourceRef{Kind: cap.ResourceKind, ID: "project"},
		Limits{MaxPayloadBytes: 64})
	dec, err := r.Evaluate(admission.RuleInput{Event: contract.Event{
		Type:  cap.ObservedType,
		Actor: "codex@project",
		Payload: map[string]any{
			"summary": strings.Repeat("x", 256),
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
	cap := EmbeddedCatalog()["progress_digest"]
	r := cap.Rule("codex@project", contract.ResourceRef{Kind: cap.ResourceKind, ID: "project"}, Limits{})
	dec, err := r.Evaluate(admission.RuleInput{Event: contract.Event{
		Type:  cap.ObservedType,
		Actor: "codex@project",
		Payload: map[string]any{
			"summary": strings.Repeat("x", 256),
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if dec.Verdict != contract.VerdictPropose {
		t.Fatalf("zero limit must not bound, got %v (reasons %v)", dec.Verdict, dec.Reasons)
	}
}
