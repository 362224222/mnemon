package replay

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
)

// Test-only declaration spec: only the enum message differs, so Shadow can detect a minimal
// rule-behavior change without relying on a product capability name.
func declarationSpecWithMessage(message string) policy.CapabilitySpec {
	return policy.CapabilitySpec{
		SchemaVersion: 1, Name: "fixture_declaration",
		ObservedType: "fixture_declaration.write_candidate.observed", ProposedType: "fixture_declaration.write.proposed",
		ResourceKind: "fixture_declaration", ItemsField: "declarations",
		Fields: []policy.FieldSpec{
			{Name: "declaration_id", Validators: []policy.ValidatorRef{
				{ID: "required", Params: map[string]string{"missing_style": "missing"}},
				{ID: "format:identifier"},
			}},
			{Name: "name", Validators: []policy.ValidatorRef{{ID: "default-from", Params: map[string]string{"field": "declaration_id"}}}},
			{Name: "status", Validators: []policy.ValidatorRef{
				{ID: "default", Params: map[string]string{"value": "active"}},
				{ID: "enum", Params: map[string]string{"values": "active|stale|archived", "message": message}},
			}},
			{Name: "source", Validators: []policy.ValidatorRef{{ID: "required", Params: map[string]string{"missing_style": "missing"}}}},
			{Name: "confidence", Validators: []policy.ValidatorRef{{ID: "required", Params: map[string]string{"missing_style": "missing"}}}},
			{Name: "content", Validators: []policy.ValidatorRef{{ID: "safety:unsafe"}}},
		},
		Render: policy.RenderSpec{Static: map[string]string{"name": "project"}},
	}
}

func declarationRules(t *testing.T, message string) admission.RuleSet {
	t.Helper()
	cap, err := policy.FromSpec(declarationSpecWithMessage(message))
	if err != nil {
		t.Fatalf("FromSpec: %v", err)
	}
	ref := contract.ResourceRef{Kind: "fixture_declaration", ID: "project"}
	return admission.NewRuleSet(cap.Rule(gateActor, ref, policy.Limits{}))
}

// I6 制度化(规则半边):同一规则集 Shadow 必 Clean;改动一个 capability spec 的 enum
// message 编译出的 candidate 必被检出——Reasons 即行为(deny 落 durable diagnostic),
// 晋升门以此为闸。场景含一条非法 status 的 deny 观察(差异恰在其 Reason 上)。
func TestShadowCleanOnSelfAndDetectsSpecChange(t *testing.T) {
	live := declarationRules(t, "invalid status")
	subs := map[contract.ActorID]contract.Subscription{
		gateActor: {Actor: gateActor, Refs: []contract.ResourceRef{{Kind: "fixture_declaration", ID: "project"}}},
	}
	events := []contract.Event{
		{SchemaVersion: 1, ID: "e1", IngestSeq: 1, Type: "fixture_declaration.write_candidate.observed", Actor: gateActor,
			Payload: map[string]any{"declaration_id": "good-declaration", "source": "user", "confidence": "high"}},
		{SchemaVersion: 1, ID: "e2", IngestSeq: 2, Type: "fixture_declaration.write_candidate.observed", Actor: gateActor,
			Payload: map[string]any{"declaration_id": "bad-declaration", "status": "frozen", "source": "user", "confidence": "high"}},
	}

	if rep := Shadow(events, subs, live, live); !rep.Clean || rep.Diffs != 0 {
		t.Fatalf("self-shadow must be clean, got %+v", rep)
	}
	mutated := declarationRules(t, "bad status") // 仅 deny 消息变化
	rep := Shadow(events, subs, live, mutated)
	if rep.Clean || rep.Diffs == 0 {
		t.Fatalf("a spec-level rule change (Reasons) must be detected, got %+v", rep)
	}
}
