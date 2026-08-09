package agency

import (
	"testing"
	"time"
)

var testTime = time.Date(2026, time.August, 3, 8, 0, 0, 0, time.UTC)

func mustAttachment(t *testing.T, id string, principal AgentPrincipalID, mayInitiate bool) Attachment {
	t.Helper()
	attachment, err := NewAttachment(mustAttachmentID(t, id), principal, mayInitiate,
		testTime, testTime.Add(10*time.Minute))
	if err != nil {
		t.Fatalf("NewAttachment() error = %v", err)
	}
	return attachment
}

func mustView(t *testing.T, spec MachineViewSpec) ViewAuthority {
	t.Helper()
	view, err := NewViewAuthority(spec)
	if err != nil {
		t.Fatalf("NewViewAuthority() error = %v", err)
	}
	return view
}

func mustRootIntent(t *testing.T, successors []TargetRef, artifacts ...ArtifactInput) AgentIntent {
	t.Helper()
	intent, err := NewAgentIntent(IntentSpec{
		Kind: mustLabel(t, "future.agent.action"), Payload: mustPayload(t, "Continue the accepted responsibility."),
		Consequence: ConsequenceCreateHandlings, Successors: successors, Artifacts: artifacts,
	})
	if err != nil {
		t.Fatalf("NewAgentIntent() error = %v", err)
	}
	return intent
}

func mustBoundRoot(t *testing.T, operation string) BoundIntent {
	t.Helper()
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:root", principal, true)
	target, err := ResolveLocalTarget(SelfTarget(), principal)
	if err != nil {
		t.Fatalf("ResolveLocalTarget() error = %v", err)
	}
	request, err := BindIntent(BoundIntentSpec{
		Intent: mustRootIntent(t, []TargetRef{SelfTarget()}), OperationKey: mustOperation(t, operation),
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{target}}),
	})
	if err != nil {
		t.Fatalf("BindIntent() error = %v", err)
	}
	return request
}

func mustLabel(t *testing.T, value string) SemanticLabel {
	t.Helper()
	result, err := NewSemanticLabel(value)
	if err != nil {
		t.Fatalf("NewSemanticLabel(%q) error = %v", value, err)
	}
	return result
}

func mustReferenceKey(t *testing.T, value string) ReferenceKey {
	t.Helper()
	result, err := NewReferenceKey(value)
	if err != nil {
		t.Fatalf("NewReferenceKey(%q) error = %v", value, err)
	}
	return result
}

func mustPayload(t *testing.T, value string) SemanticPayload {
	t.Helper()
	result, err := NewSemanticPayload(value)
	if err != nil {
		t.Fatalf("NewSemanticPayload() error = %v", err)
	}
	return result
}

func mustHandle(t *testing.T, value string) OpaqueHandle {
	t.Helper()
	result, err := NewOpaqueHandle(value)
	if err != nil {
		t.Fatalf("NewOpaqueHandle(%q) error = %v", value, err)
	}
	return result
}

func mustAliasTarget(t *testing.T, value string) TargetRef {
	t.Helper()
	target, err := AliasTarget(mustHandle(t, value))
	if err != nil {
		t.Fatalf("AliasTarget() error = %v", err)
	}
	return target
}

func mustCandidate(t *testing.T, value string) ArtifactInput {
	t.Helper()
	result, err := NewArtifactCandidate(mustHandle(t, value))
	if err != nil {
		t.Fatalf("NewArtifactCandidate() error = %v", err)
	}
	return result
}

func mustViewArtifact(t *testing.T, value string) ArtifactInput {
	t.Helper()
	result, err := NewArtifactViewHandle(mustHandle(t, value))
	if err != nil {
		t.Fatalf("NewArtifactViewHandle() error = %v", err)
	}
	return result
}

func mustCaptured(t *testing.T, operation OperationKey, input ArtifactInput, body string) CapturedCandidate {
	t.Helper()
	result, err := NewCapturedCandidate(operation, input, Sum([]byte(body)))
	if err != nil {
		t.Fatalf("NewCapturedCandidate() error = %v", err)
	}
	return result
}

func mustViewOffer(t *testing.T, handle OpaqueHandle, body string) ViewArtifactOffer {
	t.Helper()
	result, err := NewViewArtifactOffer(handle, Sum([]byte(body)))
	if err != nil {
		t.Fatalf("NewViewArtifactOffer() error = %v", err)
	}
	return result
}

func mustPrincipal(t *testing.T, value string) AgentPrincipalID {
	t.Helper()
	result, err := NewAgentPrincipalID(value)
	if err != nil {
		t.Fatalf("NewAgentPrincipalID(%q) error = %v", value, err)
	}
	return result
}

func mustAttachmentID(t *testing.T, value string) AttachmentID {
	t.Helper()
	result, err := NewAttachmentID(value)
	if err != nil {
		t.Fatalf("NewAttachmentID(%q) error = %v", value, err)
	}
	return result
}

func mustEventID(t *testing.T, value string) EventID {
	t.Helper()
	result, err := NewEventID(value)
	if err != nil {
		t.Fatalf("NewEventID(%q) error = %v", value, err)
	}
	return result
}

func mustHandlingID(t *testing.T, value string) HandlingID {
	t.Helper()
	result, err := NewHandlingID(value)
	if err != nil {
		t.Fatalf("NewHandlingID(%q) error = %v", value, err)
	}
	return result
}

func mustOperation(t *testing.T, value string) OperationKey {
	t.Helper()
	result, err := NewOperationKey(value)
	if err != nil {
		t.Fatalf("NewOperationKey(%q) error = %v", value, err)
	}
	return result
}

func mustRoute(t *testing.T, value string) RouteID {
	t.Helper()
	result, err := NewRouteID(value)
	if err != nil {
		t.Fatalf("NewRouteID(%q) error = %v", value, err)
	}
	return result
}

func mustDeliveryID(t *testing.T, value string) DeliveryID {
	t.Helper()
	return DeliveryID{digest: Sum([]byte(value))}
}

func mustEventRef(t *testing.T, id, body string) EventRef {
	t.Helper()
	result, err := NewEventRef(mustEventID(t, id), Sum([]byte(body)))
	if err != nil {
		t.Fatalf("NewEventRef() error = %v", err)
	}
	return result
}

func mustSubject(t *testing.T, handle OpaqueHandle, id, event, body string, fence uint64) SubjectBinding {
	t.Helper()
	result, err := NewSubjectBinding(handle, mustHandlingID(t, id), mustEventRef(t, event, body), fence, 0)
	if err != nil {
		t.Fatalf("NewSubjectBinding() error = %v", err)
	}
	return result
}

func mustReference(t *testing.T, handle OpaqueHandle, key, event, body string) ReferenceExpectation {
	t.Helper()
	result, err := ExpectReferenceHead(handle, mustReferenceKey(t, key), mustEventRef(t, event, body))
	if err != nil {
		t.Fatalf("ExpectReferenceHead() error = %v", err)
	}
	return result
}

func mustProvenance(t *testing.T, handle OpaqueHandle, event, body string) ProvenanceOffer {
	t.Helper()
	result, err := NewProvenanceOffer(handle, mustEventRef(t, event, body))
	if err != nil {
		t.Fatalf("NewProvenanceOffer() error = %v", err)
	}
	return result
}
