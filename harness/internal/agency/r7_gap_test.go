package agency

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestR7GapP01UnofferedHandlesFailClosed(t *testing.T) {
	principal := mustPrincipal(t, "agent:gap-p01")
	attachment := mustAttachment(t, "attachment:gap-p01", principal, true)
	self, err := ResolveLocalTarget(SelfTarget(), principal)
	if err != nil {
		t.Fatalf("ResolveLocalTarget() error = %v", err)
	}

	tests := []struct {
		name string
		spec func(*testing.T) BoundIntentSpec
	}{
		{
			name: "target",
			spec: func(t *testing.T) BoundIntentSpec {
				intent := mustRootIntent(t, []TargetRef{mustAliasTarget(t, "target:not-offered")})
				return BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:gap-target"),
					View: mustView(t, MachineViewSpec{Attachment: attachment,
						Consequences: []Consequence{ConsequenceCreateHandlings},
						Targets:      []ResolvedTarget{self}})}
			},
		},
		{
			name: "subject",
			spec: func(t *testing.T) BoundIntentSpec {
				intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.subject.action"),
					Consequence:     ConsequenceAdvanceHandling,
					SubjectHandling: mustHandle(t, "subject:not-offered")})
				if err != nil {
					t.Fatalf("NewAgentIntent() error = %v", err)
				}
				return BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:gap-subject"),
					View: mustView(t, MachineViewSpec{Attachment: attachment,
						Consequences: []Consequence{ConsequenceAdvanceHandling}})}
			},
		},
		{
			name: "reference",
			spec: func(t *testing.T) BoundIntentSpec {
				operation := mustOperation(t, "op:gap-reference")
				artifact := mustCandidate(t, "candidate:gap-reference")
				intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.reference.action"),
					Consequence:   ConsequenceSupersedeReference,
					ReferenceHead: mustHandle(t, "reference:not-offered"),
					Artifacts:     []ArtifactInput{artifact}})
				if err != nil {
					t.Fatalf("NewAgentIntent() error = %v", err)
				}
				return BoundIntentSpec{Intent: intent, OperationKey: operation,
					View: mustView(t, MachineViewSpec{Attachment: attachment,
						Consequences: []Consequence{ConsequenceSupersedeReference}}),
					Candidates: []CapturedCandidate{mustCaptured(t, operation, artifact, "replacement")}}
			},
		},
		{
			name: "artifact",
			spec: func(t *testing.T) BoundIntentSpec {
				intent := mustRootIntent(t, []TargetRef{SelfTarget()},
					mustViewArtifact(t, "artifact:not-offered"))
				return BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:gap-artifact"),
					View: mustView(t, MachineViewSpec{Attachment: attachment,
						Consequences: []Consequence{ConsequenceCreateHandlings},
						Targets:      []ResolvedTarget{self}})}
			},
		},
		{
			name: "causation",
			spec: func(t *testing.T) BoundIntentSpec {
				intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.causal.action"),
					Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget()},
					CausationHandles: []OpaqueHandle{mustHandle(t, "cause:not-offered")}})
				if err != nil {
					t.Fatalf("NewAgentIntent() error = %v", err)
				}
				return BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:gap-causation"),
					View: mustView(t, MachineViewSpec{Attachment: attachment,
						Consequences: []Consequence{ConsequenceCreateHandlings},
						Targets:      []ResolvedTarget{self}})}
			},
		},
		{
			name: "correlation",
			spec: func(t *testing.T) BoundIntentSpec {
				intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.correlated.action"),
					Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget()},
					CorrelationHandle: mustHandle(t, "correlation:not-offered")})
				if err != nil {
					t.Fatalf("NewAgentIntent() error = %v", err)
				}
				return BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:gap-correlation"),
					View: mustView(t, MachineViewSpec{Attachment: attachment,
						Consequences: []Consequence{ConsequenceCreateHandlings},
						Targets:      []ResolvedTarget{self}})}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BindIntent(test.spec(t)); !errors.Is(err, ErrInvariant) {
				t.Fatalf("BindIntent() error = %v, want ErrInvariant", err)
			}
		})
	}
}

func TestR7GapP02OpenLabelsAndClosedShapes(t *testing.T) {
	principal := mustPrincipal(t, "agent:gap-p02")
	attachment := mustAttachment(t, "attachment:gap-p02", principal, true)
	self, err := ResolveLocalTarget(SelfTarget(), principal)
	if err != nil {
		t.Fatalf("ResolveLocalTarget() error = %v", err)
	}

	t.Run("unregistered-kind", func(t *testing.T) {
		kind := mustLabel(t, "future.unregistered.capability.v937")
		intent, err := NewAgentIntent(IntentSpec{Kind: kind,
			Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget()}})
		if err != nil {
			t.Fatalf("NewAgentIntent() error = %v", err)
		}
		request, err := BindIntent(BoundIntentSpec{Intent: intent,
			OperationKey: mustOperation(t, "op:gap-open-kind"),
			View: mustView(t, MachineViewSpec{Attachment: attachment,
				Consequences: []Consequence{ConsequenceCreateHandlings},
				Targets:      []ResolvedTarget{self}})})
		if err != nil {
			t.Fatalf("BindIntent() error = %v", err)
		}
		if request.Intent().Kind() != kind {
			t.Fatalf("bound kind = %q, want %q", request.Intent().Kind().String(), kind.String())
		}
	})

	t.Run("unregistered-first-publish-key", func(t *testing.T) {
		operation := mustOperation(t, "op:gap-open-key")
		key := mustReferenceKey(t, "future-unregistered-reference-v937")
		artifact := mustCandidate(t, "candidate:gap-open-key")
		intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.reference.publish"),
			Consequence: ConsequencePublishReference, ReferenceKey: key,
			Artifacts: []ArtifactInput{artifact}})
		if err != nil {
			t.Fatalf("NewAgentIntent() error = %v", err)
		}
		request, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation,
			View: mustView(t, MachineViewSpec{Attachment: attachment,
				Consequences: []Consequence{ConsequencePublishReference}}),
			Candidates: []CapturedCandidate{mustCaptured(t, operation, artifact, "new reference")}})
		if err != nil {
			t.Fatalf("BindIntent() error = %v", err)
		}
		expected, exists := request.ExpectedReference()
		if !exists || !expected.IsAbsent() || expected.Key() != key {
			t.Fatalf("first-publish expectation = %#v, %v", expected, exists)
		}
	})

	artifact := mustCandidate(t, "candidate:gap-illegal-shape")
	invalid := []struct {
		name string
		spec IntentSpec
		want error
	}{
		{name: "unknown-consequence", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: Consequence(255), Successors: []TargetRef{SelfTarget()}}, want: ErrInvalid},
		{name: "root-without-successor", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: ConsequenceCreateHandlings}, want: ErrInvariant},
		{name: "root-with-subject", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: ConsequenceCreateHandlings, SubjectHandling: mustHandle(t, "subject:illegal"),
			Successors: []TargetRef{SelfTarget()}}, want: ErrInvariant},
		{name: "advance-without-subject", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: ConsequenceAdvanceHandling}, want: ErrInvariant},
		{name: "publish-with-successor", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: ConsequencePublishReference, ReferenceKey: mustReferenceKey(t, "illegal-publish"),
			Successors: []TargetRef{SelfTarget()}, Artifacts: []ArtifactInput{artifact}}, want: ErrInvariant},
		{name: "supersede-without-head", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: ConsequenceSupersedeReference, Artifacts: []ArtifactInput{artifact}}, want: ErrInvariant},
		{name: "retract-with-artifact", spec: IntentSpec{Kind: mustLabel(t, "future.invalid"),
			Consequence: ConsequenceRetractReference, ReferenceHead: mustHandle(t, "reference:illegal"),
			Artifacts: []ArtifactInput{artifact}}, want: ErrInvariant},
	}
	for _, test := range invalid {
		t.Run("illegal-"+test.name, func(t *testing.T) {
			if _, err := NewAgentIntent(test.spec); !errors.Is(err, test.want) {
				t.Fatalf("NewAgentIntent() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestR7GapP08InvalidReferenceKeysFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  error
	}{
		{name: "empty", value: "", want: ErrInvalid},
		{name: "leading-separator", value: "-playbook", want: ErrInvalid},
		{name: "uppercase", value: "Playbook.review", want: ErrInvalid},
		{name: "slash", value: "playbook/review", want: ErrInvalid},
		{name: "trailing-separator", value: "playbook.review-", want: ErrInvalid},
		{name: "too-long", value: strings.Repeat("a", MaxReferenceKeyBytes+1), want: ErrLimit},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewReferenceKey(test.value); !errors.Is(err, test.want) {
				t.Fatalf("NewReferenceKey(%q) error = %v, want %v", test.value, err, test.want)
			}
		})
	}
}

func TestR7GapP09SuccessorBoundFailsClosed(t *testing.T) {
	successors := make([]TargetRef, 0, MaxSuccessors+1)
	for index := 0; index <= MaxSuccessors; index++ {
		successors = append(successors,
			mustAliasTarget(t, fmt.Sprintf("target:gap-successor-%02d", index)))
	}

	if _, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.boundary.action"),
		Consequence: ConsequenceCreateHandlings,
		Successors:  append([]TargetRef(nil), successors[:MaxSuccessors]...)}); err != nil {
		t.Fatalf("NewAgentIntent(exact limit) error = %v", err)
	}
	if _, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.boundary.action"),
		Consequence: ConsequenceCreateHandlings,
		Successors:  successors}); !errors.Is(err, ErrLimit) {
		t.Fatalf("NewAgentIntent(MaxSuccessors+1) error = %v, want ErrLimit", err)
	}
}
