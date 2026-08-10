package agency

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestR7GapP02OpenLabelsAndClosedShapes(t *testing.T) {
	kind := mustLabel(t, "future.unregistered.capability.v937")
	intent, err := NewAgentIntent(IntentSpec{Kind: kind,
		Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget()}})
	if err != nil {
		t.Fatalf("NewAgentIntent(unregistered kind) error = %v", err)
	}
	if intent.Kind() != kind {
		t.Fatalf("Intent kind = %q, want %q", intent.Kind().String(), kind.String())
	}

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
