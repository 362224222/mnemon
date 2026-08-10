package agency

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestViewAuthorityIsCanonicalAndEnvelopeIndependent(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	firstAttachment := mustAttachment(t, "attachment:first", principal, true)
	secondAttachment, err := NewAttachment(mustAttachmentID(t, "attachment:second"), principal, true,
		testTime.Add(time.Minute), testTime.Add(11*time.Minute))
	if err != nil {
		t.Fatalf("NewAttachment() error = %v", err)
	}
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	aliasRef := mustAliasTarget(t, "target:local-helper")
	alias, _ := ResolveLocalTarget(aliasRef, mustPrincipal(t, "agent:helper"))
	firstReferenceHandle := mustHandle(t, "reference:first")
	secondReferenceHandle := mustHandle(t, "reference:second")
	firstArtifactHandle := mustHandle(t, "artifact:first")
	secondArtifactHandle := mustHandle(t, "artifact:second")
	firstProvenanceHandle := mustHandle(t, "provenance:first")
	secondProvenanceHandle := mustHandle(t, "provenance:second")

	firstSpec := MachineViewSpec{
		Attachment: firstAttachment,
		Consequences: []Consequence{
			ConsequenceAdvanceHandling, ConsequenceCreateHandlings,
		},
		References: []ReferenceExpectation{
			mustReference(t, firstReferenceHandle, "knowledge-first", "event:ref-one", "ref-one"),
			mustReference(t, secondReferenceHandle, "knowledge-second", "event:ref-two", "ref-two"),
		},
		Targets: []ResolvedTarget{self, alias},
		Artifacts: []ViewArtifactOffer{
			mustViewOffer(t, firstArtifactHandle, "artifact-one"),
			mustViewOffer(t, secondArtifactHandle, "artifact-two"),
		},
		Provenance: []ProvenanceOffer{
			mustProvenance(t, firstProvenanceHandle, "event:cause-one", "cause-one"),
			mustProvenance(t, secondProvenanceHandle, "event:cause-two", "cause-two"),
		},
	}
	secondSpec := MachineViewSpec{
		Attachment: secondAttachment,
		Consequences: []Consequence{
			ConsequenceCreateHandlings, ConsequenceAdvanceHandling,
		},
		References: []ReferenceExpectation{firstSpec.References[1], firstSpec.References[0]},
		Targets:    []ResolvedTarget{alias, self},
		Artifacts:  []ViewArtifactOffer{firstSpec.Artifacts[1], firstSpec.Artifacts[0]},
		Provenance: []ProvenanceOffer{firstSpec.Provenance[1], firstSpec.Provenance[0]},
	}
	firstView := mustView(t, firstSpec)
	secondView := mustView(t, secondSpec)
	if firstView.Digest() != secondView.Digest() ||
		!bytes.Equal(firstView.CanonicalJSON(), secondView.CanonicalJSON()) {
		t.Fatal("View canonicalization changed with offer order or short-lived Attachment envelope")
	}

	changedAlias, _ := ResolveLocalTarget(aliasRef, mustPrincipal(t, "agent:other"))
	changedSpec := secondSpec
	changedSpec.Targets = []ResolvedTarget{self, changedAlias}
	changedView := mustView(t, changedSpec)
	if changedView.Digest() == firstView.Digest() {
		t.Fatal("View digest did not bind exact target resolution")
	}
	wrongSelf, _ := ResolveLocalTarget(SelfTarget(), mustPrincipal(t, "agent:not-self"))
	if _, err := NewViewAuthority(MachineViewSpec{Attachment: firstAttachment,
		Targets: []ResolvedTarget{wrongSelf}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("wrong self resolution error = %v, want ErrInvariant", err)
	}
}

func TestTargetAliasCannotCollideWithSelfSentinel(t *testing.T) {
	selfAlias, err := NewOpaqueHandle("self")
	if err != nil {
		t.Fatalf("NewOpaqueHandle(self) error = %v", err)
	}
	if _, err := AliasTarget(selfAlias); !errors.Is(err, ErrInvalid) {
		t.Fatalf("AliasTarget(self) error = %v, want ErrInvalid", err)
	}

	raw := []byte(`{"kind":"agent.request","payload":"","consequence":"handling.create","successors":[{"alias":"self"}]}`)
	if _, err := ParseAgentIntentJSON(raw); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseAgentIntentJSON(self alias) error = %v, want ErrInvalid", err)
	}
}

func TestReceiptBindsExactOperationAndMonotonicTime(t *testing.T) {
	first := mustBoundRoot(t, "op:first")
	second := mustBoundRoot(t, "op:second")
	if first.RequestDigest() != second.RequestDigest() {
		t.Fatal("fixtures must differ only by operation key")
	}
	event, err := NewEvent(first, EventStamp{ID: mustEventID(t, "event:first"),
		AcceptedAt: testTime, OriginSequence: 1})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if _, err := NewAcceptedReceipt(second, event, testTime.Add(time.Second)); !errors.Is(err, ErrInvariant) {
		t.Fatalf("operation-mismatch Receipt error = %v, want ErrInvariant", err)
	}
	if _, err := NewAcceptedReceipt(first, event, testTime.Add(-time.Nanosecond)); !errors.Is(err, ErrInvariant) {
		t.Fatalf("backdated Receipt error = %v, want ErrInvariant", err)
	}
	if _, err := NewAcceptedReceipt(first, event, testTime); err != nil {
		t.Fatalf("same-time Receipt error = %v", err)
	}
	if !bytes.Contains(event.CanonicalJSON(), []byte(`"operation_key":"op:first"`)) {
		t.Fatalf("Event does not bind operation key: %s", event.CanonicalJSON())
	}
}

func TestCanonicalObjectsHaveHardTotalByteLimits(t *testing.T) {
	successors := make([]TargetRef, 0, MaxSuccessors)
	artifacts := make([]ArtifactInput, 0, MaxArtifactInputs)
	causation := make([]OpaqueHandle, 0, MaxCausationHandles)
	for index := 0; index < MaxSuccessors; index++ {
		successors = append(successors, mustAliasTarget(t, longToken("target", index, MaxOpaqueHandleBytes)))
	}
	for index := 0; index < MaxArtifactInputs; index++ {
		artifacts = append(artifacts, mustCandidate(t, longToken("artifact", index, MaxOpaqueHandleBytes)))
	}
	for index := 0; index < MaxCausationHandles; index++ {
		causation = append(causation, mustHandle(t, longToken("cause", index, MaxOpaqueHandleBytes)))
	}
	_, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.agent.action"),
		Payload:     mustPayload(t, strings.Repeat("p", MaxSemanticPayloadBytes)),
		Consequence: ConsequenceCreateHandlings, Successors: successors, Artifacts: artifacts,
		CausationHandles: causation})
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized canonical Intent error = %v, want ErrLimit", err)
	}

	principal := mustPrincipal(t, "agent:local")
	provenance := make([]ProvenanceOffer, 0, MaxViewHandles)
	for index := 0; index < MaxViewHandles; index++ {
		handle := mustHandle(t, longToken("provenance", index, MaxOpaqueHandleBytes))
		eventID := mustEventID(t, longToken("event", index, MaxOpaqueHandleBytes))
		event, eventErr := NewEventRef(eventID, Sum([]byte(fmt.Sprintf("event-%d", index))))
		if eventErr != nil {
			t.Fatalf("NewEventRef() error = %v", eventErr)
		}
		offer, offerErr := NewProvenanceOffer(handle, event)
		if offerErr != nil {
			t.Fatalf("NewProvenanceOffer() error = %v", offerErr)
		}
		provenance = append(provenance, offer)
	}
	_, err = NewViewAuthority(MachineViewSpec{
		Attachment:   mustAttachment(t, "attachment:large", principal, true),
		Consequences: []Consequence{ConsequenceCreateHandlings}, Provenance: provenance,
	})
	if !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized canonical View error = %v, want ErrLimit", err)
	}
}

func TestViewOfferCountsFailClosedBeforeUse(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:bounds", principal, true)
	consequences := make([]Consequence, MaxViewConsequences+1)
	for index := range consequences {
		consequences[index] = ConsequenceCreateHandlings
	}
	if _, err := NewViewAuthority(MachineViewSpec{Attachment: attachment,
		Consequences: consequences}); !errors.Is(err, ErrLimit) {
		t.Fatalf("consequence limit error = %v, want ErrLimit", err)
	}
	subjectHandle := mustHandle(t, "handling:duplicate")
	subject := mustSubject(t, subjectHandle, "handling:one", "event:one", "one", 1)
	if _, err := NewViewAuthority(MachineViewSpec{Attachment: attachment,
		Subjects: []SubjectBinding{subject, subject}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate subject error = %v, want ErrInvalid", err)
	}
	targets := make([]ResolvedTarget, 0, MaxViewTargets+1)
	for index := 0; index <= MaxViewTargets; index++ {
		requested := mustAliasTarget(t, fmt.Sprintf("target:%d", index))
		resolved, _ := ResolveLocalTarget(requested, principal)
		targets = append(targets, resolved)
	}
	if _, err := NewViewAuthority(MachineViewSpec{Attachment: attachment,
		Targets: targets}); !errors.Is(err, ErrLimit) {
		t.Fatalf("target limit error = %v, want ErrLimit", err)
	}
	tooMany := make([]ProvenanceOffer, 0, MaxViewHandles+1)
	for index := 0; index <= MaxViewHandles; index++ {
		tooMany = append(tooMany, mustProvenance(t, mustHandle(t, fmt.Sprintf("source:%d", index)),
			fmt.Sprintf("event:%d", index), fmt.Sprintf("body-%d", index)))
	}
	if _, err := NewViewAuthority(MachineViewSpec{Attachment: attachment,
		Provenance: tooMany}); !errors.Is(err, ErrLimit) {
		t.Fatalf("handle limit error = %v, want ErrLimit", err)
	}
}

func longToken(prefix string, index, length int) string {
	suffix := fmt.Sprintf("%d", index)
	padding := length - len(prefix) - len(suffix) - 1
	return prefix + ":" + strings.Repeat("a", padding) + suffix
}
