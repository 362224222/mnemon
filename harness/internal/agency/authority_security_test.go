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
	if firstView.Digest() != secondView.Digest() || !bytes.Equal(firstView.CanonicalJSON(), secondView.CanonicalJSON()) {
		t.Fatal("View canonicalization changed with offer order or short-lived Attachment envelope")
	}

	intent := mustRootIntent(t, []TargetRef{SelfTarget(), aliasRef})
	firstRequest, err := BindIntent(BoundIntentSpec{Intent: intent,
		OperationKey: mustOperation(t, "op:first"), View: firstView})
	if err != nil {
		t.Fatalf("BindIntent(first) error = %v", err)
	}
	secondRequest, err := BindIntent(BoundIntentSpec{Intent: intent,
		OperationKey: mustOperation(t, "op:second"), View: secondView})
	if err != nil {
		t.Fatalf("BindIntent(second) error = %v", err)
	}
	if firstRequest.RequestDigest() != secondRequest.RequestDigest() {
		t.Fatal("RequestDigest changed with operation or short-lived Attachment envelope")
	}
	if bytes.Equal(firstRequest.CanonicalJSON(), secondRequest.CanonicalJSON()) {
		t.Fatal("operation envelopes unexpectedly equal")
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

func TestTypedViewOffersCannotBeRepurposed(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:typed", principal, true)
	shared := mustHandle(t, "opaque:shared")
	intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "example.advance"),
		Consequence: ConsequenceAdvanceHandling, SubjectHandling: shared})
	if err != nil {
		t.Fatalf("NewAgentIntent() error = %v", err)
	}
	view := mustView(t, MachineViewSpec{
		Attachment: attachment, Consequences: []Consequence{ConsequenceAdvanceHandling},
		Artifacts:  []ViewArtifactOffer{mustViewOffer(t, shared, "offered bytes")},
		Provenance: []ProvenanceOffer{mustProvenance(t, shared, "event:shared", "shared event")},
	})
	if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:typed"),
		View: view}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("cross-type handle error = %v, want ErrInvariant", err)
	}

	requested := mustAliasTarget(t, "target:selected")
	exactPrincipal := mustPrincipal(t, "agent:selected")
	resolved, _ := ResolveLocalTarget(requested, exactPrincipal)
	root := mustRootIntent(t, []TargetRef{requested})
	request, err := BindIntent(BoundIntentSpec{Intent: root, OperationKey: mustOperation(t, "op:target"),
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{resolved}})})
	if err != nil {
		t.Fatalf("BindIntent(target) error = %v", err)
	}
	if got := request.Targets()[0].LocalPrincipal(); got != exactPrincipal {
		t.Fatalf("target Principal = %v, want sealed %v", got, exactPrincipal)
	}
}

func TestSelectedAliasesCannotDuplicateResolvedDestinations(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:destinations", principal, true)
	firstAlias := mustAliasTarget(t, "target:first")
	secondAlias := mustAliasTarget(t, "target:second")
	intent := mustRootIntent(t, []TargetRef{firstAlias, secondAlias})

	localDestination := mustPrincipal(t, "agent:destination")
	firstLocal, _ := ResolveLocalTarget(firstAlias, localDestination)
	secondLocal, _ := ResolveLocalTarget(secondAlias, localDestination)
	localView := mustView(t, MachineViewSpec{
		Attachment: attachment, Consequences: []Consequence{ConsequenceCreateHandlings},
		Targets: []ResolvedTarget{firstLocal, secondLocal},
	})
	if _, err := BindIntent(BoundIntentSpec{Intent: intent,
		OperationKey: mustOperation(t, "op:duplicate-local"), View: localView,
	}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("duplicate local destination error = %v, want ErrInvariant", err)
	}

	route := mustRoute(t, "route:destination")
	remoteAlias := mustHandle(t, "peer:destination")
	firstRemote, _ := ResolveRemoteTarget(firstAlias, route, remoteAlias)
	secondRemote, _ := ResolveRemoteTarget(secondAlias, route, remoteAlias)
	remoteView := mustView(t, MachineViewSpec{
		Attachment: attachment, Consequences: []Consequence{ConsequenceCreateHandlings},
		Targets: []ResolvedTarget{firstRemote, secondRemote},
	})
	if _, err := BindIntent(BoundIntentSpec{Intent: intent,
		OperationKey: mustOperation(t, "op:duplicate-remote"), View: remoteView,
	}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("duplicate remote destination error = %v, want ErrInvariant", err)
	}
}

func TestArtifactCapturesAreOperationScopedAndLaneExact(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:artifacts", principal, true)
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	first := mustCandidate(t, "candidate:first")
	second := mustCandidate(t, "candidate:second")
	intent := mustRootIntent(t, []TargetRef{SelfTarget()}, first, second)
	operation := mustOperation(t, "op:artifacts")
	view := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{self}})
	firstCapture := mustCaptured(t, operation, first, "first")
	secondCapture := mustCaptured(t, operation, second, "second")
	request, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation, View: view,
		Candidates: []CapturedCandidate{secondCapture, firstCapture}})
	if err != nil || len(request.Artifacts()) != 2 {
		t.Fatalf("reordered sealed captures = %#v, %v", request, err)
	}

	wrongOperation := mustCaptured(t, mustOperation(t, "op:other"), first, "first")
	if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation, View: view,
		Candidates: []CapturedCandidate{wrongOperation, secondCapture}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong-operation capture error = %v, want ErrInvalid", err)
	}
	if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation, View: view,
		Candidates: []CapturedCandidate{firstCapture}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing capture error = %v, want ErrInvariant", err)
	}
	unused := mustCaptured(t, operation, mustCandidate(t, "candidate:unused"), "unused")
	if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation, View: view,
		Candidates: []CapturedCandidate{firstCapture, secondCapture, unused}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("unused capture error = %v, want ErrInvariant", err)
	}
	duplicateFirst := mustCaptured(t, operation, first, "same")
	duplicateSecond := mustCaptured(t, operation, second, "same")
	if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation, View: view,
		Candidates: []CapturedCandidate{duplicateFirst, duplicateSecond}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate digest error = %v, want ErrInvalid", err)
	}

	subjectHandle := mustHandle(t, "handling:current")
	artifactHandle := mustHandle(t, "artifact:offered")
	viewInput, _ := NewArtifactViewHandle(artifactHandle)
	complete, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "example.complete"),
		Consequence: ConsequenceResolveCompleted, SubjectHandling: subjectHandle,
		Artifacts: []ArtifactInput{viewInput}})
	if err != nil {
		t.Fatalf("NewAgentIntent(complete) error = %v", err)
	}
	subject := mustSubject(t, subjectHandle, "handling:one", "event:subject", "subject", 1)
	completeView := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceResolveCompleted}, Subjects: []SubjectBinding{subject},
		Artifacts: []ViewArtifactOffer{mustViewOffer(t, artifactHandle, "evidence")}})
	if _, err := BindIntent(BoundIntentSpec{Intent: complete, OperationKey: mustOperation(t, "op:complete"),
		View: completeView}); err != nil {
		t.Fatalf("View Artifact binding error = %v", err)
	}
	candidateInput, _ := NewArtifactCandidate(artifactHandle)
	candidateCapture := mustCaptured(t, mustOperation(t, "op:cross-lane"), candidateInput, "evidence")
	missingView := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceResolveCompleted}, Subjects: []SubjectBinding{subject}})
	if _, err := BindIntent(BoundIntentSpec{Intent: complete, OperationKey: mustOperation(t, "op:cross-lane"),
		View: missingView, Candidates: []CapturedCandidate{candidateCapture}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("cross-lane Artifact error = %v, want ErrInvariant", err)
	}
}

func TestResolvedProvenanceEntersRequestAndEvent(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:provenance", principal, true)
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	firstHandle := mustHandle(t, "cause:first")
	secondHandle := mustHandle(t, "cause:second")
	correlationHandle := mustHandle(t, "correlation:root")
	first := mustProvenance(t, firstHandle, "event:cause-first", "cause-first")
	second := mustProvenance(t, secondHandle, "event:cause-second", "cause-second")
	correlation := mustProvenance(t, correlationHandle, "event:correlation", "correlation")
	intent, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "future.agent.action"),
		Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget()},
		CausationHandles: []OpaqueHandle{secondHandle, firstHandle}, CorrelationHandle: correlationHandle})
	if err != nil {
		t.Fatalf("NewAgentIntent() error = %v", err)
	}
	request, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:provenance"),
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{self},
			Provenance: []ProvenanceOffer{correlation, second, first}})})
	if err != nil {
		t.Fatalf("BindIntent() error = %v", err)
	}
	causation := request.Causation()
	gotCorrelation, exists := request.Correlation()
	if len(causation) != 2 || causation[0] != first.Event() || causation[1] != second.Event() ||
		!exists || gotCorrelation != correlation.Event() {
		t.Fatalf("resolved provenance = %#v, %#v, %v", causation, gotCorrelation, exists)
	}
	for _, eventRef := range append(causation, gotCorrelation) {
		if !bytes.Contains(request.CanonicalJSON(), []byte(eventRef.ID().String())) ||
			!bytes.Contains(request.CanonicalJSON(), []byte(eventRef.Digest().String())) {
			t.Fatalf("request wire lacks exact provenance %v", eventRef)
		}
	}
	event, err := NewEvent(request, EventStamp{ID: mustEventID(t, "event:provenance"),
		AcceptedAt: testTime, OriginSequence: 1})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if len(event.Causation()) != 2 {
		t.Fatalf("Event causation = %#v", event.Causation())
	}
	if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: mustOperation(t, "op:missing-provenance"),
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{self}})}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing provenance error = %v, want ErrInvariant", err)
	}
}

func TestRemoteEffectsRequireLocalResponsibilityAnchor(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:remote", principal, true)
	self, _ := ResolveLocalTarget(SelfTarget(), principal)
	remoteRef := mustAliasTarget(t, "target:remote")
	remote, _ := ResolveRemoteTarget(remoteRef, mustRoute(t, "route:peer"), mustHandle(t, "peer:agent"))
	remoteView := func(consequence Consequence, subject *SubjectBinding, includeLocal bool) ViewAuthority {
		t.Helper()
		spec := MachineViewSpec{Attachment: attachment, Consequences: []Consequence{consequence},
			Targets: []ResolvedTarget{remote}}
		if subject != nil {
			spec.Subjects = []SubjectBinding{*subject}
		}
		if includeLocal {
			spec.Targets = append(spec.Targets, self)
		}
		return mustView(t, spec)
	}

	rootRemote := mustRootIntent(t, []TargetRef{remoteRef})
	if _, err := BindIntent(BoundIntentSpec{Intent: rootRemote, OperationKey: mustOperation(t, "op:root-remote"),
		View: remoteView(ConsequenceCreateHandlings, nil, false)}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("remote-only root error = %v, want ErrInvariant", err)
	}
	rootAnchored := mustRootIntent(t, []TargetRef{remoteRef, SelfTarget()})
	if _, err := BindIntent(BoundIntentSpec{Intent: rootAnchored,
		OperationKey: mustOperation(t, "op:root-anchored"),
		View:         remoteView(ConsequenceCreateHandlings, nil, true)}); err != nil {
		t.Fatalf("anchored root error = %v", err)
	}

	subjectHandle := mustHandle(t, "handling:current")
	subject := mustSubject(t, subjectHandle, "handling:actual", "event:subject", "subject", 3)
	advance, _ := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "example.advance"),
		Consequence: ConsequenceAdvanceHandling, SubjectHandling: subjectHandle,
		Successors: []TargetRef{remoteRef}})
	if _, err := BindIntent(BoundIntentSpec{Intent: advance, OperationKey: mustOperation(t, "op:advance-remote"),
		View: remoteView(ConsequenceAdvanceHandling, &subject, false)}); err != nil {
		t.Fatalf("advance with current local anchor error = %v", err)
	}

	for _, consequence := range []Consequence{
		ConsequenceResolveCompleted, ConsequenceResolveDeclined, ConsequenceResolveUnresolved,
	} {
		spec := IntentSpec{Kind: mustLabel(t, "example.resolve"), Consequence: consequence,
			SubjectHandling: subjectHandle, Successors: []TargetRef{remoteRef}}
		if consequence == ConsequenceResolveCompleted {
			spec.Artifacts = []ArtifactInput{mustCandidate(t, "candidate:completion")}
		}
		intent, err := NewAgentIntent(spec)
		if err != nil {
			t.Fatalf("NewAgentIntent(%v) error = %v", consequence, err)
		}
		operation := mustOperation(t, "op:"+strings.ReplaceAll(consequence.String(), ".", "-"))
		if _, err := BindIntent(BoundIntentSpec{Intent: intent, OperationKey: operation,
			View: remoteView(consequence, &subject, false)}); !errors.Is(err, ErrInvariant) {
			t.Fatalf("remote-only %v error = %v, want ErrInvariant", consequence, err)
		}

		anchoredSpec := spec
		anchoredSpec.Successors = []TargetRef{remoteRef, SelfTarget()}
		anchoredIntent, err := NewAgentIntent(anchoredSpec)
		if err != nil {
			t.Fatalf("NewAgentIntent(anchored %v) error = %v", consequence, err)
		}
		anchoredOperation := mustOperation(t, "op:anchored-"+strings.ReplaceAll(consequence.String(), ".", "-"))
		var captures []CapturedCandidate
		if consequence == ConsequenceResolveCompleted {
			captures = []CapturedCandidate{mustCaptured(t, anchoredOperation,
				anchoredSpec.Artifacts[0], "completion evidence")}
		}
		if _, err := BindIntent(BoundIntentSpec{Intent: anchoredIntent, OperationKey: anchoredOperation,
			View: remoteView(consequence, &subject, true), Candidates: captures}); err != nil {
			t.Fatalf("anchored %v error = %v", consequence, err)
		}
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
	_, err = NewViewAuthority(MachineViewSpec{Attachment: mustAttachment(t, "attachment:large", principal, true),
		Consequences: []Consequence{ConsequenceCreateHandlings}, Provenance: provenance})
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
