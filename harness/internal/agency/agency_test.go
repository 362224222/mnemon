package agency

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDigestUsesOneCanonicalSyntax(t *testing.T) {
	digest := Sum([]byte("artifact"))
	if !strings.HasPrefix(digest.String(), "sha256:") || len(digest.String()) != len("sha256:")+64 {
		t.Fatalf("Digest.String() = %q", digest.String())
	}
	parsed, err := ParseDigest(digest.String())
	if err != nil || parsed != digest {
		t.Fatalf("ParseDigest() = %v, %v", parsed, err)
	}
	for _, value := range []string{
		strings.TrimPrefix(digest.String(), "sha256:"),
		"SHA256:" + strings.TrimPrefix(digest.String(), "sha256:"),
		"sha256:" + strings.ToUpper(strings.TrimPrefix(digest.String(), "sha256:")),
	} {
		if _, err := ParseDigest(value); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ParseDigest(%q) error = %v, want ErrInvalid", value, err)
		}
	}
}

func TestAgentIntentKeepsLabelsOpenAndConsequencesClosed(t *testing.T) {
	kind := mustLabel(t, "future.agent.action")
	intent, err := NewAgentIntent(IntentSpec{
		Kind: kind, Payload: mustPayload(t, "Agent-defined semantics remain opaque."),
		Consequence: ConsequenceCreateHandlings, Successors: []TargetRef{SelfTarget()},
	})
	if err != nil {
		t.Fatalf("NewAgentIntent() error = %v", err)
	}
	if intent.Kind() != kind || intent.Consequence() != ConsequenceCreateHandlings {
		t.Fatalf("Intent = %#v", intent)
	}
	for _, forbidden := range []string{"operation_key", "attachment_id", "source_principal", "fence"} {
		if bytes.Contains(intent.CanonicalJSON(), []byte(forbidden)) {
			t.Fatalf("Agent canonical JSON contains machine field %q: %s", forbidden, intent.CanonicalJSON())
		}
	}

	invalidSpecs := []IntentSpec{
		{Kind: kind, Consequence: ConsequenceInvalid, Successors: []TargetRef{SelfTarget()}},
		{Kind: kind, Consequence: ConsequenceCreateHandlings},
		{Kind: kind, Consequence: ConsequenceAdvanceHandling},
		{Kind: kind, Consequence: ConsequencePublishReference,
			ReferenceKey: mustReferenceKey(t, "knowledge-guide")},
		{Kind: kind, Consequence: ConsequenceRetractReference,
			ReferenceHead: mustHandle(t, "reference:head"), Artifacts: []ArtifactInput{mustCandidate(t, "artifact:candidate")}},
	}
	for index, spec := range invalidSpecs {
		if _, err := NewAgentIntent(spec); err == nil {
			t.Fatalf("invalid spec %d unexpectedly succeeded", index)
		}
	}
}

func TestIntentRejectsDuplicateInputs(t *testing.T) {
	kind := mustLabel(t, "future.agent.action")
	if _, err := NewAgentIntent(IntentSpec{Kind: kind, Consequence: ConsequenceCreateHandlings,
		Successors: []TargetRef{SelfTarget(), SelfTarget()}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate successor error = %v, want ErrInvalid", err)
	}
	artifact := mustCandidate(t, "candidate:one")
	if _, err := NewAgentIntent(IntentSpec{Kind: kind, Consequence: ConsequenceCreateHandlings,
		Successors: []TargetRef{SelfTarget()}, Artifacts: []ArtifactInput{artifact, artifact}}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate Artifact input error = %v, want ErrInvalid", err)
	}
}

func TestBindIntentUsesOnlySealedViewAuthority(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:one", principal, true)
	target, err := ResolveLocalTarget(SelfTarget(), principal)
	if err != nil {
		t.Fatalf("ResolveLocalTarget() error = %v", err)
	}
	view := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{target}})
	request, err := BindIntent(BoundIntentSpec{Intent: mustRootIntent(t, []TargetRef{SelfTarget()}),
		OperationKey: mustOperation(t, "op:one"), View: view})
	if err != nil {
		t.Fatalf("BindIntent() error = %v", err)
	}
	if request.Attachment() != attachment || request.Targets()[0].LocalPrincipal() != principal {
		t.Fatalf("request did not retain exact View authority")
	}
	if !bytes.Contains(request.CanonicalJSON(), []byte(`"source_principal":"agent:local"`)) {
		t.Fatalf("Bound canonical JSON lacks derived source: %s", request.CanonicalJSON())
	}

	unoffered := mustView(t, MachineViewSpec{Attachment: attachment,
		Consequences: []Consequence{ConsequenceAdvanceHandling}, Targets: []ResolvedTarget{target}})
	if _, err := BindIntent(BoundIntentSpec{Intent: mustRootIntent(t, []TargetRef{SelfTarget()}),
		OperationKey: mustOperation(t, "op:two"), View: unoffered}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("unoffered consequence error = %v, want ErrInvariant", err)
	}

	managed := mustAttachment(t, "attachment:managed", principal, false)
	managedView := mustView(t, MachineViewSpec{Attachment: managed,
		Consequences: []Consequence{ConsequenceCreateHandlings}, Targets: []ResolvedTarget{target}})
	if _, err := BindIntent(BoundIntentSpec{Intent: mustRootIntent(t, []TargetRef{SelfTarget()}),
		OperationKey: mustOperation(t, "op:three"), View: managedView}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("managed root error = %v, want ErrInvariant", err)
	}
}

func TestReferenceAndSubjectBindingsRemainExact(t *testing.T) {
	principal := mustPrincipal(t, "agent:local")
	attachment := mustAttachment(t, "attachment:effects", principal, true)

	key := mustReferenceKey(t, "knowledge-guide")
	artifact := mustCandidate(t, "candidate:guide")
	publish, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "knowledge.publish"),
		Consequence: ConsequencePublishReference, ReferenceKey: key,
		Artifacts: []ArtifactInput{artifact}})
	if err != nil {
		t.Fatalf("NewAgentIntent(publish) error = %v", err)
	}
	operation := mustOperation(t, "op:publish")
	publishRequest, err := BindIntent(BoundIntentSpec{Intent: publish, OperationKey: operation,
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequencePublishReference}}),
		Candidates: []CapturedCandidate{mustCaptured(t, operation, artifact, "guide bytes")}})
	if err != nil {
		t.Fatalf("BindIntent(publish) error = %v", err)
	}
	expected, exists := publishRequest.ExpectedReference()
	if !exists || !expected.IsAbsent() || expected.Key() != key {
		t.Fatalf("first publish expectation = %#v, %v", expected, exists)
	}

	subjectHandle := mustHandle(t, "handling:current")
	subject := mustSubject(t, subjectHandle, "handling:actual", "event:head", "head", 7)
	advance, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "example.advance"),
		Consequence: ConsequenceAdvanceHandling, SubjectHandling: subjectHandle})
	if err != nil {
		t.Fatalf("NewAgentIntent(advance) error = %v", err)
	}
	advanceRequest, err := BindIntent(BoundIntentSpec{Intent: advance, OperationKey: mustOperation(t, "op:advance"),
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequenceAdvanceHandling}, Subjects: []SubjectBinding{subject}})})
	if err != nil {
		t.Fatalf("BindIntent(advance) error = %v", err)
	}
	gotSubject, exists := advanceRequest.Subject()
	if !exists || gotSubject != subject {
		t.Fatalf("subject = %#v, %v; want %#v", gotSubject, exists, subject)
	}

	referenceHandle := mustHandle(t, "reference:current")
	reference := mustReference(t, referenceHandle, "knowledge-guide", "event:reference-head", "reference-head")
	next := mustCandidate(t, "candidate:next-guide")
	supersede, err := NewAgentIntent(IntentSpec{Kind: mustLabel(t, "knowledge.update"),
		Consequence: ConsequenceSupersedeReference, ReferenceHead: referenceHandle,
		Artifacts: []ArtifactInput{next}})
	if err != nil {
		t.Fatalf("NewAgentIntent(supersede) error = %v", err)
	}
	nextOperation := mustOperation(t, "op:supersede")
	supersedeRequest, err := BindIntent(BoundIntentSpec{Intent: supersede, OperationKey: nextOperation,
		View: mustView(t, MachineViewSpec{Attachment: attachment,
			Consequences: []Consequence{ConsequenceSupersedeReference}, References: []ReferenceExpectation{reference}}),
		Candidates: []CapturedCandidate{mustCaptured(t, nextOperation, next, "next guide")}})
	if err != nil {
		t.Fatalf("BindIntent(supersede) error = %v", err)
	}
	gotReference, exists := supersedeRequest.ExpectedReference()
	if !exists || gotReference != reference {
		t.Fatalf("Reference = %#v, %v; want %#v", gotReference, exists, reference)
	}
}

func TestEventSeparatesMachineSemanticAndEvidence(t *testing.T) {
	request := mustBoundRoot(t, "op:event")
	event, err := NewEvent(request, EventStamp{ID: mustEventID(t, "event:accepted"),
		AcceptedAt: testTime, OriginSequence: 1})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if event.Source() != request.Attachment().Principal() || event.OperationKey() != request.OperationKey() ||
		event.RequestDigest() != request.RequestDigest() || event.Kind() != request.Intent().Kind() {
		t.Fatal("Event authority or semantics disagree with request")
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(event.CanonicalJSON(), &wire); err != nil {
		t.Fatalf("json.Unmarshal(Event) error = %v", err)
	}
	for _, section := range []string{"machine", "semantic", "evidence"} {
		if len(wire[section]) == 0 {
			t.Fatalf("Event lacks %q section: %s", section, event.CanonicalJSON())
		}
	}
	if bytes.Contains(event.CanonicalJSON(), []byte(`"intent"`)) ||
		bytes.Count(event.CanonicalJSON(), []byte(request.Intent().Kind().String())) != 1 ||
		bytes.Count(event.CanonicalJSON(), []byte(request.Intent().Payload().String())) != 1 {
		t.Fatalf("Event repeats or nests semantic content: %s", event.CanonicalJSON())
	}
}

func TestEventAndReceiptCanonicalRebuild(t *testing.T) {
	request := mustBoundRoot(t, "op:event-rebuild")
	event, err := NewEvent(request, EventStamp{ID: mustEventID(t, "event:rebuild"),
		AcceptedAt: testTime, OriginSequence: 1})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	rebuilt, err := NewEvent(request, EventStamp{ID: event.ID(), AcceptedAt: testTime, OriginSequence: 1})
	if err != nil || !bytes.Equal(event.CanonicalJSON(), rebuilt.CanonicalJSON()) || event.Digest() != rebuilt.Digest() {
		t.Fatalf("canonical Event rebuild mismatch: %v", err)
	}
	accepted, err := NewAcceptedReceipt(request, event, testTime.Add(time.Second))
	if err != nil || accepted.Outcome() != ReceiptOutcomeAccepted {
		t.Fatalf("NewAcceptedReceipt() = %#v, %v", accepted, err)
	}
	replay, err := NewAcceptedReceipt(request, event, testTime.Add(time.Second))
	if err != nil || !bytes.Equal(accepted.CanonicalJSON(), replay.CanonicalJSON()) {
		t.Fatalf("Receipt replay bytes differ: %v", err)
	}
	rejected, err := NewRejectedReceipt(request, mustLabel(t, "stale.view"),
		"The offered authority is stale.", testTime.Add(time.Second))
	if err != nil {
		t.Fatalf("NewRejectedReceipt() error = %v", err)
	}
	if _, exists := rejected.Event(); exists || rejected.Outcome() != ReceiptOutcomeRejected {
		t.Fatal("rejected Receipt unexpectedly carries Event")
	}
}

func TestCanonicalTimeRequiresBothWireRoundTrips(t *testing.T) {
	zone := time.FixedZone("test-offset", 9*60*60)
	input := time.Date(2026, time.August, 3, 17, 2, 3, 456789123, zone)
	canonical, err := canonicalTime("test time", input)
	if err != nil {
		t.Fatalf("canonicalTime() error = %v", err)
	}
	if canonical.Location() != time.UTC || canonical.UnixNano() != input.UnixNano() ||
		canonical.Format(time.RFC3339Nano) != "2026-08-03T08:02:03.456789123Z" {
		t.Fatalf("canonicalTime() = %v", canonical)
	}
	if _, err := canonicalTime("test time", time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("out-of-range time error = %v, want ErrInvalid", err)
	}
}

func FuzzSemanticValues(f *testing.F) {
	f.Add("future.agent.action", "bounded payload")
	f.Add("BAD KIND", string([]byte{0xff}))
	f.Fuzz(func(t *testing.T, labelValue, payloadValue string) {
		label, labelErr := NewSemanticLabel(labelValue)
		if labelErr == nil && (label.IsZero() || len(label.String()) > MaxSemanticLabelBytes) {
			t.Fatalf("accepted invalid label %q", label.String())
		}
		payload, payloadErr := NewSemanticPayload(payloadValue)
		if payloadErr == nil && len(payload.String()) > MaxSemanticPayloadBytes {
			t.Fatal("accepted oversized payload")
		}
	})
}
