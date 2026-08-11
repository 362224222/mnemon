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

func TestEventSeparatesMachineSemanticAndEvidence(t *testing.T) {
	request := mustBoundRoot(t, "op:event")
	event, err := NewEvent(request, EventStamp{ID: mustEventID(t, "event:accepted"),
		AcceptedAt: testTime, OriginSequence: 1, CausalDepth: 3})
	if err != nil {
		t.Fatalf("NewEvent() error = %v", err)
	}
	if event.Source() != request.Attachment().Principal() || event.OperationKey() != request.OperationKey() ||
		event.RequestDigest() != request.RequestDigest() || event.Kind() != request.Intent().Kind() ||
		event.CausalDepth() != 3 {
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
	if !bytes.Contains(event.CanonicalJSON(), []byte(`"schema_version":3`)) ||
		!bytes.Contains(event.CanonicalJSON(), []byte(`"causal_depth":3`)) {
		t.Fatalf("Event does not explicitly version canonical causal depth: %s", event.CanonicalJSON())
	}
	if bytes.Contains(event.CanonicalJSON(), []byte(`"intent"`)) ||
		bytes.Count(event.CanonicalJSON(), []byte(request.Intent().Kind().String())) != 1 ||
		bytes.Count(event.CanonicalJSON(), []byte(request.Intent().Payload().String())) != 1 {
		t.Fatalf("Event repeats or nests semantic content: %s", event.CanonicalJSON())
	}
	if _, err := NewEvent(request, EventStamp{ID: mustEventID(t, "event:too-deep"),
		AcceptedAt: testTime, OriginSequence: 2, CausalDepth: MaxPeerCausalDepth + 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("NewEvent(excess depth) error = %v, want ErrInvalid", err)
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
