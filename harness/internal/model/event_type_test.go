package model

import "testing"

type eventTypePolicy struct {
	event                EventType
	home, participant    bool
	agent, controller    bool
	artifacts, causality bool
}

func TestEventTypeIsClosed(t *testing.T) {
	t.Parallel()

	want := []eventTypePolicy{
		{EventReviewOffered, true, false, true, false, true, false},
		{EventReviewAcceptRequested, false, true, true, false, false, true},
		{EventReviewDeclineRequested, false, true, true, false, false, true},
		{EventReviewDeliveryReady, false, true, true, false, true, true},
		{EventReviewAccepted, true, false, false, true, false, true},
		{EventReviewAcceptRejected, true, false, false, true, false, true},
		{EventReviewDelivered, true, false, false, true, true, true},
		{EventReviewReworkRequested, true, false, true, false, true, true},
		{EventReviewClosed, true, false, true, false, false, true},
		{EventReviewDeclined, true, false, false, true, false, true},
		{EventReviewCancelled, true, false, true, false, false, true},
		{EventReviewExpired, true, false, false, true, false, true},
		{EventReviewOutcome, false, false, false, true, false, true},
	}
	descriptors := EventTypeDescriptors()
	if len(descriptors) != len(want) {
		t.Fatalf("EventTypeDescriptors() length = %d, want %d", len(descriptors), len(want))
	}
	seen := make(map[EventType]struct{}, len(descriptors))
	for index, expected := range want {
		descriptor := descriptors[index]
		assertEventTypeDescriptor(t, index, descriptor, expected)
		if _, duplicate := seen[expected.event]; duplicate {
			t.Fatalf("duplicate EventType descriptor %q", expected.event)
		}
		seen[expected.event] = struct{}{}
		assertEventTypeProjection(t, descriptor, expected)
	}
	for _, value := range []EventType{"", "memory.updated", "review.generic"} {
		assertUnknownEventType(t, value)
	}

	// The exported projection is a copy: even same-package mutation cannot
	// change the canonical registry or its deterministic order.
	descriptors[0] = EventTypeDescriptor{}
	if got := EventTypeDescriptors()[0].Type(); got != EventReviewOffered {
		t.Fatalf("mutating descriptor projection changed authority to %q", got)
	}
}

func assertEventTypeDescriptor(t *testing.T, index int, descriptor EventTypeDescriptor,
	expected eventTypePolicy,
) {
	t.Helper()
	if descriptor.Type() != expected.event ||
		descriptor.HomeAuthoritative() != expected.home ||
		descriptor.ParticipantInput() != expected.participant ||
		descriptor.AgentAdmitted() != expected.agent ||
		descriptor.ControllerAdmitted() != expected.controller ||
		descriptor.AllowsArtifacts() != expected.artifacts ||
		descriptor.RequiresAdmissionCausality() != expected.causality {
		t.Fatalf("EventType descriptor %d = %#v, want %#v", index, descriptor, expected)
	}
}

func assertEventTypeProjection(t *testing.T, descriptor EventTypeDescriptor,
	expected eventTypePolicy,
) {
	t.Helper()
	lookedUp, valid := expected.event.Descriptor()
	if !valid || lookedUp != descriptor || !expected.event.Valid() ||
		expected.event.HomeAuthoritative() != expected.home ||
		expected.event.ParticipantInput() != expected.participant ||
		expected.event.AgentAdmitted() != expected.agent ||
		expected.event.ControllerAdmitted() != expected.controller ||
		expected.event.AllowsArtifacts() != expected.artifacts ||
		expected.event.RequiresAdmissionCausality() != expected.causality {
		t.Fatalf("EventType(%q) does not derive from its descriptor", expected.event)
	}
}

func assertUnknownEventType(t *testing.T, value EventType) {
	t.Helper()
	if _, valid := value.Descriptor(); valid || value.Valid() || value.HomeAuthoritative() ||
		value.ParticipantInput() || value.AgentAdmitted() || value.ControllerAdmitted() ||
		value.AllowsArtifacts() || value.RequiresAdmissionCausality() {
		t.Fatalf("unknown EventType(%q) acquired descriptor policy", value)
	}
}
