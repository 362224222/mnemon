package model

import "testing"

type eventTypePolicy struct {
	event                EventType
	operation            OperationKind
	participantResponse  EventType
	home, participant    bool
	agent, controller    bool
	artifacts, causality bool
}

func TestEventTypeIsClosed(t *testing.T) {
	t.Parallel()

	want := []eventTypePolicy{
		{EventReviewOffered, OperationTeamworkOffer, "", true, false, true, false, true, false},
		{EventReviewAcceptRequested, OperationTeamworkAccept, EventReviewAccepted,
			false, true, true, false, false, true},
		{EventReviewDeclineRequested, OperationTeamworkDecline, EventReviewDeclined,
			false, true, true, false, false, true},
		{EventReviewDeliveryReady, OperationTeamworkDeliver, EventReviewDelivered,
			false, true, true, false, true, true},
		{EventReviewAccepted, "", "", true, false, false, true, false, true},
		{EventReviewAcceptRejected, "", "", true, false, false, true, false, true},
		{EventReviewDelivered, "", "", true, false, false, true, true, true},
		{EventReviewReworkRequested, OperationTeamworkRework, "",
			true, false, true, false, true, true},
		{EventReviewClosed, OperationTeamworkClose, "", true, false, true, false, false, true},
		{EventReviewDeclined, "", "", true, false, false, true, false, true},
		{EventReviewCancelled, OperationTeamworkCancel, "", true, false, true, false, false, true},
		{EventReviewExpired, "", "", true, false, false, true, false, true},
		{EventReviewOutcome, "", "", false, false, false, true, false, true},
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
	for _, expected := range want {
		if expected.operation == "" {
			continue
		}
		got, valid := EventTypeForAgentOperation(expected.operation)
		if !valid || got != expected.event {
			t.Fatalf("EventTypeForAgentOperation(%s) = (%s, %t)", expected.operation, got, valid)
		}
	}
	if _, valid := EventTypeForAgentOperation(""); valid {
		t.Fatal("zero operation resolved an Agent Event")
	}
	if _, valid := EventTypeForAgentOperation(OperationResolveRetry); valid {
		t.Fatal("resolve operation resolved an Agent Event")
	}
}

func TestEventTypeDescriptorRejectsHiddenParticipantResponse(t *testing.T) {
	t.Parallel()
	nonParticipant, _ := EventReviewOffered.Descriptor()
	nonParticipant.participantResponse = EventReviewDelivered
	participant, _ := EventReviewAcceptRequested.Descriptor()
	participant.participantResponse = ""
	if validParticipantEventResponse(nonParticipant) || validParticipantEventResponse(participant) {
		t.Fatal("descriptor accepted a hidden or missing participant response")
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
	operation, hasOperation := descriptor.AgentOperation()
	response, hasResponse := descriptor.ParticipantResponse()
	if operation != expected.operation || hasOperation != (expected.operation != "") ||
		response != expected.participantResponse || hasResponse != (expected.participantResponse != "") {
		t.Fatalf("EventType descriptor %d action binding = (%s, %t, %s, %t)",
			index, operation, hasOperation, response, hasResponse)
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
	operation, hasOperation := expected.event.AgentOperation()
	response, hasResponse := expected.event.ParticipantResponse()
	if operation != expected.operation || hasOperation != (expected.operation != "") ||
		response != expected.participantResponse || hasResponse != (expected.participantResponse != "") {
		t.Fatalf("EventType(%q) action projection = (%s, %t, %s, %t)",
			expected.event, operation, hasOperation, response, hasResponse)
	}
}

func assertUnknownEventType(t *testing.T, value EventType) {
	t.Helper()
	if _, valid := value.Descriptor(); valid || value.Valid() || value.HomeAuthoritative() ||
		value.ParticipantInput() || value.AgentAdmitted() || value.ControllerAdmitted() ||
		value.AllowsArtifacts() || value.RequiresAdmissionCausality() {
		t.Fatalf("unknown EventType(%q) acquired descriptor policy", value)
	}
	if _, valid := value.AgentOperation(); valid {
		t.Fatalf("unknown EventType(%q) acquired Agent operation", value)
	}
	if _, valid := value.ParticipantResponse(); valid {
		t.Fatalf("unknown EventType(%q) acquired participant response", value)
	}
}
