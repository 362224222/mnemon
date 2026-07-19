package model

import "strings"

type EventType string

const (
	EventReviewOffered          EventType = "review.offered"
	EventReviewAcceptRequested  EventType = "review.accept.requested"
	EventReviewDeclineRequested EventType = "review.decline.requested"
	EventReviewDeliveryReady    EventType = "review.delivery.ready"
	EventReviewAccepted         EventType = "review.accepted"
	EventReviewAcceptRejected   EventType = "review.accept_rejected"
	EventReviewDelivered        EventType = "review.delivered"
	EventReviewReworkRequested  EventType = "review.rework_requested"
	EventReviewClosed           EventType = "review.closed"
	EventReviewDeclined         EventType = "review.declined"
	EventReviewCancelled        EventType = "review.cancelled"
	EventReviewExpired          EventType = "review.expired"
	EventReviewOutcome          EventType = "review.outcome"
)

type eventScopeAuthority uint8

const (
	eventScopeHome eventScopeAuthority = iota + 1
	eventScopeParticipant
	eventScopeFlexible
)

type eventAdmissionAuthority uint8

const (
	eventAdmissionAgent eventAdmissionAuthority = iota + 1
	eventAdmissionController
)

// EventTypeDescriptor is the immutable policy source for one closed EventType.
// Its private fields allow inspection without granting mutation authority.
type EventTypeDescriptor struct {
	eventType                  EventType
	scopeAuthority             eventScopeAuthority
	admissionAuthority         eventAdmissionAuthority
	agentOperation             OperationKind
	participantResponse        EventType
	allowsArtifacts            bool
	requiresAdmissionCausality bool
}

var eventTypeDescriptors = [...]EventTypeDescriptor{
	{EventReviewOffered, eventScopeHome, eventAdmissionAgent,
		OperationTeamworkOffer, "", true, false},
	{EventReviewAcceptRequested, eventScopeParticipant, eventAdmissionAgent,
		OperationTeamworkAccept, EventReviewAccepted, false, true},
	{EventReviewDeclineRequested, eventScopeParticipant, eventAdmissionAgent,
		OperationTeamworkDecline, EventReviewDeclined, false, true},
	{EventReviewDeliveryReady, eventScopeParticipant, eventAdmissionAgent,
		OperationTeamworkDeliver, EventReviewDelivered, true, true},
	{EventReviewAccepted, eventScopeHome, eventAdmissionController, "", "", false, true},
	{EventReviewAcceptRejected, eventScopeHome, eventAdmissionController, "", "", false, true},
	{EventReviewDelivered, eventScopeHome, eventAdmissionController, "", "", true, true},
	{EventReviewReworkRequested, eventScopeHome, eventAdmissionAgent,
		OperationTeamworkRework, "", true, true},
	{EventReviewClosed, eventScopeHome, eventAdmissionAgent,
		OperationTeamworkClose, "", false, true},
	{EventReviewDeclined, eventScopeHome, eventAdmissionController, "", "", false, true},
	{EventReviewCancelled, eventScopeHome, eventAdmissionAgent,
		OperationTeamworkCancel, "", false, true},
	{EventReviewExpired, eventScopeHome, eventAdmissionController, "", "", false, true},
	{EventReviewOutcome, eventScopeFlexible, eventAdmissionController, "", "", false, true},
}

func init() {
	if !validEventTypeDescriptorRegistry() {
		panic("model: invalid EventType descriptor registry")
	}
}

func validEventTypeDescriptorRegistry() bool {
	seen := make(map[EventType]struct{}, len(eventTypeDescriptors))
	operations := make(map[OperationKind]struct{}, TeamworkActionCount)
	for _, descriptor := range eventTypeDescriptors {
		_, duplicate := seen[descriptor.eventType]
		if duplicate || !validEventTypeDescriptorShape(descriptor) ||
			!recordAgentEventOperation(descriptor, operations) {
			return false
		}
		seen[descriptor.eventType] = struct{}{}
	}
	if len(operations) != TeamworkActionCount {
		return false
	}
	for _, descriptor := range eventTypeDescriptors {
		if !validParticipantEventResponse(descriptor) {
			return false
		}
	}
	return true
}

func validEventTypeDescriptorShape(descriptor EventTypeDescriptor) bool {
	validScope := descriptor.scopeAuthority == eventScopeHome ||
		descriptor.scopeAuthority == eventScopeParticipant ||
		descriptor.scopeAuthority == eventScopeFlexible
	validAdmission := descriptor.admissionAuthority == eventAdmissionAgent ||
		descriptor.admissionAuthority == eventAdmissionController
	return descriptor.eventType != "" && validScope && validAdmission
}

func recordAgentEventOperation(descriptor EventTypeDescriptor,
	operations map[OperationKind]struct{},
) bool {
	if !descriptor.AgentAdmitted() {
		return descriptor.agentOperation == ""
	}
	if !descriptor.agentOperation.Valid() ||
		!strings.HasPrefix(string(descriptor.agentOperation), "teamwork.") {
		return false
	}
	if _, duplicate := operations[descriptor.agentOperation]; duplicate {
		return false
	}
	operations[descriptor.agentOperation] = struct{}{}
	return true
}

func validParticipantEventResponse(descriptor EventTypeDescriptor) bool {
	if !descriptor.ParticipantInput() {
		return descriptor.participantResponse == ""
	}
	response, hasResponse := descriptor.ParticipantResponse()
	if !hasResponse {
		return false
	}
	target, valid := response.Descriptor()
	return valid && target.HomeAuthoritative() && target.ControllerAdmitted()
}

// EventTypeDescriptors returns a deterministic copy of the canonical registry.
func EventTypeDescriptors() []EventTypeDescriptor {
	return append([]EventTypeDescriptor(nil), eventTypeDescriptors[:]...)
}

func (descriptor EventTypeDescriptor) Type() EventType { return descriptor.eventType }
func (descriptor EventTypeDescriptor) HomeAuthoritative() bool {
	return descriptor.scopeAuthority == eventScopeHome
}
func (descriptor EventTypeDescriptor) ParticipantInput() bool {
	return descriptor.scopeAuthority == eventScopeParticipant
}
func (descriptor EventTypeDescriptor) AgentAdmitted() bool {
	return descriptor.admissionAuthority == eventAdmissionAgent
}
func (descriptor EventTypeDescriptor) ControllerAdmitted() bool {
	return descriptor.admissionAuthority == eventAdmissionController
}
func (descriptor EventTypeDescriptor) AgentOperation() (OperationKind, bool) {
	return descriptor.agentOperation, descriptor.AgentAdmitted() && descriptor.agentOperation.Valid()
}
func (descriptor EventTypeDescriptor) ParticipantResponse() (EventType, bool) {
	return descriptor.participantResponse,
		descriptor.ParticipantInput() && descriptor.participantResponse.Valid()
}
func (descriptor EventTypeDescriptor) AllowsArtifacts() bool { return descriptor.allowsArtifacts }
func (descriptor EventTypeDescriptor) RequiresAdmissionCausality() bool {
	return descriptor.requiresAdmissionCausality
}

// Descriptor returns an immutable copy of this EventType's descriptor.
func (t EventType) Descriptor() (EventTypeDescriptor, bool) {
	for _, descriptor := range eventTypeDescriptors {
		if descriptor.eventType == t {
			return descriptor, true
		}
	}
	return EventTypeDescriptor{}, false
}

func (t EventType) Valid() bool {
	_, valid := t.Descriptor()
	return valid
}

func (t EventType) HomeAuthoritative() bool {
	descriptor, valid := t.Descriptor()
	return valid && descriptor.HomeAuthoritative()
}

func (t EventType) ParticipantInput() bool {
	descriptor, valid := t.Descriptor()
	return valid && descriptor.ParticipantInput()
}

func (t EventType) AllowsArtifacts() bool {
	descriptor, valid := t.Descriptor()
	return valid && descriptor.AllowsArtifacts()
}

func (t EventType) RequiresAdmissionCausality() bool {
	descriptor, valid := t.Descriptor()
	return valid && descriptor.RequiresAdmissionCausality()
}

func (t EventType) AgentAdmitted() bool {
	descriptor, valid := t.Descriptor()
	return valid && descriptor.AgentAdmitted()
}

func (t EventType) ControllerAdmitted() bool {
	descriptor, valid := t.Descriptor()
	return valid && descriptor.ControllerAdmitted()
}

func (t EventType) AgentOperation() (OperationKind, bool) {
	descriptor, valid := t.Descriptor()
	if !valid {
		return "", false
	}
	return descriptor.AgentOperation()
}

func (t EventType) ParticipantResponse() (EventType, bool) {
	descriptor, valid := t.Descriptor()
	if !valid {
		return "", false
	}
	return descriptor.ParticipantResponse()
}

func EventTypeForAgentOperation(operation OperationKind) (EventType, bool) {
	if !operation.Valid() {
		return "", false
	}
	for _, descriptor := range eventTypeDescriptors {
		candidate, exists := descriptor.AgentOperation()
		if exists && candidate == operation {
			return descriptor.Type(), true
		}
	}
	return "", false
}
