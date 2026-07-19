package model

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
	allowsArtifacts            bool
	requiresAdmissionCausality bool
}

var eventTypeDescriptors = [...]EventTypeDescriptor{
	{EventReviewOffered, eventScopeHome, eventAdmissionAgent, true, false},
	{EventReviewAcceptRequested, eventScopeParticipant, eventAdmissionAgent, false, true},
	{EventReviewDeclineRequested, eventScopeParticipant, eventAdmissionAgent, false, true},
	{EventReviewDeliveryReady, eventScopeParticipant, eventAdmissionAgent, true, true},
	{EventReviewAccepted, eventScopeHome, eventAdmissionController, false, true},
	{EventReviewAcceptRejected, eventScopeHome, eventAdmissionController, false, true},
	{EventReviewDelivered, eventScopeHome, eventAdmissionController, true, true},
	{EventReviewReworkRequested, eventScopeHome, eventAdmissionAgent, true, true},
	{EventReviewClosed, eventScopeHome, eventAdmissionAgent, false, true},
	{EventReviewDeclined, eventScopeHome, eventAdmissionController, false, true},
	{EventReviewCancelled, eventScopeHome, eventAdmissionAgent, false, true},
	{EventReviewExpired, eventScopeHome, eventAdmissionController, false, true},
	{EventReviewOutcome, eventScopeFlexible, eventAdmissionController, false, true},
}

func init() {
	seen := make(map[EventType]struct{}, len(eventTypeDescriptors))
	for _, descriptor := range eventTypeDescriptors {
		if descriptor.eventType == "" ||
			(descriptor.scopeAuthority != eventScopeHome &&
				descriptor.scopeAuthority != eventScopeParticipant &&
				descriptor.scopeAuthority != eventScopeFlexible) ||
			(descriptor.admissionAuthority != eventAdmissionAgent &&
				descriptor.admissionAuthority != eventAdmissionController) {
			panic("model: invalid EventType descriptor")
		}
		if _, duplicate := seen[descriptor.eventType]; duplicate {
			panic("model: duplicate EventType descriptor: " + descriptor.eventType)
		}
		seen[descriptor.eventType] = struct{}{}
	}
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
