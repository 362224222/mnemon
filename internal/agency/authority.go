package agency

import "time"

// Attachment is machine-generated proof of one eligible Runtime boundary.
type Attachment struct {
	id          AttachmentID
	principal   AgentPrincipalID
	mayInitiate bool
	issuedAt    time.Time
	expiresAt   time.Time
}

func NewAttachment(id AttachmentID, principal AgentPrincipalID, mayInitiate bool,
	issuedAt, expiresAt time.Time,
) (Attachment, error) {
	if id.IsZero() || principal.IsZero() {
		return Attachment{}, invalid("attachment", "ID and Principal are required")
	}
	issued, err := canonicalTime("attachment issued time", issuedAt)
	if err != nil {
		return Attachment{}, err
	}
	expires, err := canonicalTime("attachment expiry", expiresAt)
	if err != nil {
		return Attachment{}, err
	}
	if !expires.After(issued) {
		return Attachment{}, invariant("attachment", "expiry must be after issue time")
	}
	return Attachment{id: id, principal: principal, mayInitiate: mayInitiate,
		issuedAt: issued, expiresAt: expires}, nil
}

func (a Attachment) ID() AttachmentID            { return a.id }
func (a Attachment) Principal() AgentPrincipalID { return a.principal }
func (a Attachment) MayInitiate() bool           { return a.mayInitiate }
func (a Attachment) IssuedAt() time.Time         { return a.issuedAt }
func (a Attachment) ExpiresAt() time.Time        { return a.expiresAt }

// SubjectBinding freezes the current machine-owned Handling authority behind
// one opaque View handle.
type SubjectBinding struct {
	handle              OpaqueHandle
	handlingID          HandlingID
	head                EventRef
	fence               uint64
	observationRevision uint64
}

func NewSubjectBinding(handle OpaqueHandle, handlingID HandlingID, head EventRef,
	fence, observationRevision uint64,
) (SubjectBinding, error) {
	if handle.IsZero() || handlingID.IsZero() || head.IsZero() || fence == 0 {
		return SubjectBinding{}, invalid("subject binding", "handle, Handling, head, and positive fence are required")
	}
	return SubjectBinding{handle: handle, handlingID: handlingID, head: head, fence: fence,
		observationRevision: observationRevision}, nil
}

func (b SubjectBinding) Handle() OpaqueHandle        { return b.handle }
func (b SubjectBinding) HandlingID() HandlingID      { return b.handlingID }
func (b SubjectBinding) Head() EventRef              { return b.head }
func (b SubjectBinding) Fence() uint64               { return b.fence }
func (b SubjectBinding) ObservationRevision() uint64 { return b.observationRevision }

// ReferenceExpectation freezes either the absence of a first-publish key or
// one exact locally accepted lineage head.
type ReferenceExpectation struct {
	absent   bool
	handle   OpaqueHandle
	key      ReferenceKey
	head     EventRef
	outcomes AgentViewTerminalOutcomes
}

func ExpectAbsentReference(key ReferenceKey) (ReferenceExpectation, error) {
	if key.IsZero() {
		return ReferenceExpectation{}, invalid("Reference expectation", "key is required")
	}
	return ReferenceExpectation{absent: true, key: key}, nil
}

func ExpectReferenceHead(handle OpaqueHandle, key ReferenceKey, head EventRef) (ReferenceExpectation, error) {
	return ExpectReferenceHeadWithOutcomes(handle, key, head, AgentViewTerminalOutcomes{})
}

func ExpectReferenceHeadWithOutcomes(handle OpaqueHandle, key ReferenceKey, head EventRef,
	outcomes AgentViewTerminalOutcomes,
) (ReferenceExpectation, error) {
	if handle.IsZero() || key.IsZero() || head.IsZero() {
		return ReferenceExpectation{}, invalid("Reference expectation", "handle, key, and head are required")
	}
	if err := validateTerminalOutcomes(outcomes); err != nil {
		return ReferenceExpectation{}, err
	}
	return ReferenceExpectation{handle: handle, key: key, head: head, outcomes: outcomes}, nil
}

func (expected ReferenceExpectation) Handle() OpaqueHandle { return expected.handle }
func (expected ReferenceExpectation) IsAbsent() bool       { return expected.absent }
func (expected ReferenceExpectation) Key() ReferenceKey    { return expected.key }
func (expected ReferenceExpectation) Head() EventRef       { return expected.head }
func (expected ReferenceExpectation) TerminalOutcomes() AgentViewTerminalOutcomes {
	return expected.outcomes
}

type TargetDestination uint8

const (
	TargetDestinationInvalid TargetDestination = iota
	TargetDestinationLocal
	TargetDestinationRemote
)

// ResolvedTarget freezes the machine resolution of one Agent-visible target.
type ResolvedTarget struct {
	requested      TargetRef
	destination    TargetDestination
	localPrincipal AgentPrincipalID
	remoteRoute    RouteID
	remoteAlias    OpaqueHandle
}

func ResolveLocalTarget(requested TargetRef, principal AgentPrincipalID) (ResolvedTarget, error) {
	if requested.IsZero() || principal.IsZero() {
		return ResolvedTarget{}, invalid("resolved local target", "request and Principal are required")
	}
	return ResolvedTarget{requested: requested, destination: TargetDestinationLocal,
		localPrincipal: principal}, nil
}

func ResolveRemoteTarget(requested TargetRef, route RouteID, alias OpaqueHandle) (ResolvedTarget, error) {
	if requested.IsZero() || requested.IsSelf() || route.IsZero() || alias.IsZero() {
		return ResolvedTarget{}, invalid("resolved remote target", "non-self request, route, and alias are required")
	}
	return ResolvedTarget{requested: requested, destination: TargetDestinationRemote,
		remoteRoute: route, remoteAlias: alias}, nil
}

func (target ResolvedTarget) Requested() TargetRef             { return target.requested }
func (target ResolvedTarget) Destination() TargetDestination   { return target.destination }
func (target ResolvedTarget) LocalPrincipal() AgentPrincipalID { return target.localPrincipal }
func (target ResolvedTarget) RemoteRoute() RouteID             { return target.remoteRoute }
func (target ResolvedTarget) RemoteAlias() OpaqueHandle        { return target.remoteAlias }

// ProvenanceOffer freezes one locally accepted Event behind an opaque
// causation or correlation handle.
type ProvenanceOffer struct {
	handle OpaqueHandle
	event  EventRef
}

func NewProvenanceOffer(handle OpaqueHandle, event EventRef) (ProvenanceOffer, error) {
	if handle.IsZero() || event.IsZero() {
		return ProvenanceOffer{}, invalid("provenance offer", "handle and Event are required")
	}
	return ProvenanceOffer{handle: handle, event: event}, nil
}

func (offer ProvenanceOffer) Handle() OpaqueHandle { return offer.handle }
func (offer ProvenanceOffer) Event() EventRef      { return offer.event }
