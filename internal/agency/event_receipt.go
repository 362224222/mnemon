package agency

import "time"

const (
	eventSchemaVersion     = 3
	MaxEventCanonicalBytes = 32 << 10
)

type EventStamp struct {
	ID             EventID
	AcceptedAt     time.Time
	OriginSequence uint64
	CausalDepth    uint16
}

// Event is the immutable, canonical record of one admitted BoundIntent.
// NewEvent does not perform admission; it only prevents malformed accepted
// records from being represented.
type Event struct {
	id                EventID
	acceptedAt        time.Time
	originSequence    uint64
	causalDepth       uint16
	source            AgentPrincipalID
	operationKey      OperationKey
	requestDigest     Digest
	kind              SemanticLabel
	payload           SemanticPayload
	consequence       Consequence
	subject           *SubjectBinding
	expectedRef       *ReferenceExpectation
	targets           []ResolvedTarget
	artifacts         []Digest
	causation         []EventRef
	correlation       EventRef
	inReplyToDelivery DeliveryID
	canonical         []byte
	digest            Digest
}

func NewEvent(request BoundIntent, stamp EventStamp) (Event, error) {
	if len(request.canonical) == 0 || request.operationKey.IsZero() || request.digest.IsZero() ||
		stamp.ID.IsZero() || stamp.OriginSequence == 0 || stamp.CausalDepth > MaxPeerCausalDepth {
		return Event{}, invalid("Event", "complete request, ID, and positive origin sequence are required")
	}
	acceptedAt, err := canonicalTime("Event accepted time", stamp.AcceptedAt)
	if err != nil {
		return Event{}, err
	}
	event := Event{
		id:                stamp.ID,
		acceptedAt:        acceptedAt,
		originSequence:    stamp.OriginSequence,
		causalDepth:       stamp.CausalDepth,
		source:            request.attachment.principal,
		operationKey:      request.operationKey,
		requestDigest:     request.digest,
		kind:              request.intent.kind,
		payload:           request.intent.payload,
		consequence:       request.intent.consequence,
		targets:           append([]ResolvedTarget(nil), request.targets...),
		artifacts:         append([]Digest(nil), request.artifacts...),
		causation:         append([]EventRef(nil), request.causation...),
		correlation:       request.correlation,
		inReplyToDelivery: request.inReplyToDelivery,
	}
	if request.subject != nil {
		copyValue := *request.subject
		event.subject = &copyValue
	}
	if request.expectedReference != nil {
		copyValue := *request.expectedReference
		event.expectedRef = &copyValue
	}
	if err := sealEvent(&event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func sealEvent(event *Event) error {
	canonical, digest, err := canonicalJSON(event.wire())
	if err != nil {
		return err
	}
	if len(canonical) > MaxEventCanonicalBytes {
		return limit("Event canonical bytes", len(canonical), MaxEventCanonicalBytes)
	}
	event.canonical, event.digest = canonical, digest
	return nil
}

func (event Event) ID() EventID                { return event.id }
func (event Event) AcceptedAt() time.Time      { return event.acceptedAt }
func (event Event) OriginSequence() uint64     { return event.originSequence }
func (event Event) CausalDepth() uint16        { return event.causalDepth }
func (event Event) Source() AgentPrincipalID   { return event.source }
func (event Event) OperationKey() OperationKey { return event.operationKey }
func (event Event) RequestDigest() Digest      { return event.requestDigest }
func (event Event) Kind() SemanticLabel        { return event.kind }
func (event Event) Payload() SemanticPayload   { return event.payload }
func (event Event) Consequence() Consequence   { return event.consequence }
func (event Event) Subject() (SubjectBinding, bool) {
	if event.subject == nil {
		return SubjectBinding{}, false
	}
	return *event.subject, true
}
func (event Event) ExpectedReference() (ReferenceExpectation, bool) {
	if event.expectedRef == nil {
		return ReferenceExpectation{}, false
	}
	return *event.expectedRef, true
}
func (event Event) Targets() []ResolvedTarget { return append([]ResolvedTarget(nil), event.targets...) }
func (event Event) Artifacts() []Digest       { return append([]Digest(nil), event.artifacts...) }
func (event Event) Causation() []EventRef     { return append([]EventRef(nil), event.causation...) }
func (event Event) Correlation() (EventRef, bool) {
	return event.correlation, !event.correlation.IsZero()
}
func (event Event) InReplyToDelivery() (DeliveryID, bool) {
	return event.inReplyToDelivery, !event.inReplyToDelivery.IsZero()
}
func (event Event) CanonicalJSON() []byte { return copyBytes(event.canonical) }
func (event Event) Digest() Digest        { return event.digest }
func (event Event) Ref() EventRef         { return EventRef{id: event.id, digest: event.digest} }

type eventWire struct {
	SchemaVersion int               `json:"schema_version"`
	Machine       eventMachineWire  `json:"machine"`
	Semantic      eventSemanticWire `json:"semantic"`
	Evidence      eventEvidenceWire `json:"evidence"`
}

type eventMachineWire struct {
	ID                string                    `json:"event_id"`
	AcceptedAt        string                    `json:"accepted_at"`
	OriginSequence    uint64                    `json:"origin_sequence"`
	CausalDepth       uint16                    `json:"causal_depth"`
	Source            string                    `json:"source_principal"`
	OperationKey      string                    `json:"operation_key"`
	RequestDigest     string                    `json:"request_digest"`
	Consequence       string                    `json:"consequence"`
	Subject           *subjectBindingWire       `json:"subject,omitempty"`
	ExpectedReference *referenceExpectationWire `json:"expected_reference,omitempty"`
	Targets           []resolvedTargetWire      `json:"targets,omitempty"`
	InReplyToDelivery string                    `json:"in_reply_to_delivery_id,omitempty"`
}

type eventSemanticWire struct {
	Kind    string `json:"kind"`
	Payload string `json:"payload"`
}

type eventEvidenceWire struct {
	Artifacts   []string       `json:"artifacts,omitempty"`
	Causation   []eventRefWire `json:"causation,omitempty"`
	Correlation *eventRefWire  `json:"correlation,omitempty"`
}

func (event Event) wire() eventWire {
	wire := eventWire{
		SchemaVersion: eventSchemaVersion,
		Machine: eventMachineWire{
			ID: event.id.String(), AcceptedAt: event.acceptedAt.Format(time.RFC3339Nano),
			OriginSequence: event.originSequence, CausalDepth: event.causalDepth,
			Source:       event.source.String(),
			OperationKey: event.operationKey.String(), RequestDigest: event.requestDigest.String(),
			Consequence: event.consequence.String(),
		},
		Semantic: eventSemanticWire{Kind: event.kind.String(), Payload: event.payload.String()},
	}
	if event.subject != nil {
		wire.Machine.Subject = &subjectBindingWire{HandlingID: event.subject.handlingID.String(),
			Head: event.subject.head.canonical().(eventRefWire), Fence: event.subject.fence,
			ObservationRevision: event.subject.observationRevision}
	}
	if event.expectedRef != nil {
		wire.Machine.ExpectedReference = &referenceExpectationWire{Absent: event.expectedRef.absent,
			Key: event.expectedRef.key.String()}
		if !event.expectedRef.head.IsZero() {
			head := event.expectedRef.head.canonical().(eventRefWire)
			wire.Machine.ExpectedReference.Head = &head
		}
	}
	for _, target := range event.targets {
		wire.Machine.Targets = append(wire.Machine.Targets, target.resolvedWire())
	}
	if !event.inReplyToDelivery.IsZero() {
		wire.Machine.InReplyToDelivery = event.inReplyToDelivery.String()
	}
	for _, digest := range event.artifacts {
		wire.Evidence.Artifacts = append(wire.Evidence.Artifacts, digest.String())
	}
	for _, causal := range event.causation {
		wire.Evidence.Causation = append(wire.Evidence.Causation, causal.canonical().(eventRefWire))
	}
	if !event.correlation.IsZero() {
		correlation := event.correlation.canonical().(eventRefWire)
		wire.Evidence.Correlation = &correlation
	}
	return wire
}

type ReceiptOutcome uint8

const (
	ReceiptOutcomeInvalid ReceiptOutcome = iota
	ReceiptOutcomeAccepted
	ReceiptOutcomeRejected
)

func (outcome ReceiptOutcome) String() string {
	switch outcome {
	case ReceiptOutcomeAccepted:
		return "accepted"
	case ReceiptOutcomeRejected:
		return "rejected"
	default:
		return ""
	}
}

// Receipt is the durable outcome of one operation. Replay returns these exact
// bytes; replay is therefore not a third stored outcome.
type Receipt struct {
	operationKey  OperationKey
	requestDigest Digest
	outcome       ReceiptOutcome
	recordedAt    time.Time
	event         EventRef
	code          SemanticLabel
	diagnostic    string
	canonical     []byte
	digest        Digest
}

func NewAcceptedReceipt(request BoundIntent, event Event, recordedAt time.Time) (Receipt, error) {
	canonicalRecordedAt, err := canonicalTime("Receipt recorded time", recordedAt)
	if err != nil {
		return Receipt{}, err
	}
	if request.operationKey.IsZero() || request.digest.IsZero() || event.operationKey != request.operationKey ||
		event.requestDigest != request.digest || event.digest.IsZero() {
		return Receipt{}, invariant("accepted Receipt", "Event must be the accepted result of the exact operation")
	}
	if canonicalRecordedAt.Before(event.acceptedAt) {
		return Receipt{}, invariant("accepted Receipt", "recorded time must not precede Event acceptance")
	}
	return newReceipt(request, ReceiptOutcomeAccepted, event.Ref(), SemanticLabel{}, "", canonicalRecordedAt)
}

func NewRejectedReceipt(request BoundIntent, code SemanticLabel, diagnostic string,
	recordedAt time.Time,
) (Receipt, error) {
	if code.IsZero() {
		return Receipt{}, invalid("rejected Receipt", "diagnostic code is required")
	}
	if len(diagnostic) > MaxDiagnosticBytes {
		return Receipt{}, limit("Receipt diagnostic", len(diagnostic), MaxDiagnosticBytes)
	}
	if _, err := NewSemanticPayload(diagnostic); err != nil {
		return Receipt{}, invalid("Receipt diagnostic", "must be valid UTF-8 without NUL")
	}
	return newReceipt(request, ReceiptOutcomeRejected, EventRef{}, code, diagnostic, recordedAt)
}

func newReceipt(request BoundIntent, outcome ReceiptOutcome, event EventRef, code SemanticLabel,
	diagnostic string, recordedAt time.Time,
) (Receipt, error) {
	if request.operationKey.IsZero() || request.digest.IsZero() {
		return Receipt{}, invalid("Receipt", "complete request authority is required")
	}
	canonicalRecordedAt, err := canonicalTime("Receipt recorded time", recordedAt)
	if err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{operationKey: request.operationKey, requestDigest: request.digest,
		outcome: outcome, recordedAt: canonicalRecordedAt, event: event, code: code, diagnostic: diagnostic}
	canonical, digest, err := canonicalJSON(receipt.wire())
	if err != nil {
		return Receipt{}, err
	}
	receipt.canonical, receipt.digest = canonical, digest
	return receipt, nil
}

func (receipt Receipt) OperationKey() OperationKey { return receipt.operationKey }
func (receipt Receipt) RequestDigest() Digest      { return receipt.requestDigest }
func (receipt Receipt) Outcome() ReceiptOutcome    { return receipt.outcome }
func (receipt Receipt) RecordedAt() time.Time      { return receipt.recordedAt }
func (receipt Receipt) Event() (EventRef, bool)    { return receipt.event, !receipt.event.IsZero() }
func (receipt Receipt) Code() SemanticLabel        { return receipt.code }
func (receipt Receipt) Diagnostic() string         { return receipt.diagnostic }
func (receipt Receipt) CanonicalJSON() []byte      { return copyBytes(receipt.canonical) }
func (receipt Receipt) Digest() Digest             { return receipt.digest }

type receiptWire struct {
	SchemaVersion int           `json:"schema_version"`
	OperationKey  string        `json:"operation_key"`
	RequestDigest string        `json:"request_digest"`
	Outcome       string        `json:"outcome"`
	RecordedAt    string        `json:"recorded_at"`
	Event         *eventRefWire `json:"event,omitempty"`
	Code          string        `json:"code,omitempty"`
	Diagnostic    string        `json:"diagnostic,omitempty"`
}

func (receipt Receipt) wire() receiptWire {
	wire := receiptWire{SchemaVersion: 1, OperationKey: receipt.operationKey.String(),
		RequestDigest: receipt.requestDigest.String(), Outcome: receipt.outcome.String(),
		RecordedAt: receipt.recordedAt.Format(time.RFC3339Nano), Code: receipt.code.String(),
		Diagnostic: receipt.diagnostic}
	if !receipt.event.IsZero() {
		event := receipt.event.canonical().(eventRefWire)
		wire.Event = &event
	}
	return wire
}
