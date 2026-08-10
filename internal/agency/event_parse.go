package agency

import (
	"fmt"
	"time"
)

// ParseEventCanonicalJSON reconstructs one immutable accepted Event. It
// accepts only the exact bounded encoding produced by the Event constructors;
// callers must still compare its digest and machine fields with any separately
// stored authority columns.
func ParseEventCanonicalJSON(data []byte) (Event, error) {
	var wire eventWire
	if err := decodeCanonicalObject("Event JSON", data, MaxEventCanonicalBytes, &wire); err != nil {
		return Event{}, err
	}
	event, err := eventFromWire(wire)
	if err != nil {
		return Event{}, err
	}
	if err := requireReconstructedCanonical("Event JSON", data, event.CanonicalJSON()); err != nil {
		return Event{}, err
	}
	return event, nil
}

func eventFromWire(wire eventWire) (Event, error) {
	if wire.SchemaVersion != eventSchemaVersion {
		return Event{}, invalid("Event schema version", "is unsupported")
	}
	id, err := NewEventID(wire.Machine.ID)
	if err != nil {
		return Event{}, err
	}
	acceptedAt, err := parseCanonicalEventTime(wire.Machine.AcceptedAt)
	if err != nil {
		return Event{}, err
	}
	if wire.Machine.OriginSequence == 0 {
		return Event{}, invalid("Event origin sequence", "must be positive")
	}
	if wire.Machine.CausalDepth > MaxPeerCausalDepth {
		return Event{}, limit("Event causal depth", int(wire.Machine.CausalDepth), MaxPeerCausalDepth)
	}
	source, err := NewAgentPrincipalID(wire.Machine.Source)
	if err != nil {
		return Event{}, err
	}
	operation, err := NewOperationKey(wire.Machine.OperationKey)
	if err != nil {
		return Event{}, err
	}
	requestDigest, err := ParseDigest(wire.Machine.RequestDigest)
	if err != nil {
		return Event{}, err
	}
	consequence, err := parseConsequence(wire.Machine.Consequence)
	if err != nil {
		return Event{}, err
	}
	kind, err := NewSemanticLabel(wire.Semantic.Kind)
	if err != nil {
		return Event{}, err
	}
	payload, err := NewSemanticPayload(wire.Semantic.Payload)
	if err != nil {
		return Event{}, err
	}
	subject, err := parseEventSubject(wire.Machine.Subject)
	if err != nil {
		return Event{}, err
	}
	expectedReference, err := parseEventReference(wire.Machine.ExpectedReference)
	if err != nil {
		return Event{}, err
	}
	targets, err := parseEventTargets(wire.Machine.Targets)
	if err != nil {
		return Event{}, err
	}
	artifacts, err := parseEventArtifacts(wire.Evidence.Artifacts)
	if err != nil {
		return Event{}, err
	}
	causation, err := parseEventCausation(wire.Evidence.Causation)
	if err != nil {
		return Event{}, err
	}
	correlation, err := parseOptionalEventRef("Event correlation", wire.Evidence.Correlation)
	if err != nil {
		return Event{}, err
	}
	var inReplyTo DeliveryID
	if wire.Machine.InReplyToDelivery != "" {
		inReplyTo, err = ParseDeliveryID(wire.Machine.InReplyToDelivery)
		if err != nil {
			return Event{}, err
		}
	}

	event := Event{id: id, acceptedAt: acceptedAt,
		originSequence: wire.Machine.OriginSequence, causalDepth: wire.Machine.CausalDepth,
		source: source, operationKey: operation, requestDigest: requestDigest,
		kind: kind, payload: payload, consequence: consequence, subject: subject,
		expectedRef: expectedReference, targets: targets, artifacts: artifacts,
		causation: causation, correlation: correlation, inReplyToDelivery: inReplyTo}
	if err := validateParsedEventShape(event); err != nil {
		return Event{}, err
	}
	if err := sealEvent(&event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func parseCanonicalEventTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, invalid("Event accepted time", "must use RFC3339Nano")
	}
	canonical, err := canonicalTime("Event accepted time", parsed)
	if err != nil {
		return time.Time{}, err
	}
	if value != canonical.Format(time.RFC3339Nano) {
		return time.Time{}, invalid("Event accepted time", "must use canonical UTC RFC3339Nano")
	}
	return canonical, nil
}

func parseEventSubject(wire *subjectBindingWire) (*SubjectBinding, error) {
	if wire == nil {
		return nil, nil
	}
	handling, err := NewHandlingID(wire.HandlingID)
	if err != nil {
		return nil, err
	}
	head, err := parseEventRef(wire.Head)
	if err != nil {
		return nil, err
	}
	if wire.Fence == 0 {
		return nil, invalid("Event subject fence", "must be positive")
	}
	return &SubjectBinding{handlingID: handling, head: head, fence: wire.Fence,
		observationRevision: wire.ObservationRevision}, nil
}

func parseEventReference(wire *referenceExpectationWire) (*ReferenceExpectation, error) {
	if wire == nil {
		return nil, nil
	}
	key, err := NewReferenceKey(wire.Key)
	if err != nil {
		return nil, err
	}
	if wire.Absent {
		if wire.Head != nil {
			return nil, invalid("Event absent Reference", "must not contain a head")
		}
		return &ReferenceExpectation{absent: true, key: key}, nil
	}
	if wire.Head == nil {
		return nil, invalid("Event exact Reference", "requires a head")
	}
	head, err := parseEventRef(*wire.Head)
	if err != nil {
		return nil, err
	}
	return &ReferenceExpectation{key: key, head: head}, nil
}

func parseEventTargets(wires []resolvedTargetWire) ([]ResolvedTarget, error) {
	if len(wires) > MaxSuccessors {
		return nil, limit("Event targets", len(wires), MaxSuccessors)
	}
	result := make([]ResolvedTarget, 0, len(wires))
	seen := make(map[resolvedTargetDestination]struct{}, len(wires))
	for _, wire := range wires {
		var target ResolvedTarget
		switch wire.Destination {
		case "local":
			if wire.RemoteRoute != "" || wire.RemoteAlias != "" {
				return nil, invalid("Event local target", "must not contain remote authority")
			}
			principal, err := NewAgentPrincipalID(wire.LocalPrincipal)
			if err != nil {
				return nil, err
			}
			target = ResolvedTarget{destination: TargetDestinationLocal, localPrincipal: principal}
		case "remote":
			if wire.LocalPrincipal != "" {
				return nil, invalid("Event remote target", "must not contain local authority")
			}
			route, err := NewRouteID(wire.RemoteRoute)
			if err != nil {
				return nil, err
			}
			alias, err := NewOpaqueHandle(wire.RemoteAlias)
			if err != nil {
				return nil, err
			}
			target = ResolvedTarget{destination: TargetDestinationRemote,
				remoteRoute: route, remoteAlias: alias}
		default:
			return nil, invalid("Event target", "must be local or remote")
		}
		key := target.destinationKey()
		if _, duplicate := seen[key]; duplicate {
			return nil, invalid("Event targets", "contains a duplicate resolved destination")
		}
		seen[key] = struct{}{}
		result = append(result, target)
	}
	return result, nil
}

func parseEventArtifacts(values []string) ([]Digest, error) {
	artifacts := make([]Digest, 0, len(values))
	for _, value := range values {
		digest, err := ParseDigest(value)
		if err != nil {
			return nil, fmt.Errorf("Event Artifact: %w", err)
		}
		artifacts = append(artifacts, digest)
	}
	return normalizePeerArtifacts(artifacts)
}

func parseEventCausation(wires []eventRefWire) ([]EventRef, error) {
	if len(wires) > MaxCausationHandles {
		return nil, limit("Event causation", len(wires), MaxCausationHandles)
	}
	result := make([]EventRef, 0, len(wires))
	seen := make(map[EventRef]struct{}, len(wires))
	for _, wire := range wires {
		ref, err := parseEventRef(wire)
		if err != nil {
			return nil, fmt.Errorf("Event causation: %w", err)
		}
		if _, duplicate := seen[ref]; duplicate {
			return nil, invalid("Event causation", "contains a duplicate Event reference")
		}
		seen[ref] = struct{}{}
		result = append(result, ref)
	}
	return result, nil
}

func parseOptionalEventRef(field string, wire *eventRefWire) (EventRef, error) {
	if wire == nil {
		return EventRef{}, nil
	}
	ref, err := parseEventRef(*wire)
	if err != nil {
		return EventRef{}, fmt.Errorf("%s: %w", field, err)
	}
	return ref, nil
}

func validateParsedEventShape(event Event) error {
	hasSubject := event.subject != nil
	hasReference := event.expectedRef != nil
	localTargets, remoteTargets := parsedEventTargetCounts(event.targets)
	hasReply := !event.inReplyToDelivery.IsZero()
	hasCorrelation := !event.correlation.IsZero()

	switch event.consequence {
	case ConsequenceCreateHandlings:
		if hasSubject || hasReference || len(event.targets) == 0 || hasReply {
			return invariant("Event handling.create", "requires targets and no subject, Reference, or reply binding")
		}
		if remoteTargets > 0 && localTargets == 0 {
			return invariant("Event handling.create", "remote delegation requires a local responsibility anchor")
		}
	case ConsequenceAdvanceHandling:
		if !hasSubject || hasReference || hasReply {
			return invariant("Event handling.advance", "requires a subject and no Reference or reply binding")
		}
	case ConsequenceResolveCompleted, ConsequenceResolveDeclined, ConsequenceResolveUnresolved:
		if !hasSubject || hasReference {
			return invariant("Event handling resolution", "requires a subject and no Reference")
		}
		if event.consequence == ConsequenceResolveCompleted && len(event.artifacts) == 0 {
			return invariant("Event completed resolution", "requires a verified Artifact")
		}
		if hasReply {
			if len(event.targets) != 1 || remoteTargets != 1 || !hasCorrelation {
				return invariant("Event terminal reply", "requires one remote target and correlation")
			}
		} else if remoteTargets > 0 && localTargets == 0 {
			return invariant("Event handling resolution", "remote delegation requires a local responsibility anchor")
		}
	case ConsequencePublishReference:
		if hasSubject || !hasReference || !event.expectedRef.absent || len(event.targets) != 0 ||
			len(event.artifacts) != 1 || hasReply {
			return invariant("Event reference.publish", "requires one absent key and one Artifact only")
		}
	case ConsequenceSupersedeReference:
		if hasSubject || !hasReference || event.expectedRef.absent || len(event.targets) != 0 ||
			len(event.artifacts) != 1 || hasReply {
			return invariant("Event reference.supersede", "requires one exact head and one Artifact only")
		}
	case ConsequenceRetractReference:
		if hasSubject || !hasReference || event.expectedRef.absent || len(event.targets) != 0 ||
			len(event.artifacts) != 0 || hasReply {
			return invariant("Event reference.retract", "requires one exact head and no Artifact")
		}
	case ConsequenceObserveCompleted, ConsequenceObserveDeclined, ConsequenceObserveUnresolved:
		if hasSubject || hasReference || len(event.targets) != 0 || !hasReply || !hasCorrelation ||
			len(event.causation) != 1 || event.causalDepth == 0 {
			return invariant("Event observation", "requires one direct cause, correlation, and reply binding only")
		}
		if _, err := ParseDeliveryID(event.operationKey.String()); err != nil {
			return invariant("Event observation operation", "must be the authenticated Delivery ID")
		}
		if event.consequence == ConsequenceObserveCompleted && len(event.artifacts) == 0 {
			return invariant("Event completed observation", "requires a verified Artifact")
		}
	default:
		return invalid("Event consequence", "is not closed")
	}
	if hasReply && !isTerminalConsequence(event.consequence) && !event.consequence.observation() {
		return invariant("Event reply binding", "is allowed only for terminal replies")
	}
	return nil
}

func parsedEventTargetCounts(targets []ResolvedTarget) (local, remote int) {
	for _, target := range targets {
		switch target.destination {
		case TargetDestinationLocal:
			local++
		case TargetDestinationRemote:
			remote++
		}
	}
	return local, remote
}
