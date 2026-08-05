package agency

import (
	"bytes"
	"fmt"
	"time"
)

// ParsedPeerDelivery is the trust-boundary result of strict canonical parsing
// under an authenticated, enrolled route context. Only this type can be
// promoted to VerifiedPeerDelivery.
type ParsedPeerDelivery struct{ delivery PeerDelivery }

// ParsePeerDeliveryCanonicalJSON rejects any envelope whose DeliveryID cannot
// be independently reconstructed from the authenticated route, origin Event,
// and opaque target alias. The route is not retained in the parsed value.
func ParsePeerDeliveryCanonicalJSON(data []byte, enrolledRoute RouteID) (ParsedPeerDelivery, error) {
	if enrolledRoute.IsZero() {
		return ParsedPeerDelivery{}, invalid("PeerDelivery route context", "enrolled route is required")
	}
	var wire peerDeliveryWire
	if err := decodeCanonicalObject("PeerDelivery JSON", data, MaxPeerDeliveryCanonicalBytes, &wire); err != nil {
		return ParsedPeerDelivery{}, err
	}
	delivery, err := peerDeliveryFromWire(wire, enrolledRoute)
	if err != nil {
		return ParsedPeerDelivery{}, err
	}
	if err := requireReconstructedCanonical("PeerDelivery JSON", data, delivery.CanonicalJSON()); err != nil {
		return ParsedPeerDelivery{}, err
	}
	return ParsedPeerDelivery{delivery: delivery}, nil
}

func peerDeliveryFromWire(wire peerDeliveryWire, enrolledRoute RouteID) (PeerDelivery, error) {
	if wire.SchemaVersion != 1 {
		return PeerDelivery{}, invalid("PeerDelivery schema version", "must be 1")
	}
	encodedID, err := ParseDeliveryID(wire.DeliveryID)
	if err != nil {
		return PeerDelivery{}, err
	}
	originEvent, err := parsePeerEventRef("PeerDelivery origin Event", wire.Origin.Event)
	if err != nil {
		return PeerDelivery{}, err
	}
	originAcceptedAt, err := parsePeerCanonicalTime("PeerDelivery origin accepted time", wire.Origin.AcceptedAt)
	if err != nil {
		return PeerDelivery{}, err
	}
	originSource, err := NewAgentPrincipalID(wire.Origin.Source)
	if err != nil {
		return PeerDelivery{}, err
	}
	causation, err := parsePeerEventRefs("PeerDelivery origin causation", wire.Origin.Causation)
	if err != nil {
		return PeerDelivery{}, err
	}
	correlation, err := parsePeerOptionalEventRef("PeerDelivery origin correlation", wire.Origin.Correlation)
	if err != nil {
		return PeerDelivery{}, err
	}
	target, err := NewOpaqueHandle(wire.TargetAlias)
	if err != nil {
		return PeerDelivery{}, err
	}
	kind, err := NewSemanticLabel(wire.Kind)
	if err != nil {
		return PeerDelivery{}, err
	}
	payload, err := NewSemanticPayload(wire.Payload)
	if err != nil {
		return PeerDelivery{}, err
	}
	artifacts, err := parsePeerDigests(wire.Artifacts)
	if err != nil {
		return PeerDelivery{}, err
	}
	expiresAt, err := parsePeerCanonicalTime("PeerDelivery expiry", wire.ExpiresAt)
	if err != nil {
		return PeerDelivery{}, err
	}
	delivery, err := NewPeerDelivery(enrolledRoute, PeerDeliverySpec{
		OriginEvent: originEvent, OriginSequence: wire.Origin.Sequence,
		OriginAcceptedAt: originAcceptedAt, OriginSource: originSource,
		OriginCausation: causation, OriginCorrelation: correlation,
		TargetAlias: target, Kind: kind, Payload: payload, Artifacts: artifacts,
		CausalDepth: wire.CausalDepth, ExpiresAt: expiresAt,
	})
	if err != nil {
		return PeerDelivery{}, err
	}
	if delivery.ID() != encodedID {
		return PeerDelivery{}, invariant("PeerDelivery ID", "does not match authenticated route and envelope identity")
	}
	return delivery, nil
}

func parsePeerEventRef(field string, wire eventRefWire) (EventRef, error) {
	id, err := NewEventID(wire.ID)
	if err != nil {
		return EventRef{}, fmt.Errorf("%s: %w", field, err)
	}
	digest, err := ParseDigest(wire.Digest)
	if err != nil {
		return EventRef{}, fmt.Errorf("%s: %w", field, err)
	}
	result, err := NewEventRef(id, digest)
	if err != nil {
		return EventRef{}, fmt.Errorf("%s: %w", field, err)
	}
	return result, nil
}

func parsePeerEventRefs(field string, wires []eventRefWire) ([]EventRef, error) {
	result := make([]EventRef, 0, len(wires))
	for _, wire := range wires {
		event, err := parsePeerEventRef(field, wire)
		if err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, nil
}

func parsePeerOptionalEventRef(field string, wire *eventRefWire) (EventRef, error) {
	if wire == nil {
		return EventRef{}, nil
	}
	return parsePeerEventRef(field, *wire)
}

func parsePeerDigests(values []string) ([]Digest, error) {
	result := make([]Digest, 0, len(values))
	for _, value := range values {
		digest, err := ParseDigest(value)
		if err != nil {
			return nil, err
		}
		result = append(result, digest)
	}
	return result, nil
}

func parsePeerCanonicalTime(field, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, invalid(field, "must use RFC3339Nano")
	}
	canonical, err := canonicalTime(field, parsed)
	if err != nil {
		return time.Time{}, err
	}
	if value != canonical.Format(time.RFC3339Nano) {
		return time.Time{}, invalid(field, "must use canonical UTC RFC3339Nano")
	}
	return canonical, nil
}

func (parsed ParsedPeerDelivery) Delivery() PeerDelivery { return parsed.delivery.clone() }
func (parsed ParsedPeerDelivery) ID() DeliveryID         { return parsed.delivery.ID() }
func (parsed ParsedPeerDelivery) EnvelopeDigest() Digest { return parsed.delivery.EnvelopeDigest() }
func (parsed ParsedPeerDelivery) CanonicalJSON() []byte  { return parsed.delivery.CanonicalJSON() }
func (parsed ParsedPeerDelivery) SigningMessage() []byte { return parsed.delivery.SigningMessage() }

func (parsed ParsedPeerDelivery) valid() bool {
	return !parsed.delivery.id.IsZero() && !parsed.delivery.envelopeDigest.IsZero() &&
		len(parsed.delivery.canonical) > 0 && bytes.Equal(parsed.delivery.canonical,
		parsed.delivery.wireCanonical()) && parsed.delivery.envelopeDigest ==
		domainSeparatedDigest(peerDeliveryEnvelopeDomain, parsed.delivery.canonical)
}
