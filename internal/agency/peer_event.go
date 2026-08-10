package agency

import (
	"bytes"
)

// NewPeerEvent promotes one independently verified peer candidate into the
// only local Event shape available to inbound federation. The caller supplies
// only the local Event stamp; source, target, operation identity, effect,
// semantics, evidence, and causal depth remain sealed by VerifiedPeerDelivery.
// Admission policy, replay, expiry, and durable commit stay outside this value
// constructor.
func NewPeerEvent(verified VerifiedPeerDelivery, stamp EventStamp) (Event, error) {
	delivery, artifacts, err := peerEventInputs(verified)
	if err != nil {
		return Event{}, err
	}
	if stamp.ID.IsZero() || stamp.OriginSequence == 0 {
		return Event{}, invalid("peer Event", "machine Event ID and positive local origin sequence are required")
	}
	if stamp.CausalDepth != delivery.CausalDepth() {
		return Event{}, invariant("peer Event causal depth", "must equal the verified Delivery depth")
	}
	acceptedAt, err := canonicalTime("peer Event accepted time", stamp.AcceptedAt)
	if err != nil {
		return Event{}, err
	}
	operationKey, err := NewOperationKey(delivery.ID().String())
	if err != nil {
		return Event{}, err
	}
	consequence := verified.Consequence()
	var targets []ResolvedTarget
	if verified.SuccessorCount() == 1 {
		requestedTarget, err := AliasTarget(delivery.TargetAlias())
		if err != nil {
			return Event{}, err
		}
		target, err := ResolveLocalTarget(requestedTarget, verified.target)
		if err != nil {
			return Event{}, err
		}
		targets = []ResolvedTarget{target}
	}
	correlation, _ := delivery.OriginCorrelation()
	inReplyToDelivery, _ := verified.InReplyToDelivery()
	event := Event{
		id:             stamp.ID,
		acceptedAt:     acceptedAt,
		originSequence: stamp.OriginSequence,
		causalDepth:    delivery.CausalDepth(),
		source:         verified.source,
		operationKey:   operationKey,
		requestDigest:  delivery.EnvelopeDigest(),
		kind:           delivery.Kind(),
		payload:        delivery.Payload(),
		consequence:    consequence,
		targets:        targets,
		artifacts:      artifacts,
		// The local Event records only the immediate cross-node edge. The
		// signed Delivery remains the authoritative container for the full
		// remote ancestry, keeping the local Agent View bounded without
		// discarding provenance.
		causation:         []EventRef{delivery.OriginEvent()},
		correlation:       correlation,
		inReplyToDelivery: inReplyToDelivery,
	}
	if err := sealEvent(&event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func peerEventInputs(verified VerifiedPeerDelivery) (PeerDelivery, []Digest, error) {
	delivery := verified.delivery
	if verified.source.IsZero() || verified.target.IsZero() || delivery.id.IsZero() ||
		delivery.envelopeDigest.IsZero() || len(delivery.canonical) == 0 ||
		!bytes.Equal(delivery.canonical, delivery.wireCanonical()) ||
		delivery.envelopeDigest != domainSeparatedDigest(peerDeliveryEnvelopeDomain, delivery.canonical) {
		return PeerDelivery{}, nil, invalid("peer Event", "complete verified Delivery authority is required")
	}
	verifiedArtifacts, err := requireCompletePeerArtifacts(delivery.artifacts, verified.artifacts)
	if err != nil {
		return PeerDelivery{}, nil, err
	}
	artifacts := make([]Digest, len(verifiedArtifacts))
	for index, artifact := range verifiedArtifacts {
		artifacts[index] = artifact.digest
	}
	return delivery.clone(), artifacts, nil
}
