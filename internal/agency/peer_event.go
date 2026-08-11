package agency

import (
	"bytes"
)

// NewPeerEvent promotes one independently verified peer candidate into the
// only local Event shape available to inbound federation. The caller supplies
// the receiver-local consequence and targets already decided by authority;
// this constructor checks that they are structurally compatible with the
// verified candidate and then seals the immutable Event. Admission policy,
// replay, expiry, and durable commit stay outside this value constructor.
func NewPeerEvent(verified VerifiedPeerDelivery, stamp EventStamp,
	consequence Consequence, targets []ResolvedTarget,
) (Event, error) {
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
	if err := validateDecidedPeerEffect(delivery, verified.target, consequence, targets); err != nil {
		return Event{}, err
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
		targets:        append([]ResolvedTarget(nil), targets...),
		artifacts:      artifacts,
		// The local Event records only the immediate cross-node edge. The
		// signed Delivery remains the authoritative container for the full
		// remote ancestry, keeping the local Agent View bounded without
		// discarding provenance.
		causation:         []EventRef{delivery.OriginEvent()},
		correlation:       correlation,
		inReplyToDelivery: inReplyToDelivery,
	}
	if err := validateParsedEventShape(event); err != nil {
		return Event{}, err
	}
	if err := sealEvent(&event); err != nil {
		return Event{}, err
	}
	return event, nil
}

func validateDecidedPeerEffect(delivery PeerDelivery, localTarget AgentPrincipalID,
	consequence Consequence, targets []ResolvedTarget,
) error {
	switch consequence {
	case ConsequenceCreateHandlings, ConsequenceObserveCompleted,
		ConsequenceObserveDeclined, ConsequenceObserveUnresolved:
	default:
		return invalid("peer Event consequence", "must be a closed inbound effect")
	}
	if len(targets) > 1 {
		return limit("peer Event targets", len(targets), 1)
	}
	for _, target := range targets {
		requested := target.Requested()
		if target.Destination() != TargetDestinationLocal ||
			target.LocalPrincipal() != localTarget || requested.IsSelf() ||
			requested.Alias() != delivery.TargetAlias() ||
			!target.RemoteRoute().IsZero() || !target.RemoteAlias().IsZero() {
			return invariant("peer Event target",
				"must be the exact receiver-resolved local target")
		}
	}
	return nil
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
