package agency

// ParsePeerAdmissionReceiptCanonicalJSON reconstructs only an exact canonical
// peer admission outcome bound to the expected delivery. Signature and trust
// verification remain outside this protocol-neutral value layer.
func ParsePeerAdmissionReceiptCanonicalJSON(data []byte,
	delivery PeerDelivery,
) (PeerAdmissionReceipt, error) {
	if delivery.id.IsZero() || delivery.envelopeDigest.IsZero() || len(delivery.canonical) == 0 {
		return PeerAdmissionReceipt{}, invalid("peer admission Receipt binding", "complete delivery is required")
	}
	var wire peerAdmissionReceiptWire
	if err := decodeCanonicalObject("peer admission Receipt JSON", data,
		MaxPeerAdmissionReceiptCanonicalBytes, &wire); err != nil {
		return PeerAdmissionReceipt{}, err
	}
	receipt, err := peerAdmissionReceiptFromWire(wire)
	if err != nil {
		return PeerAdmissionReceipt{}, err
	}
	if err := requireReconstructedCanonical("peer admission Receipt JSON", data,
		receipt.CanonicalJSON()); err != nil {
		return PeerAdmissionReceipt{}, err
	}
	if err := requirePeerAdmissionReceiptBinding(receipt, delivery); err != nil {
		return PeerAdmissionReceipt{}, err
	}
	return receipt, nil
}

func requirePeerAdmissionReceiptBinding(receipt PeerAdmissionReceipt, delivery PeerDelivery) error {
	if receipt.deliveryID != delivery.id || receipt.envelopeDigest != delivery.envelopeDigest {
		return invariant("peer admission Receipt binding", "delivery ID or envelope digest differs")
	}
	if receipt.outcome == PeerAdmissionOutcomeAccepted && receipt.localEvent == delivery.originEvent {
		return invariant("peer admission Receipt binding", "accepted outcome must name a new local Event")
	}
	return nil
}

func peerAdmissionReceiptFromWire(wire peerAdmissionReceiptWire) (PeerAdmissionReceipt, error) {
	if wire.SchemaVersion != 1 {
		return PeerAdmissionReceipt{}, invalid("peer admission Receipt schema version", "must be 1")
	}
	deliveryID, err := ParseDeliveryID(wire.DeliveryID)
	if err != nil {
		return PeerAdmissionReceipt{}, err
	}
	envelopeDigest, err := ParseDigest(wire.EnvelopeDigest)
	if err != nil {
		return PeerAdmissionReceipt{}, err
	}
	recordedAt, err := parsePeerCanonicalTime("peer admission Receipt recorded time", wire.RecordedAt)
	if err != nil {
		return PeerAdmissionReceipt{}, err
	}
	receipt := PeerAdmissionReceipt{
		deliveryID: deliveryID, envelopeDigest: envelopeDigest, recordedAt: recordedAt,
		code: SemanticLabel{}, diagnostic: wire.Diagnostic,
	}
	switch wire.Outcome {
	case "accepted":
		receipt.outcome = PeerAdmissionOutcomeAccepted
		if wire.LocalEvent == nil || wire.Code != "" || wire.Diagnostic != "" {
			return PeerAdmissionReceipt{}, invalid("accepted peer admission Receipt",
				"requires one local Event and no rejection fields")
		}
		receipt.localEvent, err = parsePeerEventRef("peer admission Receipt local Event", *wire.LocalEvent)
	case "rejected":
		receipt.outcome = PeerAdmissionOutcomeRejected
		if wire.LocalEvent != nil || wire.Code == "" {
			return PeerAdmissionReceipt{}, invalid("rejected peer admission Receipt",
				"requires a code and no local Event")
		}
		receipt.code, err = NewSemanticLabel(wire.Code)
		if err == nil {
			err = validatePeerDiagnostic(wire.Diagnostic)
		}
	default:
		return PeerAdmissionReceipt{}, invalid("peer admission Receipt outcome", "must be accepted or rejected")
	}
	if err != nil {
		return PeerAdmissionReceipt{}, err
	}
	canonical, _, err := canonicalJSON(receipt.wire())
	if err != nil {
		return PeerAdmissionReceipt{}, err
	}
	receipt.canonical = canonical
	receipt.digest = domainSeparatedDigest(peerAdmissionReceiptDigestDomain, canonical)
	return receipt, nil
}
