package peer

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestAgencyDeliveryFramesPreserveRealR7CanonicalBytes(t *testing.T) {
	route, delivery, receipt := agencyFrameDomainFixture(t)
	offer, err := NewAgencyDeliveryOffer(AgencyDeliveryOfferSpec{
		DeliveryID: delivery.ID().String(), EnvelopeDigest: delivery.EnvelopeDigest().String(),
		CanonicalDelivery: delivery.CanonicalJSON(), Signature: bytes.Repeat([]byte{1}, ed25519.SignatureSize),
	})
	if err != nil {
		t.Fatalf("NewAgencyDeliveryOffer() error = %v", err)
	}
	parsedOffer := roundTripAgencyDeliveryPayload(t, offer).(AgencyDeliveryOffer)
	if !bytes.Equal(parsedOffer.CanonicalDelivery(), delivery.CanonicalJSON()) {
		t.Fatal("Delivery transport changed R7 canonical field order")
	}
	if _, err := agency.ParsePeerDeliveryCanonicalJSON(parsedOffer.CanonicalDelivery(), route); err != nil {
		t.Fatalf("transported PeerDelivery no longer parses in R7 authority: %v", err)
	}

	receiptEnvelope, err := NewAgencyAdmissionReceipt(AgencyAdmissionReceiptSpec{
		DeliveryID: delivery.ID().String(), EnvelopeDigest: delivery.EnvelopeDigest().String(),
		CanonicalReceipt: receipt.CanonicalJSON(), Signature: bytes.Repeat([]byte{2}, ed25519.SignatureSize),
	})
	if err != nil {
		t.Fatalf("NewAgencyAdmissionReceipt() error = %v", err)
	}
	parsedReceipt := roundTripAgencyDeliveryPayload(t, receiptEnvelope).(AgencyAdmissionReceipt)
	if !bytes.Equal(parsedReceipt.CanonicalReceipt(), receipt.CanonicalJSON()) {
		t.Fatal("Delivery transport changed R7 Receipt canonical field order")
	}
	parsedDelivery, err := agency.ParsePeerDeliveryCanonicalJSON(delivery.CanonicalJSON(), route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agency.ParsePeerAdmissionReceiptCanonicalJSON(parsedReceipt.CanonicalReceipt(),
		parsedDelivery.Delivery()); err != nil {
		t.Fatalf("transported PeerAdmissionReceipt no longer parses in R7 authority: %v", err)
	}

	ack, err := NewAgencyTransportAck(delivery.ID().String(), delivery.EnvelopeDigest().String())
	if err != nil {
		t.Fatal(err)
	}
	parsedAck := roundTripAgencyDeliveryPayload(t, ack)
	if _, isReceipt := parsedAck.(AgencyAdmissionReceipt); isReceipt {
		t.Fatal("transport ACK was reinterpreted as an admission Receipt")
	}
	if _, isAck := parsedAck.(AgencyTransportAck); !isAck {
		t.Fatalf("ACK round trip type = %T", parsedAck)
	}
}

func TestAgencyWireBoundsCoverAuthorityBounds(t *testing.T) {
	if AgencyDeliveryCanonicalBytes < agency.MaxPeerDeliveryCanonicalBytes {
		t.Fatalf("Delivery wire bound %d is below authority bound %d",
			AgencyDeliveryCanonicalBytes, agency.MaxPeerDeliveryCanonicalBytes)
	}
	if AgencyAdmissionReceiptBytes < agency.MaxPeerAdmissionReceiptCanonicalBytes {
		t.Fatalf("Receipt wire bound %d is below authority bound %d",
			AgencyAdmissionReceiptBytes, agency.MaxPeerAdmissionReceiptCanonicalBytes)
	}
	if AgencyObjectBytes < agency.MaxPeerArtifactBytes {
		t.Fatalf("Object wire bound %d is below authority bound %d",
			AgencyObjectBytes, agency.MaxPeerArtifactBytes)
	}
}

func TestAgencyDeliveryFramesRejectNoncanonicalAndUnboundedWire(t *testing.T) {
	_, delivery, _ := agencyFrameDomainFixture(t)
	valid := AgencyDeliveryOfferSpec{DeliveryID: delivery.ID().String(),
		EnvelopeDigest:    delivery.EnvelopeDigest().String(),
		CanonicalDelivery: delivery.CanonicalJSON(),
		Signature:         bytes.Repeat([]byte{1}, ed25519.SignatureSize)}
	invalid := []AgencyDeliveryOfferSpec{
		{DeliveryID: strings.ToUpper(valid.DeliveryID), EnvelopeDigest: valid.EnvelopeDigest,
			CanonicalDelivery: valid.CanonicalDelivery, Signature: valid.Signature},
		{DeliveryID: valid.DeliveryID, EnvelopeDigest: valid.EnvelopeDigest,
			CanonicalDelivery: append([]byte(" "), valid.CanonicalDelivery...), Signature: valid.Signature},
		{DeliveryID: valid.DeliveryID, EnvelopeDigest: valid.EnvelopeDigest,
			CanonicalDelivery: []byte(`{"a":1,"a":2}`), Signature: valid.Signature},
		{DeliveryID: valid.DeliveryID, EnvelopeDigest: valid.EnvelopeDigest,
			CanonicalDelivery: []byte(`{"a":1.5}`), Signature: valid.Signature},
		{DeliveryID: valid.DeliveryID, EnvelopeDigest: valid.EnvelopeDigest,
			CanonicalDelivery: valid.CanonicalDelivery, Signature: valid.Signature[:len(valid.Signature)-1]},
	}
	for index, spec := range invalid {
		if _, err := NewAgencyDeliveryOffer(spec); !errors.Is(err, ErrAgencyFrame) {
			t.Errorf("invalid offer %d error = %v", index, err)
		}
	}

	offer, err := NewAgencyDeliveryOffer(valid)
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := NewAgencyDeliveryFrame(offer)
	noncanonical := append([]byte(" "), frame.CanonicalJSON().Bytes()...)
	if _, err := ParseAgencyDeliveryFrame(noncanonical); !errors.Is(err, ErrAgencyFrame) {
		t.Fatalf("noncanonical frame error = %v", err)
	}
	duplicate := bytes.Replace(frame.CanonicalJSON().Bytes(), []byte(`"version":1`),
		[]byte(`"version":1,"version":1`), 1)
	if _, err := ParseAgencyDeliveryFrame(duplicate); !errors.Is(err, ErrAgencyFrame) {
		t.Fatalf("duplicate frame field error = %v", err)
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], agencyDeliveryFrameBytes+1)
	if _, err := ReadAgencyDeliveryFrame(bytes.NewReader(prefix[:])); !errors.Is(err, ErrAgencyFrame) {
		t.Fatalf("oversized declared frame error = %v", err)
	}

	copyDelivery := offer.CanonicalDelivery()
	copySignature := offer.Signature()
	copyDelivery[0], copySignature[0] = '!', '!'
	if offer.CanonicalDelivery()[0] == '!' || offer.Signature()[0] == '!' {
		t.Fatal("Delivery offer exposed mutable bytes")
	}
}

func TestAgencyObjectFrameRequiresDeliveryScopeAndVerifiesRawBytes(t *testing.T) {
	_, delivery, _ := agencyFrameDomainFixture(t)
	body := []byte{0, 1, 2, 0xff, 'x'}
	digest := agencyObjectDigest(body)
	request, err := NewAgencyObjectRequest(delivery.ID().String(),
		delivery.EnvelopeDigest().String(), digest)
	if err != nil {
		t.Fatal(err)
	}
	response, err := NewAgencyObjectResponse(AgencyObjectResponseSpec{
		DeliveryID: request.DeliveryID(), EnvelopeDigest: request.EnvelopeDigest(),
		ObjectDigest: request.ObjectDigest(), Bytes: body})
	if err != nil {
		t.Fatal(err)
	}
	body[0] = 9
	if response.Bytes()[0] == 9 {
		t.Fatal("Object response retained caller-owned bytes")
	}
	body[0] = 0
	for _, payload := range []AgencyObjectPayload{request, response} {
		frame, err := NewAgencyObjectFrame(payload)
		if err != nil {
			t.Fatal(err)
		}
		var wire bytes.Buffer
		if err := WriteAgencyObjectFrame(&wire, frame); err != nil {
			t.Fatal(err)
		}
		parsed, err := ReadAgencyObjectFrame(&wire)
		if err != nil {
			t.Fatal(err)
		}
		if parsed.Type() != frame.Type() {
			t.Fatalf("Object type = %s, want %s", parsed.Type(), frame.Type())
		}
		if object, ok := parsed.Payload().(AgencyObjectResponse); ok &&
			!bytes.Equal(object.Bytes(), body) {
			t.Fatal("raw Object bytes changed in transit")
		}
	}
	if _, err := NewAgencyObjectRequest("", request.EnvelopeDigest(), digest); !errors.Is(err, ErrAgencyFrame) {
		t.Fatalf("bare digest Object request error = %v", err)
	}
	if _, err := NewAgencyObjectResponse(AgencyObjectResponseSpec{
		DeliveryID: request.DeliveryID(), EnvelopeDigest: request.EnvelopeDigest(),
		ObjectDigest: agencyObjectDigest([]byte("other")), Bytes: body,
	}); !errors.Is(err, ErrAgencyFrame) {
		t.Fatalf("mismatched Object digest error = %v", err)
	}

	frame, _ := NewAgencyObjectFrame(response)
	var encoded bytes.Buffer
	if err := WriteAgencyObjectFrame(&encoded, frame); err != nil {
		t.Fatal(err)
	}
	tampered := encoded.Bytes()
	tampered[len(tampered)-1] ^= 0xff
	if _, err := ReadAgencyObjectFrame(bytes.NewReader(tampered)); !errors.Is(err, ErrAgencyFrame) {
		t.Fatalf("tampered Object body error = %v", err)
	}
}

func roundTripAgencyDeliveryPayload(t *testing.T,
	payload AgencyDeliveryPayload,
) AgencyDeliveryPayload {
	t.Helper()
	frame, err := NewAgencyDeliveryFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	if err := WriteAgencyDeliveryFrame(&wire, frame); err != nil {
		t.Fatal(err)
	}
	parsed, err := ReadAgencyDeliveryFrame(&wire)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Payload()
}

func agencyFrameDomainFixture(t *testing.T) (agency.RouteID, agency.PeerDelivery,
	agency.PeerAdmissionReceipt,
) {
	t.Helper()
	route, err := agency.NewRouteID("route:frame")
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := agency.NewEventID("event:origin")
	eventRef, _ := agency.NewEventRef(eventID, agency.Sum([]byte("origin")))
	principal, _ := agency.NewAgentPrincipalID("agent:origin")
	target, _ := agency.NewOpaqueHandle("remote/target")
	kind, _ := agency.NewSemanticLabel("custom.agent.signal")
	payload, _ := agency.NewSemanticPayload("Review the referenced Artifact.")
	at := time.Date(2026, 8, 3, 8, 0, 0, 0, time.UTC)
	delivery, err := agency.NewPeerDelivery(route, agency.PeerDeliverySpec{
		OriginEvent: eventRef, OriginSequence: 1, OriginAcceptedAt: at,
		OriginSource: principal, TargetAlias: target, Kind: kind, Payload: payload,
		Artifacts: []agency.Digest{agency.Sum([]byte("artifact"))}, CausalDepth: 1,
		ExpiresAt: at.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	localID, _ := agency.NewEventID("event:local")
	local, _ := agency.NewEventRef(localID, agency.Sum([]byte("local")))
	receipt, err := agency.NewAcceptedPeerAdmissionReceipt(delivery, local, at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	return route, delivery, receipt
}
