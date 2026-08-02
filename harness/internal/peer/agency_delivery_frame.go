package peer

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"io"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	AgencyDeliveryCanonicalBytes = 32 << 10
	AgencyAdmissionReceiptBytes  = 4 << 10

	agencyDeliveryFrameVersion = 1
	agencyDeliveryFrameBytes   = 48 << 10
	agencyDeliveryReplyBytes   = 8 << 10
)

type AgencyDeliveryFrameType string

const (
	AgencyDeliveryFrameOffer   AgencyDeliveryFrameType = "delivery_offer"
	AgencyDeliveryFrameAck     AgencyDeliveryFrameType = "transport_ack"
	AgencyDeliveryFrameReceipt AgencyDeliveryFrameType = "admission_receipt"
	AgencyDeliveryFrameError   AgencyDeliveryFrameType = "protocol_error"
)

func (frameType AgencyDeliveryFrameType) Valid() bool {
	switch frameType {
	case AgencyDeliveryFrameOffer, AgencyDeliveryFrameAck,
		AgencyDeliveryFrameReceipt, AgencyDeliveryFrameError:
		return true
	default:
		return false
	}
}

type AgencyDeliveryPayload interface {
	CanonicalJSON() model.JSON
	IsZero() bool
	agencyDeliveryPayload()
}

// AgencyDeliveryReply is deliberately split between a transport-only ACK and
// an opaque signed admission Receipt. Callers must not treat the former as the
// latter or infer a domain outcome from stream success.
type AgencyDeliveryReply interface {
	AgencyDeliveryPayload
	agencyDeliveryReply()
}

type AgencyDeliveryFrame struct {
	frameType AgencyDeliveryFrameType
	payload   AgencyDeliveryPayload
	canonical model.JSON
}

type agencyDeliveryFrameWire struct {
	Payload json.RawMessage         `json:"payload"`
	Type    AgencyDeliveryFrameType `json:"type"`
	Version uint8                   `json:"version"`
}

func NewAgencyDeliveryFrame(payload AgencyDeliveryPayload) (AgencyDeliveryFrame, error) {
	frameType, canonicalPayload, err := canonicalAgencyDeliveryPayload(payload)
	if err != nil {
		return AgencyDeliveryFrame{}, err
	}
	canonical, err := canonicalAgencyJSON(agencyDeliveryFrameWire{Payload: canonicalPayload.Bytes(),
		Type: frameType, Version: agencyDeliveryFrameVersion}, agencyDeliveryFrameMaximum(frameType))
	if err != nil {
		return AgencyDeliveryFrame{}, err
	}
	return AgencyDeliveryFrame{frameType: frameType, payload: payload, canonical: canonical}, nil
}

func canonicalAgencyDeliveryPayload(payload AgencyDeliveryPayload) (
	AgencyDeliveryFrameType, model.JSON, error,
) {
	if isNilAgencyValue(payload) || payload.IsZero() {
		return "", model.JSON{}, agencyFrameError("complete Delivery payload is required", nil)
	}
	var frameType AgencyDeliveryFrameType
	switch payload.(type) {
	case AgencyDeliveryOffer:
		frameType = AgencyDeliveryFrameOffer
	case AgencyTransportAck:
		frameType = AgencyDeliveryFrameAck
	case AgencyAdmissionReceipt:
		frameType = AgencyDeliveryFrameReceipt
	case AgencyProtocolError:
		frameType = AgencyDeliveryFrameError
	default:
		return "", model.JSON{}, agencyFrameError("unknown Delivery payload implementation", nil)
	}
	return frameType, payload.CanonicalJSON(), nil
}

func ParseAgencyDeliveryFrame(raw []byte) (AgencyDeliveryFrame, error) {
	var wire agencyDeliveryFrameWire
	if err := decodeExactAgencyJSON(raw, agencyDeliveryFrameBytes, &wire); err != nil {
		return AgencyDeliveryFrame{}, err
	}
	if wire.Version != agencyDeliveryFrameVersion || !wire.Type.Valid() ||
		len(raw) > agencyDeliveryFrameMaximum(wire.Type) {
		return AgencyDeliveryFrame{}, agencyFrameError("unsupported or oversized Delivery frame", nil)
	}
	payload, err := parseAgencyDeliveryPayload(wire.Type, wire.Payload)
	if err != nil {
		return AgencyDeliveryFrame{}, err
	}
	frame, err := NewAgencyDeliveryFrame(payload)
	if err != nil {
		return AgencyDeliveryFrame{}, err
	}
	if frame.frameType != wire.Type || !bytes.Equal(frame.canonical.Bytes(), raw) {
		return AgencyDeliveryFrame{}, agencyFrameError("Delivery frame is not canonical", nil)
	}
	return frame, nil
}

func parseAgencyDeliveryPayload(frameType AgencyDeliveryFrameType,
	raw []byte,
) (AgencyDeliveryPayload, error) {
	switch frameType {
	case AgencyDeliveryFrameOffer:
		return parseAgencyDeliveryOffer(raw)
	case AgencyDeliveryFrameAck:
		return parseAgencyTransportAck(raw)
	case AgencyDeliveryFrameReceipt:
		return parseAgencyAdmissionReceipt(raw)
	case AgencyDeliveryFrameError:
		return parseAgencyProtocolError(raw)
	default:
		return nil, agencyFrameError("unknown Delivery frame type", nil)
	}
}

func ReadAgencyDeliveryFrame(reader io.Reader) (AgencyDeliveryFrame, error) {
	raw, release, err := readAgencyJSON(reader, nil, agencyDeliveryFrameBytes)
	if release != nil {
		defer release()
	}
	if err != nil {
		return AgencyDeliveryFrame{}, err
	}
	return ParseAgencyDeliveryFrame(raw)
}

func readAgencyDeliveryStreamFrame(stream network.Stream,
	maximum int,
) (AgencyDeliveryFrame, func(), error) {
	if stream == nil || stream.Scope() == nil || maximum <= 0 || maximum > agencyDeliveryFrameBytes {
		return AgencyDeliveryFrame{}, nil, agencyFrameError("live Delivery stream and valid bound are required", nil)
	}
	raw, release, err := readAgencyJSON(stream, stream.Scope(), maximum)
	if err != nil {
		return AgencyDeliveryFrame{}, nil, err
	}
	frame, err := ParseAgencyDeliveryFrame(raw)
	if err != nil {
		release()
		return AgencyDeliveryFrame{}, nil, err
	}
	return frame, release, nil
}

func WriteAgencyDeliveryFrame(writer io.Writer, frame AgencyDeliveryFrame) error {
	if frame.IsZero() {
		return agencyFrameError("complete Delivery frame is required", nil)
	}
	return writeAgencyJSON(writer, frame.canonical, agencyDeliveryFrameMaximum(frame.frameType))
}

func agencyDeliveryFrameMaximum(frameType AgencyDeliveryFrameType) int {
	switch frameType {
	case AgencyDeliveryFrameOffer:
		return agencyDeliveryFrameBytes
	case AgencyDeliveryFrameAck, AgencyDeliveryFrameReceipt, AgencyDeliveryFrameError:
		return agencyDeliveryReplyBytes
	default:
		return 0
	}
}

func (frame AgencyDeliveryFrame) Type() AgencyDeliveryFrameType  { return frame.frameType }
func (frame AgencyDeliveryFrame) Payload() AgencyDeliveryPayload { return frame.payload }
func (frame AgencyDeliveryFrame) CanonicalJSON() model.JSON      { return frame.canonical }
func (frame AgencyDeliveryFrame) IsZero() bool {
	return !frame.frameType.Valid() || frame.payload == nil || frame.canonical.IsZero()
}

type AgencyDeliveryOfferSpec struct {
	DeliveryID        string
	EnvelopeDigest    string
	CanonicalDelivery []byte
	Signature         []byte
}

type AgencyDeliveryOffer struct {
	deliveryID        string
	envelopeDigest    string
	canonicalDelivery []byte
	signature         []byte
	canonical         model.JSON
}

type agencyDeliveryOfferWire struct {
	CanonicalDelivery []byte `json:"canonical_delivery"`
	DeliveryID        string `json:"delivery_id"`
	EnvelopeDigest    string `json:"envelope_digest"`
	Signature         []byte `json:"signature"`
}

func NewAgencyDeliveryOffer(spec AgencyDeliveryOfferSpec) (AgencyDeliveryOffer, error) {
	delivery, err := exactAgencyEmbeddedJSON(spec.CanonicalDelivery, AgencyDeliveryCanonicalBytes)
	if err != nil || !validateAgencyDeliveryID(spec.DeliveryID) ||
		!validateAgencyDigest(spec.EnvelopeDigest) || len(spec.Signature) != ed25519.SignatureSize {
		return AgencyDeliveryOffer{}, agencyFrameError("invalid Delivery offer", err)
	}
	wire := agencyDeliveryOfferWire{CanonicalDelivery: delivery,
		DeliveryID: spec.DeliveryID, EnvelopeDigest: spec.EnvelopeDigest,
		Signature: append([]byte(nil), spec.Signature...)}
	canonical, err := canonicalAgencyJSON(wire, agencyDeliveryFrameBytes)
	if err != nil {
		return AgencyDeliveryOffer{}, err
	}
	return AgencyDeliveryOffer{deliveryID: spec.DeliveryID,
		envelopeDigest: spec.EnvelopeDigest, canonicalDelivery: delivery,
		signature: append([]byte(nil), spec.Signature...), canonical: canonical}, nil
}

func parseAgencyDeliveryOffer(raw []byte) (AgencyDeliveryOffer, error) {
	var wire agencyDeliveryOfferWire
	if err := decodeExactAgencyJSON(raw, agencyDeliveryFrameBytes, &wire); err != nil {
		return AgencyDeliveryOffer{}, err
	}
	payload, err := NewAgencyDeliveryOffer(AgencyDeliveryOfferSpec{DeliveryID: wire.DeliveryID,
		EnvelopeDigest: wire.EnvelopeDigest, CanonicalDelivery: wire.CanonicalDelivery,
		Signature: wire.Signature})
	if err != nil {
		return AgencyDeliveryOffer{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return AgencyDeliveryOffer{}, agencyFrameError("Delivery offer is not canonical", nil)
	}
	return payload, nil
}

func (payload AgencyDeliveryOffer) DeliveryID() string     { return payload.deliveryID }
func (payload AgencyDeliveryOffer) EnvelopeDigest() string { return payload.envelopeDigest }
func (payload AgencyDeliveryOffer) CanonicalDelivery() []byte {
	return append([]byte(nil), payload.canonicalDelivery...)
}
func (payload AgencyDeliveryOffer) Signature() []byte {
	return append([]byte(nil), payload.signature...)
}
func (payload AgencyDeliveryOffer) CanonicalJSON() model.JSON { return payload.canonical }
func (payload AgencyDeliveryOffer) IsZero() bool {
	return !validateAgencyDeliveryID(payload.deliveryID) || payload.canonical.IsZero()
}
func (AgencyDeliveryOffer) agencyDeliveryPayload() {}

type AgencyTransportAck struct {
	deliveryID     string
	envelopeDigest string
	canonical      model.JSON
}

type agencyTransportAckWire struct {
	DeliveryID     string `json:"delivery_id"`
	EnvelopeDigest string `json:"envelope_digest"`
}

func NewAgencyTransportAck(deliveryID, envelopeDigest string) (AgencyTransportAck, error) {
	if !validateAgencyDeliveryID(deliveryID) || !validateAgencyDigest(envelopeDigest) {
		return AgencyTransportAck{}, agencyFrameError("invalid transport ACK binding", nil)
	}
	canonical, err := canonicalAgencyJSON(agencyTransportAckWire{DeliveryID: deliveryID,
		EnvelopeDigest: envelopeDigest}, agencyDeliveryReplyBytes)
	if err != nil {
		return AgencyTransportAck{}, err
	}
	return AgencyTransportAck{deliveryID: deliveryID, envelopeDigest: envelopeDigest,
		canonical: canonical}, nil
}

func parseAgencyTransportAck(raw []byte) (AgencyTransportAck, error) {
	var wire agencyTransportAckWire
	if err := decodeExactAgencyJSON(raw, agencyDeliveryReplyBytes, &wire); err != nil {
		return AgencyTransportAck{}, err
	}
	payload, err := NewAgencyTransportAck(wire.DeliveryID, wire.EnvelopeDigest)
	if err != nil || !bytes.Equal(payload.canonical.Bytes(), raw) {
		return AgencyTransportAck{}, agencyFrameError("transport ACK is not canonical", err)
	}
	return payload, nil
}

func (payload AgencyTransportAck) DeliveryID() string        { return payload.deliveryID }
func (payload AgencyTransportAck) EnvelopeDigest() string    { return payload.envelopeDigest }
func (payload AgencyTransportAck) CanonicalJSON() model.JSON { return payload.canonical }
func (payload AgencyTransportAck) IsZero() bool              { return payload.canonical.IsZero() }
func (AgencyTransportAck) agencyDeliveryPayload()            {}
func (AgencyTransportAck) agencyDeliveryReply()              {}

type AgencyAdmissionReceiptSpec struct {
	DeliveryID       string
	EnvelopeDigest   string
	CanonicalReceipt []byte
	Signature        []byte
}

type AgencyAdmissionReceipt struct {
	deliveryID       string
	envelopeDigest   string
	canonicalReceipt []byte
	signature        []byte
	canonical        model.JSON
}

type agencyAdmissionReceiptWire struct {
	CanonicalReceipt []byte `json:"canonical_receipt"`
	DeliveryID       string `json:"delivery_id"`
	EnvelopeDigest   string `json:"envelope_digest"`
	Signature        []byte `json:"signature"`
}

func NewAgencyAdmissionReceipt(spec AgencyAdmissionReceiptSpec) (AgencyAdmissionReceipt, error) {
	receipt, err := exactAgencyEmbeddedJSON(spec.CanonicalReceipt, AgencyAdmissionReceiptBytes)
	if err != nil || !validateAgencyDeliveryID(spec.DeliveryID) ||
		!validateAgencyDigest(spec.EnvelopeDigest) || len(spec.Signature) != ed25519.SignatureSize {
		return AgencyAdmissionReceipt{}, agencyFrameError("invalid admission Receipt envelope", err)
	}
	canonical, err := canonicalAgencyJSON(agencyAdmissionReceiptWire{CanonicalReceipt: receipt,
		DeliveryID: spec.DeliveryID, EnvelopeDigest: spec.EnvelopeDigest,
		Signature: append([]byte(nil), spec.Signature...)}, agencyDeliveryReplyBytes)
	if err != nil {
		return AgencyAdmissionReceipt{}, err
	}
	return AgencyAdmissionReceipt{deliveryID: spec.DeliveryID,
		envelopeDigest: spec.EnvelopeDigest, canonicalReceipt: receipt,
		signature: append([]byte(nil), spec.Signature...), canonical: canonical}, nil
}

func parseAgencyAdmissionReceipt(raw []byte) (AgencyAdmissionReceipt, error) {
	var wire agencyAdmissionReceiptWire
	if err := decodeExactAgencyJSON(raw, agencyDeliveryReplyBytes, &wire); err != nil {
		return AgencyAdmissionReceipt{}, err
	}
	payload, err := NewAgencyAdmissionReceipt(AgencyAdmissionReceiptSpec{DeliveryID: wire.DeliveryID,
		EnvelopeDigest: wire.EnvelopeDigest, CanonicalReceipt: wire.CanonicalReceipt,
		Signature: wire.Signature})
	if err != nil || !bytes.Equal(payload.canonical.Bytes(), raw) {
		return AgencyAdmissionReceipt{}, agencyFrameError("admission Receipt envelope is not canonical", err)
	}
	return payload, nil
}

func (payload AgencyAdmissionReceipt) DeliveryID() string     { return payload.deliveryID }
func (payload AgencyAdmissionReceipt) EnvelopeDigest() string { return payload.envelopeDigest }
func (payload AgencyAdmissionReceipt) CanonicalReceipt() []byte {
	return append([]byte(nil), payload.canonicalReceipt...)
}
func (payload AgencyAdmissionReceipt) Signature() []byte {
	return append([]byte(nil), payload.signature...)
}
func (payload AgencyAdmissionReceipt) CanonicalJSON() model.JSON { return payload.canonical }
func (payload AgencyAdmissionReceipt) IsZero() bool              { return payload.canonical.IsZero() }
func (AgencyAdmissionReceipt) agencyDeliveryPayload()            {}
func (AgencyAdmissionReceipt) agencyDeliveryReply()              {}

func (AgencyProtocolError) agencyDeliveryPayload() {}

func exactAgencyEmbeddedJSON(raw []byte, maximum int) ([]byte, error) {
	if len(raw) == 0 || len(raw) > maximum {
		return nil, agencyFrameError("embedded canonical JSON exceeds its bound", nil)
	}
	// R7 agency values intentionally freeze encoding/json struct-field order,
	// while the older model.JSON type sorts object keys. Validate duplicate
	// keys, integer-only values, Unicode, and compactness here without changing
	// byte order. The authority layer later runs the exact R7 typed parser.
	if _, err := model.CanonicalizeJSON(raw); err != nil {
		return nil, agencyFrameError("embedded value is not strict JSON", err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil || !bytes.Equal(compact.Bytes(), raw) {
		return nil, agencyFrameError("embedded value must be compact JSON", err)
	}
	return append([]byte(nil), raw...), nil
}
