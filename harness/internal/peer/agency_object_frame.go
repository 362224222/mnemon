package peer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	AgencyObjectBytes        = 4 << 20
	agencyObjectFrameVersion = 1
	agencyObjectHeaderBytes  = 4 << 10
)

type AgencyObjectFrameType string

const (
	AgencyObjectFrameRequest AgencyObjectFrameType = "object_request"
	AgencyObjectFrameObject  AgencyObjectFrameType = "object_response"
	AgencyObjectFrameError   AgencyObjectFrameType = "protocol_error"
)

func (frameType AgencyObjectFrameType) Valid() bool {
	switch frameType {
	case AgencyObjectFrameRequest, AgencyObjectFrameObject, AgencyObjectFrameError:
		return true
	default:
		return false
	}
}

type AgencyObjectPayload interface {
	CanonicalJSON() model.JSON
	IsZero() bool
	agencyObjectPayload()
}

type AgencyObjectReply interface {
	AgencyObjectPayload
	agencyObjectReply()
}

// AgencyObjectFrame keeps arbitrary Artifact bytes outside JSON. Its exact
// canonical header declares the body length and digest; the body follows
// immediately and is always scoped by a DeliveryID.
type AgencyObjectFrame struct {
	frameType AgencyObjectFrameType
	payload   AgencyObjectPayload
	header    model.JSON
	body      []byte
}

type agencyObjectFrameWire struct {
	Payload json.RawMessage       `json:"payload"`
	Type    AgencyObjectFrameType `json:"type"`
	Version uint8                 `json:"version"`
}

func NewAgencyObjectFrame(payload AgencyObjectPayload) (AgencyObjectFrame, error) {
	frameType, canonicalPayload, body, err := canonicalAgencyObjectPayload(payload)
	if err != nil {
		return AgencyObjectFrame{}, err
	}
	header, err := canonicalAgencyJSON(agencyObjectFrameWire{Payload: canonicalPayload.Bytes(),
		Type: frameType, Version: agencyObjectFrameVersion}, agencyObjectHeaderBytes)
	if err != nil {
		return AgencyObjectFrame{}, err
	}
	return AgencyObjectFrame{frameType: frameType, payload: payload,
		header: header, body: body}, nil
}

func canonicalAgencyObjectPayload(payload AgencyObjectPayload) (
	AgencyObjectFrameType, model.JSON, []byte, error,
) {
	if isNilAgencyValue(payload) || payload.IsZero() {
		return "", model.JSON{}, nil, agencyFrameError("complete Object payload is required", nil)
	}
	switch value := payload.(type) {
	case AgencyObjectRequest:
		return AgencyObjectFrameRequest, value.CanonicalJSON(), nil, nil
	case AgencyObjectResponse:
		return AgencyObjectFrameObject, value.CanonicalJSON(), value.bytes, nil
	case AgencyProtocolError:
		return AgencyObjectFrameError, value.CanonicalJSON(), nil, nil
	default:
		return "", model.JSON{}, nil, agencyFrameError("unknown Object payload implementation", nil)
	}
}

func ReadAgencyObjectFrame(reader io.Reader) (AgencyObjectFrame, error) {
	frame, release, err := readAgencyObjectFrame(reader, nil)
	if release != nil {
		defer release()
	}
	return frame, err
}

func readAgencyObjectStreamFrame(stream network.Stream) (AgencyObjectFrame, func(), error) {
	if stream == nil || stream.Scope() == nil {
		return AgencyObjectFrame{}, nil, agencyFrameError("live Object stream is required", nil)
	}
	return readAgencyObjectFrame(stream, stream.Scope())
}

func readAgencyObjectFrame(reader io.Reader,
	scope network.ResourceScope,
) (AgencyObjectFrame, func(), error) {
	header, releaseHeader, err := readAgencyJSON(reader, scope, agencyObjectHeaderBytes)
	if err != nil {
		return AgencyObjectFrame{}, nil, err
	}
	wire, bodyBytes, err := parseAgencyObjectHeader(header)
	if err != nil {
		if releaseHeader != nil {
			releaseHeader()
		}
		return AgencyObjectFrame{}, nil, err
	}
	var releaseBody func()
	if bodyBytes > 0 && scope != nil {
		if err := scope.ReserveMemory(bodyBytes, network.ReservationPriorityAlways); err != nil {
			if releaseHeader != nil {
				releaseHeader()
			}
			return AgencyObjectFrame{}, nil, agencyFrameError("reserve Object body memory", err)
		}
		var once sync.Once
		releaseBody = func() { once.Do(func() { scope.ReleaseMemory(bodyBytes) }) }
	}
	release := func() {
		if releaseBody != nil {
			releaseBody()
		}
		if releaseHeader != nil {
			releaseHeader()
		}
	}
	body := make([]byte, bodyBytes)
	if _, err := io.ReadFull(reader, body); err != nil {
		release()
		return AgencyObjectFrame{}, nil, agencyFrameError("read declared Object body", err)
	}
	payload, err := parseAgencyObjectPayload(wire.Type, wire.Payload, body)
	if err != nil {
		release()
		return AgencyObjectFrame{}, nil, err
	}
	frame, err := NewAgencyObjectFrame(payload)
	if err != nil || !bytes.Equal(frame.header.Bytes(), header) {
		release()
		return AgencyObjectFrame{}, nil, agencyFrameError("Object frame is not canonical", err)
	}
	return frame, release, nil
}

func parseAgencyObjectHeader(raw []byte) (agencyObjectFrameWire, int, error) {
	var wire agencyObjectFrameWire
	if err := decodeExactAgencyJSON(raw, agencyObjectHeaderBytes, &wire); err != nil {
		return agencyObjectFrameWire{}, 0, err
	}
	if wire.Version != agencyObjectFrameVersion || !wire.Type.Valid() {
		return agencyObjectFrameWire{}, 0,
			agencyFrameError("unsupported Object frame", nil)
	}
	if wire.Type != AgencyObjectFrameObject {
		return wire, 0, nil
	}
	var objectHeader agencyObjectResponseWire
	if err := decodeExactAgencyJSON(wire.Payload, agencyObjectHeaderBytes, &objectHeader); err != nil {
		return agencyObjectFrameWire{}, 0, err
	}
	if objectHeader.BodyBytes < 0 || objectHeader.BodyBytes > AgencyObjectBytes {
		return agencyObjectFrameWire{}, 0, agencyFrameError("Object body exceeds its wire bound", nil)
	}
	return wire, objectHeader.BodyBytes, nil
}

func parseAgencyObjectPayload(frameType AgencyObjectFrameType, raw, body []byte) (
	AgencyObjectPayload, error,
) {
	switch frameType {
	case AgencyObjectFrameRequest:
		if len(body) != 0 {
			return nil, agencyFrameError("Object request cannot carry a body", nil)
		}
		return parseAgencyObjectRequest(raw)
	case AgencyObjectFrameObject:
		return parseAgencyObjectResponse(raw, body)
	case AgencyObjectFrameError:
		if len(body) != 0 {
			return nil, agencyFrameError("protocol failure cannot carry a body", nil)
		}
		return parseAgencyProtocolError(raw)
	default:
		return nil, agencyFrameError("unknown Object frame type", nil)
	}
}

func WriteAgencyObjectFrame(writer io.Writer, frame AgencyObjectFrame) error {
	if frame.IsZero() {
		return agencyFrameError("complete Object frame is required", nil)
	}
	if err := writeAgencyJSON(writer, frame.header, agencyObjectHeaderBytes); err != nil {
		return err
	}
	if err := writeFullAgencyBytes(writer, frame.body); err != nil {
		return agencyFrameError("write Object body", err)
	}
	return nil
}

func (frame AgencyObjectFrame) Type() AgencyObjectFrameType  { return frame.frameType }
func (frame AgencyObjectFrame) Payload() AgencyObjectPayload { return frame.payload }
func (frame AgencyObjectFrame) CanonicalHeader() model.JSON  { return frame.header }
func (frame AgencyObjectFrame) IsZero() bool {
	return !frame.frameType.Valid() || frame.payload == nil || frame.header.IsZero()
}

type AgencyObjectRequest struct {
	deliveryID     string
	envelopeDigest string
	objectDigest   string
	canonical      model.JSON
}

type agencyObjectRequestWire struct {
	DeliveryID     string `json:"delivery_id"`
	EnvelopeDigest string `json:"envelope_digest"`
	ObjectDigest   string `json:"object_digest"`
}

func NewAgencyObjectRequest(deliveryID, envelopeDigest,
	objectDigest string,
) (AgencyObjectRequest, error) {
	if !validateAgencyDeliveryID(deliveryID) || !validateAgencyDigest(envelopeDigest) ||
		!validateAgencyDigest(objectDigest) {
		return AgencyObjectRequest{}, agencyFrameError("invalid delivery-scoped Object request", nil)
	}
	canonical, err := canonicalAgencyJSON(agencyObjectRequestWire{DeliveryID: deliveryID,
		EnvelopeDigest: envelopeDigest, ObjectDigest: objectDigest}, agencyObjectHeaderBytes)
	if err != nil {
		return AgencyObjectRequest{}, err
	}
	return AgencyObjectRequest{deliveryID: deliveryID, envelopeDigest: envelopeDigest,
		objectDigest: objectDigest, canonical: canonical}, nil
}

func parseAgencyObjectRequest(raw []byte) (AgencyObjectRequest, error) {
	var wire agencyObjectRequestWire
	if err := decodeExactAgencyJSON(raw, agencyObjectHeaderBytes, &wire); err != nil {
		return AgencyObjectRequest{}, err
	}
	payload, err := NewAgencyObjectRequest(wire.DeliveryID, wire.EnvelopeDigest, wire.ObjectDigest)
	if err != nil || !bytes.Equal(payload.canonical.Bytes(), raw) {
		return AgencyObjectRequest{}, agencyFrameError("Object request is not canonical", err)
	}
	return payload, nil
}

func (payload AgencyObjectRequest) DeliveryID() string        { return payload.deliveryID }
func (payload AgencyObjectRequest) EnvelopeDigest() string    { return payload.envelopeDigest }
func (payload AgencyObjectRequest) ObjectDigest() string      { return payload.objectDigest }
func (payload AgencyObjectRequest) CanonicalJSON() model.JSON { return payload.canonical }
func (payload AgencyObjectRequest) IsZero() bool              { return payload.canonical.IsZero() }
func (AgencyObjectRequest) agencyObjectPayload()              {}

type AgencyObjectResponseSpec struct {
	DeliveryID     string
	EnvelopeDigest string
	ObjectDigest   string
	Bytes          []byte
}

type AgencyObjectResponse struct {
	deliveryID     string
	envelopeDigest string
	objectDigest   string
	bytes          []byte
	canonical      model.JSON
}

type agencyObjectResponseWire struct {
	BodyBytes      int    `json:"body_bytes"`
	DeliveryID     string `json:"delivery_id"`
	EnvelopeDigest string `json:"envelope_digest"`
	ObjectDigest   string `json:"object_digest"`
}

func NewAgencyObjectResponse(spec AgencyObjectResponseSpec) (AgencyObjectResponse, error) {
	return newAgencyObjectResponse(spec, false)
}

func newAgencyObjectResponse(spec AgencyObjectResponseSpec,
	takeBytes bool,
) (AgencyObjectResponse, error) {
	if !validateAgencyDeliveryID(spec.DeliveryID) || !validateAgencyDigest(spec.EnvelopeDigest) ||
		!validateAgencyDigest(spec.ObjectDigest) || len(spec.Bytes) > AgencyObjectBytes ||
		agencyObjectDigest(spec.Bytes) != spec.ObjectDigest {
		return AgencyObjectResponse{}, agencyFrameError("invalid delivery-scoped Object response", nil)
	}
	canonical, err := canonicalAgencyJSON(agencyObjectResponseWire{BodyBytes: len(spec.Bytes),
		DeliveryID: spec.DeliveryID, EnvelopeDigest: spec.EnvelopeDigest,
		ObjectDigest: spec.ObjectDigest}, agencyObjectHeaderBytes)
	if err != nil {
		return AgencyObjectResponse{}, err
	}
	body := spec.Bytes
	if !takeBytes {
		body = append([]byte(nil), spec.Bytes...)
	}
	return AgencyObjectResponse{deliveryID: spec.DeliveryID,
		envelopeDigest: spec.EnvelopeDigest, objectDigest: spec.ObjectDigest,
		bytes: body, canonical: canonical}, nil
}

func parseAgencyObjectResponse(raw, body []byte) (AgencyObjectResponse, error) {
	var wire agencyObjectResponseWire
	if err := decodeExactAgencyJSON(raw, agencyObjectHeaderBytes, &wire); err != nil {
		return AgencyObjectResponse{}, err
	}
	if wire.BodyBytes != len(body) {
		return AgencyObjectResponse{}, agencyFrameError("Object body length does not match its header", nil)
	}
	payload, err := newAgencyObjectResponse(AgencyObjectResponseSpec{DeliveryID: wire.DeliveryID,
		EnvelopeDigest: wire.EnvelopeDigest, ObjectDigest: wire.ObjectDigest, Bytes: body}, true)
	if err != nil || !bytes.Equal(payload.canonical.Bytes(), raw) {
		return AgencyObjectResponse{}, agencyFrameError("Object response is not canonical", err)
	}
	return payload, nil
}

func (payload AgencyObjectResponse) DeliveryID() string        { return payload.deliveryID }
func (payload AgencyObjectResponse) EnvelopeDigest() string    { return payload.envelopeDigest }
func (payload AgencyObjectResponse) ObjectDigest() string      { return payload.objectDigest }
func (payload AgencyObjectResponse) Bytes() []byte             { return append([]byte(nil), payload.bytes...) }
func (payload AgencyObjectResponse) CanonicalJSON() model.JSON { return payload.canonical }
func (payload AgencyObjectResponse) IsZero() bool {
	return payload.canonical.IsZero() || len(payload.bytes) > AgencyObjectBytes
}
func (AgencyObjectResponse) agencyObjectPayload() {}
func (AgencyObjectResponse) agencyObjectReply()   {}

func (AgencyProtocolError) agencyObjectPayload() {}

func agencyObjectDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}
