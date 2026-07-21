package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const (
	ChannelLeaveRequestSignatureDomain = "mnemon/r5/channel-leave-request/1"
	ChannelLeaveReceiptSignatureDomain = "mnemon/r5/channel-leave-receipt/1"
)

// ChannelLeaveRequestID is derived from the signed request digest. Callers
// cannot select a second durable identity for the same voluntary leave.
type ChannelLeaveRequestID struct{ identifier }

func ParseChannelLeaveRequestID(value string) (ChannelLeaveRequestID, error) {
	id, err := newIdentifier("channel_leave_request_id", value)
	return ChannelLeaveRequestID{id}, err
}

func ChannelLeaveRequestIDForDigest(digest Digest) (ChannelLeaveRequestID, error) {
	if digest.IsZero() {
		return ChannelLeaveRequestID{}, invalid("Channel leave request ID", "request digest is required")
	}
	return ParseChannelLeaveRequestID("channel-leave-request-" + hex.EncodeToString(digest.Bytes()))
}

type ChannelLeaveRequestRecordSpec struct {
	ChannelID        ChannelID
	MemberPeerID     PeerID
	ActiveMemberHead RecordHead
	KnownRosterHead  RecordHead
	RequestedAt      time.Time
}

// ChannelLeaveRequestRecord freezes the exact active membership and roster
// prefix from which a non-owner exits locally. The owner may acknowledge this
// request only by extending that prefix with owner-signed roster authority.
type ChannelLeaveRequestRecord struct {
	spec        ChannelLeaveRequestRecordSpec
	requestedAt time.Time
	canonical   JSON
	digest      Digest
}

type channelLeaveRequestRecordWire struct {
	ActiveMemberHead recordHeadWire `json:"active_member_head"`
	ChannelID        string         `json:"channel_id"`
	KnownRosterHead  recordHeadWire `json:"known_roster_head"`
	MemberPeerID     string         `json:"member_peer_id"`
	RequestedAt      string         `json:"requested_at"`
	SchemaVersion    int            `json:"schema_version"`
}

type recordHeadWire struct {
	Digest   string `json:"digest"`
	Revision uint64 `json:"revision"`
}

func NewChannelLeaveRequestRecord(spec ChannelLeaveRequestRecordSpec) (ChannelLeaveRequestRecord, error) {
	if spec.ChannelID.IsZero() || spec.MemberPeerID.IsZero() || spec.ActiveMemberHead.IsZero() ||
		spec.KnownRosterHead.IsZero() {
		return ChannelLeaveRequestRecord{}, invalid("Channel leave request",
			"Channel, member and both roster heads are required")
	}
	if spec.ActiveMemberHead.Revision() > spec.KnownRosterHead.Revision() ||
		spec.KnownRosterHead.Revision() > MaxMemberRecordsPerChannel {
		return ChannelLeaveRequestRecord{}, invariant("Channel leave request heads are out of bounds")
	}
	if _, err := CanonicalPeerIDBytes(spec.MemberPeerID); err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	requestedAt, err := canonicalTime(spec.RequestedAt)
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	wire := channelLeaveRequestRecordWire{ActiveMemberHead: leaveRecordHeadWire(spec.ActiveMemberHead),
		ChannelID: spec.ChannelID.String(), KnownRosterHead: leaveRecordHeadWire(spec.KnownRosterHead),
		MemberPeerID: spec.MemberPeerID.String(), RequestedAt: formatTime(requestedAt),
		SchemaVersion: SchemaVersion}
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxChannelRecordBytes {
		if err != nil {
			return ChannelLeaveRequestRecord{}, err
		}
		return ChannelLeaveRequestRecord{}, limit("Channel leave request", len(canonical.raw),
			MaxChannelRecordBytes)
	}
	spec.RequestedAt = requestedAt
	return ChannelLeaveRequestRecord{spec: spec, requestedAt: requestedAt, canonical: canonical,
		digest: Sum(canonical.Bytes())}, nil
}

func ParseChannelLeaveRequestRecord(raw []byte) (ChannelLeaveRequestRecord, error) {
	var wire channelLeaveRequestRecordWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return ChannelLeaveRequestRecord{}, fmt.Errorf("parse Channel leave request: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion {
		return ChannelLeaveRequestRecord{}, invalid("Channel leave request", "unsupported schema version")
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	memberPeerID, err := ParsePeerID(wire.MemberPeerID)
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	activeHead, err := parseLeaveRecordHead(wire.ActiveMemberHead)
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	knownHead, err := parseLeaveRecordHead(wire.KnownRosterHead)
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	requestedAt, err := parseLeaveTime("requested_at", wire.RequestedAt)
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	record, err := NewChannelLeaveRequestRecord(ChannelLeaveRequestRecordSpec{ChannelID: channelID,
		MemberPeerID: memberPeerID, ActiveMemberHead: activeHead, KnownRosterHead: knownHead,
		RequestedAt: requestedAt})
	if err != nil {
		return ChannelLeaveRequestRecord{}, err
	}
	if !bytes.Equal(record.canonical.Bytes(), raw) {
		return ChannelLeaveRequestRecord{}, invalid("Channel leave request", "wire bytes are not canonical")
	}
	return record, nil
}

func ChannelLeaveRequestSigningMessage(channelID ChannelID, digest Digest) ([]byte, error) {
	return channelSigningMessage(ChannelLeaveRequestSignatureDomain, channelID, digest)
}

type SignedChannelLeaveRequest struct {
	record    ChannelLeaveRequestRecord
	signature string
	wire      JSON
}

type signedChannelLeaveRequestWire struct {
	MemberSignature []byte          `json:"member_signature"`
	Request         json.RawMessage `json:"request"`
	RequestDigest   string          `json:"request_digest"`
}

func AttachChannelLeaveRequestSignature(record ChannelLeaveRequestRecord,
	signature []byte,
) (SignedChannelLeaveRequest, error) {
	if record.IsZero() || len(signature) != ed25519.SignatureSize {
		return SignedChannelLeaveRequest{}, invalid("signed Channel leave request",
			"request and a 64-byte member signature are required")
	}
	copySignature := append([]byte(nil), signature...)
	wire, err := JSONFrom(struct {
		MemberSignature []byte `json:"member_signature"`
		Request         JSON   `json:"request"`
		RequestDigest   Digest `json:"request_digest"`
	}{copySignature, record.CanonicalJSON(), record.Digest()})
	if err != nil || len(wire.raw) > MaxChannelRecordBytes {
		if err != nil {
			return SignedChannelLeaveRequest{}, err
		}
		return SignedChannelLeaveRequest{}, limit("signed Channel leave request", len(wire.raw),
			MaxChannelRecordBytes)
	}
	return SignedChannelLeaveRequest{record: record, signature: string(copySignature), wire: wire}, nil
}

func ParseSignedChannelLeaveRequest(raw []byte) (SignedChannelLeaveRequest, error) {
	var wire signedChannelLeaveRequestWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return SignedChannelLeaveRequest{}, fmt.Errorf("parse signed Channel leave request: %w", err)
	}
	record, err := ParseChannelLeaveRequestRecord(wire.Request)
	if err != nil {
		return SignedChannelLeaveRequest{}, err
	}
	digest, err := ParseDigest(wire.RequestDigest)
	if err != nil || digest != record.Digest() {
		return SignedChannelLeaveRequest{}, invalid("signed Channel leave request",
			"digest does not match request")
	}
	request, err := AttachChannelLeaveRequestSignature(record, wire.MemberSignature)
	if err != nil {
		return SignedChannelLeaveRequest{}, err
	}
	if !bytes.Equal(request.wire.Bytes(), raw) {
		return SignedChannelLeaveRequest{}, invalid("signed Channel leave request",
			"wire bytes are not canonical")
	}
	return request, nil
}

func VerifyChannelLeaveRequest(descriptor SignedChannelDescriptor, activeMember Member,
	request SignedChannelLeaveRequest,
) error {
	if err := VerifyChannelDescriptor(descriptor); err != nil || request.IsZero() || activeMember.IsZero() {
		return invalid("signed Channel leave request", "verified descriptor, member and request are required")
	}
	if err := VerifyMember(descriptor, activeMember); err != nil {
		return err
	}
	record := request.record
	if activeMember.Status() != MemberActive || record.ChannelID() != descriptor.Descriptor().ID() ||
		record.ChannelID() != activeMember.ChannelID() || record.MemberPeerID() != activeMember.PeerID() ||
		record.ActiveMemberHead() != activeMember.Head() || record.RequestedAt().Before(activeMember.CreatedAt()) {
		return invalid("signed Channel leave request", "request does not bind the active member authority")
	}
	message, err := ChannelLeaveRequestSigningMessage(record.ChannelID(), record.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(activeMember.PublicKey()), message,
		request.MemberSignature()) {
		return invalid("signed Channel leave request", "member signature is invalid")
	}
	return nil
}

func (record ChannelLeaveRequestRecord) ChannelID() ChannelID { return record.spec.ChannelID }
func (record ChannelLeaveRequestRecord) MemberPeerID() PeerID { return record.spec.MemberPeerID }
func (record ChannelLeaveRequestRecord) ActiveMemberHead() RecordHead {
	return record.spec.ActiveMemberHead
}
func (record ChannelLeaveRequestRecord) KnownRosterHead() RecordHead {
	return record.spec.KnownRosterHead
}
func (record ChannelLeaveRequestRecord) RequestedAt() time.Time { return record.requestedAt }
func (record ChannelLeaveRequestRecord) CanonicalJSON() JSON    { return record.canonical }
func (record ChannelLeaveRequestRecord) Digest() Digest         { return record.digest }
func (record ChannelLeaveRequestRecord) IsZero() bool {
	return record.canonical.IsZero() || record.digest.IsZero()
}
func (request SignedChannelLeaveRequest) Record() ChannelLeaveRequestRecord { return request.record }
func (request SignedChannelLeaveRequest) MemberSignature() []byte {
	return append([]byte(nil), request.signature...)
}
func (request SignedChannelLeaveRequest) WireJSON() JSON { return request.wire }
func (request SignedChannelLeaveRequest) Digest() Digest { return request.record.Digest() }
func (request SignedChannelLeaveRequest) RequestID() ChannelLeaveRequestID {
	id, _ := ChannelLeaveRequestIDForDigest(request.Digest())
	return id
}
func (request SignedChannelLeaveRequest) IsZero() bool {
	return request.record.IsZero() || len(request.signature) != ed25519.SignatureSize || request.wire.IsZero()
}

func leaveRecordHeadWire(head RecordHead) recordHeadWire {
	return recordHeadWire{Digest: head.Digest().String(), Revision: head.Revision()}
}

func parseLeaveRecordHead(wire recordHeadWire) (RecordHead, error) {
	digest, err := ParseDigest(wire.Digest)
	if err != nil {
		return RecordHead{}, err
	}
	return NewRecordHead(wire.Revision, digest)
}

func parseLeaveTime(field, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || formatTime(parsed) != value {
		return time.Time{}, invalid("Channel leave "+field, "must be canonical UTC RFC3339Nano")
	}
	return parsed, nil
}

func decodeExactChannelLeaveJSON(raw []byte, destination any, maximum int) error {
	if len(raw) == 0 || len(raw) > maximum {
		return limit("Channel leave wire value", len(raw), maximum)
	}
	canonical, err := NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return invalid("Channel leave wire value", "must use exact canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return invalid("Channel leave wire value", "must contain exactly one JSON value")
	}
	return nil
}
