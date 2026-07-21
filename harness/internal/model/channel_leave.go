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

type ChannelLeaveReceiptRecordSpec struct {
	ChannelID       ChannelID
	MemberPeerID    PeerID
	RequestDigest   Digest
	RosterRecords   []Member
	FinalRosterHead RecordHead
	AcceptedAt      time.Time
}

// ChannelLeaveReceiptRecord binds one request digest to the complete signed
// suffix needed by the leaving replica. RosterRecords is bounded by the global
// roster cardinality and can therefore be validated before any durable merge.
type ChannelLeaveReceiptRecord struct {
	spec          ChannelLeaveReceiptRecordSpec
	rosterRecords []Member
	acceptedAt    time.Time
	canonical     JSON
	digest        Digest
}

type channelLeaveReceiptRecordWire struct {
	AcceptedAt      string            `json:"accepted_at"`
	ChannelID       string            `json:"channel_id"`
	FinalRosterHead recordHeadWire    `json:"final_roster_head"`
	MemberPeerID    string            `json:"member_peer_id"`
	RequestDigest   string            `json:"request_digest"`
	RosterRecords   []json.RawMessage `json:"roster_records"`
	SchemaVersion   int               `json:"schema_version"`
}

func NewChannelLeaveReceiptRecord(spec ChannelLeaveReceiptRecordSpec) (ChannelLeaveReceiptRecord, error) {
	if spec.ChannelID.IsZero() || spec.MemberPeerID.IsZero() || spec.RequestDigest.IsZero() ||
		spec.FinalRosterHead.IsZero() || len(spec.RosterRecords) == 0 ||
		len(spec.RosterRecords) > MaxMemberRecordsPerChannel {
		return ChannelLeaveReceiptRecord{}, invalid("Channel leave receipt",
			"Channel, member, request, final head and bounded roster suffix are required")
	}
	acceptedAt, err := canonicalTime(spec.AcceptedAt)
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	records := append([]Member(nil), spec.RosterRecords...)
	if records[len(records)-1].Head() != spec.FinalRosterHead ||
		!leaveReceiptRecordsWellFormed(spec.ChannelID, spec.MemberPeerID, records, acceptedAt) {
		return ChannelLeaveReceiptRecord{}, invariant("Channel leave receipt roster suffix is invalid")
	}
	wireRecords := make([]json.RawMessage, len(records))
	for index, record := range records {
		wireRecords[index] = record.WireJSON().Bytes()
	}
	wire := channelLeaveReceiptRecordWire{AcceptedAt: formatTime(acceptedAt),
		ChannelID: spec.ChannelID.String(), FinalRosterHead: leaveRecordHeadWire(spec.FinalRosterHead),
		MemberPeerID: spec.MemberPeerID.String(), RequestDigest: spec.RequestDigest.String(),
		RosterRecords: wireRecords, SchemaVersion: SchemaVersion}
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxCanonicalJSONBytes {
		if err != nil {
			return ChannelLeaveReceiptRecord{}, err
		}
		return ChannelLeaveReceiptRecord{}, limit("Channel leave receipt", len(canonical.raw),
			MaxCanonicalJSONBytes)
	}
	spec.RosterRecords = nil
	spec.AcceptedAt = acceptedAt
	return ChannelLeaveReceiptRecord{spec: spec, rosterRecords: records, acceptedAt: acceptedAt,
		canonical: canonical, digest: Sum(canonical.Bytes())}, nil
}

func ParseChannelLeaveReceiptRecord(raw []byte) (ChannelLeaveReceiptRecord, error) {
	var wire channelLeaveReceiptRecordWire
	if err := decodeExactChannelLeaveJSON(raw, &wire, MaxCanonicalJSONBytes); err != nil {
		return ChannelLeaveReceiptRecord{}, fmt.Errorf("parse Channel leave receipt: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion || len(wire.RosterRecords) == 0 ||
		len(wire.RosterRecords) > MaxMemberRecordsPerChannel {
		return ChannelLeaveReceiptRecord{}, invalid("Channel leave receipt", "invalid schema or suffix size")
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	memberPeerID, err := ParsePeerID(wire.MemberPeerID)
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	requestDigest, err := ParseDigest(wire.RequestDigest)
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	finalHead, err := parseLeaveRecordHead(wire.FinalRosterHead)
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	acceptedAt, err := parseLeaveTime("accepted_at", wire.AcceptedAt)
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	records := make([]Member, len(wire.RosterRecords))
	for index, rawRecord := range wire.RosterRecords {
		records[index], err = ParseMember(rawRecord)
		if err != nil {
			return ChannelLeaveReceiptRecord{}, err
		}
	}
	record, err := NewChannelLeaveReceiptRecord(ChannelLeaveReceiptRecordSpec{ChannelID: channelID,
		MemberPeerID: memberPeerID, RequestDigest: requestDigest, RosterRecords: records,
		FinalRosterHead: finalHead, AcceptedAt: acceptedAt})
	if err != nil {
		return ChannelLeaveReceiptRecord{}, err
	}
	if !bytes.Equal(record.canonical.Bytes(), raw) {
		return ChannelLeaveReceiptRecord{}, invalid("Channel leave receipt", "wire bytes are not canonical")
	}
	return record, nil
}

func ChannelLeaveReceiptSigningMessage(channelID ChannelID, digest Digest) ([]byte, error) {
	return channelSigningMessage(ChannelLeaveReceiptSignatureDomain, channelID, digest)
}

type SignedChannelLeaveReceipt struct {
	record    ChannelLeaveReceiptRecord
	signature string
	wire      JSON
}

type signedChannelLeaveReceiptWire struct {
	OwnerSignature []byte          `json:"owner_signature"`
	Receipt        json.RawMessage `json:"receipt"`
	ReceiptDigest  string          `json:"receipt_digest"`
}

func AttachChannelLeaveReceiptSignature(record ChannelLeaveReceiptRecord,
	signature []byte,
) (SignedChannelLeaveReceipt, error) {
	if record.IsZero() || len(signature) != ed25519.SignatureSize {
		return SignedChannelLeaveReceipt{}, invalid("signed Channel leave receipt",
			"receipt and a 64-byte owner signature are required")
	}
	copySignature := append([]byte(nil), signature...)
	wire, err := JSONFrom(struct {
		OwnerSignature []byte `json:"owner_signature"`
		Receipt        JSON   `json:"receipt"`
		ReceiptDigest  Digest `json:"receipt_digest"`
	}{copySignature, record.CanonicalJSON(), record.Digest()})
	if err != nil || len(wire.raw) > MaxCanonicalJSONBytes {
		if err != nil {
			return SignedChannelLeaveReceipt{}, err
		}
		return SignedChannelLeaveReceipt{}, limit("signed Channel leave receipt", len(wire.raw),
			MaxCanonicalJSONBytes)
	}
	return SignedChannelLeaveReceipt{record: record, signature: string(copySignature), wire: wire}, nil
}

func ParseSignedChannelLeaveReceipt(raw []byte) (SignedChannelLeaveReceipt, error) {
	var wire signedChannelLeaveReceiptWire
	if err := decodeExactChannelLeaveJSON(raw, &wire, MaxCanonicalJSONBytes); err != nil {
		return SignedChannelLeaveReceipt{}, fmt.Errorf("parse signed Channel leave receipt: %w", err)
	}
	record, err := ParseChannelLeaveReceiptRecord(wire.Receipt)
	if err != nil {
		return SignedChannelLeaveReceipt{}, err
	}
	digest, err := ParseDigest(wire.ReceiptDigest)
	if err != nil || digest != record.Digest() {
		return SignedChannelLeaveReceipt{}, invalid("signed Channel leave receipt",
			"digest does not match receipt")
	}
	receipt, err := AttachChannelLeaveReceiptSignature(record, wire.OwnerSignature)
	if err != nil {
		return SignedChannelLeaveReceipt{}, err
	}
	if !bytes.Equal(receipt.wire.Bytes(), raw) {
		return SignedChannelLeaveReceipt{}, invalid("signed Channel leave receipt",
			"wire bytes are not canonical")
	}
	return receipt, nil
}

func VerifyChannelLeaveReceipt(descriptor SignedChannelDescriptor, activeMember Member,
	request SignedChannelLeaveRequest, receipt SignedChannelLeaveReceipt,
) error {
	if err := VerifyChannelLeaveRequest(descriptor, activeMember, request); err != nil || receipt.IsZero() {
		return invalid("signed Channel leave receipt", "verified request and complete receipt are required")
	}
	record := receipt.record
	if record.ChannelID() != request.Record().ChannelID() ||
		record.MemberPeerID() != request.Record().MemberPeerID() ||
		record.RequestDigest() != request.Digest() || record.AcceptedAt().Before(request.Record().RequestedAt()) {
		return invalid("signed Channel leave receipt", "receipt does not bind the leave request")
	}
	message, err := ChannelLeaveReceiptSigningMessage(record.ChannelID(), record.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(descriptor.Descriptor().OwnerPublicKey()), message,
		receipt.OwnerSignature()) {
		return invalid("signed Channel leave receipt", "owner signature is invalid")
	}
	previous := request.Record().KnownRosterHead()
	for _, member := range record.RosterRecords() {
		priorDigest, ok := member.PreviousDigest()
		if !ok || member.Head().Revision() != previous.Revision()+1 || priorDigest != previous.Digest() ||
			VerifyMember(descriptor, member) != nil {
			return invalid("signed Channel leave receipt", "owner-signed roster suffix is discontinuous")
		}
		previous = member.Head()
	}
	if previous != record.FinalRosterHead() {
		return invalid("signed Channel leave receipt", "final roster head does not match suffix")
	}
	return nil
}

func leaveReceiptRecordsWellFormed(channelID ChannelID, memberPeerID PeerID, records []Member,
	acceptedAt time.Time,
) bool {
	terminal := false
	var previous Member
	for index, member := range records {
		if member.IsZero() || member.ChannelID() != channelID || member.CreatedAt().After(acceptedAt) {
			return false
		}
		if index > 0 {
			digest, ok := member.PreviousDigest()
			if !ok || member.Head().Revision() != previous.Head().Revision()+1 ||
				digest != previous.Head().Digest() || member.CreatedAt().Before(previous.CreatedAt()) {
				return false
			}
		}
		if member.PeerID() == memberPeerID && member.Status().Terminal() {
			terminal = true
		}
		previous = member
	}
	return terminal
}

func (record ChannelLeaveReceiptRecord) ChannelID() ChannelID  { return record.spec.ChannelID }
func (record ChannelLeaveReceiptRecord) MemberPeerID() PeerID  { return record.spec.MemberPeerID }
func (record ChannelLeaveReceiptRecord) RequestDigest() Digest { return record.spec.RequestDigest }
func (record ChannelLeaveReceiptRecord) RosterRecords() []Member {
	return append([]Member(nil), record.rosterRecords...)
}
func (record ChannelLeaveReceiptRecord) FinalRosterHead() RecordHead {
	return record.spec.FinalRosterHead
}
func (record ChannelLeaveReceiptRecord) AcceptedAt() time.Time { return record.acceptedAt }
func (record ChannelLeaveReceiptRecord) CanonicalJSON() JSON   { return record.canonical }
func (record ChannelLeaveReceiptRecord) Digest() Digest        { return record.digest }
func (record ChannelLeaveReceiptRecord) IsZero() bool {
	return record.canonical.IsZero() || record.digest.IsZero()
}
func (receipt SignedChannelLeaveReceipt) Record() ChannelLeaveReceiptRecord { return receipt.record }
func (receipt SignedChannelLeaveReceipt) OwnerSignature() []byte {
	return append([]byte(nil), receipt.signature...)
}
func (receipt SignedChannelLeaveReceipt) WireJSON() JSON { return receipt.wire }
func (receipt SignedChannelLeaveReceipt) IsZero() bool {
	return receipt.record.IsZero() || len(receipt.signature) != ed25519.SignatureSize || receipt.wire.IsZero()
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
