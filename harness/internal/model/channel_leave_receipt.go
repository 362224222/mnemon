package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"
)

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
		!leaveReceiptRecordsWellFormed(spec.ChannelID, records, acceptedAt) {
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
	if !validChannelLeaveReceiptTerminal(descriptor, request, record.RosterRecords()) {
		return invalid("signed Channel leave receipt", "suffix has no applicable terminal authority")
	}
	return nil
}

func validChannelLeaveReceiptTerminal(descriptor SignedChannelDescriptor,
	request SignedChannelLeaveRequest, records []Member,
) bool {
	requester := request.Record().MemberPeerID()
	owner := descriptor.Descriptor().OwnerPeerID()
	applicable := false
	for index, member := range records {
		if member.PeerID() == owner && member.Status() == MemberLeft {
			if index != len(records)-1 {
				return false
			}
			applicable = true
		}
		if member.PeerID() == requester && member.Status().Terminal() {
			applicable = true
		}
	}
	return applicable
}

func leaveReceiptRecordsWellFormed(channelID ChannelID, records []Member,
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
		if member.Status().Terminal() {
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
