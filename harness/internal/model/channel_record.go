package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"

	ma "github.com/multiformats/go-multiaddr"
)

const (
	MemberRecordSignatureDomain = "mnemon/r5/channel-member-record/1"
	HermeticNetworkProfileName  = "r5-hermetic-v1"
)

var requiredMemberProtocols = []string{
	"/mnemon/artifacts/1",
	"/mnemon/channel/1",
	"/mnemon/events/1",
}

type MemberRecordSpec struct {
	ChannelID        ChannelID
	DescriptorDigest Digest
	Revision         uint64
	PreviousDigest   *Digest
	PeerID           PeerID
	OriginEpoch      OriginEpoch
	DisplayLabel     string
	PublicKey        []byte
	Multiaddrs       []string
	Protocols        []string
	Limits           JSON
	Status           MemberStatus
	CreatedAt        time.Time
}

// MemberRecord is one canonical entry in the creator-signed, append-only
// roster. Protocols and limits are part of the signed descriptor from which a
// PeerBinding is derived; handshake self-report cannot replace them.
type MemberRecord struct {
	spec             MemberRecordSpec
	previousDigest   Digest
	hasPrevious      bool
	publicKey        string
	multiaddrs       []string
	protocols        []string
	canonical        JSON
	digest           Digest
	canonicalCreated time.Time
}

type memberRecordWire struct {
	ChannelID        string          `json:"channel_id"`
	CreatedAt        string          `json:"created_at"`
	DescriptorDigest string          `json:"descriptor_digest"`
	DisplayLabel     string          `json:"display_label"`
	Limits           json.RawMessage `json:"limits"`
	Multiaddrs       []string        `json:"multiaddrs"`
	OriginEpoch      string          `json:"origin_epoch"`
	PeerID           string          `json:"peer_id"`
	PreviousDigest   *string         `json:"previous_digest"`
	Protocols        []string        `json:"protocols"`
	PublicKey        []byte          `json:"public_key"`
	Revision         uint64          `json:"revision"`
	SchemaVersion    int             `json:"schema_version"`
	Status           MemberStatus    `json:"status"`
}

func NewMemberRecord(spec MemberRecordSpec) (MemberRecord, error) {
	if spec.ChannelID.IsZero() || spec.DescriptorDigest.IsZero() || spec.PeerID.IsZero() ||
		spec.OriginEpoch.IsZero() {
		return MemberRecord{}, invalid("MemberRecord", "Channel, descriptor, PeerID and epoch are required")
	}
	if err := validateSQLitePositive("MemberRecord revision", spec.Revision); err != nil {
		return MemberRecord{}, err
	}
	if spec.Revision == 1 && spec.PreviousDigest != nil {
		return MemberRecord{}, invariant("genesis MemberRecord cannot have a previous digest")
	}
	if spec.Revision > 1 && (spec.PreviousDigest == nil || spec.PreviousDigest.IsZero()) {
		return MemberRecord{}, invariant("non-genesis MemberRecord requires a previous digest")
	}
	if err := validateRuleText("display label", spec.DisplayLabel, MaxLabelBytes); err != nil {
		return MemberRecord{}, err
	}
	if !spec.Status.Valid() {
		return MemberRecord{}, invalid("MemberRecord status", "unknown status")
	}
	if len(spec.PublicKey) != ed25519.PublicKeySize {
		return MemberRecord{}, invalid("member public key", "must be a 32-byte Ed25519 key")
	}
	if err := validatePeerPublicKey(spec.PeerID, spec.PublicKey); err != nil {
		return MemberRecord{}, fmt.Errorf("MemberRecord identity: %w", err)
	}
	if len(spec.Multiaddrs) == 0 || len(spec.Multiaddrs) > MaxMemberMultiaddrs {
		return MemberRecord{}, limit("MemberRecord multiaddrs", len(spec.Multiaddrs), MaxMemberMultiaddrs)
	}
	if len(spec.Protocols) != MaxMemberProtocols {
		return MemberRecord{}, invalid("MemberRecord protocols", "must declare the three R5 direct protocols")
	}
	multiaddrs, err := normalizeMemberMultiaddrs(spec.Multiaddrs)
	if err != nil {
		return MemberRecord{}, err
	}
	protocols, err := normalizeRuleStrings("protocols", spec.Protocols)
	if err != nil {
		return MemberRecord{}, err
	}
	for index := range requiredMemberProtocols {
		if protocols[index] != requiredMemberProtocols[index] {
			return MemberRecord{}, invalid("MemberRecord protocols", "must equal the exact R5 direct protocol set")
		}
	}
	if spec.Limits.String() != DefaultMemberLimits().String() {
		return MemberRecord{}, invalid("MemberRecord limits", "must equal the R5 hermetic network profile")
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return MemberRecord{}, err
	}
	var previous *string
	if spec.PreviousDigest != nil {
		value := spec.PreviousDigest.String()
		previous = &value
	}
	wire := memberRecordWire{ChannelID: spec.ChannelID.String(), CreatedAt: formatTime(createdAt),
		DescriptorDigest: spec.DescriptorDigest.String(), DisplayLabel: spec.DisplayLabel,
		Limits: spec.Limits.Bytes(), Multiaddrs: multiaddrs, OriginEpoch: spec.OriginEpoch.String(),
		PeerID: spec.PeerID.String(), PreviousDigest: previous, Protocols: protocols,
		PublicKey: append([]byte(nil), spec.PublicKey...), Revision: spec.Revision,
		SchemaVersion: SchemaVersion, Status: spec.Status}
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxChannelRecordBytes {
		if err != nil {
			return MemberRecord{}, err
		}
		return MemberRecord{}, limit("MemberRecord", len(canonical.raw), MaxChannelRecordBytes)
	}
	result := MemberRecord{spec: spec, publicKey: string(append([]byte(nil), spec.PublicKey...)),
		multiaddrs: multiaddrs, protocols: protocols, canonical: canonical, digest: Sum(canonical.Bytes()),
		canonicalCreated: createdAt}
	result.spec.PublicKey, result.spec.Multiaddrs, result.spec.Protocols, result.spec.PreviousDigest = nil, nil, nil, nil
	result.spec.CreatedAt = createdAt
	if spec.PreviousDigest != nil {
		result.previousDigest, result.hasPrevious = *spec.PreviousDigest, true
	}
	return result, nil
}

// DefaultMemberLimits is the only limits descriptor accepted in the R5 T0
// protocol. Detailed resource values remain local implementation policy;
// signed roster authority binds the closed profile name, not tunable numbers.
func DefaultMemberLimits() JSON {
	limits, err := JSONFrom(struct {
		Profile string `json:"profile"`
	}{Profile: HermeticNetworkProfileName})
	if err != nil {
		panic(err)
	}
	return limits
}

func normalizeMemberMultiaddrs(values []string) ([]string, error) {
	normalized, err := normalizeRuleStrings("multiaddrs", values)
	if err != nil {
		return nil, err
	}
	for _, value := range normalized {
		address, err := ma.NewMultiaddr(value)
		if err != nil || address.String() != value {
			return nil, invalid("MemberRecord multiaddrs", "must contain canonical libp2p multiaddrs")
		}
	}
	return normalized, nil
}

func ParseMemberRecord(raw []byte) (MemberRecord, error) {
	var wire memberRecordWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return MemberRecord{}, fmt.Errorf("parse MemberRecord: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion {
		return MemberRecord{}, invalid("MemberRecord", "unsupported schema version")
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return MemberRecord{}, err
	}
	descriptorDigest, err := ParseDigest(wire.DescriptorDigest)
	if err != nil {
		return MemberRecord{}, err
	}
	peerID, err := ParsePeerID(wire.PeerID)
	if err != nil {
		return MemberRecord{}, err
	}
	epoch, err := ParseOriginEpoch(wire.OriginEpoch)
	if err != nil {
		return MemberRecord{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil || formatTime(createdAt) != wire.CreatedAt {
		return MemberRecord{}, invalid("MemberRecord created_at", "must be canonical UTC RFC3339Nano")
	}
	limits, err := NewJSON(wire.Limits)
	if err != nil {
		return MemberRecord{}, err
	}
	var previous *Digest
	if wire.PreviousDigest != nil {
		digest, err := ParseDigest(*wire.PreviousDigest)
		if err != nil {
			return MemberRecord{}, err
		}
		previous = &digest
	}
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: channelID, DescriptorDigest: descriptorDigest,
		Revision: wire.Revision, PreviousDigest: previous, PeerID: peerID, OriginEpoch: epoch,
		DisplayLabel: wire.DisplayLabel, PublicKey: wire.PublicKey, Multiaddrs: wire.Multiaddrs,
		Protocols: wire.Protocols, Limits: limits, Status: wire.Status, CreatedAt: createdAt})
	if err != nil {
		return MemberRecord{}, err
	}
	if !bytes.Equal(record.canonical.Bytes(), raw) {
		return MemberRecord{}, invalid("MemberRecord", "field order or value encoding is not canonical")
	}
	return record, nil
}

func (record MemberRecord) ChannelID() ChannelID     { return record.spec.ChannelID }
func (record MemberRecord) DescriptorDigest() Digest { return record.spec.DescriptorDigest }
func (record MemberRecord) Head() RecordHead {
	head, _ := NewRecordHead(record.spec.Revision, record.digest)
	return head
}
func (record MemberRecord) PreviousDigest() (Digest, bool) {
	return record.previousDigest, record.hasPrevious
}
func (record MemberRecord) PeerID() PeerID           { return record.spec.PeerID }
func (record MemberRecord) OriginEpoch() OriginEpoch { return record.spec.OriginEpoch }
func (record MemberRecord) DisplayLabel() string     { return record.spec.DisplayLabel }
func (record MemberRecord) PublicKey() []byte        { return append([]byte(nil), record.publicKey...) }
func (record MemberRecord) Multiaddrs() []string     { return append([]string(nil), record.multiaddrs...) }
func (record MemberRecord) Protocols() []string      { return append([]string(nil), record.protocols...) }
func (record MemberRecord) Limits() JSON             { return record.spec.Limits }
func (record MemberRecord) Status() MemberStatus     { return record.spec.Status }
func (record MemberRecord) CreatedAt() time.Time     { return record.canonicalCreated }
func (record MemberRecord) CanonicalJSON() JSON      { return record.canonical }
func (record MemberRecord) Digest() Digest           { return record.digest }
func (record MemberRecord) IsZero() bool             { return record.canonical.IsZero() || record.digest.IsZero() }

func MemberRecordSigningMessage(channelID ChannelID, digest Digest) ([]byte, error) {
	return channelSigningMessage(MemberRecordSignatureDomain, channelID, digest)
}

type Member struct {
	record    MemberRecord
	signature string
	wire      JSON
}

func AttachMemberSignature(record MemberRecord, signature []byte) (Member, error) {
	if record.IsZero() || len(signature) != ed25519.SignatureSize {
		return Member{}, invalid("signed MemberRecord", "record and a 64-byte owner signature are required")
	}
	copySignature := append([]byte(nil), signature...)
	wire, err := JSONFrom(struct {
		MemberRecord   JSON   `json:"member_record"`
		OwnerSignature []byte `json:"owner_signature"`
		RecordDigest   Digest `json:"record_digest"`
	}{record.CanonicalJSON(), copySignature, record.Digest()})
	if err != nil || len(wire.raw) > MaxChannelRecordBytes {
		if err != nil {
			return Member{}, err
		}
		return Member{}, limit("signed MemberRecord", len(wire.raw), MaxChannelRecordBytes)
	}
	return Member{record: record, signature: string(copySignature), wire: wire}, nil
}

type signedMemberRecordWire struct {
	MemberRecord   json.RawMessage `json:"member_record"`
	OwnerSignature []byte          `json:"owner_signature"`
	RecordDigest   string          `json:"record_digest"`
}

func ParseMember(raw []byte) (Member, error) {
	var wire signedMemberRecordWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return Member{}, fmt.Errorf("parse signed MemberRecord: %w", err)
	}
	record, err := ParseMemberRecord(wire.MemberRecord)
	if err != nil {
		return Member{}, err
	}
	digest, err := ParseDigest(wire.RecordDigest)
	if err != nil || digest != record.Digest() {
		return Member{}, invalid("signed MemberRecord", "digest does not match record")
	}
	member, err := AttachMemberSignature(record, wire.OwnerSignature)
	if err != nil {
		return Member{}, err
	}
	if !bytes.Equal(member.wire.Bytes(), raw) {
		return Member{}, invalid("signed MemberRecord", "wire bytes are not canonical")
	}
	return member, nil
}

func VerifyMember(descriptor SignedChannelDescriptor, member Member) error {
	if err := VerifyChannelDescriptor(descriptor); err != nil || member.IsZero() {
		return invalid("signed MemberRecord", "verified Channel descriptor and complete member are required")
	}
	channel := descriptor.Descriptor()
	if member.ChannelID() != channel.ID() || member.DescriptorDigest() != channel.Digest() {
		return invalid("signed MemberRecord", "record is not bound to the Channel descriptor")
	}
	if member.CreatedAt().Before(channel.CreatedAt()) {
		return invalid("signed MemberRecord", "record predates the Channel descriptor")
	}
	if member.Head().Revision() == 1 && (member.PeerID() != channel.OwnerPeerID() ||
		member.Status() != MemberActive || !bytes.Equal(member.PublicKey(), channel.OwnerPublicKey()) ||
		!member.CreatedAt().Equal(channel.CreatedAt())) {
		return invalid("signed MemberRecord", "genesis must be the active Channel owner at descriptor creation")
	}
	message, err := MemberRecordSigningMessage(member.ChannelID(), member.record.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(channel.OwnerPublicKey()), message, member.OwnerSignature()) {
		return invalid("signed MemberRecord", "owner signature is invalid")
	}
	return nil
}

func (member Member) Record() MemberRecord           { return member.record }
func (member Member) ChannelID() ChannelID           { return member.record.ChannelID() }
func (member Member) DescriptorDigest() Digest       { return member.record.DescriptorDigest() }
func (member Member) Head() RecordHead               { return member.record.Head() }
func (member Member) PreviousDigest() (Digest, bool) { return member.record.PreviousDigest() }
func (member Member) PeerID() PeerID                 { return member.record.PeerID() }
func (member Member) OriginEpoch() OriginEpoch       { return member.record.OriginEpoch() }
func (member Member) DisplayLabel() string           { return member.record.DisplayLabel() }
func (member Member) PublicKey() []byte              { return member.record.PublicKey() }
func (member Member) Multiaddrs() []string           { return member.record.Multiaddrs() }
func (member Member) Protocols() []string            { return member.record.Protocols() }
func (member Member) Limits() JSON                   { return member.record.Limits() }
func (member Member) Status() MemberStatus           { return member.record.Status() }
func (member Member) SignedRecord() JSON             { return member.record.CanonicalJSON() }
func (member Member) OwnerSignature() []byte         { return append([]byte(nil), member.signature...) }
func (member Member) CreatedAt() time.Time           { return member.record.CreatedAt() }
func (member Member) WireJSON() JSON                 { return member.wire }
func (member Member) IsZero() bool {
	return member.record.IsZero() || len(member.signature) != ed25519.SignatureSize || member.wire.IsZero()
}
