package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

const ChannelDescriptorSignatureDomain = "mnemon/r5/channel-descriptor/1"

type ChannelDescriptorSpec struct {
	ID             ChannelID
	Name           string
	OwnerPeerID    PeerID
	OwnerPublicKey []byte
	MemberLimit    uint8
	CreatedAt      time.Time
}

// ChannelDescriptor is the immutable, owner-authenticated identity shared by
// every replica. Local alias, topic state and reachability are deliberately
// absent from these signed bytes.
type ChannelDescriptor struct {
	spec           ChannelDescriptorSpec
	ownerPublicKey string
	canonical      JSON
	digest         Digest
}

type channelDescriptorWire struct {
	ChannelID      string `json:"channel_id"`
	CreatedAt      string `json:"created_at"`
	MemberLimit    uint8  `json:"member_limit"`
	Name           string `json:"name"`
	OwnerPeerID    string `json:"owner_peer_id"`
	OwnerPublicKey []byte `json:"owner_public_key"`
	SchemaVersion  int    `json:"schema_version"`
}

func NewChannelDescriptor(spec ChannelDescriptorSpec) (ChannelDescriptor, error) {
	if spec.ID.IsZero() || spec.OwnerPeerID.IsZero() {
		return ChannelDescriptor{}, invalid("Channel descriptor", "Channel and owner are required")
	}
	if strings.ContainsAny(spec.ID.String(), `/\`) {
		return ChannelDescriptor{}, invalid("Channel descriptor ID",
			"must be one canonical GossipSub topic segment")
	}
	if err := validateRuleText("channel name", spec.Name, MaxLabelBytes); err != nil {
		return ChannelDescriptor{}, err
	}
	if spec.MemberLimit < 2 || spec.MemberLimit > MaxMembersPerChannel {
		return ChannelDescriptor{}, limit("member limit", int(spec.MemberLimit), MaxMembersPerChannel)
	}
	if len(spec.OwnerPublicKey) != ed25519.PublicKeySize {
		return ChannelDescriptor{}, invalid("owner public key", "must be a 32-byte Ed25519 key")
	}
	if err := validatePeerPublicKey(spec.OwnerPeerID, spec.OwnerPublicKey); err != nil {
		return ChannelDescriptor{}, fmt.Errorf("Channel descriptor owner: %w", err)
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return ChannelDescriptor{}, err
	}
	publicKey := append([]byte(nil), spec.OwnerPublicKey...)
	wire := channelDescriptorWire{ChannelID: spec.ID.String(), CreatedAt: formatTime(createdAt),
		MemberLimit: spec.MemberLimit, Name: spec.Name, OwnerPeerID: spec.OwnerPeerID.String(),
		OwnerPublicKey: publicKey, SchemaVersion: SchemaVersion}
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxChannelRecordBytes {
		if err != nil {
			return ChannelDescriptor{}, err
		}
		return ChannelDescriptor{}, limit("Channel descriptor", len(canonical.raw), MaxChannelRecordBytes)
	}
	spec.OwnerPublicKey = nil
	spec.CreatedAt = createdAt
	return ChannelDescriptor{spec: spec, ownerPublicKey: string(publicKey), canonical: canonical,
		digest: Sum(canonical.Bytes())}, nil
}

func ParseChannelDescriptor(raw []byte) (ChannelDescriptor, error) {
	var wire channelDescriptorWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return ChannelDescriptor{}, fmt.Errorf("parse Channel descriptor: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion {
		return ChannelDescriptor{}, invalid("Channel descriptor", "unsupported schema version")
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return ChannelDescriptor{}, err
	}
	ownerPeerID, err := ParsePeerID(wire.OwnerPeerID)
	if err != nil {
		return ChannelDescriptor{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil || formatTime(createdAt) != wire.CreatedAt {
		return ChannelDescriptor{}, invalid("Channel descriptor created_at", "must be canonical UTC RFC3339Nano")
	}
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: wire.Name,
		OwnerPeerID: ownerPeerID, OwnerPublicKey: wire.OwnerPublicKey, MemberLimit: wire.MemberLimit,
		CreatedAt: createdAt})
	if err != nil {
		return ChannelDescriptor{}, err
	}
	if !bytes.Equal(descriptor.canonical.Bytes(), raw) {
		return ChannelDescriptor{}, invalid("Channel descriptor", "field order or value encoding is not canonical")
	}
	return descriptor, nil
}

func (descriptor ChannelDescriptor) ID() ChannelID       { return descriptor.spec.ID }
func (descriptor ChannelDescriptor) Name() string        { return descriptor.spec.Name }
func (descriptor ChannelDescriptor) OwnerPeerID() PeerID { return descriptor.spec.OwnerPeerID }
func (descriptor ChannelDescriptor) OwnerPublicKey() []byte {
	return append([]byte(nil), descriptor.ownerPublicKey...)
}
func (descriptor ChannelDescriptor) MemberLimit() uint8   { return descriptor.spec.MemberLimit }
func (descriptor ChannelDescriptor) CreatedAt() time.Time { return descriptor.spec.CreatedAt }
func (descriptor ChannelDescriptor) CanonicalJSON() JSON  { return descriptor.canonical }
func (descriptor ChannelDescriptor) Digest() Digest       { return descriptor.digest }
func (descriptor ChannelDescriptor) IsZero() bool {
	return descriptor.canonical.IsZero() || descriptor.digest.IsZero()
}

func ChannelDescriptorSigningMessage(channelID ChannelID, digest Digest) ([]byte, error) {
	return channelSigningMessage(ChannelDescriptorSignatureDomain, channelID, digest)
}

type SignedChannelDescriptor struct {
	descriptor ChannelDescriptor
	signature  string
	wire       JSON
}

func AttachChannelDescriptorSignature(descriptor ChannelDescriptor,
	signature []byte,
) (SignedChannelDescriptor, error) {
	if descriptor.IsZero() || len(signature) != ed25519.SignatureSize {
		return SignedChannelDescriptor{}, invalid("signed Channel descriptor",
			"descriptor and a 64-byte Ed25519 signature are required")
	}
	message, _ := ChannelDescriptorSigningMessage(descriptor.ID(), descriptor.Digest())
	if !ed25519.Verify(ed25519.PublicKey(descriptor.OwnerPublicKey()), message, signature) {
		return SignedChannelDescriptor{}, invalid("signed Channel descriptor", "owner signature is invalid")
	}
	copySignature := append([]byte(nil), signature...)
	wire, err := JSONFrom(struct {
		Descriptor       JSON   `json:"descriptor"`
		DescriptorDigest Digest `json:"descriptor_digest"`
		OwnerSignature   []byte `json:"owner_signature"`
	}{descriptor.CanonicalJSON(), descriptor.Digest(), copySignature})
	if err != nil || len(wire.raw) > MaxChannelRecordBytes {
		if err != nil {
			return SignedChannelDescriptor{}, err
		}
		return SignedChannelDescriptor{}, limit("signed Channel descriptor", len(wire.raw), MaxChannelRecordBytes)
	}
	return SignedChannelDescriptor{descriptor: descriptor, signature: string(copySignature), wire: wire}, nil
}

type signedChannelDescriptorWire struct {
	Descriptor       json.RawMessage `json:"descriptor"`
	DescriptorDigest string          `json:"descriptor_digest"`
	OwnerSignature   []byte          `json:"owner_signature"`
}

func ParseSignedChannelDescriptor(raw []byte) (SignedChannelDescriptor, error) {
	var wire signedChannelDescriptorWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return SignedChannelDescriptor{}, fmt.Errorf("parse signed Channel descriptor: %w", err)
	}
	descriptor, err := ParseChannelDescriptor(wire.Descriptor)
	if err != nil {
		return SignedChannelDescriptor{}, err
	}
	digest, err := ParseDigest(wire.DescriptorDigest)
	if err != nil || digest != descriptor.Digest() {
		return SignedChannelDescriptor{}, invalid("signed Channel descriptor", "digest does not match descriptor")
	}
	signed, err := AttachChannelDescriptorSignature(descriptor, wire.OwnerSignature)
	if err != nil {
		return SignedChannelDescriptor{}, err
	}
	if !bytes.Equal(signed.wire.Bytes(), raw) {
		return SignedChannelDescriptor{}, invalid("signed Channel descriptor", "wire bytes are not canonical")
	}
	return signed, nil
}

func VerifyChannelDescriptor(signed SignedChannelDescriptor) error {
	if signed.IsZero() {
		return invalid("signed Channel descriptor", "value is incomplete")
	}
	message, err := ChannelDescriptorSigningMessage(signed.descriptor.ID(), signed.descriptor.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(signed.descriptor.OwnerPublicKey()), message,
		signed.OwnerSignature()) {
		return invalid("signed Channel descriptor", "owner signature is invalid")
	}
	return nil
}

func (signed SignedChannelDescriptor) Descriptor() ChannelDescriptor { return signed.descriptor }
func (signed SignedChannelDescriptor) OwnerSignature() []byte {
	return append([]byte(nil), signed.signature...)
}
func (signed SignedChannelDescriptor) WireJSON() JSON { return signed.wire }
func (signed SignedChannelDescriptor) IsZero() bool {
	return signed.descriptor.IsZero() || len(signed.signature) != ed25519.SignatureSize || signed.wire.IsZero()
}

func channelSigningMessage(domain string, channelID ChannelID, digest Digest) ([]byte, error) {
	if domain == "" || channelID.IsZero() || digest.IsZero() {
		return nil, invalid("Channel signing message", "domain, Channel and digest are required")
	}
	message := make([]byte, 0, len(domain)+2+len(channelID.String())+len(digest.Bytes()))
	message = append(message, domain...)
	message = append(message, 0)
	message = append(message, channelID.String()...)
	message = append(message, 0)
	return append(message, digest.Bytes()...), nil
}

func decodeExactChannelJSON(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > MaxChannelRecordBytes {
		return limit("Channel wire value", len(raw), MaxChannelRecordBytes)
	}
	canonical, err := NewJSON(raw)
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical.Bytes(), raw) {
		return invalid("Channel wire value", "must use exact canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return invalid("Channel wire value", "contains a trailing value")
	}
	return nil
}
