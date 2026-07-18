package model

import (
	"bytes"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

const (
	EnrollmentTokenPrefix                  = "mnch1_"
	EnrollmentTokenSignatureDomain         = "mnemon/r5/enrollment-token/1"
	EnrollmentVerifierDomain               = "mnemon/r5/enrollment-verifier/1"
	EnrollmentProofDomain                  = "mnemon/r5/enrollment-proof/1"
	EnrollmentJoinIdentityDomain           = "mnemon/r5/enrollment-join-identity/1"
	EnrollmentRequestIDDomain              = "mnemon/r5/enrollment-request-id/1"
	EnrollmentUseIDDomain                  = "mnemon/r5/enrollment-use-id/1"
	EnrollmentReceiptIDDomain              = "mnemon/r5/enrollment-receipt-id/1"
	EnrollmentReceiptSignatureDomain       = "mnemon/r5/enrollment-receipt/1"
	EnrollmentProtocolMinVersion     uint8 = 1
	EnrollmentProtocolMaxVersion     uint8 = 1
	EnrollmentSecretBytes                  = 32
	EnrollmentNonceBytes                   = 32
)

// OpenEnrollmentGrant is the durable, bearer-secret-free half of an invite.
// The bearer secret and encoded token never enter SQLite.
type OpenEnrollmentGrant struct {
	id        GrantID
	channelID ChannelID
	verifier  EnrollmentVerifier
	expiresAt time.Time
	maxUses   uint8
	createdAt time.Time
}

func newOpenEnrollmentGrant(id GrantID, channelID ChannelID, verifier EnrollmentVerifier,
	expiresAt time.Time, maxUses uint8, createdAt time.Time,
) (OpenEnrollmentGrant, error) {
	if id.IsZero() || channelID.IsZero() || verifier.IsZero() {
		return OpenEnrollmentGrant{}, invalid("enrollment grant", "ID, Channel and verifier are required")
	}
	if maxUses == 0 || maxUses >= MaxMembersPerChannel {
		return OpenEnrollmentGrant{}, limit("enrollment grant uses", int(maxUses), MaxMembersPerChannel-1)
	}
	created, err := canonicalTime(createdAt)
	if err != nil {
		return OpenEnrollmentGrant{}, err
	}
	expires, err := canonicalTime(expiresAt)
	if err != nil {
		return OpenEnrollmentGrant{}, err
	}
	if !expires.After(created) {
		return OpenEnrollmentGrant{}, invariant("enrollment grant must expire after creation")
	}
	return OpenEnrollmentGrant{id: id, channelID: channelID, verifier: verifier,
		expiresAt: expires, maxUses: maxUses, createdAt: created}, nil
}

func (grant OpenEnrollmentGrant) ID() GrantID          { return grant.id }
func (grant OpenEnrollmentGrant) ChannelID() ChannelID { return grant.channelID }
func (grant OpenEnrollmentGrant) Verifier() EnrollmentVerifier {
	return grant.verifier
}
func (grant OpenEnrollmentGrant) ExpiresAt() time.Time { return grant.expiresAt }
func (grant OpenEnrollmentGrant) MaxUses() uint8       { return grant.maxUses }
func (grant OpenEnrollmentGrant) CreatedAt() time.Time { return grant.createdAt }
func (grant OpenEnrollmentGrant) IsZero() bool {
	return grant.id.IsZero() || grant.channelID.IsZero() || grant.verifier.IsZero()
}
func (OpenEnrollmentGrant) String() string   { return "[enrollment grant; verifier REDACTED]" }
func (OpenEnrollmentGrant) GoString() string { return "[enrollment grant; verifier REDACTED]" }
func (OpenEnrollmentGrant) Format(state fmt.State, _ rune) {
	formatEnrollmentCredential(state, "[enrollment grant; verifier REDACTED]")
}
func (grant OpenEnrollmentGrant) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("grant_id", grant.ID().String()),
		slog.String("channel_id", grant.ChannelID().String()),
		slog.Time("expires_at", grant.ExpiresAt()),
		slog.Uint64("max_uses", uint64(grant.MaxUses())),
		slog.String("verifier", "REDACTED"),
	)
}

// EnrollmentTokenSpec contains the transient bearer material returned once by
// create/invite. Only VerifierForEnrollment may cross the durable Store
// boundary; neither this spec nor EnrollmentToken may be persisted there.
type EnrollmentTokenSpec struct {
	Descriptor         SignedChannelDescriptor
	OwnerMultiaddrs    []string
	GrantID            GrantID
	BearerSecret       []byte
	ExpiresAt          time.Time
	MaxUses            uint8
	ProtocolMinVersion uint8
	ProtocolMaxVersion uint8
}

func (EnrollmentTokenSpec) String() string   { return "[REDACTED enrollment token spec]" }
func (EnrollmentTokenSpec) GoString() string { return "[REDACTED enrollment token spec]" }
func (EnrollmentTokenSpec) Format(state fmt.State, _ rune) {
	formatEnrollmentCredential(state, "[REDACTED enrollment token spec]")
}
func (EnrollmentTokenSpec) LogValue() slog.Value {
	return slog.StringValue("[REDACTED enrollment token spec]")
}
func (EnrollmentTokenSpec) MarshalJSON() ([]byte, error) {
	return nil, invalid("enrollment token spec", "transient bearer material must not be serialized")
}

type EnrollmentTokenPayload struct {
	descriptor      SignedChannelDescriptor
	ownerMultiaddrs []string
	grantID         GrantID
	bearerSecret    string
	expiresAt       time.Time
	maxUses         uint8
	canonical       JSON
	digest          Digest
}

type enrollmentTokenPayloadWire struct {
	BearerSecret       []byte          `json:"bearer_secret"`
	ChannelDescriptor  json.RawMessage `json:"channel_descriptor"`
	ExpiresAt          string          `json:"expires_at"`
	GrantID            string          `json:"grant_id"`
	MaxUses            uint8           `json:"max_uses"`
	OwnerMultiaddrs    []string        `json:"owner_multiaddrs"`
	ProtocolMaxVersion uint8           `json:"protocol_max_version"`
	ProtocolMinVersion uint8           `json:"protocol_min_version"`
	SchemaVersion      int             `json:"schema_version"`
}

func NewEnrollmentTokenPayload(spec EnrollmentTokenSpec) (EnrollmentTokenPayload, error) {
	if err := VerifyChannelDescriptor(spec.Descriptor); err != nil || spec.GrantID.IsZero() {
		return EnrollmentTokenPayload{}, invalid("enrollment token", "signed descriptor and grant are required")
	}
	if len(spec.BearerSecret) != EnrollmentSecretBytes {
		return EnrollmentTokenPayload{}, invalid("enrollment token bearer secret", "must contain exactly 32 bytes")
	}
	if spec.ProtocolMinVersion != EnrollmentProtocolMinVersion ||
		spec.ProtocolMaxVersion != EnrollmentProtocolMaxVersion {
		return EnrollmentTokenPayload{}, invalid("enrollment token protocol range", "T0 requires the exact range 1..1")
	}
	descriptor := spec.Descriptor.Descriptor()
	if spec.MaxUses == 0 || spec.MaxUses >= MaxMembersPerChannel || spec.MaxUses >= descriptor.MemberLimit() {
		return EnrollmentTokenPayload{}, limit("enrollment token uses", int(spec.MaxUses), int(descriptor.MemberLimit())-1)
	}
	if len(spec.OwnerMultiaddrs) == 0 || len(spec.OwnerMultiaddrs) > MaxMemberMultiaddrs {
		return EnrollmentTokenPayload{}, limit("enrollment token owner multiaddrs",
			len(spec.OwnerMultiaddrs), MaxMemberMultiaddrs)
	}
	addresses, err := normalizeMemberMultiaddrs(spec.OwnerMultiaddrs)
	if err != nil {
		return EnrollmentTokenPayload{}, err
	}
	expiresAt, err := canonicalTime(spec.ExpiresAt)
	if err != nil {
		return EnrollmentTokenPayload{}, err
	}
	if !expiresAt.After(descriptor.CreatedAt()) {
		return EnrollmentTokenPayload{}, invariant("enrollment token must expire after Channel creation")
	}
	secret := append([]byte(nil), spec.BearerSecret...)
	wire := enrollmentTokenPayloadWire{BearerSecret: secret,
		ChannelDescriptor: spec.Descriptor.WireJSON().Bytes(), ExpiresAt: formatTime(expiresAt),
		GrantID: spec.GrantID.String(), MaxUses: spec.MaxUses, OwnerMultiaddrs: addresses,
		ProtocolMaxVersion: EnrollmentProtocolMaxVersion,
		ProtocolMinVersion: EnrollmentProtocolMinVersion, SchemaVersion: SchemaVersion}
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxChannelRecordBytes {
		if err != nil {
			return EnrollmentTokenPayload{}, err
		}
		return EnrollmentTokenPayload{}, limit("enrollment token payload", len(canonical.raw), MaxChannelRecordBytes)
	}
	return EnrollmentTokenPayload{descriptor: spec.Descriptor, ownerMultiaddrs: addresses,
		grantID: spec.GrantID, bearerSecret: string(secret), expiresAt: expiresAt,
		maxUses: spec.MaxUses, canonical: canonical, digest: Sum(canonical.Bytes())}, nil
}

func ParseEnrollmentTokenPayload(raw []byte) (EnrollmentTokenPayload, error) {
	var wire enrollmentTokenPayloadWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return EnrollmentTokenPayload{}, fmt.Errorf("parse enrollment token payload: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion {
		return EnrollmentTokenPayload{}, invalid("enrollment token", "unsupported schema version")
	}
	descriptor, err := ParseSignedChannelDescriptor(wire.ChannelDescriptor)
	if err != nil {
		return EnrollmentTokenPayload{}, err
	}
	grantID, err := ParseGrantID(wire.GrantID)
	if err != nil {
		return EnrollmentTokenPayload{}, err
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, wire.ExpiresAt)
	if err != nil || formatTime(expiresAt) != wire.ExpiresAt {
		return EnrollmentTokenPayload{}, invalid("enrollment token expires_at", "must be canonical UTC RFC3339Nano")
	}
	payload, err := NewEnrollmentTokenPayload(EnrollmentTokenSpec{Descriptor: descriptor,
		OwnerMultiaddrs: wire.OwnerMultiaddrs, GrantID: grantID, BearerSecret: wire.BearerSecret,
		ExpiresAt: expiresAt, MaxUses: wire.MaxUses, ProtocolMinVersion: wire.ProtocolMinVersion,
		ProtocolMaxVersion: wire.ProtocolMaxVersion})
	if err != nil {
		return EnrollmentTokenPayload{}, err
	}
	if !bytes.Equal(payload.canonical.Bytes(), raw) {
		return EnrollmentTokenPayload{}, invalid("enrollment token payload", "wire bytes are not canonical")
	}
	return payload, nil
}

func (payload EnrollmentTokenPayload) Descriptor() SignedChannelDescriptor { return payload.descriptor }
func (payload EnrollmentTokenPayload) OwnerMultiaddrs() []string {
	return append([]string(nil), payload.ownerMultiaddrs...)
}
func (payload EnrollmentTokenPayload) GrantID() GrantID { return payload.grantID }
func (payload EnrollmentTokenPayload) BearerSecret() []byte {
	return append([]byte(nil), payload.bearerSecret...)
}
func (payload EnrollmentTokenPayload) ExpiresAt() time.Time { return payload.expiresAt }
func (payload EnrollmentTokenPayload) MaxUses() uint8       { return payload.maxUses }
func (payload EnrollmentTokenPayload) ProtocolMin() uint8   { return EnrollmentProtocolMinVersion }
func (payload EnrollmentTokenPayload) ProtocolMax() uint8   { return EnrollmentProtocolMaxVersion }

// RevealCanonicalJSON returns bearer material for signing or an explicit
// credential handoff. General formatting and logging remain redacted.
func (payload EnrollmentTokenPayload) RevealCanonicalJSON() JSON { return payload.canonical }
func (payload EnrollmentTokenPayload) Digest() Digest            { return payload.digest }
func (payload EnrollmentTokenPayload) IsZero() bool {
	return payload.canonical.IsZero() || payload.digest.IsZero()
}

func (EnrollmentTokenPayload) String() string   { return "[REDACTED enrollment token payload]" }
func (EnrollmentTokenPayload) GoString() string { return "[REDACTED enrollment token payload]" }
func (EnrollmentTokenPayload) Format(state fmt.State, _ rune) {
	formatEnrollmentCredential(state, "[REDACTED enrollment token payload]")
}
func (EnrollmentTokenPayload) LogValue() slog.Value {
	return slog.StringValue("[REDACTED enrollment token payload]")
}

func EnrollmentTokenSigningMessage(channelID ChannelID, digest Digest) ([]byte, error) {
	return channelSigningMessage(EnrollmentTokenSignatureDomain, channelID, digest)
}

type EnrollmentToken struct {
	payload   EnrollmentTokenPayload
	signature string
	wire      JSON
	encoded   string
}

type enrollmentTokenWire struct {
	OwnerSignature []byte          `json:"owner_signature"`
	Payload        json.RawMessage `json:"payload"`
	PayloadDigest  string          `json:"payload_digest"`
}

func AttachEnrollmentTokenSignature(payload EnrollmentTokenPayload, signature []byte) (EnrollmentToken, error) {
	if payload.IsZero() || len(signature) != ed25519.SignatureSize {
		return EnrollmentToken{}, invalid("signed enrollment token", "payload and a 64-byte owner signature are required")
	}
	message, err := EnrollmentTokenSigningMessage(payload.Descriptor().Descriptor().ID(), payload.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(payload.Descriptor().Descriptor().OwnerPublicKey()),
		message, signature) {
		return EnrollmentToken{}, invalid("signed enrollment token", "owner signature is invalid")
	}
	copySignature := append([]byte(nil), signature...)
	wire, err := JSONFrom(struct {
		OwnerSignature []byte `json:"owner_signature"`
		Payload        JSON   `json:"payload"`
		PayloadDigest  Digest `json:"payload_digest"`
	}{copySignature, payload.RevealCanonicalJSON(), payload.Digest()})
	if err != nil || len(wire.raw) > MaxChannelRecordBytes {
		if err != nil {
			return EnrollmentToken{}, err
		}
		return EnrollmentToken{}, limit("signed enrollment token", len(wire.raw), MaxChannelRecordBytes)
	}
	encoded := EnrollmentTokenPrefix + base64.RawURLEncoding.EncodeToString(wire.Bytes())
	return EnrollmentToken{payload: payload, signature: string(copySignature), wire: wire, encoded: encoded}, nil
}

func ParseEnrollmentToken(value string) (EnrollmentToken, error) {
	if !strings.HasPrefix(value, EnrollmentTokenPrefix) || len(value) <= len(EnrollmentTokenPrefix) {
		return EnrollmentToken{}, invalid("enrollment token", "must use the mnch1_ prefix")
	}
	encoded := strings.TrimPrefix(value, EnrollmentTokenPrefix)
	if len(encoded) > base64.RawURLEncoding.EncodedLen(MaxChannelRecordBytes) {
		return EnrollmentToken{}, limit("enrollment token", len(encoded), base64.RawURLEncoding.EncodedLen(MaxChannelRecordBytes))
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != encoded {
		return EnrollmentToken{}, invalid("enrollment token", "envelope must use unpadded canonical base64url")
	}
	var wire enrollmentTokenWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return EnrollmentToken{}, fmt.Errorf("parse enrollment token: %w", err)
	}
	payload, err := ParseEnrollmentTokenPayload(wire.Payload)
	if err != nil {
		return EnrollmentToken{}, err
	}
	digest, err := ParseDigest(wire.PayloadDigest)
	if err != nil || digest != payload.Digest() {
		return EnrollmentToken{}, invalid("signed enrollment token", "digest does not match payload")
	}
	token, err := AttachEnrollmentTokenSignature(payload, wire.OwnerSignature)
	if err != nil {
		return EnrollmentToken{}, err
	}
	if !bytes.Equal(token.wire.Bytes(), raw) || token.encoded != value {
		return EnrollmentToken{}, invalid("signed enrollment token", "wire bytes are not canonical")
	}
	return token, nil
}

func VerifyEnrollmentToken(token EnrollmentToken) error {
	if token.IsZero() {
		return invalid("signed enrollment token", "value is incomplete")
	}
	if err := VerifyChannelDescriptor(token.payload.Descriptor()); err != nil {
		return err
	}
	message, err := EnrollmentTokenSigningMessage(token.payload.Descriptor().Descriptor().ID(), token.payload.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(token.payload.Descriptor().Descriptor().OwnerPublicKey()),
		message, token.OwnerSignature()) {
		return invalid("signed enrollment token", "owner signature is invalid")
	}
	return nil
}

func (token EnrollmentToken) Payload() EnrollmentTokenPayload { return token.payload }
func (token EnrollmentToken) OwnerSignature() []byte          { return append([]byte(nil), token.signature...) }

// RevealWireJSON returns the bearer envelope for protocol code that explicitly
// needs its canonical bytes.
func (token EnrollmentToken) RevealWireJSON() JSON { return token.wire }

// Reveal returns the bearer credential for an explicit handoff to its intended
// recipient. String, GoString and structured logging deliberately redact it.
func (token EnrollmentToken) Reveal() string { return token.encoded }
func (EnrollmentToken) String() string       { return "[REDACTED enrollment token]" }
func (EnrollmentToken) GoString() string     { return "[REDACTED enrollment token]" }
func (EnrollmentToken) Format(state fmt.State, _ rune) {
	formatEnrollmentCredential(state, "[REDACTED enrollment token]")
}
func (EnrollmentToken) LogValue() slog.Value { return slog.StringValue("[REDACTED enrollment token]") }
func (token EnrollmentToken) IsZero() bool {
	return token.payload.IsZero() || len(token.signature) != ed25519.SignatureSize ||
		token.wire.IsZero() || token.encoded == ""
}

// EnrollmentVerifier is the durable, bearer-secret-free derivation used to
// validate an enrollment proof. It is intentionally distinct from a general
// Digest so callers cannot accidentally construct a grant from unrelated bytes.
type EnrollmentVerifier struct {
	digest Digest
}

// EnrollmentVerifierFromStoredBytes is the explicit SQLite reconstruction
// boundary. New verifiers must otherwise be derived with VerifierForEnrollment.
func EnrollmentVerifierFromStoredBytes(value []byte) (EnrollmentVerifier, error) {
	digest, err := DigestFromBytes(value)
	if err != nil || digest.IsZero() {
		return EnrollmentVerifier{}, invalid("stored enrollment verifier", "must contain exactly 32 nonzero bytes")
	}
	return EnrollmentVerifier{digest: digest}, nil
}

func (verifier EnrollmentVerifier) Bytes() []byte { return verifier.digest.Bytes() }
func (verifier EnrollmentVerifier) IsZero() bool  { return verifier.digest.IsZero() }
func (EnrollmentVerifier) String() string         { return "[REDACTED enrollment verifier]" }
func (EnrollmentVerifier) GoString() string       { return "[REDACTED enrollment verifier]" }
func (EnrollmentVerifier) Format(state fmt.State, _ rune) {
	formatEnrollmentCredential(state, "[REDACTED enrollment verifier]")
}
func (EnrollmentVerifier) LogValue() slog.Value {
	return slog.StringValue("[REDACTED enrollment verifier]")
}

func formatEnrollmentCredential(state fmt.State, redacted string) {
	_, _ = state.Write([]byte(redacted))
}

// VerifierForEnrollment derives the only bearer-derived value that may be
// persisted. The length prefixes make Channel/grant concatenation unambiguous.
func VerifierForEnrollment(secret []byte, channelID ChannelID, grantID GrantID) (EnrollmentVerifier, error) {
	if len(secret) != EnrollmentSecretBytes || channelID.IsZero() || grantID.IsZero() {
		return EnrollmentVerifier{}, invalid("enrollment verifier", "32-byte secret, Channel and grant are required")
	}
	message, err := lengthSafeDomainMessage(EnrollmentVerifierDomain,
		[]byte(channelID.String()), []byte(grantID.String()))
	if err != nil {
		return EnrollmentVerifier{}, err
	}
	digest, err := hmacDigest(secret, message)
	if err != nil {
		return EnrollmentVerifier{}, err
	}
	return EnrollmentVerifier{digest: digest}, nil
}

// NewOpenEnrollmentGrantForToken builds the exact durable half of a signed
// transient token, preventing expiry, capacity or verifier drift at issuance.
func NewOpenEnrollmentGrantForToken(token EnrollmentToken,
	createdAt time.Time,
) (OpenEnrollmentGrant, error) {
	if err := VerifyEnrollmentToken(token); err != nil {
		return OpenEnrollmentGrant{}, fmt.Errorf("create enrollment grant from token: %w", err)
	}
	payload := token.Payload()
	verifier, err := VerifierForEnrollment(payload.BearerSecret(),
		payload.Descriptor().Descriptor().ID(), payload.GrantID())
	if err != nil {
		return OpenEnrollmentGrant{}, err
	}
	return newOpenEnrollmentGrant(payload.GrantID(), payload.Descriptor().Descriptor().ID(),
		verifier, payload.ExpiresAt(), payload.MaxUses(), createdAt)
}

// VerifyEnrollmentTokenAgainstGrant proves that the signed bearer token is
// exactly the transient half of this durable grant.
func VerifyEnrollmentTokenAgainstGrant(token EnrollmentToken, grant OpenEnrollmentGrant) error {
	if err := VerifyEnrollmentToken(token); err != nil || grant.IsZero() {
		return invalid("enrollment token grant binding", "complete verified token and grant are required")
	}
	payload := token.Payload()
	verifier, err := VerifierForEnrollment(payload.BearerSecret(),
		payload.Descriptor().Descriptor().ID(), payload.GrantID())
	if err != nil || payload.GrantID() != grant.ID() ||
		payload.Descriptor().Descriptor().ID() != grant.ChannelID() ||
		!payload.ExpiresAt().Equal(grant.ExpiresAt()) || payload.MaxUses() != grant.MaxUses() ||
		verifier != grant.Verifier() {
		return invalid("enrollment token grant binding", "token does not match the durable grant")
	}
	return nil
}

type EnrollmentTranscriptSpec struct {
	ChannelID            ChannelID
	GrantID              GrantID
	RequestID            EnrollmentRequestID
	OwnerPeerID          PeerID
	JoinerPeerID         PeerID
	OwnerNonce           []byte
	JoinerNonce          []byte
	SelectedVersion      uint8
	Limits               JSON
	JoinerOriginEpoch    OriginEpoch
	JoinerDisplayLabel   string
	JoinerPublicKey      []byte
	AdvertisedMultiaddrs []string
	RosterHead           RecordHead
}

type EnrollmentTranscript struct {
	spec                    EnrollmentTranscriptSpec
	ownerNonce              string
	joinerNonce             string
	joinerPublicKey         string
	advertisedAddressDigest Digest
	canonical               JSON
}

type enrollmentTranscriptWire struct {
	AdvertisedAddressDigest string          `json:"advertised_address_digest"`
	ChannelID               string          `json:"channel_id"`
	GrantID                 string          `json:"grant_id"`
	JoinerDisplayLabel      string          `json:"joiner_display_label"`
	JoinerNonce             []byte          `json:"joiner_nonce"`
	JoinerOriginEpoch       string          `json:"joiner_origin_epoch"`
	JoinerPeerID            string          `json:"joiner_peer_id"`
	JoinerPublicKey         []byte          `json:"joiner_public_key"`
	Limits                  json.RawMessage `json:"limits"`
	OwnerNonce              []byte          `json:"owner_nonce"`
	OwnerPeerID             string          `json:"owner_peer_id"`
	RequestID               string          `json:"request_id"`
	RosterHead              struct {
		Digest   string `json:"digest"`
		Revision uint64 `json:"revision"`
	} `json:"roster_head"`
	SchemaVersion   int   `json:"schema_version"`
	SelectedVersion uint8 `json:"selected_version"`
}

func NewEnrollmentTranscript(spec EnrollmentTranscriptSpec) (EnrollmentTranscript, error) {
	if spec.ChannelID.IsZero() || spec.GrantID.IsZero() || spec.RequestID.IsZero() ||
		spec.OwnerPeerID.IsZero() || spec.JoinerPeerID.IsZero() || spec.JoinerOriginEpoch.IsZero() ||
		spec.RosterHead.IsZero() {
		return EnrollmentTranscript{}, invalid("enrollment transcript", "Channel, grant, request, roster and both peer identities are required")
	}
	if spec.OwnerPeerID == spec.JoinerPeerID {
		return EnrollmentTranscript{}, invariant("Channel owner cannot enroll itself as a new member")
	}
	if _, err := CanonicalPeerIDBytes(spec.OwnerPeerID); err != nil {
		return EnrollmentTranscript{}, err
	}
	if len(spec.OwnerNonce) != EnrollmentNonceBytes || len(spec.JoinerNonce) != EnrollmentNonceBytes {
		return EnrollmentTranscript{}, invalid("enrollment transcript nonce", "both nonces must contain exactly 32 bytes")
	}
	if spec.SelectedVersion != EnrollmentProtocolMinVersion {
		return EnrollmentTranscript{}, invalid("enrollment transcript protocol", "T0 selected version must be 1")
	}
	if spec.Limits.String() != DefaultMemberLimits().String() {
		return EnrollmentTranscript{}, invalid("enrollment transcript limits", "must equal the R5 hermetic network profile")
	}
	if err := validateRuleText("joiner display label", spec.JoinerDisplayLabel, MaxLabelBytes); err != nil {
		return EnrollmentTranscript{}, err
	}
	if len(spec.JoinerPublicKey) != ed25519.PublicKeySize {
		return EnrollmentTranscript{}, invalid("joiner public key", "must be a 32-byte Ed25519 key")
	}
	if err := validatePeerPublicKey(spec.JoinerPeerID, spec.JoinerPublicKey); err != nil {
		return EnrollmentTranscript{}, fmt.Errorf("enrollment transcript joiner: %w", err)
	}
	addressDigest, err := AdvertisedAddressDigest(spec.AdvertisedMultiaddrs)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	ownerNonce := append([]byte(nil), spec.OwnerNonce...)
	joinerNonce := append([]byte(nil), spec.JoinerNonce...)
	joinerKey := append([]byte(nil), spec.JoinerPublicKey...)
	wire := enrollmentTranscriptWire{AdvertisedAddressDigest: addressDigest.String(),
		ChannelID: spec.ChannelID.String(), GrantID: spec.GrantID.String(),
		JoinerDisplayLabel: spec.JoinerDisplayLabel, JoinerNonce: joinerNonce,
		JoinerOriginEpoch: spec.JoinerOriginEpoch.String(), JoinerPeerID: spec.JoinerPeerID.String(),
		JoinerPublicKey: joinerKey, Limits: spec.Limits.Bytes(), OwnerNonce: ownerNonce,
		OwnerPeerID: spec.OwnerPeerID.String(), RequestID: spec.RequestID.String(), SchemaVersion: SchemaVersion,
		SelectedVersion: spec.SelectedVersion}
	wire.RosterHead.Digest = spec.RosterHead.Digest().String()
	wire.RosterHead.Revision = spec.RosterHead.Revision()
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxChannelRecordBytes {
		if err != nil {
			return EnrollmentTranscript{}, err
		}
		return EnrollmentTranscript{}, limit("enrollment transcript", len(canonical.raw), MaxChannelRecordBytes)
	}
	cleanSpec := spec
	cleanSpec.OwnerNonce, cleanSpec.JoinerNonce, cleanSpec.JoinerPublicKey = nil, nil, nil
	cleanSpec.AdvertisedMultiaddrs = nil
	return EnrollmentTranscript{spec: cleanSpec, ownerNonce: string(ownerNonce),
		joinerNonce: string(joinerNonce), joinerPublicKey: string(joinerKey),
		advertisedAddressDigest: addressDigest, canonical: canonical}, nil
}

func ParseEnrollmentTranscript(raw []byte) (EnrollmentTranscript, error) {
	var wire enrollmentTranscriptWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return EnrollmentTranscript{}, fmt.Errorf("parse enrollment transcript: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion {
		return EnrollmentTranscript{}, invalid("enrollment transcript", "unsupported schema version")
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	grantID, err := ParseGrantID(wire.GrantID)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	requestID, err := ParseEnrollmentRequestID(wire.RequestID)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	ownerPeerID, err := ParsePeerID(wire.OwnerPeerID)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	joinerPeerID, err := ParsePeerID(wire.JoinerPeerID)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	epoch, err := ParseOriginEpoch(wire.JoinerOriginEpoch)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	limits, err := NewJSON(wire.Limits)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	digest, err := ParseDigest(wire.AdvertisedAddressDigest)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	rosterDigest, err := ParseDigest(wire.RosterHead.Digest)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	rosterHead, err := NewRecordHead(wire.RosterHead.Revision, rosterDigest)
	if err != nil {
		return EnrollmentTranscript{}, err
	}
	// Parsing does not have the advertised addresses themselves, so rebuild the
	// exact canonical transcript directly after validating every other field.
	spec := EnrollmentTranscriptSpec{ChannelID: channelID, GrantID: grantID, RequestID: requestID,
		OwnerPeerID:  ownerPeerID,
		JoinerPeerID: joinerPeerID, OwnerNonce: wire.OwnerNonce, JoinerNonce: wire.JoinerNonce,
		SelectedVersion: wire.SelectedVersion, Limits: limits, JoinerOriginEpoch: epoch,
		JoinerDisplayLabel: wire.JoinerDisplayLabel, JoinerPublicKey: wire.JoinerPublicKey,
		RosterHead: rosterHead}
	if _, err := CanonicalPeerIDBytes(spec.OwnerPeerID); err != nil {
		return EnrollmentTranscript{}, err
	}
	if len(spec.OwnerNonce) != EnrollmentNonceBytes || len(spec.JoinerNonce) != EnrollmentNonceBytes ||
		spec.SelectedVersion != EnrollmentProtocolMinVersion || limits.String() != DefaultMemberLimits().String() {
		return EnrollmentTranscript{}, invalid("enrollment transcript", "nonce, version or limits are invalid")
	}
	if spec.OwnerPeerID == spec.JoinerPeerID {
		return EnrollmentTranscript{}, invariant("Channel owner cannot enroll itself as a new member")
	}
	if err := validateRuleText("joiner display label", spec.JoinerDisplayLabel, MaxLabelBytes); err != nil {
		return EnrollmentTranscript{}, err
	}
	if len(spec.JoinerPublicKey) != ed25519.PublicKeySize {
		return EnrollmentTranscript{}, invalid("joiner public key", "must be a 32-byte Ed25519 key")
	}
	if err := validatePeerPublicKey(spec.JoinerPeerID, spec.JoinerPublicKey); err != nil {
		return EnrollmentTranscript{}, err
	}
	ownerNonce := append([]byte(nil), spec.OwnerNonce...)
	joinerNonce := append([]byte(nil), spec.JoinerNonce...)
	joinerKey := append([]byte(nil), spec.JoinerPublicKey...)
	expectedWire := enrollmentTranscriptWire{AdvertisedAddressDigest: digest.String(),
		ChannelID: spec.ChannelID.String(), GrantID: spec.GrantID.String(),
		JoinerDisplayLabel: spec.JoinerDisplayLabel, JoinerNonce: joinerNonce,
		JoinerOriginEpoch: spec.JoinerOriginEpoch.String(), JoinerPeerID: spec.JoinerPeerID.String(),
		JoinerPublicKey: joinerKey, Limits: spec.Limits.Bytes(), OwnerNonce: ownerNonce,
		OwnerPeerID: spec.OwnerPeerID.String(), RequestID: spec.RequestID.String(),
		SchemaVersion: SchemaVersion, SelectedVersion: spec.SelectedVersion}
	expectedWire.RosterHead.Digest = spec.RosterHead.Digest().String()
	expectedWire.RosterHead.Revision = spec.RosterHead.Revision()
	canonical, err := JSONFrom(expectedWire)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return EnrollmentTranscript{}, invalid("enrollment transcript", "wire bytes are not canonical")
	}
	result := EnrollmentTranscript{spec: spec, ownerNonce: string(ownerNonce),
		joinerNonce: string(joinerNonce), joinerPublicKey: string(joinerKey),
		advertisedAddressDigest: digest, canonical: canonical}
	result.spec.OwnerNonce, result.spec.JoinerNonce, result.spec.JoinerPublicKey = nil, nil, nil
	return result, nil
}

func AdvertisedAddressDigest(addresses []string) (Digest, error) {
	if len(addresses) == 0 || len(addresses) > MaxMemberMultiaddrs {
		return Digest{}, limit("enrollment advertised multiaddrs", len(addresses), MaxMemberMultiaddrs)
	}
	normalized, err := normalizeMemberMultiaddrs(addresses)
	if err != nil {
		return Digest{}, err
	}
	canonical, err := JSONFrom(normalized)
	if err != nil {
		return Digest{}, err
	}
	return Sum(canonical.Bytes()), nil
}

func (transcript EnrollmentTranscript) ChannelID() ChannelID { return transcript.spec.ChannelID }
func (transcript EnrollmentTranscript) GrantID() GrantID     { return transcript.spec.GrantID }
func (transcript EnrollmentTranscript) RequestID() EnrollmentRequestID {
	return transcript.spec.RequestID
}
func (transcript EnrollmentTranscript) OwnerPeerID() PeerID  { return transcript.spec.OwnerPeerID }
func (transcript EnrollmentTranscript) JoinerPeerID() PeerID { return transcript.spec.JoinerPeerID }
func (transcript EnrollmentTranscript) OwnerNonce() []byte {
	return append([]byte(nil), transcript.ownerNonce...)
}
func (transcript EnrollmentTranscript) JoinerNonce() []byte {
	return append([]byte(nil), transcript.joinerNonce...)
}
func (transcript EnrollmentTranscript) SelectedVersion() uint8 {
	return transcript.spec.SelectedVersion
}
func (transcript EnrollmentTranscript) Limits() JSON { return transcript.spec.Limits }
func (transcript EnrollmentTranscript) JoinerOriginEpoch() OriginEpoch {
	return transcript.spec.JoinerOriginEpoch
}
func (transcript EnrollmentTranscript) JoinerDisplayLabel() string {
	return transcript.spec.JoinerDisplayLabel
}
func (transcript EnrollmentTranscript) JoinerPublicKey() []byte {
	return append([]byte(nil), transcript.joinerPublicKey...)
}
func (transcript EnrollmentTranscript) AdvertisedAddressDigest() Digest {
	return transcript.advertisedAddressDigest
}
func (transcript EnrollmentTranscript) RosterHead() RecordHead { return transcript.spec.RosterHead }
func (transcript EnrollmentTranscript) CanonicalJSON() JSON    { return transcript.canonical }
func (transcript EnrollmentTranscript) IsZero() bool           { return transcript.canonical.IsZero() }

func ComputeEnrollmentProof(verifier EnrollmentVerifier, transcript EnrollmentTranscript) (Digest, error) {
	if verifier.IsZero() || transcript.IsZero() {
		return Digest{}, invalid("enrollment proof", "verifier and canonical transcript are required")
	}
	message, err := lengthSafeDomainMessage(EnrollmentProofDomain, transcript.CanonicalJSON().Bytes())
	if err != nil {
		return Digest{}, err
	}
	return hmacDigest(verifier.Bytes(), message)
}

func VerifyEnrollmentProof(verifier EnrollmentVerifier, transcript EnrollmentTranscript, proof Digest) error {
	expected, err := ComputeEnrollmentProof(verifier, transcript)
	if err != nil || proof.IsZero() || !hmac.Equal(expected.Bytes(), proof.Bytes()) {
		return invalid("enrollment proof", "transcript proof is invalid")
	}
	return nil
}

func EnrollmentJoinIdentityDigest(channelID ChannelID, grantID GrantID, authenticatedPeerID PeerID,
	publicKey []byte, originEpoch OriginEpoch,
) (Digest, error) {
	if channelID.IsZero() || grantID.IsZero() || authenticatedPeerID.IsZero() || originEpoch.IsZero() ||
		len(publicKey) != ed25519.PublicKeySize {
		return Digest{}, invalid("enrollment join identity", "Channel, grant, peer, key and epoch are required")
	}
	if err := validatePeerPublicKey(authenticatedPeerID, publicKey); err != nil {
		return Digest{}, err
	}
	message, err := lengthSafeDomainMessage(EnrollmentJoinIdentityDomain,
		[]byte(channelID.String()), []byte(grantID.String()), []byte(authenticatedPeerID.String()),
		publicKey, []byte(originEpoch.String()))
	if err != nil {
		return Digest{}, err
	}
	return Sum(message), nil
}

func (transcript EnrollmentTranscript) JoinIdentityDigest() (Digest, error) {
	return EnrollmentJoinIdentityDigest(transcript.ChannelID(), transcript.GrantID(),
		transcript.JoinerPeerID(), transcript.JoinerPublicKey(), transcript.JoinerOriginEpoch())
}

type EnrollmentUseID struct{ identifier }
type EnrollmentReceiptID struct{ identifier }
type EnrollmentRequestID struct{ identifier }

func ParseEnrollmentUseID(value string) (EnrollmentUseID, error) {
	id, err := newIdentifier("enrollment_use_id", value)
	return EnrollmentUseID{id}, err
}

func ParseEnrollmentReceiptID(value string) (EnrollmentReceiptID, error) {
	id, err := newIdentifier("enrollment_receipt_id", value)
	return EnrollmentReceiptID{id}, err
}

func ParseEnrollmentRequestID(value string) (EnrollmentRequestID, error) {
	id, err := newIdentifier("enrollment_request_id", value)
	return EnrollmentRequestID{id}, err
}

// EnrollmentEvidenceIDs derives durable evidence identifiers from the stable
// join identity. Retries with fresh nonces therefore address the same use and
// receipt without accepting caller-selected durable keys.
func EnrollmentEvidenceIDs(joinIdentity Digest) (EnrollmentUseID, EnrollmentReceiptID, error) {
	if joinIdentity.IsZero() {
		return EnrollmentUseID{}, EnrollmentReceiptID{},
			invalid("enrollment evidence IDs", "stable join identity is required")
	}
	useValue, err := enrollmentEvidenceID("enrollment-use-", EnrollmentUseIDDomain, joinIdentity)
	if err != nil {
		return EnrollmentUseID{}, EnrollmentReceiptID{}, err
	}
	receiptValue, err := enrollmentEvidenceID("enrollment-receipt-", EnrollmentReceiptIDDomain, joinIdentity)
	if err != nil {
		return EnrollmentUseID{}, EnrollmentReceiptID{}, err
	}
	useID, err := ParseEnrollmentUseID(useValue)
	if err != nil {
		return EnrollmentUseID{}, EnrollmentReceiptID{}, err
	}
	receiptID, err := ParseEnrollmentReceiptID(receiptValue)
	if err != nil {
		return EnrollmentUseID{}, EnrollmentReceiptID{}, err
	}
	return useID, receiptID, nil
}

// EnrollmentRequestIDForJoinIdentity derives the stable request correlation
// used across response loss and process restart. It contains no bearer secret;
// the stable join identity already binds Channel, grant, authenticated PeerID,
// public key, and origin epoch.
func EnrollmentRequestIDForJoinIdentity(joinIdentity Digest) (EnrollmentRequestID, error) {
	if joinIdentity.IsZero() {
		return EnrollmentRequestID{}, invalid("enrollment request ID", "stable join identity is required")
	}
	message, err := lengthSafeDomainMessage(EnrollmentRequestIDDomain, joinIdentity.Bytes())
	if err != nil {
		return EnrollmentRequestID{}, err
	}
	return ParseEnrollmentRequestID("enrollment-request-" + hex.EncodeToString(Sum(message).Bytes()))
}

func enrollmentEvidenceID(prefix, domain string, joinIdentity Digest) (string, error) {
	message, err := lengthSafeDomainMessage(domain, joinIdentity.Bytes())
	if err != nil {
		return "", err
	}
	digest := Sum(message)
	return prefix + hex.EncodeToString(digest.Bytes()), nil
}

type EnrollmentReceiptRecordSpec struct {
	ReceiptID          EnrollmentReceiptID
	RequestID          EnrollmentRequestID
	GrantID            GrantID
	ChannelID          ChannelID
	MemberPeerID       PeerID
	JoinIdentityDigest Digest
	MemberHead         RecordHead
	AcceptedAt         time.Time
}

type EnrollmentReceiptRecord struct {
	spec       EnrollmentReceiptRecordSpec
	acceptedAt time.Time
	canonical  JSON
	digest     Digest
}

type enrollmentReceiptRecordWire struct {
	AcceptedAt         string `json:"accepted_at"`
	ChannelID          string `json:"channel_id"`
	GrantID            string `json:"grant_id"`
	JoinIdentityDigest string `json:"join_identity_digest"`
	MemberHead         struct {
		Digest   string `json:"digest"`
		Revision uint64 `json:"revision"`
	} `json:"member_head"`
	MemberPeerID  string `json:"member_peer_id"`
	ReceiptID     string `json:"receipt_id"`
	RequestID     string `json:"request_id"`
	SchemaVersion int    `json:"schema_version"`
}

func NewEnrollmentReceiptRecord(spec EnrollmentReceiptRecordSpec) (EnrollmentReceiptRecord, error) {
	if spec.ReceiptID.IsZero() || spec.RequestID.IsZero() || spec.GrantID.IsZero() || spec.ChannelID.IsZero() ||
		spec.MemberPeerID.IsZero() || spec.JoinIdentityDigest.IsZero() || spec.MemberHead.IsZero() {
		return EnrollmentReceiptRecord{}, invalid("enrollment receipt", "IDs, join identity and member head are required")
	}
	if spec.MemberHead.Revision() <= 1 {
		return EnrollmentReceiptRecord{}, invariant("enrollment receipt cannot acknowledge owner genesis")
	}
	if _, err := CanonicalPeerIDBytes(spec.MemberPeerID); err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	acceptedAt, err := canonicalTime(spec.AcceptedAt)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	wire := enrollmentReceiptRecordWire{AcceptedAt: formatTime(acceptedAt),
		ChannelID: spec.ChannelID.String(), GrantID: spec.GrantID.String(),
		JoinIdentityDigest: spec.JoinIdentityDigest.String(), MemberPeerID: spec.MemberPeerID.String(),
		ReceiptID: spec.ReceiptID.String(), RequestID: spec.RequestID.String(), SchemaVersion: SchemaVersion}
	wire.MemberHead.Digest = spec.MemberHead.Digest().String()
	wire.MemberHead.Revision = spec.MemberHead.Revision()
	canonical, err := JSONFrom(wire)
	if err != nil || len(canonical.raw) > MaxChannelRecordBytes {
		if err != nil {
			return EnrollmentReceiptRecord{}, err
		}
		return EnrollmentReceiptRecord{}, limit("enrollment receipt", len(canonical.raw), MaxChannelRecordBytes)
	}
	spec.AcceptedAt = acceptedAt
	return EnrollmentReceiptRecord{spec: spec, acceptedAt: acceptedAt, canonical: canonical,
		digest: Sum(canonical.Bytes())}, nil
}

func ParseEnrollmentReceiptRecord(raw []byte) (EnrollmentReceiptRecord, error) {
	var wire enrollmentReceiptRecordWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return EnrollmentReceiptRecord{}, fmt.Errorf("parse enrollment receipt: %w", err)
	}
	if wire.SchemaVersion != SchemaVersion {
		return EnrollmentReceiptRecord{}, invalid("enrollment receipt", "unsupported schema version")
	}
	receiptID, err := ParseEnrollmentReceiptID(wire.ReceiptID)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	requestID, err := ParseEnrollmentRequestID(wire.RequestID)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	grantID, err := ParseGrantID(wire.GrantID)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	memberPeerID, err := ParsePeerID(wire.MemberPeerID)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	identityDigest, err := ParseDigest(wire.JoinIdentityDigest)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	memberDigest, err := ParseDigest(wire.MemberHead.Digest)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	memberHead, err := NewRecordHead(wire.MemberHead.Revision, memberDigest)
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	acceptedAt, err := time.Parse(time.RFC3339Nano, wire.AcceptedAt)
	if err != nil || formatTime(acceptedAt) != wire.AcceptedAt {
		return EnrollmentReceiptRecord{}, invalid("enrollment receipt accepted_at", "must be canonical UTC RFC3339Nano")
	}
	record, err := NewEnrollmentReceiptRecord(EnrollmentReceiptRecordSpec{ReceiptID: receiptID,
		RequestID: requestID, GrantID: grantID, ChannelID: channelID, MemberPeerID: memberPeerID,
		JoinIdentityDigest: identityDigest, MemberHead: memberHead, AcceptedAt: acceptedAt})
	if err != nil {
		return EnrollmentReceiptRecord{}, err
	}
	if !bytes.Equal(record.canonical.Bytes(), raw) {
		return EnrollmentReceiptRecord{}, invalid("enrollment receipt", "wire bytes are not canonical")
	}
	return record, nil
}

func (record EnrollmentReceiptRecord) ReceiptID() EnrollmentReceiptID { return record.spec.ReceiptID }
func (record EnrollmentReceiptRecord) RequestID() EnrollmentRequestID { return record.spec.RequestID }
func (record EnrollmentReceiptRecord) GrantID() GrantID               { return record.spec.GrantID }
func (record EnrollmentReceiptRecord) ChannelID() ChannelID           { return record.spec.ChannelID }
func (record EnrollmentReceiptRecord) MemberPeerID() PeerID           { return record.spec.MemberPeerID }
func (record EnrollmentReceiptRecord) JoinIdentityDigest() Digest {
	return record.spec.JoinIdentityDigest
}
func (record EnrollmentReceiptRecord) MemberHead() RecordHead { return record.spec.MemberHead }
func (record EnrollmentReceiptRecord) AcceptedAt() time.Time  { return record.acceptedAt }
func (record EnrollmentReceiptRecord) CanonicalJSON() JSON    { return record.canonical }
func (record EnrollmentReceiptRecord) Digest() Digest         { return record.digest }
func (record EnrollmentReceiptRecord) IsZero() bool {
	return record.canonical.IsZero() || record.digest.IsZero()
}

func EnrollmentReceiptSigningMessage(channelID ChannelID, digest Digest) ([]byte, error) {
	return channelSigningMessage(EnrollmentReceiptSignatureDomain, channelID, digest)
}

type EnrollmentReceipt struct {
	record    EnrollmentReceiptRecord
	signature string
	wire      JSON
}

type enrollmentReceiptWire struct {
	OwnerSignature []byte          `json:"owner_signature"`
	Receipt        json.RawMessage `json:"receipt"`
	ReceiptDigest  string          `json:"receipt_digest"`
}

func AttachEnrollmentReceiptSignature(record EnrollmentReceiptRecord, signature []byte) (EnrollmentReceipt, error) {
	if record.IsZero() || len(signature) != ed25519.SignatureSize {
		return EnrollmentReceipt{}, invalid("signed enrollment receipt", "record and a 64-byte owner signature are required")
	}
	copySignature := append([]byte(nil), signature...)
	wire, err := JSONFrom(struct {
		OwnerSignature []byte `json:"owner_signature"`
		Receipt        JSON   `json:"receipt"`
		ReceiptDigest  Digest `json:"receipt_digest"`
	}{copySignature, record.CanonicalJSON(), record.Digest()})
	if err != nil || len(wire.raw) > MaxChannelRecordBytes {
		if err != nil {
			return EnrollmentReceipt{}, err
		}
		return EnrollmentReceipt{}, limit("signed enrollment receipt", len(wire.raw), MaxChannelRecordBytes)
	}
	return EnrollmentReceipt{record: record, signature: string(copySignature), wire: wire}, nil
}

func ParseEnrollmentReceipt(raw []byte) (EnrollmentReceipt, error) {
	var wire enrollmentReceiptWire
	if err := decodeExactChannelJSON(raw, &wire); err != nil {
		return EnrollmentReceipt{}, fmt.Errorf("parse signed enrollment receipt: %w", err)
	}
	record, err := ParseEnrollmentReceiptRecord(wire.Receipt)
	if err != nil {
		return EnrollmentReceipt{}, err
	}
	digest, err := ParseDigest(wire.ReceiptDigest)
	if err != nil || digest != record.Digest() {
		return EnrollmentReceipt{}, invalid("signed enrollment receipt", "digest does not match receipt")
	}
	receipt, err := AttachEnrollmentReceiptSignature(record, wire.OwnerSignature)
	if err != nil {
		return EnrollmentReceipt{}, err
	}
	if !bytes.Equal(receipt.wire.Bytes(), raw) {
		return EnrollmentReceipt{}, invalid("signed enrollment receipt", "wire bytes are not canonical")
	}
	return receipt, nil
}

// VerifyEnrollmentReceiptEvidence verifies only durable enrollment evidence.
// It deliberately needs no bearer secret, nonce, address list or transient
// handshake transcript, so replicas can revalidate a committed DB row after a
// restart.
func VerifyEnrollmentReceiptEvidence(descriptor SignedChannelDescriptor, member Member,
	receipt EnrollmentReceipt,
) error {
	if err := VerifyChannelDescriptor(descriptor); err != nil || receipt.IsZero() {
		return invalid("signed enrollment receipt", "verified descriptor and complete receipt are required")
	}
	if err := VerifyMember(descriptor, member); err != nil {
		return err
	}
	record := receipt.record
	if record.ChannelID() != descriptor.Descriptor().ID() || record.ChannelID() != member.ChannelID() ||
		record.MemberPeerID() != member.PeerID() || record.MemberHead() != member.Head() ||
		member.Status() != MemberActive || record.AcceptedAt().Before(member.CreatedAt()) {
		return invalid("signed enrollment receipt", "receipt does not match the joining MemberRecord")
	}
	identity, err := EnrollmentJoinIdentityDigest(record.ChannelID(), record.GrantID(), member.PeerID(),
		member.PublicKey(), member.OriginEpoch())
	if err != nil || identity != record.JoinIdentityDigest() {
		return invalid("signed enrollment receipt", "stable join identity does not match member")
	}
	message, err := EnrollmentReceiptSigningMessage(record.ChannelID(), record.Digest())
	if err != nil || !ed25519.Verify(ed25519.PublicKey(descriptor.Descriptor().OwnerPublicKey()),
		message, receipt.OwnerSignature()) {
		return invalid("signed enrollment receipt", "owner signature is invalid")
	}
	return nil
}

func VerifyEnrollmentReceipt(descriptor SignedChannelDescriptor, member Member,
	transcript EnrollmentTranscript, receipt EnrollmentReceipt,
) error {
	if transcript.IsZero() {
		return invalid("signed enrollment receipt", "complete enrollment transcript is required")
	}
	if err := VerifyEnrollmentReceiptEvidence(descriptor, member, receipt); err != nil {
		return err
	}
	record := receipt.record
	previous, hasPrevious := member.PreviousDigest()
	memberAddressDigest, addressErr := AdvertisedAddressDigest(member.Multiaddrs())
	if transcript.ChannelID() != record.ChannelID() || transcript.GrantID() != record.GrantID() ||
		transcript.RequestID() != record.RequestID() ||
		transcript.OwnerPeerID() != descriptor.Descriptor().OwnerPeerID() ||
		transcript.JoinerPeerID() != member.PeerID() ||
		!bytes.Equal(transcript.JoinerPublicKey(), member.PublicKey()) ||
		transcript.JoinerOriginEpoch() != member.OriginEpoch() ||
		transcript.JoinerDisplayLabel() != member.DisplayLabel() ||
		transcript.Limits().String() != member.Limits().String() || addressErr != nil ||
		transcript.AdvertisedAddressDigest() != memberAddressDigest || !hasPrevious ||
		member.Head().Revision() != transcript.RosterHead().Revision()+1 ||
		previous != transcript.RosterHead().Digest() {
		return invalid("signed enrollment receipt", "receipt and joining member do not match the enrollment transcript")
	}
	return nil
}

func (receipt EnrollmentReceipt) Record() EnrollmentReceiptRecord { return receipt.record }
func (receipt EnrollmentReceipt) ReceiptID() EnrollmentReceiptID  { return receipt.record.ReceiptID() }
func (receipt EnrollmentReceipt) RequestID() EnrollmentRequestID  { return receipt.record.RequestID() }
func (receipt EnrollmentReceipt) GrantID() GrantID                { return receipt.record.GrantID() }
func (receipt EnrollmentReceipt) ChannelID() ChannelID            { return receipt.record.ChannelID() }
func (receipt EnrollmentReceipt) MemberPeerID() PeerID            { return receipt.record.MemberPeerID() }
func (receipt EnrollmentReceipt) JoinIdentityDigest() Digest {
	return receipt.record.JoinIdentityDigest()
}
func (receipt EnrollmentReceipt) MemberHead() RecordHead { return receipt.record.MemberHead() }
func (receipt EnrollmentReceipt) AcceptedAt() time.Time  { return receipt.record.AcceptedAt() }
func (receipt EnrollmentReceipt) ReceiptJSON() JSON      { return receipt.record.CanonicalJSON() }
func (receipt EnrollmentReceipt) OwnerSignature() []byte {
	return append([]byte(nil), receipt.signature...)
}
func (receipt EnrollmentReceipt) WireJSON() JSON { return receipt.wire }
func (receipt EnrollmentReceipt) IsZero() bool {
	return receipt.record.IsZero() || len(receipt.signature) != ed25519.SignatureSize || receipt.wire.IsZero()
}

func lengthSafeDomainMessage(domain string, fields ...[]byte) ([]byte, error) {
	if domain == "" {
		return nil, invalid("domain message", "domain is required")
	}
	parts := append([][]byte{[]byte(domain)}, fields...)
	total := 0
	for _, part := range parts {
		if uint64(len(part)) > uint64(^uint32(0)) {
			return nil, limit("domain message field", len(part), int(^uint32(0)))
		}
		total += 4 + len(part)
		if total > MaxCanonicalJSONBytes+MaxIdentifierBytes*8 {
			return nil, limit("domain message", total, MaxCanonicalJSONBytes+MaxIdentifierBytes*8)
		}
	}
	message := make([]byte, 0, total)
	var length [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(length[:], uint32(len(part)))
		message = append(message, length[:]...)
		message = append(message, part...)
	}
	return message, nil
}

func hmacDigest(key, message []byte) (Digest, error) {
	if len(key) == 0 || len(message) == 0 {
		return Digest{}, invalid("HMAC", "key and message are required")
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(message)
	return DigestFromBytes(mac.Sum(nil))
}
