package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const PublicationSignatureDomain = "mnemon/r5/channel-publication/1"

// PublicationSigningMessage binds one immutable publication digest to its
// Channel. The same bytes are signed once by admission and verified by the
// store, Gossip validator and direct Pull receiver.
func PublicationSigningMessage(channelID ChannelID, publicationDigest Digest) ([]byte, error) {
	if channelID.IsZero() || publicationDigest.IsZero() {
		return nil, invalid("publication signature message", "Channel and digest are required")
	}
	prefix := []byte(PublicationSignatureDomain)
	channel := []byte(channelID.String())
	digest := publicationDigest.Bytes()
	message := make([]byte, 0, len(prefix)+2+len(channel)+len(digest))
	message = append(message, prefix...)
	message = append(message, 0)
	message = append(message, channel...)
	message = append(message, 0)
	return append(message, digest...), nil
}

// VerifyPublication authenticates the durable application signature. T0 Node
// identities use Ed25519 libp2p keys and MemberRecord stores the raw 32-byte
// public key; transport-level Gossip signing remains a separate proof.
func VerifyPublication(publicKey []byte, publication SignedPublication) error {
	return verifyPublicationSignature(publicKey, publication.Key().ChannelID(),
		publication.Digest(), publication.OriginSignature())
}

type PublicationBody struct {
	event     Event
	canonical JSON
	digest    Digest
}

// NewPublicationBody freezes the bytes and digest before a signing layer
// applies the R5 domain-separated origin signature.
func NewPublicationBody(event Event) (PublicationBody, error) {
	if event.ID().IsZero() {
		return PublicationBody{}, invalid("publication body", "Event is required")
	}
	scope := event.Scope()
	body, err := JSONFrom(struct {
		SchemaVersion int         `json:"schema_version"`
		ChannelID     ChannelID   `json:"channel_id"`
		OriginPeerID  PeerID      `json:"origin_peer_id"`
		OriginEpoch   OriginEpoch `json:"origin_epoch"`
		OriginSeq     uint64      `json:"origin_seq"`
		ChannelSeq    uint64      `json:"channel_seq"`
		EventID       EventID     `json:"event_id"`
		Event         Event       `json:"event"`
		EventDigest   Digest      `json:"event_digest"`
		OriginMember  RecordHead  `json:"origin_member"`
		RosterHead    RecordHead  `json:"publication_roster"`
		Audience      Audience    `json:"audience"`
	}{SchemaVersion, scope.ChannelID(), scope.OriginPeerID(), scope.OriginEpoch(), scope.OriginSequence(),
		scope.ChannelSequence(), event.ID(), event, event.Digest(), scope.OriginMember(),
		scope.PublicationRoster(), event.Audience()})
	if err != nil {
		return PublicationBody{}, err
	}
	return PublicationBody{event: event, canonical: body, digest: Sum(body.Bytes())}, nil
}

func (b PublicationBody) Event() Event        { return b.event }
func (b PublicationBody) Key() PublicationKey { key, _ := b.event.Scope().PublicationKey(); return key }
func (b PublicationBody) CanonicalJSON() JSON { return b.canonical }
func (b PublicationBody) Digest() Digest      { return b.digest }

type SignedPublication struct {
	body            PublicationBody
	originSignature string
	wire            JSON
}

// AttachSignature cannot run until the caller has signed PublicationBody's
// digest with the frozen R5 domain and ChannelID.
func AttachSignature(body PublicationBody, originSignature []byte) (SignedPublication, error) {
	if body.Event().ID().IsZero() || body.Digest().IsZero() || len(originSignature) != ed25519.SignatureSize {
		return SignedPublication{}, invalid("signed publication", "complete body and a 64-byte Ed25519 signature are required")
	}
	signature := string(append([]byte(nil), originSignature...))
	wire, err := JSONFrom(struct {
		Publication       JSON   `json:"publication"`
		PublicationDigest Digest `json:"publication_digest"`
		OriginSignature   []byte `json:"origin_signature"`
	}{body.CanonicalJSON(), body.Digest(), []byte(signature)})
	if err != nil {
		return SignedPublication{}, err
	}
	if len(wire.raw) > MaxPublicationBytes {
		return SignedPublication{}, limit("signed publication", len(wire.raw), MaxPublicationBytes)
	}
	return SignedPublication{body: body, originSignature: signature, wire: wire}, nil
}

func (p SignedPublication) Body() PublicationBody   { return p.body }
func (p SignedPublication) Event() Event            { return p.body.Event() }
func (p SignedPublication) Key() PublicationKey     { return p.body.Key() }
func (p SignedPublication) CanonicalJSON() JSON     { return p.body.CanonicalJSON() }
func (p SignedPublication) Digest() Digest          { return p.body.Digest() }
func (p SignedPublication) OriginSignature() []byte { return append([]byte(nil), p.originSignature...) }
func (p SignedPublication) WireJSON() JSON          { return p.wire }

type publicationRecordHeadWire struct {
	Digest   string `json:"digest"`
	Revision uint64 `json:"revision"`
}

type publicationWorkRefWire struct {
	HomePeerID string `json:"home_peer_id"`
	WorkID     string `json:"work_id"`
}

type publicationEventKeyWire struct {
	EventID      string `json:"event_id"`
	OriginEpoch  string `json:"origin_epoch"`
	OriginPeerID string `json:"origin_peer_id"`
}

type publicationArtifactRefWire struct {
	Role       ArtifactRole `json:"role"`
	RootDigest string       `json:"root_digest"`
}

type publicationEventWire struct {
	AcceptedAt        string                       `json:"accepted_at"`
	ActorPrincipal    string                       `json:"actor_principal"`
	ArtifactRoots     []publicationArtifactRefWire `json:"artifact_roots"`
	Audience          []string                     `json:"audience"`
	CausedBy          []publicationEventKeyWire    `json:"caused_by"`
	ChannelID         string                       `json:"channel_id"`
	ChannelSeq        uint64                       `json:"channel_seq"`
	CreatedAt         string                       `json:"created_at"`
	EventID           string                       `json:"event_id"`
	EventType         EventType                    `json:"event_type"`
	OriginEpoch       string                       `json:"origin_epoch"`
	OriginMember      publicationRecordHeadWire    `json:"origin_member"`
	OriginPeerID      string                       `json:"origin_peer_id"`
	OriginSeq         uint64                       `json:"origin_seq"`
	Payload           json.RawMessage              `json:"payload"`
	PublicationRoster publicationRecordHeadWire    `json:"publication_roster"`
	Resource          publicationWorkRefWire       `json:"resource"`
	SchemaVersion     int                          `json:"schema_version"`
	Summary           string                       `json:"summary"`
}

type publicationBodyWire struct {
	Audience          []string                  `json:"audience"`
	ChannelID         string                    `json:"channel_id"`
	ChannelSeq        uint64                    `json:"channel_seq"`
	Event             json.RawMessage           `json:"event"`
	EventDigest       string                    `json:"event_digest"`
	EventID           string                    `json:"event_id"`
	OriginEpoch       string                    `json:"origin_epoch"`
	OriginMember      publicationRecordHeadWire `json:"origin_member"`
	OriginPeerID      string                    `json:"origin_peer_id"`
	OriginSeq         uint64                    `json:"origin_seq"`
	PublicationRoster publicationRecordHeadWire `json:"publication_roster"`
	SchemaVersion     int                       `json:"schema_version"`
}

type signedPublicationWire struct {
	OriginSignature   []byte          `json:"origin_signature"`
	Publication       json.RawMessage `json:"publication"`
	PublicationDigest string          `json:"publication_digest"`
}

// PublicationEvidence is the bounded, authenticatable-header projection of one
// exact signed publication wire. It deliberately does not expose a generic
// envelope or partially decoded Event: a future or permanently unsupported
// publication can be retained as evidence without being admitted as a domain
// Event.
type PublicationEvidence struct {
	schemaVersion     uint64
	channelID         ChannelID
	originPeerID      PeerID
	originEpoch       OriginEpoch
	originSequence    uint64
	channelSequence   uint64
	eventID           EventID
	eventDigest       Digest
	originMember      RecordHead
	publicationRoster RecordHead
	audience          Audience
	publicationDigest Digest
	originSignature   string
	publicationBody   JSON
	wire              JSON
	supported         bool
}

func (e PublicationEvidence) SchemaVersion() uint64         { return e.schemaVersion }
func (e PublicationEvidence) ChannelID() ChannelID          { return e.channelID }
func (e PublicationEvidence) OriginPeerID() PeerID          { return e.originPeerID }
func (e PublicationEvidence) OriginEpoch() OriginEpoch      { return e.originEpoch }
func (e PublicationEvidence) OriginSequence() uint64        { return e.originSequence }
func (e PublicationEvidence) ChannelSequence() uint64       { return e.channelSequence }
func (e PublicationEvidence) EventID() EventID              { return e.eventID }
func (e PublicationEvidence) EventDigest() Digest           { return e.eventDigest }
func (e PublicationEvidence) OriginMember() RecordHead      { return e.originMember }
func (e PublicationEvidence) PublicationRoster() RecordHead { return e.publicationRoster }
func (e PublicationEvidence) Audience() Audience            { return e.audience }
func (e PublicationEvidence) Digest() Digest                { return e.publicationDigest }
func (e PublicationEvidence) OriginSignature() []byte {
	return append([]byte(nil), e.originSignature...)
}
func (e PublicationEvidence) WireJSON() JSON { return e.wire }
func (e PublicationEvidence) IsZero() bool {
	return e.wire.IsZero() || e.channelID.IsZero() || e.originPeerID.IsZero() ||
		e.originEpoch.IsZero() || e.eventID.IsZero() || e.publicationDigest.IsZero()
}

// IsSupported reports whether the complete wire passed the current strict
// SignedPublication decoder. Schema v1 with an unknown field, Event type or
// inconsistent embedded Event remains valid evidence but is not supported.
func (e PublicationEvidence) IsSupported() bool { return !e.IsZero() && e.supported }

type publicationEvidenceHeader struct {
	audience          []string
	channelID         string
	channelSequence   uint64
	eventDigest       string
	eventID           string
	originEpoch       string
	originMember      RecordHead
	originPeerID      string
	originSequence    uint64
	publicationRoster RecordHead
	schemaVersion     uint64
}

// ParsePublicationEvidence validates the exact outer signed-publication shape
// and the stable routing/identity header while allowing unknown fields inside
// the publication body. The claimed digest always covers the complete exact
// canonical body, including every unknown field.
func ParsePublicationEvidence(raw []byte) (PublicationEvidence, error) {
	evidence, err := parsePublicationEvidenceHeader(raw)
	if err != nil {
		return PublicationEvidence{}, err
	}
	_, strictErr := signedPublicationFromEvidence(evidence)
	evidence.supported = strictErr == nil
	return evidence, nil
}

func parsePublicationEvidenceHeader(raw []byte) (PublicationEvidence, error) {
	if len(raw) > MaxPublicationBytes {
		return PublicationEvidence{}, limit("publication evidence", len(raw), MaxPublicationBytes)
	}
	canonical, err := NewJSON(raw)
	if err != nil {
		return PublicationEvidence{}, fmt.Errorf("parse publication evidence: %w", err)
	}
	if !bytes.Equal(canonical.Bytes(), raw) {
		return PublicationEvidence{}, invalid("publication evidence", "must use exact canonical JSON")
	}

	wire, err := decodePublicationEvidenceOuter(raw)
	if err != nil {
		return PublicationEvidence{}, err
	}
	rebuiltOuter, err := JSONFrom(wire)
	if err != nil || !bytes.Equal(rebuiltOuter.Bytes(), raw) {
		return PublicationEvidence{}, invalid("publication evidence outer envelope", "must use the exact signed publication shape")
	}
	if len(wire.OriginSignature) != ed25519.SignatureSize {
		return PublicationEvidence{}, invalid("publication evidence signature", "must contain exactly 64 bytes")
	}
	body, err := NewJSON(wire.Publication)
	if err != nil || !bytes.Equal(body.Bytes(), wire.Publication) {
		return PublicationEvidence{}, invalid("publication evidence body", "must be exact canonical JSON")
	}
	header, err := decodePublicationEvidenceHeader(body.Bytes())
	if err != nil {
		return PublicationEvidence{}, err
	}
	if err := validateSQLitePositive("publication schema version", header.schemaVersion); err != nil {
		return PublicationEvidence{}, err
	}
	if err := validateSQLitePositive("publication origin sequence", header.originSequence); err != nil {
		return PublicationEvidence{}, err
	}
	if err := validateSQLitePositive("publication Channel sequence", header.channelSequence); err != nil {
		return PublicationEvidence{}, err
	}
	channelID, err := ParseChannelID(header.channelID)
	if err != nil {
		return PublicationEvidence{}, err
	}
	originPeerID, err := ParsePeerID(header.originPeerID)
	if err != nil {
		return PublicationEvidence{}, err
	}
	originEpoch, err := ParseOriginEpoch(header.originEpoch)
	if err != nil {
		return PublicationEvidence{}, err
	}
	eventID, err := ParseEventID(header.eventID)
	if err != nil {
		return PublicationEvidence{}, err
	}
	eventDigest, err := ParseDigest(header.eventDigest)
	if err != nil {
		return PublicationEvidence{}, err
	}
	audience, err := publicationEvidenceAudience(header.audience)
	if err != nil {
		return PublicationEvidence{}, err
	}
	if audience.Contains(originPeerID) {
		return PublicationEvidence{}, invalid("publication evidence audience", "must exclude its origin")
	}
	claimedDigest, err := ParseDigest(wire.PublicationDigest)
	if err != nil {
		return PublicationEvidence{}, err
	}
	if claimedDigest != Sum(body.Bytes()) {
		return PublicationEvidence{}, invariant("publication evidence digest does not cover its complete canonical body")
	}

	return PublicationEvidence{schemaVersion: header.schemaVersion, channelID: channelID,
		originPeerID: originPeerID, originEpoch: originEpoch, originSequence: header.originSequence,
		channelSequence: header.channelSequence, eventID: eventID, eventDigest: eventDigest,
		originMember: header.originMember, publicationRoster: header.publicationRoster, audience: audience,
		publicationDigest: claimedDigest, originSignature: string(wire.OriginSignature),
		publicationBody: body, wire: canonical}, nil
}

func decodePublicationEvidenceOuter(raw []byte) (signedPublicationWire, error) {
	fields, err := publicationEvidenceObject(raw, "outer envelope")
	if err != nil {
		return signedPublicationWire{}, err
	}
	if len(fields) != 3 {
		return signedPublicationWire{}, invalid("publication evidence outer envelope", "must contain exactly three fields")
	}
	var wire signedPublicationWire
	if err := publicationEvidenceField(fields, "origin_signature", &wire.OriginSignature); err != nil {
		return signedPublicationWire{}, err
	}
	if err := publicationEvidenceField(fields, "publication", &wire.Publication); err != nil {
		return signedPublicationWire{}, err
	}
	if err := publicationEvidenceField(fields, "publication_digest", &wire.PublicationDigest); err != nil {
		return signedPublicationWire{}, err
	}
	return wire, nil
}

func decodePublicationEvidenceHeader(raw []byte) (publicationEvidenceHeader, error) {
	fields, err := publicationEvidenceObject(raw, "body")
	if err != nil {
		return publicationEvidenceHeader{}, err
	}
	var header publicationEvidenceHeader
	for _, field := range []struct {
		name        string
		destination any
	}{
		{"audience", &header.audience},
		{"channel_id", &header.channelID},
		{"channel_seq", &header.channelSequence},
		{"event_digest", &header.eventDigest},
		{"event_id", &header.eventID},
		{"origin_epoch", &header.originEpoch},
		{"origin_peer_id", &header.originPeerID},
		{"origin_seq", &header.originSequence},
		{"schema_version", &header.schemaVersion},
	} {
		if err := publicationEvidenceField(fields, field.name, field.destination); err != nil {
			return publicationEvidenceHeader{}, err
		}
	}
	header.originMember, err = publicationEvidenceRecordHead(fields, "origin_member")
	if err != nil {
		return publicationEvidenceHeader{}, err
	}
	header.publicationRoster, err = publicationEvidenceRecordHead(fields, "publication_roster")
	if err != nil {
		return publicationEvidenceHeader{}, err
	}
	return header, nil
}

func publicationEvidenceRecordHead(fields map[string]json.RawMessage,
	name string,
) (RecordHead, error) {
	raw, ok := fields[name]
	if !ok {
		return RecordHead{}, invalid("publication evidence header", "missing exact "+name)
	}
	headFields, err := publicationEvidenceObject(raw, name)
	if err != nil {
		return RecordHead{}, err
	}
	var revision uint64
	var digestText string
	if err := publicationEvidenceField(headFields, "revision", &revision); err != nil {
		return RecordHead{}, err
	}
	if err := publicationEvidenceField(headFields, "digest", &digestText); err != nil {
		return RecordHead{}, err
	}
	digest, err := ParseDigest(digestText)
	if err != nil {
		return RecordHead{}, err
	}
	return NewRecordHead(revision, digest)
}

func publicationEvidenceObject(raw []byte, field string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, invalid("publication evidence "+field, "must be a canonical JSON object")
	}
	return object, nil
}

func publicationEvidenceField(fields map[string]json.RawMessage, name string,
	destination any,
) error {
	raw, ok := fields[name]
	if !ok {
		return invalid("publication evidence header", "missing exact "+name)
	}
	if err := json.Unmarshal(raw, destination); err != nil {
		return fmt.Errorf("parse publication evidence %s: %w", name, err)
	}
	return nil
}

func publicationEvidenceAudience(wire []string) (Audience, error) {
	peers := make([]PeerID, len(wire))
	for index, value := range wire {
		peerID, err := ParsePeerID(value)
		if err != nil {
			return Audience{}, err
		}
		peers[index] = peerID
	}
	audience, err := NewAudience(peers)
	if err != nil {
		return Audience{}, err
	}
	canonical := audience.Peers()
	for index := range peers {
		if peers[index] != canonical[index] {
			return Audience{}, invalid("publication evidence audience", "must use canonical PeerID order")
		}
	}
	return audience, nil
}

// VerifyPublicationEvidence authenticates even an unsupported body without
// interpreting it as a current Event. Callers must separately verify the
// origin member and publication roster heads before retaining peer evidence.
func VerifyPublicationEvidence(publicKey []byte, evidence PublicationEvidence) error {
	if evidence.IsZero() {
		return invalid("publication evidence", "complete evidence is required")
	}
	return verifyPublicationSignature(publicKey, evidence.ChannelID(), evidence.Digest(),
		evidence.OriginSignature())
}

func verifyPublicationSignature(publicKey []byte, channelID ChannelID, digest Digest,
	signature []byte,
) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return fmt.Errorf("publication signature: public key has %d bytes, want %d", len(publicKey), ed25519.PublicKeySize)
	}
	if len(signature) != ed25519.SignatureSize {
		return fmt.Errorf("publication signature: signature has %d bytes, want %d", len(signature), ed25519.SignatureSize)
	}
	message, err := PublicationSigningMessage(channelID, digest)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), message, signature) {
		return fmt.Errorf("publication signature: Ed25519 verification failed")
	}
	return nil
}

// ParseSignedPublication is the sole decoder for canonical bytes received by
// Gossip and direct Pull. It rebuilds every value through the domain
// constructors, so duplicated binding fields, collection order, digests and
// the exact schema-v1 shape cannot disagree with the embedded Event.
//
// The immutable origin Event is reconstructed with local source projection:
// source is deliberately absent from the wire identity. Inbox admission may
// later create an imported local projection without changing these bytes.
func ParseSignedPublication(raw []byte) (SignedPublication, error) {
	evidence, err := parsePublicationEvidenceHeader(raw)
	if err != nil {
		return SignedPublication{}, err
	}
	return signedPublicationFromEvidence(evidence)
}

func signedPublicationFromEvidence(evidence PublicationEvidence) (SignedPublication, error) {
	if evidence.SchemaVersion() != SchemaVersion {
		return SignedPublication{}, invalid("signed publication", "unsupported schema version")
	}
	var bodyWire publicationBodyWire
	if err := decodePublicationWire(evidence.publicationBody.Bytes(), &bodyWire); err != nil {
		return SignedPublication{}, err
	}
	if bodyWire.SchemaVersion != SchemaVersion {
		return SignedPublication{}, invalid("signed publication", "unsupported schema version")
	}

	event, err := eventFromPublicationWire(bodyWire.Event)
	if err != nil {
		return SignedPublication{}, err
	}
	body, err := NewPublicationBody(event)
	if err != nil {
		return SignedPublication{}, err
	}
	if evidence.Digest() != body.Digest() ||
		!bytes.Equal(body.CanonicalJSON().Bytes(), evidence.publicationBody.Bytes()) {
		return SignedPublication{}, invariant("publication body or digest differs from its embedded Event")
	}
	publication, err := AttachSignature(body, evidence.OriginSignature())
	if err != nil {
		return SignedPublication{}, err
	}
	if !bytes.Equal(publication.WireJSON().Bytes(), evidence.WireJSON().Bytes()) {
		return SignedPublication{}, invalid("signed publication", "omits, reorders or changes a schema-v1 field")
	}
	return publication, nil
}

func eventFromPublicationWire(raw []byte) (Event, error) {
	var wire publicationEventWire
	if err := decodePublicationWire(raw, &wire); err != nil {
		return Event{}, err
	}
	if wire.SchemaVersion != SchemaVersion {
		return Event{}, invalid("publication Event", "unsupported schema version")
	}
	channelID, err := ParseChannelID(wire.ChannelID)
	if err != nil {
		return Event{}, err
	}
	originPeerID, err := ParsePeerID(wire.OriginPeerID)
	if err != nil {
		return Event{}, err
	}
	originEpoch, err := ParseOriginEpoch(wire.OriginEpoch)
	if err != nil {
		return Event{}, err
	}
	originMember, err := recordHeadFromPublicationWire(wire.OriginMember)
	if err != nil {
		return Event{}, err
	}
	rosterHead, err := recordHeadFromPublicationWire(wire.PublicationRoster)
	if err != nil {
		return Event{}, err
	}
	workRef, err := workRefFromPublicationWire(wire.Resource)
	if err != nil {
		return Event{}, err
	}
	scope, err := NewEventScope(channelID, originPeerID, originEpoch, wire.OriginSeq,
		wire.ChannelSeq, originMember, rosterHead, workRef)
	if err != nil {
		return Event{}, err
	}
	eventID, err := ParseEventID(wire.EventID)
	if err != nil {
		return Event{}, err
	}
	audience, err := audienceFromPublicationWire(wire.Audience)
	if err != nil {
		return Event{}, err
	}
	payload, err := NewJSON(wire.Payload)
	if err != nil {
		return Event{}, err
	}
	artifacts, err := artifactRefsFromPublicationWire(wire.ArtifactRoots)
	if err != nil {
		return Event{}, err
	}
	causedBy, err := eventKeysFromPublicationWire(wire.CausedBy)
	if err != nil {
		return Event{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil {
		return Event{}, invalid("publication Event created_at", "must be RFC3339Nano")
	}
	acceptedAt, err := time.Parse(time.RFC3339Nano, wire.AcceptedAt)
	if err != nil {
		return Event{}, invalid("publication Event accepted_at", "must be RFC3339Nano")
	}
	event, err := NewEvent(EventSpec{ID: eventID, Scope: scope, Source: EventSourceLocal,
		ActorPrincipal: wire.ActorPrincipal, Type: wire.EventType, Audience: audience,
		Summary: wire.Summary, Payload: payload, Artifacts: artifacts, CausedBy: causedBy,
		CreatedAt: createdAt, AcceptedAt: acceptedAt})
	if err != nil {
		return Event{}, err
	}
	if !bytes.Equal(event.CanonicalJSON().Bytes(), raw) {
		return Event{}, invalid("publication Event", "omits, reorders or changes a schema-v1 field")
	}
	return event, nil
}

func decodePublicationWire(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("parse signed publication: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid("signed publication", "unexpected trailing JSON value")
	}
	return nil
}

func recordHeadFromPublicationWire(wire publicationRecordHeadWire) (RecordHead, error) {
	digest, err := ParseDigest(wire.Digest)
	if err != nil {
		return RecordHead{}, err
	}
	return NewRecordHead(wire.Revision, digest)
}

func workRefFromPublicationWire(wire publicationWorkRefWire) (WorkRef, error) {
	peerID, err := ParsePeerID(wire.HomePeerID)
	if err != nil {
		return WorkRef{}, err
	}
	workID, err := ParseWorkID(wire.WorkID)
	if err != nil {
		return WorkRef{}, err
	}
	return NewWorkRef(peerID, workID)
}

func audienceFromPublicationWire(wire []string) (Audience, error) {
	peers := make([]PeerID, len(wire))
	for index, value := range wire {
		peerID, err := ParsePeerID(value)
		if err != nil {
			return Audience{}, err
		}
		peers[index] = peerID
	}
	return NewAudience(peers)
}

func artifactRefsFromPublicationWire(wire []publicationArtifactRefWire) ([]ArtifactRef, error) {
	refs := make([]ArtifactRef, len(wire))
	for index, value := range wire {
		digest, err := ParseDigest(value.RootDigest)
		if err != nil {
			return nil, err
		}
		refs[index], err = NewArtifactRef(digest, value.Role)
		if err != nil {
			return nil, err
		}
	}
	return refs, nil
}

func eventKeysFromPublicationWire(wire []publicationEventKeyWire) ([]EventKey, error) {
	keys := make([]EventKey, len(wire))
	for index, value := range wire {
		peerID, err := ParsePeerID(value.OriginPeerID)
		if err != nil {
			return nil, err
		}
		epoch, err := ParseOriginEpoch(value.OriginEpoch)
		if err != nil {
			return nil, err
		}
		eventID, err := ParseEventID(value.EventID)
		if err != nil {
			return nil, err
		}
		keys[index], err = NewEventKey(peerID, epoch, eventID)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

type PublicationStatus string

const (
	PublicationQueued    PublicationStatus = "queued"
	PublicationLeased    PublicationStatus = "leased"
	PublicationPublished PublicationStatus = "published"
	PublicationBlocked   PublicationStatus = "blocked"
	PublicationAbandoned PublicationStatus = "abandoned"
)

func (s PublicationStatus) Valid() bool {
	return s == PublicationQueued || s == PublicationLeased || s == PublicationPublished ||
		s == PublicationBlocked || s == PublicationAbandoned
}

type PublicationRecordSpec struct {
	Publication   SignedPublication
	Status        PublicationStatus
	Attempts      uint32
	NextAttemptAt time.Time
	LeaseOwner    string
	LeaseUntil    *time.Time
	PublishedAt   *time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type PublicationRecord struct {
	spec         PublicationRecordSpec
	leaseUntil   time.Time
	hasLease     bool
	publishedAt  time.Time
	hasPublished bool
}

func NewPublicationRecord(spec PublicationRecordSpec) (PublicationRecord, error) {
	if spec.Publication.Event().ID().IsZero() || spec.Publication.Event().Source() != EventSourceLocal {
		return PublicationRecord{}, invariant("publication queue accepts only local-origin accepted Events")
	}
	if !spec.Status.Valid() {
		return PublicationRecord{}, invalid("publication status", "unknown closed enum value")
	}
	if err := validateText("publication last error", spec.LastError, MaxContentBytes, true); err != nil {
		return PublicationRecord{}, err
	}
	nextAttempt, err := canonicalTime(spec.NextAttemptAt)
	if err != nil {
		return PublicationRecord{}, err
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return PublicationRecord{}, err
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return PublicationRecord{}, err
	}
	if updatedAt.Before(createdAt) {
		return PublicationRecord{}, invariant("publication update precedes creation")
	}
	result := PublicationRecord{spec: spec}
	result.spec.NextAttemptAt, result.spec.CreatedAt, result.spec.UpdatedAt = nextAttempt, createdAt, updatedAt
	result.spec.LeaseUntil, result.spec.PublishedAt = nil, nil
	if spec.Status == PublicationLeased {
		if spec.LeaseOwner == "" || spec.LeaseUntil == nil {
			return PublicationRecord{}, invariant("leased publication requires owner and deadline")
		}
		if err := validateIdentifier("publication lease owner", spec.LeaseOwner); err != nil {
			return PublicationRecord{}, err
		}
		leaseUntil, err := canonicalTime(*spec.LeaseUntil)
		if err != nil {
			return PublicationRecord{}, err
		}
		if !leaseUntil.After(updatedAt) {
			return PublicationRecord{}, invariant("publication lease must end after update time")
		}
		result.leaseUntil, result.hasLease = leaseUntil, true
	} else if spec.LeaseOwner != "" || spec.LeaseUntil != nil {
		return PublicationRecord{}, invariant("non-leased publication cannot retain lease authority")
	}
	if spec.Status == PublicationPublished {
		if spec.PublishedAt == nil {
			return PublicationRecord{}, invariant("published publication requires published_at")
		}
		publishedAt, err := canonicalTime(*spec.PublishedAt)
		if err != nil {
			return PublicationRecord{}, err
		}
		if publishedAt.Before(createdAt) || updatedAt.Before(publishedAt) {
			return PublicationRecord{}, invariant("published_at must fall between creation and update")
		}
		result.publishedAt, result.hasPublished = publishedAt, true
	} else if spec.PublishedAt != nil {
		return PublicationRecord{}, invariant("only published publication may carry published_at")
	}
	return result, nil
}

func (r PublicationRecord) Publication() SignedPublication { return r.spec.Publication }
func (r PublicationRecord) Status() PublicationStatus      { return r.spec.Status }
func (r PublicationRecord) Attempts() uint32               { return r.spec.Attempts }
func (r PublicationRecord) NextAttemptAt() time.Time       { return r.spec.NextAttemptAt }
func (r PublicationRecord) LeaseOwner() string             { return r.spec.LeaseOwner }
func (r PublicationRecord) LeaseUntil() (time.Time, bool)  { return r.leaseUntil, r.hasLease }
func (r PublicationRecord) PublishedAt() (time.Time, bool) { return r.publishedAt, r.hasPublished }
func (r PublicationRecord) LastError() string              { return r.spec.LastError }
func (r PublicationRecord) CreatedAt() time.Time           { return r.spec.CreatedAt }
func (r PublicationRecord) UpdatedAt() time.Time           { return r.spec.UpdatedAt }
