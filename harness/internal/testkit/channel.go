package testkit

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const channelFixtureTimeLayout = time.RFC3339Nano

var channelFixtureEpoch = time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)

// Identity is a deterministic Ed25519 identity suitable for both model and
// live libp2p tests. Its signing bytes and mutable projections remain private;
// callers receive defensive copies through the accessors below.
type Identity struct {
	peerID      model.PeerID
	originEpoch model.OriginEpoch
	displayName string
	publicKey   string
	privateKey  string
	multiaddrs  []string
}

// NewIdentity creates the same identity for the same non-empty seed in every
// test process. A shared seed can deliberately represent one node in multiple
// Channel fixtures.
func NewIdentity(t testing.TB, seed string) Identity {
	t.Helper()
	identity, err := newIdentity(seed)
	if err != nil {
		t.Fatalf("create deterministic R5 identity: %v", err)
	}
	return identity
}

func newIdentity(seed string) (Identity, error) {
	if seed == "" {
		return Identity{}, fmt.Errorf("identity seed is empty")
	}
	seedDigest := sha256.Sum256([]byte("mnemon/r5/testkit/identity\x00" + seed))
	privateKey := ed25519.NewKeyFromSeed(seedDigest[:])
	publicKey := privateKey.Public().(ed25519.PublicKey)
	libp2pPublicKey, err := libp2pcrypto.UnmarshalEd25519PublicKey(publicKey)
	if err != nil {
		return Identity{}, fmt.Errorf("decode public key: %w", err)
	}
	libp2pID, err := libp2ppeer.IDFromPublicKey(libp2pPublicKey)
	if err != nil {
		return Identity{}, fmt.Errorf("derive libp2p PeerID: %w", err)
	}
	peerID, err := model.ParsePeerID(libp2pID.String())
	if err != nil {
		return Identity{}, fmt.Errorf("parse model PeerID: %w", err)
	}
	epochDigest := sha256.Sum256([]byte("mnemon/r5/testkit/origin-epoch\x00" + seed))
	originEpoch, err := model.ParseOriginEpoch("epoch-" + hex.EncodeToString(epochDigest[:8]))
	if err != nil {
		return Identity{}, fmt.Errorf("parse origin epoch: %w", err)
	}
	port := 20000 + int(uint16(seedDigest[30])<<8|uint16(seedDigest[31]))%20000
	return Identity{
		peerID:      peerID,
		originEpoch: originEpoch,
		displayName: "peer-" + hex.EncodeToString(seedDigest[:4]),
		publicKey:   string(append([]byte(nil), publicKey...)),
		privateKey:  string(append([]byte(nil), privateKey...)),
		multiaddrs:  []string{fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)},
	}, nil
}

func (identity Identity) PeerID() model.PeerID           { return identity.peerID }
func (identity Identity) OriginEpoch() model.OriginEpoch { return identity.originEpoch }
func (identity Identity) DisplayName() string            { return identity.displayName }
func (identity Identity) PublicKey() []byte              { return append([]byte(nil), identity.publicKey...) }
func (identity Identity) Multiaddrs() []string {
	return append([]string(nil), identity.multiaddrs...)
}

// Libp2pPrivateKey returns a decoded copy for constructing a real libp2p host.
func (identity Identity) Libp2pPrivateKey() (libp2pcrypto.PrivKey, error) {
	if identity.IsZero() {
		return nil, fmt.Errorf("identity is incomplete")
	}
	return libp2pcrypto.UnmarshalEd25519PrivateKey([]byte(identity.privateKey))
}

func (identity Identity) IsZero() bool {
	return identity.peerID.IsZero() || identity.originEpoch.IsZero() || identity.publicKey == "" ||
		identity.privateKey == "" || len(identity.multiaddrs) == 0
}

// MemberFixture couples an identity with one immutable, owner-signed roster
// record. Successive records for the same identity produce distinct fixtures.
type MemberFixture struct {
	identity Identity
	member   model.Member
}

func (fixture MemberFixture) Identity() Identity   { return fixture.identity }
func (fixture MemberFixture) Member() model.Member { return fixture.member }

// MemberProjection is the exact relational projection used by the
// channel_members persistence boundary. Every byte slice is caller-owned.
type MemberProjection struct {
	ChannelID        string
	Revision         uint64
	RecordHash       []byte
	PreviousHash     []byte
	MemberPeerID     string
	OriginEpoch      string
	DisplayLabel     string
	PublicKey        []byte
	MultiaddrsJSON   []byte
	ProtocolsJSON    []byte
	LimitsJSON       []byte
	Status           string
	SignedRecordJSON []byte
	OwnerSignature   []byte
	WireJSON         []byte
	CreatedAt        string
}

func (fixture MemberFixture) Projection() MemberProjection {
	member := fixture.member
	previous, hasPrevious := member.PreviousDigest()
	var previousBytes []byte
	if hasPrevious {
		previousBytes = previous.Bytes()
	}
	return MemberProjection{
		ChannelID:        member.ChannelID().String(),
		Revision:         member.Head().Revision(),
		RecordHash:       member.Head().Digest().Bytes(),
		PreviousHash:     previousBytes,
		MemberPeerID:     member.PeerID().String(),
		OriginEpoch:      member.OriginEpoch().String(),
		DisplayLabel:     member.DisplayLabel(),
		PublicKey:        member.PublicKey(),
		MultiaddrsJSON:   mustCanonicalFixtureJSON(member.Multiaddrs()),
		ProtocolsJSON:    mustCanonicalFixtureJSON(member.Protocols()),
		LimitsJSON:       member.Limits().Bytes(),
		Status:           string(member.Status()),
		SignedRecordJSON: member.SignedRecord().Bytes(),
		OwnerSignature:   member.OwnerSignature(),
		WireJSON:         member.WireJSON().Bytes(),
		CreatedAt:        member.CreatedAt().UTC().Format(channelFixtureTimeLayout),
	}
}

// ChannelProjection is the exact relational projection used by the channels
// persistence boundary. DescriptorJSON is the signed descriptor body; the
// signature is deliberately stored in its own evidence column.
type ChannelProjection struct {
	ChannelID           string
	Name                string
	LocalAlias          string
	OwnerPeerID         string
	OwnerPublicKey      []byte
	DescriptorJSON      []byte
	DescriptorDigest    []byte
	DescriptorSignature []byte
	DescriptorWireJSON  []byte
	MemberLimit         uint8
	RosterHeadRevision  uint64
	RosterHeadHash      []byte
	Status              string
	TopicState          string
	CreatedAt           string
	UpdatedAt           string
}

// SignedChannel owns the only roster signing key and exposes only model-
// verified authority values. Append methods validate the complete candidate
// roster before making it visible.
type SignedChannel struct {
	owner      Identity
	descriptor model.SignedChannelDescriptor
	channel    model.Channel
	roster     model.VerifiedRoster
	members    []MemberFixture
	identities map[model.PeerID]Identity
}

// NewSignedChannel creates a deterministic active Channel with its valid
// owner genesis record. The seed controls Channel ID, alias and owner identity.
func NewSignedChannel(t testing.TB, seed string) *SignedChannel {
	t.Helper()
	return NewSignedChannelAt(t, seed, channelFixtureEpoch)
}

// NewSignedChannelAt creates the same fixture at a caller-selected canonical
// time so Store transaction scenarios can share real authority.
func NewSignedChannelAt(t testing.TB, seed string, createdAt time.Time) *SignedChannel {
	t.Helper()
	owner := NewIdentity(t, "owner:"+seed)
	return NewSignedChannelForOwnerAt(t, seed, owner, createdAt)
}

// NewSignedChannelForOwnerAt creates an independent Channel for an existing
// Node identity, which is useful for overlapping multi-Channel scenarios.
func NewSignedChannelForOwnerAt(t testing.TB, seed string, owner Identity,
	createdAt time.Time,
) *SignedChannel {
	t.Helper()
	fixture, err := newSignedChannel(seed, owner, createdAt)
	if err != nil {
		t.Fatalf("create signed R5 Channel fixture: %v", err)
	}
	return fixture
}

func newSignedChannel(seed string, owner Identity, createdAt time.Time) (*SignedChannel, error) {
	if seed == "" || owner.IsZero() {
		return nil, fmt.Errorf("Channel seed and owner are required")
	}
	seedDigest := sha256.Sum256([]byte("mnemon/r5/testkit/channel\x00" + seed))
	channelID, err := model.ParseChannelID("channel-" + hex.EncodeToString(seedDigest[:8]))
	if err != nil {
		return nil, err
	}
	descriptor, err := model.NewChannelDescriptor(model.ChannelDescriptorSpec{
		ID:             channelID,
		Name:           "Channel " + hex.EncodeToString(seedDigest[:4]),
		OwnerPeerID:    owner.PeerID(),
		OwnerPublicKey: owner.PublicKey(),
		MemberLimit:    model.MaxMembersPerChannel,
		CreatedAt:      createdAt,
	})
	if err != nil {
		return nil, err
	}
	descriptorMessage, err := model.ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	if err != nil {
		return nil, err
	}
	signedDescriptor, err := model.AttachChannelDescriptorSignature(descriptor,
		ed25519.Sign(ed25519.PrivateKey(owner.privateKey), descriptorMessage))
	if err != nil {
		return nil, err
	}
	fixture := &SignedChannel{
		owner:      owner,
		descriptor: signedDescriptor,
		identities: map[model.PeerID]Identity{owner.PeerID(): owner},
	}
	genesis, roster, channel, err := fixture.buildAppend(owner, model.MemberActive)
	if err != nil {
		return nil, err
	}
	fixture.members = []MemberFixture{genesis}
	fixture.roster = roster
	fixture.channel = channel
	return fixture, nil
}

// AppendActiveUpdate appends a new current active record for address/label
// refresh while preserving the member's signed identity and epoch.
func (fixture *SignedChannel) AppendActiveUpdate(t testing.TB, peerID model.PeerID) MemberFixture {
	t.Helper()
	identity, exists := fixture.identities[peerID]
	current, currentExists := fixture.roster.CurrentMember(peerID)
	if !exists || !currentExists || current.Status() != model.MemberActive {
		t.Fatalf("append active update: peer %s is not currently active", peerID.String())
	}
	member, roster, channel, err := fixture.buildAppend(identity, model.MemberActive)
	if err != nil {
		t.Fatalf("append active update: %v", err)
	}
	fixture.members = append(fixture.members, member)
	fixture.roster, fixture.channel = roster, channel
	return member
}

func (fixture *SignedChannel) Owner() Identity { return fixture.owner }
func (fixture *SignedChannel) Descriptor() model.SignedChannelDescriptor {
	return fixture.descriptor
}
func (fixture *SignedChannel) Channel() model.Channel       { return fixture.channel }
func (fixture *SignedChannel) Roster() model.VerifiedRoster { return fixture.roster }
func (fixture *SignedChannel) OwnerMember() MemberFixture   { return fixture.members[0] }
func (fixture *SignedChannel) Members() []MemberFixture {
	return append([]MemberFixture(nil), fixture.members...)
}

// AppendActive creates a deterministic identity and appends its first active
// authority record.
func (fixture *SignedChannel) AppendActive(t testing.TB, seed string) MemberFixture {
	t.Helper()
	return fixture.AppendActiveIdentity(t, NewIdentity(t, seed))
}

// AppendActiveIdentity permits the same deterministic node to participate in
// overlapping Channel fixtures.
func (fixture *SignedChannel) AppendActiveIdentity(t testing.TB, identity Identity) MemberFixture {
	t.Helper()
	member, err := fixture.appendActive(identity)
	if err != nil {
		t.Fatalf("append active R5 member: %v", err)
	}
	return member
}

func (fixture *SignedChannel) appendActive(identity Identity) (MemberFixture, error) {
	if fixture == nil || identity.IsZero() {
		return MemberFixture{}, fmt.Errorf("Channel fixture and identity are required")
	}
	if _, exists := fixture.identities[identity.PeerID()]; exists {
		return MemberFixture{}, fmt.Errorf("peer %s already has Channel authority", identity.PeerID().String())
	}
	member, roster, channel, err := fixture.buildAppend(identity, model.MemberActive)
	if err != nil {
		return MemberFixture{}, err
	}
	fixture.members = append(fixture.members, member)
	fixture.identities[identity.PeerID()] = identity
	fixture.roster = roster
	fixture.channel = channel
	return member, nil
}

// AppendTerminal appends a left or revoked record for a currently active peer.
func (fixture *SignedChannel) AppendTerminal(t testing.TB, peerID model.PeerID,
	status model.MemberStatus,
) MemberFixture {
	t.Helper()
	member, err := fixture.appendTerminal(peerID, status)
	if err != nil {
		t.Fatalf("append terminal R5 member: %v", err)
	}
	return member
}

func (fixture *SignedChannel) appendTerminal(peerID model.PeerID,
	status model.MemberStatus,
) (MemberFixture, error) {
	if fixture == nil || peerID.IsZero() || !status.Terminal() {
		return MemberFixture{}, fmt.Errorf("Channel fixture, peer and terminal member status are required")
	}
	identity, exists := fixture.identities[peerID]
	current, currentExists := fixture.roster.CurrentMember(peerID)
	if !exists || !currentExists || current.Status() != model.MemberActive {
		return MemberFixture{}, fmt.Errorf("peer %s is not currently active", peerID.String())
	}
	member, roster, channel, err := fixture.buildAppend(identity, status)
	if err != nil {
		return MemberFixture{}, err
	}
	fixture.members = append(fixture.members, member)
	fixture.roster = roster
	fixture.channel = channel
	return member, nil
}

func (fixture *SignedChannel) buildAppend(identity Identity,
	status model.MemberStatus,
) (MemberFixture, model.VerifiedRoster, model.Channel, error) {
	revision := uint64(len(fixture.members) + 1)
	var previous *model.Digest
	if len(fixture.members) > 0 {
		digest := fixture.roster.Head().Digest()
		previous = &digest
	}
	createdAt := fixture.descriptor.Descriptor().CreatedAt().Add(time.Duration(revision-1) * time.Second)
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID:        fixture.descriptor.Descriptor().ID(),
		DescriptorDigest: fixture.descriptor.Descriptor().Digest(),
		Revision:         revision,
		PreviousDigest:   previous,
		PeerID:           identity.PeerID(),
		OriginEpoch:      identity.OriginEpoch(),
		DisplayLabel:     identity.DisplayName(),
		PublicKey:        identity.PublicKey(),
		Multiaddrs:       identity.Multiaddrs(),
		Protocols:        ChannelProtocols(),
		Limits:           model.DefaultMemberLimits(),
		Status:           status,
		CreatedAt:        createdAt,
	})
	if err != nil {
		return MemberFixture{}, model.VerifiedRoster{}, model.Channel{}, err
	}
	message, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		return MemberFixture{}, model.VerifiedRoster{}, model.Channel{}, err
	}
	member, err := model.AttachMemberSignature(record,
		ed25519.Sign(ed25519.PrivateKey(fixture.owner.privateKey), message))
	if err != nil {
		return MemberFixture{}, model.VerifiedRoster{}, model.Channel{}, err
	}
	candidate := make([]model.Member, 0, len(fixture.members)+1)
	for _, existing := range fixture.members {
		candidate = append(candidate, existing.Member())
	}
	candidate = append(candidate, member)
	roster, err := model.NewVerifiedRoster(fixture.descriptor, candidate)
	if err != nil {
		return MemberFixture{}, model.VerifiedRoster{}, model.Channel{}, err
	}
	channel, err := model.NewChannel(model.ChannelSpec{
		Descriptor: fixture.descriptor,
		LocalAlias: "channel-" + hex.EncodeToString(fixture.descriptor.Descriptor().Digest().Bytes()[:4]),
		RosterHead: roster.Head(),
		Status:     model.ChannelActive,
		TopicState: model.TopicNotJoined,
		UpdatedAt:  createdAt,
	})
	if err != nil {
		return MemberFixture{}, model.VerifiedRoster{}, model.Channel{}, err
	}
	return MemberFixture{identity: identity, member: member}, roster, channel, nil
}

// ChannelProtocols returns the closed, canonical R5 T0 direct protocol set.
func ChannelProtocols() []string {
	return []string{"/mnemon/artifacts/1", "/mnemon/channel/1", "/mnemon/events/1"}
}

func (fixture *SignedChannel) Projection() ChannelProjection {
	descriptor := fixture.descriptor.Descriptor()
	channel := fixture.channel
	return ChannelProjection{
		ChannelID:           channel.ID().String(),
		Name:                channel.Name(),
		LocalAlias:          channel.LocalAlias(),
		OwnerPeerID:         channel.OwnerPeerID().String(),
		OwnerPublicKey:      channel.OwnerPublicKey(),
		DescriptorJSON:      descriptor.CanonicalJSON().Bytes(),
		DescriptorDigest:    descriptor.Digest().Bytes(),
		DescriptorSignature: fixture.descriptor.OwnerSignature(),
		DescriptorWireJSON:  fixture.descriptor.WireJSON().Bytes(),
		MemberLimit:         channel.MemberLimit(),
		RosterHeadRevision:  channel.RosterHead().Revision(),
		RosterHeadHash:      channel.RosterHead().Digest().Bytes(),
		Status:              string(channel.Status()),
		TopicState:          string(channel.TopicState()),
		CreatedAt:           channel.CreatedAt().UTC().Format(channelFixtureTimeLayout),
		UpdatedAt:           channel.UpdatedAt().UTC().Format(channelFixtureTimeLayout),
	}
}

func (fixture *SignedChannel) MemberProjections() []MemberProjection {
	projections := make([]MemberProjection, len(fixture.members))
	for index, member := range fixture.members {
		projections[index] = member.Projection()
	}
	return projections
}

func mustCanonicalFixtureJSON(value any) []byte {
	canonical, err := model.CanonicalMarshal(value)
	if err != nil {
		panic(fmt.Sprintf("canonicalize verified Channel fixture value: %v", err))
	}
	return canonical
}
