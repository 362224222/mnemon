package testkit

import (
	"bytes"
	"testing"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestSignedChannelAcceptsCanonicalCreationTime(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 7, 16, 12, 0, 0, 123, time.UTC)
	fixture := NewSignedChannelAt(t, "selected-time", createdAt)
	if !fixture.Channel().CreatedAt().Equal(createdAt) ||
		!fixture.OwnerMember().Member().CreatedAt().Equal(createdAt) {
		t.Fatal("selected creation time did not reach descriptor and genesis")
	}
}

func TestSignedChannelReusesOwnerAndAppendsActiveUpdate(t *testing.T) {
	t.Parallel()
	owner := NewIdentity(t, "shared-owner")
	createdAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	first := NewSignedChannelForOwnerAt(t, "shared-first", owner, createdAt)
	second := NewSignedChannelForOwnerAt(t, "shared-second", owner, createdAt)
	if first.Owner().PeerID() != second.Owner().PeerID() || first.Channel().ID() == second.Channel().ID() {
		t.Fatal("shared owner did not produce independent Channels")
	}
	member := first.AppendActive(t, "update-member")
	updated := first.AppendActiveUpdate(t, member.Identity().PeerID())
	current, ok := first.Roster().CurrentMember(member.Identity().PeerID())
	if !ok || current.Head() != updated.Member().Head() || current.Status() != model.MemberActive {
		t.Fatal("active update did not become current authority")
	}
}

func TestIdentityIsDeterministicLibp2pIdentityWithDefensiveCopies(t *testing.T) {
	t.Parallel()
	first := NewIdentity(t, "shared-node")
	second := NewIdentity(t, "shared-node")
	other := NewIdentity(t, "other-node")
	if first.PeerID() != second.PeerID() || first.OriginEpoch() != second.OriginEpoch() ||
		!bytes.Equal(first.PublicKey(), second.PublicKey()) || first.PeerID() == other.PeerID() {
		t.Fatal("deterministic identity derivation is unstable or collided")
	}
	privateKey, err := first.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	derived, err := libp2ppeer.IDFromPrivateKey(privateKey)
	if err != nil || derived.String() != first.PeerID().String() {
		t.Fatalf("libp2p private key derives %q: %v", derived, err)
	}
	publicKey := first.PublicKey()
	addresses := first.Multiaddrs()
	publicKey[0] ^= 0xff
	addresses[0] = "changed"
	if bytes.Equal(first.PublicKey(), publicKey) || first.Multiaddrs()[0] == "changed" {
		t.Fatal("identity exposed mutable key or address storage")
	}
}

func TestSignedChannelBuildsVerifiedGenesisAndPersistenceProjections(t *testing.T) {
	t.Parallel()
	fixture := NewSignedChannel(t, "projection")
	if err := model.VerifyChannelDescriptor(fixture.Descriptor()); err != nil {
		t.Fatal(err)
	}
	if fixture.Channel().OwnerPeerID() != fixture.Owner().PeerID() ||
		fixture.Roster().Head() != fixture.OwnerMember().Member().Head() ||
		fixture.Roster().Head() != fixture.Channel().RosterHead() {
		t.Fatal("Channel, descriptor and genesis roster authority diverged")
	}
	owner, ok := fixture.Roster().CurrentMember(fixture.Owner().PeerID())
	if !ok || owner.Status() != model.MemberActive || owner.Head().Revision() != 1 {
		t.Fatal("owner genesis is not current active authority")
	}
	projection := fixture.Projection()
	parsedDescriptor, err := model.ParseChannelDescriptor(projection.DescriptorJSON)
	if err != nil {
		t.Fatal(err)
	}
	attached, err := model.AttachChannelDescriptorSignature(parsedDescriptor, projection.DescriptorSignature)
	if err != nil || !bytes.Equal(attached.WireJSON().Bytes(), projection.DescriptorWireJSON) {
		t.Fatalf("descriptor projection does not reconstruct evidence: %v", err)
	}
	if !bytes.Equal(projection.DescriptorDigest, parsedDescriptor.Digest().Bytes()) ||
		projection.RosterHeadRevision != 1 ||
		!bytes.Equal(projection.RosterHeadHash, owner.Head().Digest().Bytes()) {
		t.Fatal("Channel projection does not match signed authority")
	}
	members := fixture.MemberProjections()
	if len(members) != 1 || len(members[0].PreviousHash) != 0 ||
		!bytes.Equal(members[0].SignedRecordJSON, owner.SignedRecord().Bytes()) ||
		!bytes.Equal(members[0].WireJSON, owner.WireJSON().Bytes()) ||
		string(members[0].ProtocolsJSON) != `["/mnemon/artifacts/1","/mnemon/channel/1","/mnemon/events/1"]` ||
		string(members[0].LimitsJSON) != `{"profile":"r5-hermetic-v1"}` {
		t.Fatalf("genesis persistence projection = %#v", members[0])
	}

	projection.OwnerPublicKey[0] ^= 0xff
	projection.DescriptorJSON[0] ^= 0xff
	projection.DescriptorSignature[0] ^= 0xff
	members[0].PublicKey[0] ^= 0xff
	members[0].SignedRecordJSON[0] ^= 0xff
	freshChannel := fixture.Projection()
	freshMember := fixture.MemberProjections()[0]
	if bytes.Equal(projection.OwnerPublicKey, freshChannel.OwnerPublicKey) ||
		bytes.Equal(projection.DescriptorJSON, freshChannel.DescriptorJSON) ||
		bytes.Equal(projection.DescriptorSignature, freshChannel.DescriptorSignature) ||
		bytes.Equal(members[0].PublicKey, freshMember.PublicKey) ||
		bytes.Equal(members[0].SignedRecordJSON, freshMember.SignedRecordJSON) {
		t.Fatal("persistence projections mutated fixture authority")
	}
}

func TestSignedChannelAppendsContinuousActiveAndTerminalAuthority(t *testing.T) {
	t.Parallel()
	fixture := NewSignedChannel(t, "lifecycle")
	shared := NewIdentity(t, "shared-across-channels")
	active := fixture.AppendActiveIdentity(t, shared)
	second := fixture.AppendActive(t, "second-member")
	terminal := fixture.AppendTerminal(t, shared.PeerID(), model.MemberRevoked)

	if active.Member().Head().Revision() != 2 || second.Member().Head().Revision() != 3 ||
		terminal.Member().Head().Revision() != 4 || fixture.Roster().Head() != terminal.Member().Head() ||
		fixture.Channel().RosterHead() != terminal.Member().Head() {
		t.Fatal("append sequence did not advance one continuous committed head")
	}
	previous, ok := terminal.Member().PreviousDigest()
	if !ok || previous != second.Member().Head().Digest() {
		t.Fatal("terminal record does not extend the previous global roster head")
	}
	current, ok := fixture.Roster().CurrentMember(shared.PeerID())
	if !ok || current.Status() != model.MemberRevoked || current.Head() != terminal.Member().Head() {
		t.Fatal("terminal authority is not the peer's current roster record")
	}
	projections := fixture.MemberProjections()
	if len(projections) != 4 || !bytes.Equal(projections[3].PreviousHash, projections[2].RecordHash) ||
		projections[3].Status != string(model.MemberRevoked) ||
		projections[1].MemberPeerID != shared.PeerID().String() {
		t.Fatalf("member projections do not preserve lifecycle chain: %#v", projections)
	}

	overlap := NewSignedChannel(t, "overlap")
	overlapMember := overlap.AppendActiveIdentity(t, shared)
	if overlapMember.Identity().PeerID() != active.Identity().PeerID() ||
		overlapMember.Member().ChannelID() == active.Member().ChannelID() {
		t.Fatal("shared deterministic identity did not form overlapping Channel membership")
	}
}

func TestSignedChannelRejectsLifecycleMutationWithoutChangingHead(t *testing.T) {
	t.Parallel()
	fixture := NewSignedChannel(t, "failed-append")
	member := NewIdentity(t, "failed-member")
	fixture.AppendActiveIdentity(t, member)
	head := fixture.Roster().Head()
	count := len(fixture.Members())
	if _, err := fixture.appendActive(member); err == nil {
		t.Fatal("duplicate active authority was accepted")
	}
	if _, err := fixture.appendTerminal(member.PeerID(), model.MemberActive); err == nil {
		t.Fatal("nonterminal status was accepted by terminal append")
	}
	fixture.AppendTerminal(t, member.PeerID(), model.MemberLeft)
	terminalHead := fixture.Roster().Head()
	terminalCount := len(fixture.Members())
	if _, err := fixture.appendTerminal(member.PeerID(), model.MemberRevoked); err == nil {
		t.Fatal("terminal authority was extended")
	}
	if terminalHead != fixture.Roster().Head() || terminalCount != len(fixture.Members()) {
		t.Fatal("failed terminal append partially mutated fixture")
	}
	if head == fixture.Roster().Head() || count == len(fixture.Members()) {
		t.Fatal("valid terminal append did not mutate fixture")
	}
}
