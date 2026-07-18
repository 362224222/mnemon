package model

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

func TestPeerBindingDerivesAuthorityFromVerifiedMember(t *testing.T) {
	t.Parallel()
	local, _, _ := canonicalDescriptorIdentity(t, "binding-local")
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "binding-owner")
	remote, remoteKey, _ := canonicalDescriptorIdentity(t, "binding-remote")
	channel, _ := ParseChannelID("channel-binding")
	descriptor := signedRecordDescriptor(t, channel, owner, ownerKey, ownerPrivate)
	ownerEpoch, _ := ParseOriginEpoch("epoch-binding-owner")
	epoch, _ := ParseOriginEpoch("epoch-binding")
	limits := DefaultMemberLimits()
	now := descriptor.Descriptor().CreatedAt().Add(time.Second)
	genesis := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channel,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: owner,
		OriginEpoch: ownerEpoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"}, Protocols: requiredMemberProtocols,
		Limits: limits, Status: MemberActive, CreatedAt: descriptor.Descriptor().CreatedAt()})
	previous := genesis.Head().Digest()
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: channel,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: remote, OriginEpoch: epoch, DisplayLabel: "reviewer", PublicKey: remoteKey,
		Multiaddrs: []string{"/ip4/127.0.0.2/tcp/4001", "/ip4/127.0.0.1/tcp/4001"},
		Protocols:  []string{"/mnemon/events/1", "/mnemon/channel/1", "/mnemon/artifacts/1"},
		Limits:     limits, Status: MemberActive, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := MemberRecordSigningMessage(channel, record.Digest())
	member, _ := AttachMemberSignature(record, ed25519.Sign(ownerPrivate, message))
	roster, err := NewVerifiedRoster(descriptor, []Member{genesis, member})
	if err != nil {
		t.Fatal(err)
	}
	channelProjection, err := NewChannel(ChannelSpec{Descriptor: descriptor, LocalAlias: "binding",
		RosterHead: roster.Head(), Status: ChannelActive, TopicState: TopicJoined, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	spec := PeerBindingSpec{Channel: channelProjection, Roster: roster, PeerID: remote, EffectiveAlias: "reviewer",
		State: BindingPending, Reachability: ReachabilityUnknown, JoinedAt: now}

	binding, err := NewPeerBinding(local, spec)
	if err != nil {
		t.Fatalf("NewPeerBinding() error = %v", err)
	}
	if binding.ChannelID() != channel || binding.PeerID() != remote || binding.OriginEpoch() != epoch ||
		binding.MemberHead() != member.Head() || binding.RosterHead() != roster.Head() ||
		binding.Protocols()[0] != "/mnemon/artifacts/1" ||
		binding.Multiaddrs()[0] != "/ip4/127.0.0.1/tcp/4001" || binding.Limits().String() != limits.String() {
		t.Fatalf("derived PeerBinding = %#v", binding)
	}
	copyKey := binding.PublicKey()
	copyKey[0] ^= 0xff
	if !ed25519.PublicKey(binding.PublicKey()).Equal(ed25519.PublicKey(remoteKey)) {
		t.Fatal("PeerBinding exposed mutable member authority")
	}
	if _, err := NewPeerBinding(remote, spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("self binding error = %v", err)
	}
	revoked := spec
	revoked.State = BindingRevoked
	if _, err := NewPeerBinding(local, revoked); !errors.Is(err, ErrInvariant) {
		t.Fatalf("active-member revoked binding error = %v", err)
	}
	otherOwner, otherKey, otherPrivate := canonicalDescriptorIdentity(t, "binding-other-owner")
	otherDescriptor := signedRecordDescriptor(t, channel, otherOwner, otherKey, otherPrivate)
	forged := spec
	forged.Roster, _ = NewVerifiedRoster(otherDescriptor, []Member{genesis, member})
	if _, err := NewPeerBinding(local, forged); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-descriptor binding error = %v", err)
	}
	stale := spec
	staleHead, _ := NewRecordHead(1, genesis.Head().Digest())
	stale.Channel, _ = NewChannel(ChannelSpec{Descriptor: descriptor, LocalAlias: "binding",
		RosterHead: staleHead, Status: ChannelActive, TopicState: TopicJoined,
		UpdatedAt: descriptor.Descriptor().CreatedAt()})
	if _, err := NewPeerBinding(local, stale); !errors.Is(err, ErrInvariant) {
		t.Fatalf("stale committed roster error = %v", err)
	}
}

func TestPeerBindingEnumsAreClosed(t *testing.T) {
	t.Parallel()

	if !BindingPending.Valid() || !BindingActive.Valid() || !BindingRevoked.Valid() || BindingState("ready").Valid() {
		t.Fatalf("BindingState closed enum mismatch")
	}
	if !ReachabilityUnknown.Valid() || !ReachabilityReachable.Valid() || !ReachabilityUnreachable.Valid() || Reachability("online").Valid() {
		t.Fatalf("Reachability closed enum mismatch")
	}
}
