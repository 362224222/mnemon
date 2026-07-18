package model

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

func TestVerifiedRosterRequiresContinuousCurrentAuthority(t *testing.T) {
	t.Parallel()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "roster-owner")
	remote, remoteKey, _ := canonicalDescriptorIdentity(t, "roster-remote")
	channelID, _ := ParseChannelID("verified-roster")
	descriptor := signedRecordDescriptor(t, channelID, owner, ownerKey, ownerPrivate)
	ownerEpoch, _ := ParseOriginEpoch("epoch-roster-owner")
	remoteEpoch, _ := ParseOriginEpoch("epoch-roster-remote")
	createdAt := descriptor.Descriptor().CreatedAt()
	genesis := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: owner,
		OriginEpoch: ownerEpoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"}, Protocols: requiredMemberProtocols,
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: createdAt})
	previous := genesis.Head().Digest()
	joined := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: remote, OriginEpoch: remoteEpoch, DisplayLabel: "remote", PublicKey: remoteKey,
		Multiaddrs: []string{"/ip4/127.0.0.2/tcp/4001"}, Protocols: requiredMemberProtocols,
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: createdAt.Add(time.Second)})
	roster, err := NewVerifiedRoster(descriptor, []Member{genesis, joined})
	if err != nil {
		t.Fatal(err)
	}
	current, ok := roster.CurrentMember(remote)
	if !ok || current.Head() != joined.Head() || roster.Head() != joined.Head() {
		t.Fatal("verified roster did not expose its current member and head")
	}
	previous = joined.Head().Digest()
	revoked := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 3, PreviousDigest: &previous,
		PeerID: remote, OriginEpoch: remoteEpoch, DisplayLabel: "remote", PublicKey: remoteKey,
		Multiaddrs: []string{"/ip4/127.0.0.2/tcp/4001"}, Protocols: requiredMemberProtocols,
		Limits: DefaultMemberLimits(), Status: MemberRevoked, CreatedAt: createdAt.Add(2 * time.Second)})
	revokedRoster, err := NewVerifiedRoster(descriptor, []Member{genesis, joined, revoked})
	if err != nil {
		t.Fatal(err)
	}
	current, ok = revokedRoster.CurrentMember(remote)
	if !ok || current.Status() != MemberRevoked || current.Head() != revoked.Head() {
		t.Fatal("historical active member remained current after revocation")
	}
	if _, err := NewVerifiedRoster(descriptor, []Member{genesis, revoked}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("roster gap error = %v", err)
	}
	previous = revoked.Head().Digest()
	reactivated := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 4, PreviousDigest: &previous,
		PeerID: remote, OriginEpoch: remoteEpoch, DisplayLabel: "remote", PublicKey: remoteKey,
		Multiaddrs: []string{"/ip4/127.0.0.2/tcp/4001"}, Protocols: requiredMemberProtocols,
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: createdAt.Add(3 * time.Second)})
	if _, err := NewVerifiedRoster(descriptor, []Member{genesis, joined, revoked, reactivated}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("terminal reactivation error = %v", err)
	}
}

func TestVerifiedRosterRejectsOwnerSelfRevocation(t *testing.T) {
	t.Parallel()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "roster-owner-self-revocation")
	channelID, _ := ParseChannelID("verified-roster-owner-self-revocation")
	descriptor := signedRecordDescriptor(t, channelID, owner, ownerKey, ownerPrivate)
	epoch, _ := ParseOriginEpoch("epoch-owner-self-revocation")
	createdAt := descriptor.Descriptor().CreatedAt()
	genesis := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: owner,
		OriginEpoch: epoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4101"}, Protocols: RequiredMemberProtocols(),
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: createdAt})
	previous := genesis.Head().Digest()
	revoked := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: owner, OriginEpoch: epoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: genesis.Multiaddrs(), Protocols: genesis.Protocols(), Limits: genesis.Limits(),
		Status: MemberRevoked, CreatedAt: createdAt.Add(time.Second)})
	if _, err := NewVerifiedRoster(descriptor, []Member{genesis, revoked}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("owner self-revocation error = %v", err)
	}
}

func TestVerifiedRosterRequiresOwnerLeaveToBeFinal(t *testing.T) {
	t.Parallel()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "roster-owner-final-leave")
	remote, remoteKey, _ := canonicalDescriptorIdentity(t, "roster-post-close-remote")
	channelID, _ := ParseChannelID("verified-roster-owner-final-leave")
	descriptor := signedRecordDescriptor(t, channelID, owner, ownerKey, ownerPrivate)
	ownerEpoch, _ := ParseOriginEpoch("epoch-owner-final-leave")
	remoteEpoch, _ := ParseOriginEpoch("epoch-post-close-remote")
	createdAt := descriptor.Descriptor().CreatedAt()
	genesis := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: owner,
		OriginEpoch: ownerEpoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4201"}, Protocols: RequiredMemberProtocols(),
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: createdAt})
	previous := genesis.Head().Digest()
	left := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 2, PreviousDigest: &previous,
		PeerID: owner, OriginEpoch: ownerEpoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: genesis.Multiaddrs(), Protocols: genesis.Protocols(), Limits: genesis.Limits(),
		Status: MemberLeft, CreatedAt: createdAt.Add(time.Second)})
	if _, err := NewVerifiedRoster(descriptor, []Member{genesis, left}); err != nil {
		t.Fatalf("final owner leave rejected: %v", err)
	}
	previous = left.Head().Digest()
	postClose := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 3, PreviousDigest: &previous,
		PeerID: remote, OriginEpoch: remoteEpoch, DisplayLabel: "remote", PublicKey: remoteKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4202"}, Protocols: RequiredMemberProtocols(),
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: createdAt.Add(2 * time.Second)})
	if _, err := NewVerifiedRoster(descriptor, []Member{genesis, left, postClose}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("post-close roster append error = %v", err)
	}
}

func TestVerifiedRosterBoundsAppendOnlyHistory(t *testing.T) {
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "bounded-roster-owner")
	channelID, _ := ParseChannelID("bounded-roster-history")
	descriptor := signedRecordDescriptor(t, channelID, owner, ownerKey, ownerPrivate)
	epoch, _ := ParseOriginEpoch("epoch-bounded-roster-owner")
	genesis := signedRosterMember(t, descriptor, ownerPrivate, MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: owner,
		OriginEpoch: epoch, DisplayLabel: "owner", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4301"}, Protocols: RequiredMemberProtocols(),
		Limits: DefaultMemberLimits(), Status: MemberActive, CreatedAt: descriptor.Descriptor().CreatedAt()})
	members := []Member{genesis}
	for len(members) < MaxMemberRecordsPerChannel+1 {
		// Repeating bytes cannot form a valid chain, but the history fence must
		// reject the oversized allocation before inspecting attacker-controlled
		// record structure.
		members = append(members, members[0])
	}
	if _, err := NewVerifiedRoster(descriptor, members); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversized verified roster error = %v", err)
	}
}

func signedRosterMember(t *testing.T, descriptor SignedChannelDescriptor, owner ed25519.PrivateKey,
	spec MemberRecordSpec,
) Member {
	t.Helper()
	record, err := NewMemberRecord(spec)
	if err != nil {
		t.Fatal(err)
	}
	message, err := MemberRecordSigningMessage(descriptor.Descriptor().ID(), record.Digest())
	if err != nil {
		t.Fatal(err)
	}
	member, err := AttachMemberSignature(record, ed25519.Sign(owner, message))
	if err != nil {
		t.Fatal(err)
	}
	return member
}
