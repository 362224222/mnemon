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
