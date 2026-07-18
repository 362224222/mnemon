package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestRequiredMemberProtocolsReturnsDefensiveCanonicalCopy(t *testing.T) {
	t.Parallel()
	first := RequiredMemberProtocols()
	second := RequiredMemberProtocols()
	if len(first) != MaxMemberProtocols || !slices.Equal(first, second) {
		t.Fatalf("RequiredMemberProtocols() = %#v, %#v", first, second)
	}
	first[0] = "/tampered/1"
	if slices.Equal(first, RequiredMemberProtocols()) {
		t.Fatal("RequiredMemberProtocols exposed mutable canonical storage")
	}
}

func TestMemberRecordV1GoldenVector(t *testing.T) {
	t.Parallel()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "r5-golden-owner")
	channelID, _ := ParseChannelID("channel-golden-v1")
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: "Golden Team",
		OwnerPeerID: owner, OwnerPublicKey: ownerKey, MemberLimit: 8,
		CreatedAt: time.Date(2026, 7, 18, 1, 2, 3, 4, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	epoch, _ := ParseOriginEpoch("epoch-golden-v1")
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Digest(), Revision: 1, PeerID: owner, OriginEpoch: epoch,
		DisplayLabel: "owner", PublicKey: ownerKey, Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"},
		Protocols: requiredMemberProtocols, Limits: DefaultMemberLimits(), Status: MemberActive,
		CreatedAt: descriptor.CreatedAt()})
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical := `{"channel_id":"channel-golden-v1","created_at":"2026-07-18T01:02:03.000000004Z","descriptor_digest":"sha256:a0a33cea1efcd5dc284c39b4ca0db8ab2048a4c6bc1850d676e7dc6b1763c05c","display_label":"owner","limits":{"profile":"r5-hermetic-v1"},"multiaddrs":["/ip4/127.0.0.1/tcp/4001"],"origin_epoch":"epoch-golden-v1","peer_id":"12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B","previous_digest":null,"protocols":["/mnemon/artifacts/1","/mnemon/channel/1","/mnemon/events/1"],"public_key":"KofW9SpW4pLrOfLxG0wZvnwzsbhyWRuEKcBiiqrAR9g=","revision":1,"schema_version":1,"status":"active"}`
	message, _ := MemberRecordSigningMessage(channelID, record.Digest())
	if record.CanonicalJSON().String() != wantCanonical ||
		record.Digest().String() != "sha256:bf85971b850c3ff9a02f7f1c54042e70e1bcfc54e558059f24f6277124e2aadf" ||
		hex.EncodeToString(message) != "6d6e656d6f6e2f72352f6368616e6e656c2d6d656d6265722d7265636f72642f31006368616e6e656c2d676f6c64656e2d763100bf85971b850c3ff9a02f7f1c54042e70e1bcfc54e558059f24f6277124e2aadf" ||
		hex.EncodeToString(ed25519.Sign(ownerPrivate, message)) != "12741ca4213275f3bfc7e2c6d9e37cda519bc73e00e13ecee1374235ac627365d5bc069e781302abde9424ec7209d8b7484e20e6292082ec475b631243f7ff02" {
		t.Fatalf("MemberRecord v1 golden vector drifted: %s", record.CanonicalJSON().String())
	}
}

func TestSignedMemberRecordRoundTripFreezesProtocolsLimitsAndCopies(t *testing.T) {
	t.Parallel()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "record-owner")
	channelID, _ := ParseChannelID("channel-record")
	descriptor := signedRecordDescriptor(t, channelID, owner, ownerKey, ownerPrivate)
	epoch, _ := ParseOriginEpoch("epoch-record")
	limits := DefaultMemberLimits()
	record, err := NewMemberRecord(MemberRecordSpec{ChannelID: channelID,
		DescriptorDigest: descriptor.Descriptor().Digest(), Revision: 1, PeerID: owner, OriginEpoch: epoch,
		DisplayLabel: "Reviewer", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.2/tcp/4001", "/ip4/127.0.0.1/tcp/4001"},
		Protocols:  []string{"/mnemon/events/1", "/mnemon/channel/1", "/mnemon/artifacts/1"},
		Limits:     limits, Status: MemberActive, CreatedAt: time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := MemberRecordSigningMessage(channelID, record.Digest())
	member, err := AttachMemberSignature(record, ed25519.Sign(ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseMember(member.WireJSON().Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyMember(descriptor, parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.Head().Revision() != 1 || parsed.Head().Digest() != record.Digest() ||
		parsed.Multiaddrs()[0] != "/ip4/127.0.0.1/tcp/4001" ||
		parsed.Protocols()[0] != "/mnemon/artifacts/1" || parsed.Limits().String() != limits.String() {
		t.Fatalf("parsed MemberRecord = %#v", parsed)
	}
	key := parsed.PublicKey()
	addresses := parsed.Multiaddrs()
	protocols := parsed.Protocols()
	signature := parsed.OwnerSignature()
	key[0] ^= 0xff
	addresses[0] = "changed"
	protocols[0] = "changed"
	signature[0] ^= 0xff
	if !bytes.Equal(parsed.PublicKey(), ownerKey) || parsed.Multiaddrs()[0] == "changed" ||
		parsed.Protocols()[0] == "changed" || !bytes.Equal(parsed.OwnerSignature(), member.OwnerSignature()) {
		t.Fatal("MemberRecord exposed mutable storage")
	}
}

func TestMemberRecordRejectsBrokenChainIdentityAndOwnerSignature(t *testing.T) {
	t.Parallel()
	owner, ownerKey, ownerPrivate := canonicalDescriptorIdentity(t, "record-chain-owner")
	memberPeer, memberKey, _ := canonicalDescriptorIdentity(t, "record-chain-member")
	otherPeer, _, _ := canonicalDescriptorIdentity(t, "record-chain-other")
	channelID, _ := ParseChannelID("channel-record-chain")
	descriptor := signedRecordDescriptor(t, channelID, owner, ownerKey, ownerPrivate)
	epoch, _ := ParseOriginEpoch("epoch-record-chain")
	limits := DefaultMemberLimits()
	base := MemberRecordSpec{ChannelID: channelID, DescriptorDigest: descriptor.Descriptor().Digest(),
		Revision: 1, PeerID: owner, OriginEpoch: epoch, DisplayLabel: "Member", PublicKey: ownerKey,
		Multiaddrs: []string{"/ip4/127.0.0.1/tcp/4001"},
		Protocols:  []string{"/mnemon/channel/1", "/mnemon/events/1", "/mnemon/artifacts/1"},
		Limits:     limits, Status: MemberActive, CreatedAt: time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)}
	previous := Sum([]byte("previous"))
	broken := base
	broken.PreviousDigest = &previous
	if _, err := NewMemberRecord(broken); !errors.Is(err, ErrInvariant) {
		t.Fatalf("genesis previous error = %v", err)
	}
	broken = base
	broken.Revision = 2
	if _, err := NewMemberRecord(broken); !errors.Is(err, ErrInvariant) {
		t.Fatalf("successor previous error = %v", err)
	}
	broken = base
	broken.PeerID = otherPeer
	if _, err := NewMemberRecord(broken); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("identity mismatch error = %v", err)
	}
	broken = base
	broken.Protocols = []string{"/mnemon/channel/1", "/mnemon/events/1", "/mnemon/not-artifacts/1"}
	if _, err := NewMemberRecord(broken); !errors.Is(err, ErrInvalid) {
		t.Fatalf("alternate protocol error = %v", err)
	}
	broken = base
	broken.Limits, _ = NewJSON([]byte(`{"profile":"r5-hermetic-v2"}`))
	if _, err := NewMemberRecord(broken); !errors.Is(err, ErrInvalid) {
		t.Fatalf("alternate limits profile error = %v", err)
	}
	broken = base
	broken.Multiaddrs = []string{"not-a-multiaddr"}
	if _, err := NewMemberRecord(broken); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid multiaddr error = %v", err)
	}
	record, err := NewMemberRecord(base)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := MemberRecordSigningMessage(channelID, record.Digest())
	member, _ := AttachMemberSignature(record, ed25519.Sign(ownerPrivate, append(message, 'x')))
	if err := VerifyMember(descriptor, member); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong owner signature error = %v", err)
	}
	valid, _ := AttachMemberSignature(record, ed25519.Sign(ownerPrivate, message))
	tampered := bytes.Replace(valid.WireJSON().Bytes(), []byte("Member"), []byte("Altered"), 1)
	if _, err := ParseMember(tampered); err == nil {
		t.Fatal("tampered signed MemberRecord was accepted")
	}
	if _, err := ParseMember(append([]byte(" "), valid.WireJSON().Bytes()...)); err == nil {
		t.Fatal("noncanonical signed MemberRecord was accepted")
	}
	nonOwner := base
	nonOwner.PeerID, nonOwner.PublicKey = memberPeer, memberKey
	nonOwnerRecord, err := NewMemberRecord(nonOwner)
	if err != nil {
		t.Fatal(err)
	}
	nonOwnerMessage, _ := MemberRecordSigningMessage(channelID, nonOwnerRecord.Digest())
	nonOwnerMember, _ := AttachMemberSignature(nonOwnerRecord, ed25519.Sign(ownerPrivate, nonOwnerMessage))
	if err := VerifyMember(descriptor, nonOwnerMember); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-owner genesis error = %v", err)
	}
	terminal := base
	terminal.Status = MemberRevoked
	terminalRecord, _ := NewMemberRecord(terminal)
	terminalMessage, _ := MemberRecordSigningMessage(channelID, terminalRecord.Digest())
	terminalMember, _ := AttachMemberSignature(terminalRecord, ed25519.Sign(ownerPrivate, terminalMessage))
	if err := VerifyMember(descriptor, terminalMember); !errors.Is(err, ErrInvalid) {
		t.Fatalf("terminal genesis error = %v", err)
	}
	predating := base
	predating.CreatedAt = descriptor.Descriptor().CreatedAt().Add(-time.Nanosecond)
	predatingRecord, _ := NewMemberRecord(predating)
	predatingMessage, _ := MemberRecordSigningMessage(channelID, predatingRecord.Digest())
	predatingMember, _ := AttachMemberSignature(predatingRecord, ed25519.Sign(ownerPrivate, predatingMessage))
	if err := VerifyMember(descriptor, predatingMember); !errors.Is(err, ErrInvalid) {
		t.Fatalf("predating record error = %v", err)
	}
	otherOwner, otherKey, otherPrivate := canonicalDescriptorIdentity(t, "record-other-owner")
	otherDescriptor := signedRecordDescriptor(t, channelID, otherOwner, otherKey, otherPrivate)
	if err := VerifyMember(otherDescriptor, valid); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-descriptor verification error = %v", err)
	}
}

func signedRecordDescriptor(t *testing.T, channelID ChannelID, owner PeerID,
	ownerKey ed25519.PublicKey, ownerPrivate ed25519.PrivateKey,
) SignedChannelDescriptor {
	t.Helper()
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: "Record Channel",
		OwnerPeerID: owner, OwnerPublicKey: ownerKey, MemberLimit: 8,
		CreatedAt: time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	signed, err := AttachChannelDescriptorSignature(descriptor, ed25519.Sign(ownerPrivate, message))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}
