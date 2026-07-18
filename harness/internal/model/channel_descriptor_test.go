package model

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestChannelDescriptorV1GoldenVector(t *testing.T) {
	t.Parallel()
	peerID, publicKey, privateKey := canonicalDescriptorIdentity(t, "r5-golden-owner")
	channelID, _ := ParseChannelID("channel-golden-v1")
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: "Golden Team",
		OwnerPeerID: peerID, OwnerPublicKey: publicKey, MemberLimit: 8,
		CreatedAt: time.Date(2026, 7, 18, 1, 2, 3, 4, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	wantCanonical := `{"channel_id":"channel-golden-v1","created_at":"2026-07-18T01:02:03.000000004Z","member_limit":8,"name":"Golden Team","owner_peer_id":"12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B","owner_public_key":"KofW9SpW4pLrOfLxG0wZvnwzsbhyWRuEKcBiiqrAR9g=","schema_version":1}`
	message, _ := ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	signature := ed25519.Sign(privateKey, message)
	if peerID.String() != "12D3KooWCgPRroygp86pxPWqvQuXKSDf6CoJJHkmfEsNhm9rF46B" ||
		descriptor.CanonicalJSON().String() != wantCanonical ||
		descriptor.Digest().String() != "sha256:a0a33cea1efcd5dc284c39b4ca0db8ab2048a4c6bc1850d676e7dc6b1763c05c" ||
		hex.EncodeToString(message) != "6d6e656d6f6e2f72352f6368616e6e656c2d64657363726970746f722f31006368616e6e656c2d676f6c64656e2d763100a0a33cea1efcd5dc284c39b4ca0db8ab2048a4c6bc1850d676e7dc6b1763c05c" ||
		hex.EncodeToString(signature) != "a74ec0eb2bdf5fed8eac23abdc128c2d3e2c775842cae06df8ba790f9dde81b9a0df1b1430ea08b1488c79312814379f22371d6e6a71e26fe624b6ebd5d10c08" {
		t.Fatalf("Channel descriptor v1 golden vector drifted: %s", descriptor.CanonicalJSON().String())
	}
}

func TestSignedChannelDescriptorCanonicalRoundTripAndCopies(t *testing.T) {
	t.Parallel()
	peerID, publicKey, privateKey := canonicalDescriptorIdentity(t, "descriptor-owner")
	channelID, _ := ParseChannelID("channel-descriptor")
	createdAt := time.Date(2026, 7, 18, 10, 11, 12, 13, time.FixedZone("offset", 8*60*60))
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: "Review Team",
		OwnerPeerID: peerID, OwnerPublicKey: publicKey, MemberLimit: MaxMembersPerChannel, CreatedAt: createdAt})
	if err != nil {
		t.Fatal(err)
	}
	message, err := ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	if err != nil {
		t.Fatal(err)
	}
	signed, err := AttachChannelDescriptorSignature(descriptor, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseSignedChannelDescriptor(signed.WireJSON().Bytes())
	if err != nil || parsed.Descriptor().Digest() != descriptor.Digest() ||
		!parsed.Descriptor().CreatedAt().Equal(createdAt.UTC()) || parsed.Descriptor().MemberLimit() != 8 {
		t.Fatalf("ParseSignedChannelDescriptor() = (%#v, %v)", parsed, err)
	}
	if err := VerifyChannelDescriptor(parsed); err != nil {
		t.Fatal(err)
	}
	copyKey := parsed.Descriptor().OwnerPublicKey()
	copySignature := parsed.OwnerSignature()
	copyWire := parsed.WireJSON().Bytes()
	copyKey[0] ^= 0xff
	copySignature[0] ^= 0xff
	copyWire[0] ^= 0xff
	if !bytes.Equal(parsed.Descriptor().OwnerPublicKey(), publicKey) ||
		!bytes.Equal(parsed.WireJSON().Bytes(), signed.WireJSON().Bytes()) {
		t.Fatal("signed Channel descriptor exposed mutable storage")
	}
}

func TestSignedChannelDescriptorRejectsIdentitySignatureAndWireSubstitution(t *testing.T) {
	t.Parallel()
	owner, publicKey, privateKey := canonicalDescriptorIdentity(t, "descriptor-valid")
	other, otherKey, _ := canonicalDescriptorIdentity(t, "descriptor-other")
	channelID, _ := ParseChannelID("channel-invalid-descriptor")
	now := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	base := ChannelDescriptorSpec{ID: channelID, Name: "Channel", OwnerPeerID: owner,
		OwnerPublicKey: publicKey, MemberLimit: 8, CreatedAt: now}
	for _, unsafe := range []string{"channel/escape", `channel\escape`} {
		unsafeID, _ := ParseChannelID(unsafe)
		unsafeSpec := base
		unsafeSpec.ID = unsafeID
		if _, err := NewChannelDescriptor(unsafeSpec); !errors.Is(err, ErrInvalid) {
			t.Fatalf("unsafe topic-segment ChannelID %q error = %v", unsafe, err)
		}
	}

	mismatch := base
	mismatch.OwnerPeerID = other
	if _, err := NewChannelDescriptor(mismatch); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("mismatched owner error = %v", err)
	}
	mismatch = base
	mismatch.OwnerPublicKey = otherKey
	if _, err := NewChannelDescriptor(mismatch); !errors.Is(err, ErrPeerIDEncoding) {
		t.Fatalf("mismatched key error = %v", err)
	}
	descriptor, err := NewChannelDescriptor(base)
	if err != nil {
		t.Fatal(err)
	}
	message, _ := ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	wrongSignature := ed25519.Sign(privateKey, append(message, 'x'))
	if _, err := AttachChannelDescriptorSignature(descriptor, wrongSignature); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong signature error = %v", err)
	}
	valid, _ := AttachChannelDescriptorSignature(descriptor, ed25519.Sign(privateKey, message))

	var envelope map[string]any
	if err := json.Unmarshal(valid.WireJSON().Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["extra"] = true
	unknown, _ := CanonicalMarshal(envelope)
	if _, err := ParseSignedChannelDescriptor(unknown); err == nil {
		t.Fatal("unknown signed descriptor field was accepted")
	}
	noncanonical := append([]byte(" "), valid.WireJSON().Bytes()...)
	if _, err := ParseSignedChannelDescriptor(noncanonical); err == nil {
		t.Fatal("noncanonical signed descriptor bytes were accepted")
	}
	tampered := bytes.Replace(valid.WireJSON().Bytes(), []byte(`"Channel"`), []byte(`"Altered"`), 1)
	if _, err := ParseSignedChannelDescriptor(tampered); err == nil {
		t.Fatal("tampered descriptor was accepted")
	}
}

func canonicalDescriptorIdentity(t *testing.T, label string) (PeerID, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	peerID := canonicalModelPeerID(t, label)
	seed := Sum([]byte(label)).Bytes()
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	// canonicalModelPeerID derives from the same sha256(label) seed.
	return peerID, append(ed25519.PublicKey(nil), publicKey...), append(ed25519.PrivateKey(nil), privateKey...)
}
