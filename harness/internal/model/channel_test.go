package model

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"
)

func TestChannelEnumsAreClosed(t *testing.T) {
	t.Parallel()

	for _, status := range []ChannelStatus{ChannelActive, ChannelLeaving, ChannelConflicted, ChannelLeft, ChannelClosed, ChannelAbandoned} {
		if !status.Valid() {
			t.Fatalf("ChannelStatus(%q).Valid() = false", status)
		}
	}
	if ChannelStatus("ready").Valid() || TopicState("subscribed").Valid() || MemberStatus("joined").Valid() {
		t.Fatalf("an unknown Channel enum was accepted")
	}
}

func TestChannelStatusTopicMatrixAndCopies(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	channelID, _ := ParseChannelID("channel-a")
	owner, key, privateKey := canonicalDescriptorIdentity(t, "channel-owner")
	descriptor, err := NewChannelDescriptor(ChannelDescriptorSpec{ID: channelID, Name: "Review Team",
		OwnerPeerID: owner, OwnerPublicKey: key, MemberLimit: 8, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	message, _ := ChannelDescriptorSigningMessage(channelID, descriptor.Digest())
	signed, err := AttachChannelDescriptorSignature(descriptor, ed25519.Sign(privateKey, message))
	if err != nil {
		t.Fatal(err)
	}
	head, _ := NewRecordHead(1, Sum([]byte("roster")))
	spec := ChannelSpec{Descriptor: signed, LocalAlias: "review-team", RosterHead: head,
		Status: ChannelActive, TopicState: TopicJoined, UpdatedAt: now}
	channel, err := NewChannel(spec)
	if err != nil {
		t.Fatalf("NewChannel() error = %v", err)
	}
	key[0] = 'x'
	got := channel.OwnerPublicKey()
	got[0] = 'y'
	if !ed25519.PublicKey(channel.OwnerPublicKey()).Equal(privateKey.Public()) {
		t.Fatalf("Channel public key is mutable")
	}

	spec.Status, spec.TopicState = ChannelConflicted, TopicJoined
	if _, err := NewChannel(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("conflicted/joined error = %v", err)
	}
	spec.Status, spec.TopicState = ChannelLeft, TopicLeft
	if _, err := NewChannel(spec); err != nil {
		t.Fatalf("left/left error = %v", err)
	}
}
