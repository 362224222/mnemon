package model

import (
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
	head, _ := NewRecordHead(1, Sum([]byte("roster")))
	key := []byte("owner-public-key")
	spec := ChannelSpec{channelID, "Review Team", "review-team", mustPeer(t, "peer-owner"), key,
		8, head, ChannelActive, TopicJoined, now, now}
	channel, err := NewChannel(spec)
	if err != nil {
		t.Fatalf("NewChannel() error = %v", err)
	}
	key[0] = 'x'
	got := channel.OwnerPublicKey()
	got[0] = 'y'
	if string(channel.OwnerPublicKey()) != "owner-public-key" {
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

func TestMemberChainInvariants(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	channelID, _ := ParseChannelID("channel-a")
	epoch, _ := ParseOriginEpoch("epoch-a")
	head, _ := NewRecordHead(1, Sum([]byte("member")))
	record, _ := NewJSON([]byte(`{"revision":1}`))
	spec := MemberSpec{channelID, head, nil, mustPeer(t, "peer-a"), epoch, "Agent A", []byte("key"),
		[]string{"/ip4/127.0.0.1/tcp/4001"}, MemberActive, record, []byte("signature"), now}
	member, err := NewMember(spec)
	if err != nil || member.PeerID() != spec.PeerID {
		t.Fatalf("NewMember() = %#v, %v", member, err)
	}
	previous := Sum([]byte("previous"))
	spec.PreviousDigest = &previous
	if _, err := NewMember(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("genesis previous digest error = %v", err)
	}
	head, _ = NewRecordHead(2, Sum([]byte("member-2")))
	spec.Head, spec.PreviousDigest = head, nil
	if _, err := NewMember(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("non-genesis missing previous error = %v", err)
	}
}
