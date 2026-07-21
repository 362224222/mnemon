package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestChannelBaselineReadinessRejectsPartiallyActivatedBinding(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "partial-activation", model.TopicJoined)
	mustExec(t, fixture.store, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,0,0,0,?)`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String(), fixture.remote.Identity().OriginEpoch().String(),
		storeTime(fixture.at))
	if _, err := fixture.store.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID()); !errors.Is(err, ErrChannelBaselineAuthority) {
		t.Fatalf("partially activated binding readiness error = %v", err)
	}
}

func TestChannelMemberReadinessAuthorityUsesOneMeshGeneration(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "member-readiness-authority", model.TopicJoining)
	authority, err := fixture.store.ReadChannelMemberReadinessAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mesh := authority.MeshAuthority()
	channels := mesh.Channels()
	if mesh.LocalPeerID() != fixture.channel.Owner().PeerID() || len(channels) != 1 ||
		channels[0].Channel().ID() != fixture.channel.Channel().ID() {
		t.Fatalf("member readiness mesh = %#v", mesh)
	}
	readiness := authority.Readiness(channels[0].Channel().ID())
	if len(readiness) != 1 || readiness[0].PeerID != fixture.remote.Identity().PeerID() ||
		readiness[0].RosterHead != channels[0].Roster().Head() || readiness[0].InboundReady ||
		readiness[0].OutboundReady {
		t.Fatalf("member readiness projection = %#v", readiness)
	}
	readiness[0].OutboundReady = true
	if authority.Readiness(channels[0].Channel().ID())[0].OutboundReady {
		t.Fatal("member readiness authority exposed mutable backing state")
	}
}
