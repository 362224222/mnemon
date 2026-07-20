package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReserveOutboundChannelBaselineFreezesCurrentHeadAndReplays(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		head uint64
	}{
		{name: "empty origin", head: 0},
		{name: "nonempty origin", head: 3},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChannelBaselineFixture(t, "reserve-"+test.name, model.TopicJoined)
			fixture.advanceLocalHead(t, test.head, fixture.at)
			first, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
				ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
					TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(time.Second)})
			if err != nil || !first.Reserved ||
				first.Baseline.BaselineChannelSequence != test.head ||
				first.Baseline.OriginPeerID != fixture.channel.Owner().PeerID() ||
				first.Baseline.OriginEpoch != fixture.channel.Owner().OriginEpoch() {
				t.Fatalf("first reservation = (%#v, %v)", first, err)
			}
			var originalUpdated string
			if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks
				WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
				fixture.remote.Identity().PeerID().String()).Scan(&originalUpdated); err != nil {
				t.Fatal(err)
			}

			fixture.advanceLocalHead(t, test.head+2, fixture.at.Add(2*time.Second))
			replayed, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
				ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
					TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(3 * time.Second)})
			if err != nil || replayed.Reserved || replayed.Baseline != first.Baseline {
				t.Fatalf("replayed reservation = (%#v, %v), first %#v", replayed, err, first)
			}
			var replayUpdated string
			if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_pull_acks
				WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
				fixture.remote.Identity().PeerID().String()).Scan(&replayUpdated); err != nil {
				t.Fatal(err)
			}
			if replayUpdated != originalUpdated {
				t.Fatalf("reservation replay changed updated_at from %q to %q", originalUpdated, replayUpdated)
			}
			var baseline, acknowledged uint64
			if err := fixture.store.db.QueryRow(`SELECT baseline_channel_seq,acknowledged_channel_seq
				FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?`,
				fixture.channel.Channel().ID().String(), fixture.remote.Identity().PeerID().String()).
				Scan(&baseline, &acknowledged); err != nil || baseline != test.head || acknowledged != test.head {
				t.Fatalf("durable reservation = (%d,%d,%v)", baseline, acknowledged, err)
			}
		})
	}
}

func TestConfirmOutboundChannelBaselineIsExactDurableAndOrderIndependent(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "confirm", model.TopicJoined)
	confirmation := confirmOutboundBaseline(t, fixture)
	assertOutboundBaselineConfirmationReplays(t, fixture, confirmation)
	assertOutboundBaselineConfirmationRejectsConflicts(t, fixture, confirmation.ack)
	assertDirectionalBaselineGatesConverge(t, fixture)
}

type outboundBaselineConfirmation struct {
	ack               ChannelDataBaselineAck
	confirmed         ConfirmOutboundChannelBaselineResult
	originalConfirmed string
	originalUpdated   string
}

func confirmOutboundBaseline(t *testing.T, fixture channelBaselineFixture) outboundBaselineConfirmation {
	t.Helper()
	fixture.advanceLocalHead(t, 2, fixture.at)
	reserved, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
		ReserveOutboundChannelBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
			TargetPeerID: fixture.remote.Identity().PeerID(), At: fixture.at.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	ack := ChannelDataBaselineAck(reserved.Baseline)
	confirmed, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: ack, At: fixture.at.Add(2 * time.Second)})
	if err != nil || !confirmed.Confirmed || confirmed.Ack != ack {
		t.Fatalf("first confirmation = (%#v,%v)", confirmed, err)
	}
	var originalConfirmed, originalUpdated string
	if err := fixture.store.db.QueryRow(`SELECT baseline_confirmed_at,updated_at FROM peer_pull_acks
		WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&originalConfirmed, &originalUpdated); err != nil {
		t.Fatal(err)
	}
	return outboundBaselineConfirmation{ack: ack, confirmed: confirmed,
		originalConfirmed: originalConfirmed, originalUpdated: originalUpdated}
}

func assertOutboundBaselineConfirmationReplays(t *testing.T, fixture channelBaselineFixture,
	confirmation outboundBaselineConfirmation,
) {
	t.Helper()
	replay, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: confirmation.ack, At: fixture.at.Add(3 * time.Second)})
	if err != nil || replay.Confirmed || !replay.ConfirmedAt.Equal(confirmation.confirmed.ConfirmedAt) {
		t.Fatalf("confirmation replay = (%#v,%v), first %#v", replay, err, confirmation.confirmed)
	}
	var replayConfirmed, replayUpdated string
	if err := fixture.store.db.QueryRow(`SELECT baseline_confirmed_at,updated_at FROM peer_pull_acks
		WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&replayConfirmed, &replayUpdated); err != nil ||
		replayConfirmed != confirmation.originalConfirmed || replayUpdated != confirmation.originalUpdated {
		t.Fatalf("confirmation replay mutated times = (%q,%q,%v), want (%q,%q)",
			replayConfirmed, replayUpdated, err, confirmation.originalConfirmed, confirmation.originalUpdated)
	}
}

func assertOutboundBaselineConfirmationRejectsConflicts(t *testing.T, fixture channelBaselineFixture,
	ack ChannelDataBaselineAck,
) {
	t.Helper()
	different := ack
	different.BaselineChannelSequence++
	if _, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: different, At: fixture.at.Add(4 * time.Second)}); !errors.Is(err, ErrChannelBaselineConflict) {
		t.Fatalf("different ACK error = %v", err)
	}
	wrongEpoch := ack
	wrongEpoch.OriginEpoch, _ = model.ParseOriginEpoch("epoch-wrong-local-ack")
	if _, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(),
		ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Ack: wrongEpoch, At: fixture.at.Add(4 * time.Second)}); !errors.Is(err, ErrChannelBaselineEpochMismatch) {
		t.Fatalf("wrong ACK epoch error = %v", err)
	}
}

func assertDirectionalBaselineGatesConverge(t *testing.T, fixture channelBaselineFixture) {
	t.Helper()
	// An outbound ACK may arrive before the reverse baseline. The two gates are
	// intentionally independent and converge once the inbound transaction lands.
	readiness, err := fixture.store.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || readiness[0].InboundReady ||
		!readiness[0].OutboundReady || readiness[0].Ready() {
		t.Fatalf("pre-inbound readiness = (%#v,%v)", readiness, err)
	}
	if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(9), At: fixture.at.Add(5 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	readiness, err = fixture.store.ReadChannelBaselineReadiness(context.Background(),
		fixture.channel.Channel().ID())
	if err != nil || len(readiness) != 1 || !readiness[0].InboundReady ||
		!readiness[0].OutboundReady || !readiness[0].Ready() ||
		readiness[0].BindingState != model.BindingActive ||
		readiness[0].TopicState != model.TopicJoined ||
		readiness[0].RosterHead != fixture.channel.Roster().Head() {
		t.Fatalf("converged readiness = (%#v,%v)", readiness, err)
	}
}
