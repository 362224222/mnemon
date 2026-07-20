package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestInstallInboundChannelBaselineActivatesAtomicallyAndReplaysExactly(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "install", model.TopicJoined)
	baseline := fixture.remoteBaseline(7)
	first, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: baseline, At: fixture.at})
	if err != nil || !first.Installed || first.Baseline != baseline {
		t.Fatalf("first install = (%#v,%v)", first, err)
	}
	var state, originalUpdated string
	var initial, contiguous, observed uint64
	if err := fixture.store.db.QueryRow(`SELECT binding.state,cursor.baseline_channel_seq,
		cursor.contiguous_channel_seq,cursor.observed_channel_seq,cursor.updated_at
		FROM peer_bindings binding JOIN peer_cursors cursor ON cursor.channel_id=binding.channel_id
		AND cursor.origin_peer_id=binding.peer_id AND cursor.origin_epoch=binding.origin_epoch
		WHERE binding.channel_id=? AND binding.peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&state, &initial, &contiguous, &observed,
		&originalUpdated); err != nil || state != string(model.BindingActive) ||
		initial != 7 || contiguous != 7 || observed != 7 {
		t.Fatalf("installed state = (%q,%d,%d,%d,%v)", state, initial, contiguous, observed, err)
	}

	replay, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: baseline, At: fixture.at.Add(time.Hour)})
	if err != nil || replay.Installed || replay.Baseline != baseline ||
		!replay.InstalledAt.Equal(first.InstalledAt) {
		t.Fatalf("install replay = (%#v,%v), first %#v", replay, err, first)
	}
	var replayUpdated string
	if err := fixture.store.db.QueryRow(`SELECT updated_at FROM peer_cursors WHERE channel_id=?
		AND origin_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&replayUpdated); err != nil ||
		replayUpdated != originalUpdated {
		t.Fatalf("install replay updated_at = (%q,%v), want %q", replayUpdated, err, originalUpdated)
	}

	different := baseline
	different.BaselineChannelSequence++
	if _, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: different, At: fixture.at.Add(2 * time.Hour)}); !errors.Is(err, ErrChannelBaselineConflict) {
		t.Fatalf("different replay error = %v", err)
	}
}

func TestInstallInboundChannelBaselineRollsBackCursorWhenActivationFails(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "rollback", model.TopicJoined)
	mustExec(t, fixture.store, `CREATE TRIGGER test_baseline_activation_abort
		BEFORE UPDATE OF state ON peer_bindings WHEN NEW.state='active'
		BEGIN SELECT RAISE(ABORT, 'forced baseline activation failure'); END`)
	_, err := fixture.store.InstallInboundChannelBaseline(context.Background(),
		InstallInboundChannelBaselineSpec{AuthenticatedPeerID: fixture.remote.Identity().PeerID(),
			Baseline: fixture.remoteBaseline(4), At: fixture.at})
	if !errors.Is(err, ErrChannelBaselineConflict) {
		t.Fatalf("forced activation error = %v", err)
	}
	assertBaselineRowCount(t, fixture.store, "peer_cursors", 0)
	var state string
	if err := fixture.store.db.QueryRow(`SELECT state FROM peer_bindings WHERE channel_id=? AND peer_id=?`,
		fixture.channel.Channel().ID().String(), fixture.remote.Identity().PeerID().String()).Scan(&state); err != nil || state != string(model.BindingPending) {
		t.Fatalf("binding after rollback = (%q,%v)", state, err)
	}
}
