package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestOutboundChannelBaselineRejectsStaleRosterGeneration(t *testing.T) {
	t.Parallel()
	fixture := newChannelBaselineFixture(t, "stale-roster", model.TopicJoined)
	other := testkit.NewSignedChannel(t, "stale-roster-other")
	reserve := fixture.reserveSpec(fixture.at)
	reserve.ExpectedRosterHead = other.Roster().Head()
	if _, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(), reserve); !errors.Is(
		err, ErrChannelBaselineConflict,
	) {
		t.Fatalf("stale reservation error = %v", err)
	}
	assertBaselineRowCount(t, fixture.store, "peer_pull_acks", 0)

	reserved, err := fixture.store.ReserveOutboundChannelBaseline(context.Background(),
		fixture.reserveSpec(fixture.at))
	if err != nil {
		t.Fatal(err)
	}
	confirm := fixture.confirmSpec(ChannelDataBaselineAck(reserved.Baseline),
		fixture.at.Add(time.Second))
	confirm.ExpectedRosterHead = other.Roster().Head()
	if _, err := fixture.store.ConfirmOutboundChannelBaseline(context.Background(), confirm); !errors.Is(
		err, ErrChannelBaselineConflict,
	) {
		t.Fatalf("stale confirmation error = %v", err)
	}
	var confirmedAt sql.NullString
	if err := fixture.store.db.QueryRow(`SELECT baseline_confirmed_at FROM peer_pull_acks
		WHERE channel_id=? AND target_peer_id=?`, fixture.channel.Channel().ID().String(),
		fixture.remote.Identity().PeerID().String()).Scan(&confirmedAt); err != nil || confirmedAt.Valid {
		t.Fatalf("stale confirmation durable value = (%#v,%v)", confirmedAt, err)
	}
}
