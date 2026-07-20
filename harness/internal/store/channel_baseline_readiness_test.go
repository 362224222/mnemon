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
