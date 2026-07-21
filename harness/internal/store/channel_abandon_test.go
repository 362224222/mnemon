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

func TestAbandonChannelAtomicallyFencesScopedWorkAndReplaysExactForensics(t *testing.T) {
	fixture := acceptedGossipFixture(t, "abandon-atomic")
	claimAt := fixture.now.Add(2 * time.Second)
	lease := claimPublication(t, fixture.store, fixture.channel, "abandon-worker", claimAt,
		claimAt.Add(time.Minute)).Lease
	alias := channelAliasForTest(t, fixture.store, fixture.channel)

	// A second Channel for the same Node proves that no shared identity or
	// unrelated Channel state is touched by the scoped terminal transition.
	owner := testkit.NewIdentity(t, "owner:accept-"+t.Name())
	other := testkit.NewSignedChannelForOwnerAt(t, "abandon-unrelated", owner,
		fixture.now.Add(-30*time.Minute))
	insertSignedChannelFixture(t, fixture.store.db, other, model.TopicJoined)
	mustExec(t, fixture.store, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		other.Channel().ID().String(), fixture.node.PeerID().String(), fixture.node.OriginEpoch().String(),
		storeTime(other.Channel().UpdatedAt()))

	transitionedAt := claimAt.Add(time.Second)
	result, err := fixture.store.AbandonChannel(context.Background(), AbandonChannelSpec{
		ChannelAlias: alias, ConfirmedAlias: alias, Force: true, At: transitionedAt})
	wantCounts := ChannelForensicCounts{MemberRecords: 2, Bindings: 1, Cursors: 1,
		PullACKs: 1, Events: 1, Works: 1, Publications: 1, Deliveries: 1}
	if err != nil || !result.Changed || result.Replayed || result.Alias != alias ||
		result.ChannelID != fixture.channel || !result.TransitionedAt.Equal(transitionedAt) ||
		result.Evidence != wantCounts {
		t.Fatalf("AbandonChannel() = (%#v, %v), want counts %#v", result, err, wantCounts)
	}
	assertChannelAbandonScopedState(t, fixture, other, lease, transitionedAt)

	replay, err := fixture.store.AbandonChannel(context.Background(), AbandonChannelSpec{
		ChannelAlias: alias, ConfirmedAlias: alias, Force: true, At: transitionedAt.Add(time.Hour)})
	result.Changed = false
	result.Replayed = true
	if err != nil || replay != result {
		t.Fatalf("AbandonChannel(replay) = (%#v, %v), want %#v", replay, err, result)
	}
}

func assertChannelAbandonScopedState(t *testing.T, fixture *acceptanceFixture,
	other *testkit.SignedChannel, lease GossipPublicationLease, transitionedAt time.Time,
) {
	t.Helper()
	var channelStatus, topicStatus, publicationStatus, deliveryStatus string
	var publicationOwner, publicationUntil sql.NullString
	if err := fixture.store.db.QueryRow(`SELECT c.status,c.topic_state,p.status,d.status,
		p.lease_owner,p.lease_until FROM channels c
		JOIN gossip_publications p ON p.channel_id=c.channel_id
		JOIN peer_deliveries d ON d.channel_id=c.channel_id
		WHERE c.channel_id=?`, fixture.channel.String()).Scan(&channelStatus, &topicStatus,
		&publicationStatus, &deliveryStatus, &publicationOwner, &publicationUntil); err != nil {
		t.Fatal(err)
	}
	if channelStatus != "abandoned" || topicStatus != "left" ||
		publicationStatus != "abandoned" || deliveryStatus != "abandoned" ||
		publicationOwner.Valid || publicationUntil.Valid {
		t.Fatalf("abandon states = %q/%q publication=%q owner=%#v until=%#v delivery=%q",
			channelStatus, topicStatus, publicationStatus, publicationOwner, publicationUntil, deliveryStatus)
	}
	if _, err := fixture.store.MarkGossipPublicationPublished(context.Background(),
		MarkGossipPublicationPublishedSpec{Fence: lease.Fence, At: transitionedAt.Add(time.Second)}); !errors.Is(err, ErrGossipPublicationStale) {
		t.Fatalf("abandoned publication accepted stale lease: %v", err)
	}
	var otherStatus, otherTopic, nodePeer string
	if err := fixture.store.db.QueryRow(`SELECT status,topic_state FROM channels WHERE channel_id=?`,
		other.Channel().ID().String()).Scan(&otherStatus, &otherTopic); err != nil {
		t.Fatal(err)
	}
	if err := fixture.store.db.QueryRow(`SELECT peer_id FROM node WHERE singleton=1`).Scan(&nodePeer); err != nil {
		t.Fatal(err)
	}
	if otherStatus != "active" || otherTopic != "joined" || nodePeer != fixture.node.PeerID().String() {
		t.Fatalf("unrelated authority changed: other=%q/%q node=%q", otherStatus, otherTopic, nodePeer)
	}
}

func TestAbandonChannelRollsBackEveryStepAndRejectsConfirmationMismatch(t *testing.T) {
	fixture := acceptedGossipFixture(t, "abandon-rollback")
	alias := channelAliasForTest(t, fixture.store, fixture.channel)
	if _, err := fixture.store.AbandonChannel(context.Background(), AbandonChannelSpec{
		ChannelAlias: alias, ConfirmedAlias: "another-channel", Force: true, At: fixture.now}); !errors.Is(err, ErrChannelAbandonInput) {
		t.Fatalf("confirmation mismatch error = %v", err)
	}
	mustExec(t, fixture.store, `CREATE TRIGGER abandon_delivery_failure
		BEFORE UPDATE ON peer_deliveries BEGIN SELECT RAISE(ABORT,'forced abandon failure'); END`)
	_, err := fixture.store.AbandonChannel(context.Background(), AbandonChannelSpec{
		ChannelAlias: alias, ConfirmedAlias: alias, Force: true, At: fixture.now.Add(time.Minute)})
	if err == nil {
		t.Fatal("forced final-step failure unexpectedly committed")
	}
	var channelStatus, topicStatus, publicationStatus, deliveryStatus string
	if err := fixture.store.db.QueryRow(`SELECT c.status,c.topic_state,p.status,d.status
		FROM channels c JOIN gossip_publications p ON p.channel_id=c.channel_id
		JOIN peer_deliveries d ON d.channel_id=c.channel_id WHERE c.channel_id=?`,
		fixture.channel.String()).Scan(&channelStatus, &topicStatus, &publicationStatus,
		&deliveryStatus); err != nil {
		t.Fatal(err)
	}
	if channelStatus != "active" || topicStatus != "joined" ||
		publicationStatus != "queued" || deliveryStatus != "pending" {
		t.Fatalf("partial abandon = %q/%q publication=%q delivery=%q",
			channelStatus, topicStatus, publicationStatus, deliveryStatus)
	}
}

func channelAliasForTest(t *testing.T, st *Store, channelID model.ChannelID) string {
	t.Helper()
	var alias string
	if err := st.db.QueryRow(`SELECT local_alias FROM channels WHERE channel_id=?`,
		channelID.String()).Scan(&alias); err != nil {
		t.Fatal(err)
	}
	return alias
}
