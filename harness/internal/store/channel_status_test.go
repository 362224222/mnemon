package store

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

const maxRetainedHistoryObservationLatency = 5 * time.Second

func TestReadChannelObservationProjectsVerifiedCurrentAuthority(t *testing.T) {
	t.Parallel()
	fixture := newPeerInboxObserverFixture(t, "channel-observation-authority")

	observation, err := fixture.store.ReadChannelObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	channel := requireChannelObservationChannel(t, observation, fixture.channel.Channel().ID())
	head := channel.RosterHead()
	rosterMembers := channel.Roster().Members()
	if observation.LocalPeerID() != fixture.channel.Owner().PeerID() ||
		channel.ChannelIDDigest() != model.Sum([]byte(fixture.channel.Channel().ID().String())) ||
		head.RecordHead() != channel.Channel().RosterHead() ||
		head.OwnerPeerID() != channel.Channel().OwnerPeerID() ||
		!bytes.Equal(head.OwnerSignature(), rosterMembers[len(rosterMembers)-1].OwnerSignature()) {
		t.Fatalf("Channel observation differs from signed authority: %#v", channel)
	}
	if _, present := reflect.TypeOf(channel).FieldByName("publications"); present {
		t.Fatal("operational Channel observation retains a publication slice")
	}
}

func TestReadChannelObservationRemainsBoundedAcrossRetainedHistoryRestartAndChannels(t *testing.T) {
	fixture := newPeerInboxObserverFixture(t, "channel-observation-retained-history")
	isolated := testkit.NewSignedChannelForOwnerAt(t, "channel-observation-isolated",
		fixture.channel.Owner(), fixture.at.Add(time.Hour))
	insertSignedChannelFixture(t, fixture.store.db, isolated, model.TopicJoined)
	mustExec(t, fixture.store, `INSERT INTO publication_epochs(channel_id,origin_peer_id,
		origin_epoch,source_floor_channel_seq,source_head_channel_seq,updated_at)
		VALUES(?,?,?,1,0,?)`, isolated.Channel().ID().String(),
		isolated.Owner().PeerID().String(), isolated.Owner().OriginEpoch().String(),
		storeTime(isolated.Channel().UpdatedAt()))

	retainIgnoredPublications(t, &fixture, 1024)
	var retained uint64
	if err := fixture.store.db.QueryRow(`SELECT COUNT(*) FROM peer_inbox
		WHERE channel_id=?`, fixture.channel.Channel().ID().String()).Scan(&retained); err != nil ||
		retained != 1024 {
		t.Fatalf("retained publication oracle = (%d,%v), want 1024", retained, err)
	}
	corruptRetainedPublicationBody(t, fixture.store, fixture.channel.Channel().ID())
	assertLargeHistoryObservation(t, fixture.store, fixture.channel.Channel().ID(),
		isolated.Channel().ID())

	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen retained-history Store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	fixture.store = reopened
	assertLargeHistoryObservation(t, reopened, fixture.channel.Channel().ID(),
		isolated.Channel().ID())
}

func corruptRetainedPublicationBody(t *testing.T, st *Store, channelID model.ChannelID) {
	t.Helper()
	var triggerSQL string
	if err := st.db.QueryRow(`SELECT sql FROM sqlite_schema
		WHERE type='trigger' AND name='peer_inbox_identity_immutable'`).Scan(&triggerSQL); err != nil {
		t.Fatal(err)
	}
	mustExec(t, st, `DROP TRIGGER peer_inbox_identity_immutable`)
	mustExec(t, st, `UPDATE peer_inbox SET publication_json=?
		WHERE channel_id=? AND channel_seq=1`, []byte(`{}`), channelID.String())
	mustExec(t, st, triggerSQL)
}

func retainIgnoredPublications(t *testing.T, fixture *peerInboxFixture, count uint64) {
	t.Helper()
	const pageSize = uint64(32)
	for after := uint64(0); after < count; after += pageSize {
		scanned := after + pageSize
		publications := make([]model.SignedPublication, 0, pageSize)
		for sequence := after + 1; sequence <= scanned; sequence++ {
			publications = append(publications,
				fixture.publication(t, sequence, sequence, "retained-history", false))
		}
		result, err := fixture.store.PutPeerInboxPage(context.Background(),
			fixture.pageSpec(t, publications, after, scanned,
				fixture.at.Add(time.Duration(scanned)*time.Nanosecond)))
		if err != nil || result.Quarantined || len(result.Items) != int(pageSize) ||
			result.Cursor.ContiguousChannelSequence != scanned ||
			result.Cursor.ObservedChannelSequence != scanned {
			t.Fatalf("retain publication page ending %d = (%#v,%v)", scanned, result, err)
		}
		for index, item := range result.Items {
			if item.Disposition != PeerInboxIgnored {
				t.Fatalf("retained publication %d disposition = %q",
					after+uint64(index)+1, item.Disposition)
			}
		}
	}
}

func assertLargeHistoryObservation(t *testing.T, st *Store, historyChannel,
	isolatedChannel model.ChannelID,
) {
	t.Helper()
	started := time.Now()
	observation, err := st.ReadChannelObservation(context.Background())
	elapsed := time.Since(started)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ReadChannelObservation with 1024 retained publications completed in %s", elapsed)
	if elapsed > maxRetainedHistoryObservationLatency {
		t.Fatalf("ReadChannelObservation latency = %s, bound %s",
			elapsed, maxRetainedHistoryObservationLatency)
	}
	if len(observation.Channels()) != 2 {
		t.Fatalf("Channel observation count = %d, want 2", len(observation.Channels()))
	}
	history := requireChannelObservationChannel(t, observation, historyChannel)
	isolated := requireChannelObservationChannel(t, observation, isolatedChannel)
	if history.Progress().Inbox().Durable != 1024 ||
		history.Progress().Inbox().Ignored != 1024 {
		t.Fatalf("retained-history progress = %#v", history.Progress())
	}
	if isolated.Progress().Inbox() != (ChannelStatusInboxProgress{}) ||
		isolated.Progress().Commit() != (ChannelStatusCommitProgress{}) {
		t.Fatalf("unrelated Channel inherited history = %#v", isolated.Progress())
	}
	if _, present := reflect.TypeOf(history).FieldByName("publications"); present {
		t.Fatal("large operational observation retains a publication slice")
	}
}

func requireChannelObservationChannel(t *testing.T, observation ChannelObservation,
	channelID model.ChannelID,
) ChannelObservationChannel {
	t.Helper()
	for _, channel := range observation.Channels() {
		if channel.Channel().ID() == channelID {
			return channel
		}
	}
	t.Fatalf("Channel %s absent from observation", channelID.String())
	return ChannelObservationChannel{}
}
