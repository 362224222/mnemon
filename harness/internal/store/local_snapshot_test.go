package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestPrepareLocalAdmissionFreezesAuthorityAndSequences(t *testing.T) {
	t.Parallel()
	st, channel, local, remote := localAdmissionFixture(t)
	audience, err := model.NewAudience([]model.PeerID{remote})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.PrepareLocalAdmission(context.Background(), channel, audience, 2)
	if err != nil {
		t.Fatalf("PrepareLocalAdmission() error = %v", err)
	}
	if snapshot.Node().PeerID() != local || snapshot.Profile().ID() != model.TeamworkProfileID() ||
		snapshot.OriginMember().Revision() != 1 || snapshot.PublicationRoster().Revision() != 2 ||
		snapshot.FirstOriginSequence() != 1 || snapshot.FirstChannelSequence() != 1 || snapshot.Count() != 2 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	workID, _ := model.ParseWorkID("work-snapshot")
	work, _ := model.NewWorkRef(local, workID)
	second, err := snapshot.EventScope(1, work)
	if err != nil {
		t.Fatal(err)
	}
	if second.OriginSequence() != 2 || second.ChannelSequence() != 2 || second.WorkRef() != work {
		t.Fatalf("second Event scope lost frozen range")
	}
	if _, err := snapshot.EventScope(2, work); err == nil {
		t.Fatal("out-of-range Event scope was accepted")
	}
}

func TestPrepareLocalAdmissionRequiresReadyTopicAndOutboundBaseline(t *testing.T) {
	t.Parallel()
	st, channel, _, remote := localAdmissionFixture(t)
	audience, _ := model.NewAudience([]model.PeerID{remote})
	if _, err := st.db.Exec("DROP TRIGGER peer_pull_acks_no_delete"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("DELETE FROM peer_pull_acks"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareLocalAdmission(context.Background(), channel, audience, 1); !errors.Is(err, ErrAudienceUnavailable) {
		t.Fatalf("missing baseline error = %v", err)
	}
	var localPeer, localEpoch string
	if err := st.db.QueryRow("SELECT peer_id, origin_epoch FROM node WHERE singleton=1").Scan(&localPeer, &localEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at) VALUES(?,?,?,?,0,0,NULL,?)",
		channel.String(), remote.String(), localPeer, localEpoch, "2026-07-16T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("UPDATE peer_pull_acks SET baseline_confirmed_at=? WHERE channel_id=? AND target_peer_id=?",
		"2026-07-16T12:00:00Z", channel.String(), remote.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("UPDATE channels SET topic_state = 'not_joined'"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareLocalAdmission(context.Background(), channel, audience, 1); !errors.Is(err, ErrChannelUnavailable) {
		t.Fatalf("not-joined topic error = %v", err)
	}
}

func TestPrepareLocalAdmissionReservesRepresentableNextOriginSequence(t *testing.T) {
	t.Parallel()
	st, channel, _, remote := localAdmissionFixture(t)
	audience, _ := model.NewAudience([]model.PeerID{remote})
	if _, err := st.db.Exec("UPDATE node SET next_origin_seq = ?", model.MaxSQLiteInteger); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareLocalAdmission(context.Background(), channel, audience, 1); err == nil {
		t.Fatal("snapshot accepted a range whose next origin sequence cannot be stored")
	}
	if _, err := st.db.Exec("UPDATE node SET next_origin_seq = ?", model.MaxSQLiteInteger-1); err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.PrepareLocalAdmission(context.Background(), channel, audience, 1)
	if err != nil || snapshot.FirstOriginSequence() != model.MaxSQLiteInteger-1 {
		t.Fatalf("last representable snapshot = (%#v, %v)", snapshot, err)
	}
}

func localAdmissionFixture(t *testing.T) (*Store, model.ChannelID, model.PeerID, model.PeerID) {
	t.Helper()
	st := openTestStore(t)
	createdAt := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	signed := testkit.NewSignedChannelAt(t, "local-admission-"+t.Name(), createdAt)
	remoteIdentity := testkit.NewIdentity(t, "local-admission-remote-"+t.Name())
	remoteMember := signed.AppendActiveIdentity(t, remoteIdentity)
	node, profile := signedBootstrapValues(t, signed.Owner(), "principal-admission", "/workspace/admission",
		createdAt)
	node, profile = activateTestNode(t, st, node, profile)
	channel, remote := signed.Channel().ID(), remoteIdentity.PeerID()
	insertSignedChannelFixture(t, st.db, signed, model.TopicJoined)
	mustExec(t, st, "UPDATE channels SET local_alias='review' WHERE channel_id=?", channel.String())
	now := storeTime(signed.Channel().UpdatedAt())
	mustExec(t, st, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,?,?,?)`, channel.String(),
		node.PeerID().String(), node.OriginEpoch().String(), 1, 0, now)
	insertSignedPeerBinding(t, st.db, channel, remoteMember, "remote", model.BindingPending,
		model.ReachabilityUnknown, signed.Channel().UpdatedAt())
	mustExec(t, st, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at) VALUES(?,?,?,?,?,?,?)`,
		channel.String(), remote.String(), remoteIdentity.OriginEpoch().String(), 0, 0, 0, now)
	mustExec(t, st, "UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?",
		channel.String(), remote.String())
	mustExec(t, st, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
		VALUES(?,?,?,?,?,?,NULL,?)`, channel.String(), remote.String(), node.PeerID().String(),
		node.OriginEpoch().String(), 0, 0, now)
	mustExec(t, st, `UPDATE peer_pull_acks SET baseline_confirmed_at=?
		WHERE channel_id=? AND target_peer_id=?`, now, channel.String(), remote.String())
	return st, channel, node.PeerID(), remote
}
