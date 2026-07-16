package store

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
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
	if _, err := st.db.Exec("INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at) VALUES(?,?,?,?,0,0,?,?)",
		channel.String(), remote.String(), localPeer, localEpoch, "2026-07-16T12:00:00Z", "2026-07-16T12:00:00Z"); err != nil {
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
	node, profile := bootstrapValues(t, "peer-local-admission", "principal-admission", "/workspace/admission")
	node, profile = activateTestNode(t, st, node, profile)
	channel, _ := model.ParseChannelID("channel-admission")
	remote, _ := model.ParsePeerID("peer-remote-admission")
	localHash := model.Sum([]byte("member-local"))
	remoteHash := model.Sum([]byte("member-remote"))
	now := "2026-07-16T12:00:00Z"
	statements := []struct {
		query string
		args  []any
	}{
		{"INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,owner_public_key,member_limit,roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", []any{channel.String(), "Review", "review", node.PeerID().String(), []byte("owner-key"), 8, 2, remoteHash.Bytes(), "active", "joined", now, now}},
		{"INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", []any{channel.String(), 1, localHash.Bytes(), nil, node.PeerID().String(), node.OriginEpoch().String(), "local", []byte("local-key"), []byte("[]"), "active", []byte("{}"), []byte("sig-local"), now}},
		{"INSERT INTO channel_members(channel_id,revision,record_hash,previous_hash,member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,status,signed_record_json,owner_signature,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", []any{channel.String(), 2, remoteHash.Bytes(), localHash.Bytes(), remote.String(), "epoch-remote", "remote", []byte("remote-key"), []byte("[]"), "active", []byte("{}"), []byte("sig-remote"), now}},
		{"INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,?,?,?)", []any{channel.String(), node.PeerID().String(), node.OriginEpoch().String(), 1, 0, now}},
		{"INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,effective_alias,public_key,multiaddrs_json,protocols_json,limits_json,member_revision,member_record_hash,state,reachability,joined_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", []any{channel.String(), remote.String(), "epoch-remote", "remote", []byte("remote-key"), []byte("[]"), []byte("[]"), []byte("{}"), 2, remoteHash.Bytes(), "pending", "unknown", now}},
		{"INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at) VALUES(?,?,?,?,?,?,?)", []any{channel.String(), remote.String(), "epoch-remote", 0, 0, 0, now}},
		{"UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?", []any{channel.String(), remote.String()}},
		{"INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at) VALUES(?,?,?,?,?,?,?,?)", []any{channel.String(), remote.String(), node.PeerID().String(), node.OriginEpoch().String(), 0, 0, now, now}},
	}
	for _, statement := range statements {
		if _, err := st.db.Exec(statement.query, statement.args...); err != nil {
			t.Fatalf("fixture SQL: %v", err)
		}
	}
	return st, channel, node.PeerID(), remote
}
