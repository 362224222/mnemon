package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestReadChannelMeshAuthorityProjectsWholeNodeAndOnlyLiveBindings(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "mesh-authority-owner")
	active := testkit.NewSignedChannelForOwnerAt(t, "mesh-active", owner,
		meshTestTime(t, "2026-07-18T00:00:00Z"))
	pending := active.AppendActive(t, "mesh-pending")
	connected := active.AppendActive(t, "mesh-connected")
	former := make([]testkit.MemberFixture, 0, model.MaxMembersPerChannel)
	for index := 0; index < model.MaxMembersPerChannel; index++ {
		formerActive := active.AppendActive(t, fmt.Sprintf("mesh-former-%d", index))
		former = append(former, active.AppendTerminal(t, formerActive.Identity().PeerID(),
			model.MemberRevoked))
	}
	insertChannelTestNode(t, st.db, owner, active.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, active, model.TopicJoined)
	insertSignedPeerBinding(t, st.db, active.Channel().ID(), pending, "pending",
		model.BindingPending, model.ReachabilityUnknown, active.Channel().CreatedAt())
	insertSignedPeerBinding(t, st.db, active.Channel().ID(), connected, "connected",
		model.BindingPending, model.ReachabilityReachable, active.Channel().CreatedAt())
	insertPeerCursorForMeshTest(t, st, active.Channel().ID(), connected)
	mustExec(t, st, `UPDATE peer_bindings SET state='active' WHERE channel_id=? AND peer_id=?`,
		active.Channel().ID().String(), connected.Identity().PeerID().String())
	for index, member := range former {
		insertSignedPeerBinding(t, st.db, active.Channel().ID(), member, fmt.Sprintf("former-%d", index),
			model.BindingRevoked, model.ReachabilityUnknown, active.Channel().CreatedAt())
	}

	closed := testkit.NewSignedChannelForOwnerAt(t, "mesh-closed", owner,
		meshTestTime(t, "2026-07-18T01:00:00Z"))
	closed.AppendTerminal(t, owner.PeerID(), model.MemberLeft)
	insertSignedChannelFixture(t, st.db, closed, model.TopicLeft)
	mustExec(t, st, `UPDATE channels SET status='closed' WHERE channel_id=?`,
		closed.Channel().ID().String())

	conflicted := testkit.NewSignedChannelForOwnerAt(t, "mesh-conflicted", owner,
		meshTestTime(t, "2026-07-18T02:00:00Z"))
	incumbent := conflicted.AppendTerminal(t, owner.PeerID(), model.MemberLeft)
	challenger := ownerConflictChallenger(t, conflicted, incumbent.Member().CreatedAt())
	insertSignedChannelFixture(t, st.db, conflicted, model.TopicNotJoined)
	insertSignedConflictFixture(t, st.db, conflicted, incumbent, challenger,
		challenger.OwnerSignature(), incumbent.Member().CreatedAt())
	mustExec(t, st, `UPDATE channels SET status='conflicted',topic_state='blocked'
		WHERE channel_id=?`, conflicted.Channel().ID().String())

	snapshot, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.LocalPeerID() != owner.PeerID() {
		t.Fatalf("local PeerID = %s, want %s", snapshot.LocalPeerID(), owner.PeerID())
	}
	channels := meshChannelsByID(snapshot.Channels())
	if len(channels) != 3 {
		t.Fatalf("durable Channel count = %d, want 3", len(channels))
	}
	activeSnapshot := channels[active.Channel().ID()]
	if activeSnapshot.Channel().Status() != model.ChannelActive ||
		activeSnapshot.Roster().Head() != active.Roster().Head() ||
		len(activeSnapshot.Roster().Members()) != len(active.Members()) {
		t.Fatalf("active Channel authority is incomplete: %#v", activeSnapshot)
	}
	live := activeSnapshot.Bindings()
	states := make(map[model.BindingState]int, len(live))
	for _, binding := range live {
		states[binding.State()]++
	}
	if len(live) != 2 || states[model.BindingPending] != 1 || states[model.BindingActive] != 1 {
		t.Fatalf("live bindings = %#v, want pending and active only", live)
	}
	if channels[closed.Channel().ID()].Channel().Status() != model.ChannelClosed {
		t.Fatal("terminal Channel was omitted or changed")
	}
	if channels[conflicted.Channel().ID()].Channel().Status() != model.ChannelConflicted {
		t.Fatal("conflicted Channel was omitted or changed")
	}
}

func TestReadChannelMeshAuthorityFailsClosedOnCorruptProjection(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "mesh-corrupt")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
	mustExec(t, st, `DROP TRIGGER channels_descriptor_immutable`)
	mustExec(t, st, `UPDATE channels SET name='forged name' WHERE channel_id=?`,
		fixture.Channel().ID().String())

	_, err := st.ReadChannelMeshAuthority(context.Background())
	if !errors.Is(err, ErrChannelMeshAuthority) || !errors.Is(err, ErrChannelAuthorityInvariant) {
		t.Fatalf("corrupt mesh authority error = %v", err)
	}
}

func TestChannelMeshAuthorityIsDefensiveAndSurvivesRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	fixture := testkit.NewSignedChannel(t, "mesh-restart")
	remote := fixture.AppendActive(t, "mesh-restart-remote")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoining)
	insertSignedPeerBinding(t, st.db, fixture.Channel().ID(), remote, "remote",
		model.BindingPending, model.ReachabilityUnknown, fixture.Channel().CreatedAt())

	before, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	channels := before.Channels()
	channels[0] = ChannelMeshChannel{}
	if before.Channels()[0].Channel().ID() != fixture.Channel().ID() {
		t.Fatal("Channels returned mutable snapshot storage")
	}
	bindings := before.Channels()[0].Bindings()
	bindings[0] = model.PeerBinding{}
	if before.Channels()[0].Bindings()[0].PeerID() != remote.Identity().PeerID() {
		t.Fatal("Bindings returned mutable snapshot storage")
	}
	members := before.Channels()[0].Roster().Members()
	members[0] = model.Member{}
	if before.Channels()[0].Roster().Members()[0].IsZero() {
		t.Fatal("Roster returned mutable member storage")
	}

	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	after, err := restarted.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.LocalPeerID() != before.LocalPeerID() || len(after.Channels()) != 1 ||
		after.Channels()[0].Roster().Head() != before.Channels()[0].Roster().Head() ||
		len(after.Channels()[0].Bindings()) != 1 {
		t.Fatalf("restarted mesh snapshot = %#v", after)
	}
}

func insertPeerCursorForMeshTest(t *testing.T, st *Store, channelID model.ChannelID,
	member testkit.MemberFixture,
) {
	t.Helper()
	mustExec(t, st, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,0,0,0,?)`, channelID.String(), member.Identity().PeerID().String(),
		member.Identity().OriginEpoch().String(), storeTime(member.Member().CreatedAt()))
}

func meshChannelsByID(channels []ChannelMeshChannel) map[model.ChannelID]ChannelMeshChannel {
	result := make(map[model.ChannelID]ChannelMeshChannel, len(channels))
	for _, channel := range channels {
		result[channel.Channel().ID()] = channel
	}
	return result
}

func meshTestTime(t testing.TB, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
