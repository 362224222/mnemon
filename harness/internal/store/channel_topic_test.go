package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestBeginChannelTopicRuntimeInvalidatesOnlyStaleJoinedSessions(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "topic-startup-owner")
	base := channelTopicTestTime(t, "2026-07-19T00:00:00Z")
	insertChannelTestNode(t, st.db, owner, base)
	joined := testkit.NewSignedChannelForOwnerAt(t, "topic-startup-joined", owner, base)
	joining := testkit.NewSignedChannelForOwnerAt(t, "topic-startup-joining", owner, base.Add(time.Hour))
	notJoined := testkit.NewSignedChannelForOwnerAt(t, "topic-startup-not-joined", owner,
		base.Add(2*time.Hour))
	closed := testkit.NewSignedChannelForOwnerAt(t, "topic-startup-closed", owner, base.Add(3*time.Hour))
	closed.AppendTerminal(t, owner.PeerID(), model.MemberLeft)
	insertSignedChannelFixture(t, st.db, joined, model.TopicJoined)
	insertSignedChannelFixture(t, st.db, joining, model.TopicJoining)
	insertSignedChannelFixture(t, st.db, notJoined, model.TopicNotJoined)
	insertSignedChannelFixture(t, st.db, closed, model.TopicLeft)
	mustExec(t, st, `UPDATE channels SET status='closed' WHERE channel_id=?`, closed.Channel().ID().String())

	before := readChannelTopicRows(t, st)
	startupAt := closed.Channel().UpdatedAt().Add(time.Hour)
	first, err := st.BeginChannelTopicRuntime(context.Background(), startupAt)
	if err != nil || first.Downgraded != 1 || len(first.Topics) != 4 {
		t.Fatalf("first topic startup = (%#v,%v)", first, err)
	}
	projected := channelTopicsByID(first.Topics)
	if projected[joined.Channel().ID()].TopicState != model.TopicJoining ||
		!projected[joined.Channel().ID()].UpdatedAt.Equal(startupAt) ||
		projected[joining.Channel().ID()].TopicState != model.TopicJoining ||
		projected[notJoined.Channel().ID()].TopicState != model.TopicNotJoined ||
		projected[closed.Channel().ID()].TopicState != model.TopicLeft {
		t.Fatalf("startup projections = %#v", projected)
	}
	afterFirst := readChannelTopicRows(t, st)
	if afterFirst[joined.Channel().ID()].topic != string(model.TopicJoining) ||
		afterFirst[joined.Channel().ID()].updated != storeTime(startupAt) {
		t.Fatalf("joined startup row = %#v", afterFirst[joined.Channel().ID()])
	}
	for _, channel := range []*testkit.SignedChannel{joining, notJoined, closed} {
		if afterFirst[channel.Channel().ID()] != before[channel.Channel().ID()] {
			t.Fatalf("startup changed isolated Channel %s: before %#v after %#v",
				channel.Channel().ID(), before[channel.Channel().ID()], afterFirst[channel.Channel().ID()])
		}
	}

	second, err := st.BeginChannelTopicRuntime(context.Background(), startupAt.Add(time.Hour))
	if err != nil || second.Downgraded != 0 || len(second.Topics) != 4 {
		t.Fatalf("replayed topic startup = (%#v,%v)", second, err)
	}
	if afterSecond := readChannelTopicRows(t, st); !equalChannelTopicRows(afterFirst, afterSecond) {
		t.Fatalf("startup replay drifted rows: before %#v after %#v", afterFirst, afterSecond)
	}
	read, err := st.ReadChannelTopicRuntime(context.Background())
	if err != nil || len(read) != 4 || channelTopicsByID(read)[joined.Channel().ID()].RosterHead !=
		joined.Roster().Head() {
		t.Fatalf("ReadChannelTopicRuntime() = (%#v,%v)", read, err)
	}

	path := st.Path()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	restarted, err := reopened.BeginChannelTopicRuntime(context.Background(), startupAt.Add(2*time.Hour))
	if err != nil || restarted.Downgraded != 0 ||
		readChannelTopicRows(t, reopened)[joined.Channel().ID()].updated != storeTime(startupAt) {
		t.Fatalf("restarted topic runtime = (%#v,%v)", restarted, err)
	}
}

func TestBeginChannelTopicRuntimeRollsBackAllChannelsOnStaleClock(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "topic-startup-rollback-owner")
	base := channelTopicTestTime(t, "2026-07-19T04:00:00Z")
	insertChannelTestNode(t, st.db, owner, base)
	first := testkit.NewSignedChannelForOwnerAt(t, "topic-startup-rollback-first", owner, base)
	second := testkit.NewSignedChannelForOwnerAt(t, "topic-startup-rollback-second", owner,
		base.Add(2*time.Hour))
	insertSignedChannelFixture(t, st.db, first, model.TopicJoined)
	insertSignedChannelFixture(t, st.db, second, model.TopicJoined)
	before := readChannelTopicRows(t, st)
	if _, err := st.BeginChannelTopicRuntime(context.Background(), base.Add(time.Hour)); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("stale startup clock error = %v", err)
	}
	if after := readChannelTopicRows(t, st); !equalChannelTopicRows(before, after) {
		t.Fatalf("failed startup partially mutated Channels: before %#v after %#v", before, after)
	}
}

func TestCompareAndSetChannelTopicStateFencesAuthorityAndRejoinFailure(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	owner := testkit.NewIdentity(t, "topic-cas-owner")
	base := channelTopicTestTime(t, "2026-07-19T08:00:00Z")
	insertChannelTestNode(t, st.db, owner, base)
	primary := testkit.NewSignedChannelForOwnerAt(t, "topic-cas-primary", owner, base)
	isolated := testkit.NewSignedChannelForOwnerAt(t, "topic-cas-isolated", owner, base.Add(time.Hour))
	insertSignedChannelFixture(t, st.db, primary, model.TopicJoining)
	insertSignedChannelFixture(t, st.db, isolated, model.TopicJoined)
	joinedAt := isolated.Channel().UpdatedAt().Add(time.Hour)
	join := CompareAndSetChannelTopicStateSpec{ChannelID: primary.Channel().ID(),
		ExpectedStatus: model.ChannelActive, ExpectedRosterHead: primary.Roster().Head(),
		ExpectedTopicState: model.TopicJoining, TopicState: model.TopicJoined, At: joinedAt}
	first, err := st.CompareAndSetChannelTopicState(context.Background(), join)
	if err != nil || !first.Changed || first.Topic.TopicState != model.TopicJoined ||
		!first.Topic.UpdatedAt.Equal(joinedAt) {
		t.Fatalf("join topic CAS = (%#v,%v)", first, err)
	}
	originalRows := readChannelTopicRows(t, st)
	replay := join
	replay.At = joinedAt.Add(time.Hour)
	second, err := st.CompareAndSetChannelTopicState(context.Background(), replay)
	if err != nil || second.Changed || second.Topic != first.Topic {
		t.Fatalf("join topic replay = (%#v,%v), first %#v", second, err, first)
	}
	if rows := readChannelTopicRows(t, st); !equalChannelTopicRows(originalRows, rows) {
		t.Fatalf("join replay drifted topic timestamps: before %#v after %#v", originalRows, rows)
	}

	wrongHeadDigest := model.Sum([]byte("stale-topic-roster-head"))
	wrongHead, err := model.NewRecordHead(primary.Roster().Head().Revision(), wrongHeadDigest)
	if err != nil {
		t.Fatal(err)
	}
	staleHead := replay
	staleHead.ExpectedRosterHead = wrongHead
	if _, err := st.CompareAndSetChannelTopicState(context.Background(), staleHead); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("stale roster head error = %v", err)
	}
	staleStatus := replay
	staleStatus.ExpectedStatus = model.ChannelLeaving
	if _, err := st.CompareAndSetChannelTopicState(context.Background(), staleStatus); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("stale status error = %v", err)
	}

	joiningAt := joinedAt.Add(2 * time.Hour)
	rejoin := CompareAndSetChannelTopicStateSpec{ChannelID: primary.Channel().ID(),
		ExpectedStatus: model.ChannelActive, ExpectedRosterHead: primary.Roster().Head(),
		ExpectedTopicState: model.TopicJoined, TopicState: model.TopicJoining, At: joiningAt}
	if changed, err := st.CompareAndSetChannelTopicState(context.Background(), rejoin); err != nil || !changed.Changed {
		t.Fatalf("begin rejoin = (%#v,%v)", changed, err)
	}
	failureAt := joiningAt.Add(time.Hour)
	failure := CompareAndSetChannelTopicStateSpec{ChannelID: primary.Channel().ID(),
		ExpectedStatus: model.ChannelActive, ExpectedRosterHead: primary.Roster().Head(),
		ExpectedTopicState: model.TopicJoining, TopicState: model.TopicNotJoined, At: failureAt}
	failed, err := st.CompareAndSetChannelTopicState(context.Background(), failure)
	if err != nil || !failed.Changed || failed.Topic.TopicState != model.TopicNotJoined {
		t.Fatalf("record rejoin failure = (%#v,%v)", failed, err)
	}
	invalidSuccess := CompareAndSetChannelTopicStateSpec{ChannelID: primary.Channel().ID(),
		ExpectedStatus: model.ChannelActive, ExpectedRosterHead: primary.Roster().Head(),
		ExpectedTopicState: model.TopicNotJoined, TopicState: model.TopicJoined,
		At: failureAt.Add(time.Hour)}
	if _, err := st.CompareAndSetChannelTopicState(context.Background(), invalidSuccess); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("not_joined direct success error = %v", err)
	}
	rows := readChannelTopicRows(t, st)
	if rows[isolated.Channel().ID()] != originalRows[isolated.Channel().ID()] {
		t.Fatalf("primary CAS changed isolated Channel: before %#v after %#v",
			originalRows[isolated.Channel().ID()], rows[isolated.Channel().ID()])
	}
}

func TestCompareAndSetChannelTopicStateRejectsTerminalAndConflictedChannels(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		setup func(*testing.T, *Store, testkit.Identity, time.Time) *testkit.SignedChannel
	}{
		{name: "closed", setup: func(t *testing.T, st *Store, owner testkit.Identity,
			at time.Time) *testkit.SignedChannel {
			fixture := testkit.NewSignedChannelForOwnerAt(t, "topic-cas-closed", owner, at)
			fixture.AppendTerminal(t, owner.PeerID(), model.MemberLeft)
			insertSignedChannelFixture(t, st.db, fixture, model.TopicLeft)
			mustExec(t, st, `UPDATE channels SET status='closed' WHERE channel_id=?`,
				fixture.Channel().ID().String())
			return fixture
		}},
		{name: "conflicted", setup: func(t *testing.T, st *Store, owner testkit.Identity,
			at time.Time) *testkit.SignedChannel {
			fixture := testkit.NewSignedChannelForOwnerAt(t, "topic-cas-conflicted", owner, at)
			incumbent := fixture.AppendTerminal(t, owner.PeerID(), model.MemberLeft)
			challenger := ownerConflictChallenger(t, fixture, incumbent.Member().CreatedAt())
			insertSignedChannelFixture(t, st.db, fixture, model.TopicNotJoined)
			insertSignedConflictFixture(t, st.db, fixture, incumbent, challenger,
				challenger.OwnerSignature(), incumbent.Member().CreatedAt())
			mustExec(t, st, `UPDATE channels SET status='conflicted',topic_state='blocked'
				WHERE channel_id=?`, fixture.Channel().ID().String())
			return fixture
		}},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			st := openTestStore(t)
			owner := testkit.NewIdentity(t, "topic-cas-"+test.name+"-owner")
			base := channelTopicTestTime(t, "2026-07-19T16:00:00Z")
			insertChannelTestNode(t, st.db, owner, base)
			fixture := test.setup(t, st, owner, base)
			_, err := st.CompareAndSetChannelTopicState(context.Background(),
				CompareAndSetChannelTopicStateSpec{ChannelID: fixture.Channel().ID(),
					ExpectedStatus: model.ChannelActive, ExpectedRosterHead: fixture.Roster().Head(),
					ExpectedTopicState: model.TopicNotJoined, TopicState: model.TopicJoining,
					At: fixture.Channel().UpdatedAt().Add(time.Hour)})
			if !errors.Is(err, ErrChannelRuntimeAuthority) {
				t.Fatalf("%s topic transition error = %v", test.name, err)
			}
		})
	}
}

func TestSetPeerReachabilityIsMonotonicAndAuthorityBound(t *testing.T) {
	t.Parallel()
	fixture := newChannelReachabilityFixture(t, "reachability-monotonic")
	reachableAt := fixture.at
	base := fixture.spec(model.ReachabilityReachable, reachableAt)
	first, err := fixture.store.SetPeerReachability(context.Background(), base)
	if err != nil || !first.Changed || first.Peer.Reachability != model.ReachabilityReachable ||
		!first.Peer.HasLastSeen || !first.Peer.LastSeenAt.Equal(reachableAt) {
		t.Fatalf("first reachable = (%#v,%v)", first, err)
	}
	replayed, err := fixture.store.SetPeerReachability(context.Background(), base)
	if err != nil || replayed.Changed || replayed.Peer != first.Peer {
		t.Fatalf("reachable replay = (%#v,%v), first %#v", replayed, err, first)
	}

	unreachable := fixture.spec(model.ReachabilityUnreachable, reachableAt.Add(time.Hour))
	negative, err := fixture.store.SetPeerReachability(context.Background(), unreachable)
	if err != nil || !negative.Changed || negative.Peer.Reachability != model.ReachabilityUnreachable ||
		!negative.Peer.LastSeenAt.Equal(reachableAt) {
		t.Fatalf("unreachable observation = (%#v,%v)", negative, err)
	}
	unreachableReplay := unreachable
	unreachableReplay.At = unreachable.At.Add(time.Hour)
	replayedNegative, err := fixture.store.SetPeerReachability(context.Background(), unreachableReplay)
	if err != nil || replayedNegative.Changed || !replayedNegative.Peer.LastSeenAt.Equal(reachableAt) {
		t.Fatalf("unreachable replay = (%#v,%v)", replayedNegative, err)
	}
	if _, err := fixture.store.SetPeerReachability(context.Background(), base); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("stale reachable callback error = %v", err)
	}

	newReachableAt := reachableAt.Add(3 * time.Hour)
	newReachable := fixture.spec(model.ReachabilityReachable, newReachableAt)
	if result, err := fixture.store.SetPeerReachability(context.Background(), newReachable); err != nil || !result.Changed || !result.Peer.LastSeenAt.Equal(newReachableAt) {
		t.Fatalf("new reachable observation = (%#v,%v)", result, err)
	}
	olderUnknown := fixture.spec(model.ReachabilityUnknown, newReachableAt.Add(-time.Second))
	if _, err := fixture.store.SetPeerReachability(context.Background(), olderUnknown); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("older negative callback error = %v", err)
	}

	wrongEpoch := fixture.spec(model.ReachabilityUnreachable, newReachableAt.Add(time.Hour))
	wrongEpoch.OriginEpoch, _ = model.ParseOriginEpoch("epoch-wrong-reachability")
	if _, err := fixture.store.SetPeerReachability(context.Background(), wrongEpoch); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("wrong binding epoch error = %v", err)
	}
	wrongDigest := model.Sum([]byte("wrong-reachability-roster"))
	wrongHead, err := model.NewRecordHead(fixture.channel.Roster().Head().Revision(), wrongDigest)
	if err != nil {
		t.Fatal(err)
	}
	wrongRoster := fixture.spec(model.ReachabilityUnreachable, newReachableAt.Add(time.Hour))
	wrongRoster.ExpectedRosterHead = wrongHead
	if _, err := fixture.store.SetPeerReachability(context.Background(), wrongRoster); !errors.Is(err, ErrChannelRuntimeConflict) {
		t.Fatalf("wrong roster head error = %v", err)
	}
	read, err := fixture.store.ReadPeerReachability(context.Background(),
		fixture.channel.Channel().ID(), fixture.remote.Identity().PeerID())
	if err != nil || read.Reachability != model.ReachabilityReachable ||
		!read.LastSeenAt.Equal(newReachableAt) || read.RosterHead != fixture.channel.Roster().Head() {
		t.Fatalf("ReadPeerReachability() = (%#v,%v)", read, err)
	}
}

func TestSetPeerReachabilityRejectsRevokedBinding(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, "reachability-revoked")
	remote := channel.AppendActive(t, "reachability-revoked-remote")
	terminal := channel.AppendTerminal(t, remote.Identity().PeerID(), model.MemberRevoked)
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoining)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), terminal, "former",
		model.BindingRevoked, model.ReachabilityUnknown, remote.Member().CreatedAt())
	_, err := st.SetPeerReachability(context.Background(), SetPeerReachabilitySpec{
		ChannelID: channel.Channel().ID(), PeerID: remote.Identity().PeerID(),
		OriginEpoch: remote.Identity().OriginEpoch(), ExpectedRosterHead: channel.Roster().Head(),
		Reachability: model.ReachabilityReachable, At: channel.Channel().UpdatedAt().Add(time.Hour)})
	if !errors.Is(err, ErrChannelRuntimeAuthority) {
		t.Fatalf("revoked reachability error = %v", err)
	}
	read, err := st.ReadPeerReachability(context.Background(), channel.Channel().ID(),
		remote.Identity().PeerID())
	if err != nil || read.BindingState != model.BindingRevoked || read.HasLastSeen {
		t.Fatalf("revoked read projection = (%#v,%v)", read, err)
	}
}

func TestSetPeerReachabilityConvergesUnderConcurrencyAndRestart(t *testing.T) {
	t.Parallel()
	fixture := newChannelReachabilityFixture(t, "reachability-concurrent")
	path := fixture.store.Path()
	if err := fixture.store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenExisting(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	const callers = 16
	type outcome struct {
		result SetPeerReachabilityResult
		err    error
	}
	outcomes := make(chan outcome, callers)
	var group sync.WaitGroup
	spec := fixture.spec(model.ReachabilityReachable, fixture.at)
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := reopened.SetPeerReachability(context.Background(), spec)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	group.Wait()
	close(outcomes)
	changed := 0
	for outcome := range outcomes {
		if outcome.err != nil || outcome.result.Peer.Reachability != model.ReachabilityReachable ||
			!outcome.result.Peer.LastSeenAt.Equal(fixture.at) {
			t.Fatalf("concurrent reachability = (%#v,%v)", outcome.result, outcome.err)
		}
		if outcome.result.Changed {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("concurrent reachability mutations = %d, want 1", changed)
	}
	read, err := reopened.ReadPeerReachability(context.Background(), fixture.channel.Channel().ID(),
		fixture.remote.Identity().PeerID())
	if err != nil || !read.HasLastSeen || !read.LastSeenAt.Equal(fixture.at) {
		t.Fatalf("restarted concurrent projection = (%#v,%v)", read, err)
	}
}

type channelReachabilityFixture struct {
	store   *Store
	channel *testkit.SignedChannel
	remote  testkit.MemberFixture
	at      time.Time
}

func newChannelReachabilityFixture(t *testing.T, seed string) channelReachabilityFixture {
	t.Helper()
	st := openTestStore(t)
	channel := testkit.NewSignedChannel(t, seed)
	remote := channel.AppendActive(t, seed+"-remote")
	insertChannelTestNode(t, st.db, channel.Owner(), channel.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, channel, model.TopicJoining)
	insertSignedPeerBinding(t, st.db, channel.Channel().ID(), remote, "remote",
		model.BindingPending, model.ReachabilityUnknown, remote.Member().CreatedAt())
	return channelReachabilityFixture{store: st, channel: channel, remote: remote,
		at: channel.Channel().UpdatedAt().Add(time.Hour)}
}

func (fixture channelReachabilityFixture) spec(reachability model.Reachability,
	at time.Time,
) SetPeerReachabilitySpec {
	return SetPeerReachabilitySpec{ChannelID: fixture.channel.Channel().ID(),
		PeerID: fixture.remote.Identity().PeerID(), OriginEpoch: fixture.remote.Identity().OriginEpoch(),
		ExpectedRosterHead: fixture.channel.Roster().Head(), Reachability: reachability, At: at}
}

type channelTopicTestRow struct {
	status  string
	topic   string
	updated string
}

func readChannelTopicRows(t *testing.T, st *Store) map[model.ChannelID]channelTopicTestRow {
	t.Helper()
	rows, err := st.db.Query(`SELECT channel_id,status,topic_state,updated_at FROM channels ORDER BY channel_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := make(map[model.ChannelID]channelTopicTestRow)
	for rows.Next() {
		var idText string
		var row channelTopicTestRow
		if err := rows.Scan(&idText, &row.status, &row.topic, &row.updated); err != nil {
			t.Fatal(err)
		}
		channelID, err := model.ParseChannelID(idText)
		if err != nil {
			t.Fatal(err)
		}
		result[channelID] = row
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func equalChannelTopicRows(left, right map[model.ChannelID]channelTopicTestRow) bool {
	if len(left) != len(right) {
		return false
	}
	for channelID, row := range left {
		if right[channelID] != row {
			return false
		}
	}
	return true
}

func channelTopicsByID(topics []ChannelTopicProjection) map[model.ChannelID]ChannelTopicProjection {
	result := make(map[model.ChannelID]ChannelTopicProjection, len(topics))
	for _, topic := range topics {
		result[topic.ChannelID] = topic
	}
	return result
}

func channelTopicTestTime(t testing.TB, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
