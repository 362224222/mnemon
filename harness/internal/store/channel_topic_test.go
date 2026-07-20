package store

import (
	"context"
	"errors"
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
