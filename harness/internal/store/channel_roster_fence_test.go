package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMergeChannelRosterExpectedHeadReplaysExactResultBeforeRejectingStaleFork(t *testing.T) {
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "expected-roster-head")
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	expectedHead := fixture.Roster().Head()
	accepted := fixture.AppendActive(t, "expected-roster-head-accepted")
	spec := MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{accepted.Member()}, ExpectedRosterHead: expectedHead,
		At: accepted.Member().CreatedAt(),
	}
	applied, err := st.MergeChannelRoster(context.Background(), spec)
	if err != nil || applied.Status != ChannelRosterApplied {
		t.Fatalf("fenced roster apply = (%#v,%v)", applied, err)
	}
	replayed, err := st.MergeChannelRoster(context.Background(), spec)
	if err != nil || replayed.Status != ChannelRosterDuplicate ||
		replayed.Roster.Head() != accepted.Member().Head() {
		t.Fatalf("stale-fence exact replay = (%#v,%v)", replayed, err)
	}
	spec.AuthenticatedTransportPeerID = testkit.NewIdentity(t,
		"expected-roster-head-outsider").PeerID()
	if _, err := st.MergeChannelRoster(context.Background(), spec); !errors.Is(
		err, ErrChannelRosterInput) {
		t.Fatalf("outsider exact replay error = %v", err)
	}
	spec.AuthenticatedTransportPeerID = fixture.Owner().PeerID()

	challenger := ownerConflictChallenger(t, fixture,
		accepted.Member().CreatedAt().Add(time.Second))
	spec.Records, spec.At = []model.Member{challenger}, challenger.CreatedAt()
	if _, err := st.MergeChannelRoster(context.Background(), spec); !errors.Is(
		err, ErrChannelRosterConflict) {
		t.Fatalf("stale-fence fork error = %v", err)
	}
	assertRosterFenceRejectedWithoutConflictEvidence(t, st, fixture.Channel().ID(),
		accepted.Member().Head())
}

func assertRosterFenceRejectedWithoutConflictEvidence(t *testing.T, st *Store,
	channelID model.ChannelID, expectedHead model.RecordHead,
) {
	t.Helper()
	var status string
	var revision uint64
	var digest []byte
	var conflicts int
	if err := st.db.QueryRow(`SELECT status,roster_head_revision,roster_head_hash
		FROM channels WHERE channel_id=?`, channelID.String()).Scan(&status, &revision, &digest); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_conflicts
		WHERE channel_id=?`, channelID.String()).Scan(&conflicts); err != nil {
		t.Fatal(err)
	}
	durableDigest, err := model.DigestFromBytes(digest)
	if err != nil || status != string(model.ChannelActive) || conflicts != 0 ||
		revision != expectedHead.Revision() || durableDigest != expectedHead.Digest() {
		t.Fatalf("stale fence durable state = status %q head %d/%v conflicts %d err %v",
			status, revision, durableDigest, conflicts, err)
	}
}
