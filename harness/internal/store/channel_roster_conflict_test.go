package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelRosterMergePlanRepresentsConflictCandidate(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	fixture := testkit.NewSignedChannel(t, "roster-conflict-plan")
	fixture.AppendActiveUpdate(t, fixture.Owner().PeerID())
	insertChannelTestNode(t, st.db, fixture.Owner(), fixture.Channel().CreatedAt())
	insertSignedChannelFixture(t, st.db, fixture, model.TopicJoined)
	challenger := ownerConflictChallenger(t, fixture, fixture.Channel().UpdatedAt().Add(time.Second))
	plan, err := st.PrepareChannelRosterMerge(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{challenger}, At: challenger.CreatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChannelRosterConflictPlan(t, plan, true)
	assertChannelRosterConflictCount(t, st, 0)
	if _, err := st.CommitChannelRosterMerge(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	assertChannelRosterPlanResolution(t, st, plan, ChannelAuthorityPlanCandidate, "committed conflict")
	second := ownerConflictChallenger(t, fixture, challenger.CreatedAt().Add(time.Second))
	secondPlan, err := st.PrepareChannelRosterMerge(context.Background(), MergeChannelRosterSpec{
		ChannelID: fixture.Channel().ID(), AuthenticatedTransportPeerID: fixture.Owner().PeerID(),
		Records: []model.Member{second}, At: second.CreatedAt(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertChannelRosterConflictPlan(t, secondPlan, false)
	assertChannelRosterPlanResolution(t, st, secondPlan, ChannelAuthorityPlanUnchanged, "evidence-only preimage")
	if _, err := st.CommitChannelRosterMerge(context.Background(), secondPlan); err != nil {
		t.Fatal(err)
	}
	assertChannelRosterPlanResolution(t, st, secondPlan, ChannelAuthorityPlanCandidate, "evidence-only candidate")
	if _, err := st.CommitChannelRosterMerge(context.Background(), secondPlan); err != nil {
		t.Fatal(err)
	}
	assertChannelRosterConflictCount(t, st, 2)
}

func assertChannelRosterConflictPlan(t *testing.T, plan ChannelRosterMergePlan, changes bool) {
	t.Helper()
	channels := plan.Candidate().Channels()
	if plan.ChangesAuthority() != changes || plan.Result().Status != ChannelRosterConflicted ||
		len(channels) != 1 || channels[0].Channel().Status() != model.ChannelConflicted {
		t.Fatalf("prepared conflict plan = (%#v,%#v), changes=%t", plan.Result(), channels, plan.ChangesAuthority())
	}
}

func assertChannelRosterConflictCount(t *testing.T, st *Store, want int) {
	t.Helper()
	var count int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM channel_conflicts`).Scan(&count); err != nil || count != want {
		t.Fatalf("conflict evidence count = (%d,%v), want %d", count, err, want)
	}
}
