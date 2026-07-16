package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestGetReviewWorkRequiresAndLoadsExactlyTwoMembers(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertReviewFixture(t, st.db)
	home, _ := model.ParsePeerID("peer-home")
	workID, _ := model.ParseWorkID("work-one")
	ref, _ := model.NewWorkRef(home, workID)
	if _, err := st.GetReviewWork(context.Background(), ref); err == nil {
		t.Fatal("memberless Work was accepted")
	}
	for _, member := range []struct{ peer, role string }{{"peer-home", "initiator"}, {"peer-reviewer", "reviewer"}} {
		if _, err := st.db.Exec("INSERT INTO work_members(channel_id,home_peer_id,work_id,peer_id,role) VALUES('channel-one','peer-home','work-one',?,?)", member.peer, member.role); err != nil {
			t.Fatal(err)
		}
	}
	work, err := st.GetReviewWork(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if work.State() != model.WorkOffered || work.Participants().ReviewerPeerID().String() != "peer-reviewer" ||
		work.DeadlineUnixNano() != int64(1_800_000_000_000_000_000) {
		t.Fatalf("loaded Work = %#v", work)
	}
	if _, err := st.db.Exec("UPDATE works SET state_json=? WHERE home_peer_id='peer-home' AND work_id='work-one'",
		[]byte("{ }")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetReviewWork(context.Background(), ref); err == nil {
		t.Fatal("noncanonical durable Work state JSON was accepted")
	}
}

func TestWorkMutationConstructorsCloseCreationAndCASShape(t *testing.T) {
	t.Parallel()
	work := testReviewWorkValue(t, model.WorkOffered, 1, 1)
	if _, err := NewWorkCreation(work); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWorkTransition(work, 1, model.WorkOffered); err == nil {
		t.Fatal("transition accepted a non-next version")
	}
	active := testReviewWorkValue(t, model.WorkActive, 2, 1)
	transition, err := NewWorkTransition(active, 1, model.WorkOffered)
	if err != nil || transition.ExpectedVersion != 1 {
		t.Fatalf("NewWorkTransition() = (%#v, %v)", transition, err)
	}
	if _, err := NewWorkTransition(active, model.MaxSQLiteInteger, model.WorkOffered); err == nil {
		t.Fatal("transition accepted exhausted predecessor")
	}
}

func testReviewWorkValue(t *testing.T, state model.WorkState, version uint64, iteration uint8) model.ReviewWork {
	t.Helper()
	home, _ := model.ParsePeerID("peer-work-home")
	reviewer, _ := model.ParsePeerID("peer-work-reviewer")
	channel, _ := model.ParseChannelID("channel-work")
	workID, _ := model.ParseWorkID("work-value")
	ref, _ := model.NewWorkRef(home, workID)
	participants, _ := model.NewParticipantSnapshot(channel, 1, home, reviewer)
	stateJSON, _ := model.NewJSON([]byte(`{"state":"test"}`))
	eventID, _ := model.ParseEventID("event-work-update")
	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: ref, ChannelID: channel,
		Participants: participants, Version: version, Iteration: iteration,
		DeadlineUnixNano: now.Add(time.Hour).UnixNano(), State: state, StateData: stateJSON,
		UpdatedBy: eventID, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return work
}
