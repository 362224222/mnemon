package model

import (
	"errors"
	"testing"
	"time"
)

func TestNextReviewWorkStateIsClosedAndIterationBounded(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state     WorkState
		iteration uint8
		event     EventType
		want      WorkState
		wantIter  uint8
		ok        bool
	}{
		{WorkOffered, 1, EventReviewAccepted, WorkActive, 1, true},
		{WorkOffered, 1, EventReviewExpired, WorkExpired, 1, true},
		{WorkActive, 1, EventReviewDelivered, WorkDelivered, 1, true},
		{WorkDelivered, 1, EventReviewReworkRequested, WorkRework, 2, true},
		{WorkDelivered, 2, EventReviewReworkRequested, "", 0, false},
		{WorkDelivered, 2, EventReviewCancelled, WorkCancelled, 2, true},
		{WorkClosed, 1, EventReviewCancelled, "", 0, false},
		{WorkActive, 1, EventReviewAcceptRequested, "", 0, false},
	}
	for _, test := range tests {
		got, iteration, ok := NextReviewWorkState(test.state, test.iteration, test.event)
		if got != test.want || iteration != test.wantIter || ok != test.ok {
			t.Errorf("NextReviewWorkState(%s,%d,%s) = (%s,%d,%t), want (%s,%d,%t)",
				test.state, test.iteration, test.event, got, iteration, ok,
				test.want, test.wantIter, test.ok)
		}
	}
}

func TestWorkStateIsClosed(t *testing.T) {
	t.Parallel()

	states := []WorkState{WorkOffered, WorkActive, WorkDelivered, WorkRework,
		WorkClosed, WorkDeclined, WorkExpired, WorkCancelled}
	for _, state := range states {
		if !state.Valid() {
			t.Fatalf("WorkState(%q).Valid() = false", state)
		}
	}
	for _, state := range []WorkState{"", "STALLED", "MEMORY"} {
		if state.Valid() {
			t.Fatalf("WorkState(%q).Valid() = true", state)
		}
	}
	if !WorkClosed.Terminal() || WorkDelivered.Terminal() || !WorkRework.DeadlineEligible() {
		t.Fatalf("Work state classification mismatch")
	}
}

func TestParticipantSnapshotRejectsSelfReview(t *testing.T) {
	t.Parallel()

	channel, _ := ParseChannelID("channel-a")
	peer := mustPeer(t, "peer-a")
	if _, err := NewParticipantSnapshot(channel, 1, peer, peer); !errors.Is(err, ErrInvariant) {
		t.Fatalf("self-review error = %v, want ErrInvariant", err)
	}
}

func TestReviewWorkInvariants(t *testing.T) {
	t.Parallel()

	home := mustPeer(t, "peer-home")
	reviewer := mustPeer(t, "peer-reviewer")
	channel, _ := ParseChannelID("channel-a")
	snapshot, _ := NewParticipantSnapshot(channel, 1, home, reviewer)
	event := mustWorkEvent(t, home, home, reviewer, EventReviewOffered)
	state, _ := NewJSON([]byte(`{"goal":"review"}`))
	now := event.AcceptedAt()
	spec := ReviewWorkSpec{event.Scope().WorkRef(), channel, snapshot, 1, 1,
		now.Add(24 * time.Hour).UnixNano(), WorkOffered, state, event.ID(), now}

	work, err := NewReviewWork(spec)
	if err != nil {
		t.Fatalf("NewReviewWork() error = %v", err)
	}
	if work.Ref() != spec.Ref || work.Participants().ReviewerPeerID() != reviewer || work.Deadline().UnixNano() != spec.DeadlineUnixNano {
		t.Fatalf("ReviewWork getters mismatch")
	}
	if err := work.ValidateUpdateEvent(event); err != nil {
		t.Fatalf("ValidateUpdateEvent() error = %v", err)
	}

	bad := spec
	bad.ChannelID, _ = ParseChannelID("channel-b")
	if _, err := NewReviewWork(bad); !errors.Is(err, ErrInvariant) {
		t.Fatalf("scope mismatch error = %v", err)
	}
	bad = spec
	bad.State = WorkActive
	activeWork, err := NewReviewWork(bad)
	if err != nil {
		t.Fatalf("NewReviewWork(active) error = %v", err)
	}
	if err := activeWork.ValidateUpdateEvent(event); !errors.Is(err, ErrInvariant) {
		t.Fatalf("state/Event mismatch error = %v", err)
	}
	bad = spec
	bad.Version = 2
	if _, err := NewReviewWork(bad); !errors.Is(err, ErrInvariant) {
		t.Fatalf("initial version error = %v", err)
	}
	for _, unreachable := range []WorkState{WorkActive, WorkDeclined} {
		bad = spec
		bad.State = unreachable
		bad.Version = 3
		bad.Iteration = 2
		if _, err := NewReviewWork(bad); !errors.Is(err, ErrInvariant) {
			t.Fatalf("%s iteration 2 error = %v", unreachable, err)
		}
	}
}

func mustWorkEvent(t *testing.T, origin, home, audience PeerID, kind EventType) Event {
	t.Helper()
	spec := validEventSpec(t, mustEventScope(t, origin, home), kind, audience)
	event, err := NewEvent(spec)
	if err != nil {
		t.Fatalf("NewEvent(): %v", err)
	}
	return event
}
