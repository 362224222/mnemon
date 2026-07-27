package model

import (
	"errors"
	"testing"
	"time"
)

func TestWorkDerivationIdentity(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	operation, _ := ParseOperationID("operation-a")
	parentEvent, _ := ParseEventID("event-parent")
	parentID, _ := ParseWorkID("work-parent")
	childID, _ := ParseWorkID("work-child")
	peer, channel := mustPeer(t, "peer-a"), mustChannelID(t, "channel-a")
	parent, _ := NewWorkRef(peer, parentID)
	child, _ := NewWorkRef(peer, childID)
	spec := WorkDerivationSpec{OperationID: operation, ChildChannelID: channel, Child: child,
		ParentChannelID: channel, Parent: parent, ParentVersion: 2,
		ParentEventID: parentEvent, CreatedAt: now}
	if _, err := NewWorkDerivation(spec); err != nil {
		t.Fatalf("NewWorkDerivation() error = %v", err)
	}
	spec.Child = parent
	if _, err := NewWorkDerivation(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("self derivation error = %v", err)
	}
}

func TestWorkDerivationRequiresCommittedContextOffer(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	operationID, _ := ParseOperationID("operation-a")
	parentEvent, _ := ParseEventID("event-parent")
	parentID, _ := ParseWorkID("work-parent")
	childID, _ := ParseWorkID("work-child")
	peer, channel := mustPeer(t, "peer-a"), mustChannelID(t, "channel-a")
	parent, _ := NewWorkRef(peer, parentID)
	child, _ := NewWorkRef(peer, childID)
	derivation, _ := NewWorkDerivation(WorkDerivationSpec{OperationID: operationID,
		ChildChannelID: channel, Child: child, ParentChannelID: channel, Parent: parent,
		ParentVersion: 2, ParentEventID: parentEvent, CreatedAt: now})

	context := Sum([]byte("context"))
	result, _ := NewJSON([]byte(`{"status":"accepted"}`))
	finished := now.Add(time.Second)
	runID, _ := ParseRunID("run-derivation")
	operation, err := NewOperation(OperationSpec{
		ID: operationID, ProfileID: TeamworkProfileID(), AgentRunID: runID,
		ClientKeyHash: Sum([]byte("client")), ContextHash: &context, Kind: OperationTeamworkOffer,
		RequestDigest: Sum([]byte("request")), Status: OperationCommitted, Result: &result,
		CreatedAt: now, FinishedAt: &finished,
	})
	if err != nil {
		t.Fatalf("NewOperation() error = %v", err)
	}
	if err := derivation.ValidateOperation(operation); err != nil {
		t.Fatalf("ValidateOperation() error = %v", err)
	}
}

func TestWorkDerivationParentSourceIsExactWorkUpdateEvent(t *testing.T) {
	t.Parallel()
	home := mustPeer(t, "peer-parent-home")
	reviewer := mustPeer(t, "peer-parent-reviewer")
	scope := mustEventScope(t, home, home)
	eventSpec := validEventSpec(t, scope, EventReviewAccepted, reviewer)
	eventSpec.ID, _ = ParseEventID("event-parent-current")
	currentEvent, err := NewEvent(eventSpec)
	if err != nil {
		t.Fatal(err)
	}
	participants, _ := NewParticipantSnapshot(scope.ChannelID(), 1, home, reviewer)
	state, _ := NewJSON([]byte(`{"accepted":true}`))
	parent, err := NewReviewWork(ReviewWorkSpec{Ref: scope.WorkRef(), ChannelID: scope.ChannelID(),
		Participants: participants, Version: 2, Iteration: 1,
		DeadlineUnixNano: currentEvent.AcceptedAt().Add(time.Hour).UnixNano(), State: WorkActive,
		StateData: state, UpdatedBy: currentEvent.ID(), UpdatedAt: currentEvent.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	operationID, _ := ParseOperationID("operation-parent-source")
	childID, _ := ParseWorkID("work-parent-child")
	child, _ := NewWorkRef(reviewer, childID)
	derivation, err := NewWorkDerivation(WorkDerivationSpec{OperationID: operationID,
		ChildChannelID: scope.ChannelID(), Child: child,
		ParentChannelID: scope.ChannelID(), Parent: parent.Ref(), ParentVersion: parent.Version(),
		ParentEventID: currentEvent.ID(), CreatedAt: currentEvent.AcceptedAt()})
	if err != nil {
		t.Fatal(err)
	}
	if err := derivation.ValidateParent(parent, currentEvent); err != nil {
		t.Fatalf("ValidateParent() error = %v", err)
	}

	oldID, _ := ParseEventID("event-parent-old")
	staleSpec := derivation.spec
	staleSpec.ParentEventID = oldID
	stale, _ := NewWorkDerivation(staleSpec)
	if err := stale.ValidateParent(parent, currentEvent); !errors.Is(err, ErrInvariant) {
		t.Fatalf("stale parent source error = %v", err)
	}
}

func mustChannelID(t *testing.T, value string) ChannelID {
	t.Helper()
	channel, err := ParseChannelID(value)
	if err != nil {
		t.Fatalf("ParseChannelID(%q): %v", value, err)
	}
	return channel
}
