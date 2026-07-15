package model

import (
	"errors"
	"testing"
	"time"
)

func TestOperationEnumsAreClosed(t *testing.T) {
	t.Parallel()

	kinds := []OperationKind{OperationTeamworkOffer, OperationTeamworkAccept, OperationTeamworkDecline,
		OperationTeamworkDeliver, OperationTeamworkRework, OperationTeamworkClose, OperationTeamworkCancel,
		OperationResolveNoAction, OperationResolveRetry, OperationResolveReject}
	for _, kind := range kinds {
		if !kind.Valid() {
			t.Fatalf("OperationKind(%q).Valid() = false", kind)
		}
	}
	if OperationKind("event.emit").Valid() || OperationKind("memory.add").Valid() {
		t.Fatalf("generic/non-Teamwork operation kind was accepted")
	}
}

func TestOperationLeaseAndTerminalInvariants(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	lease := now.Add(time.Minute)
	id, _ := ParseOperationID("operation-a")
	runID, _ := ParseRunID("run-a")
	spec := OperationSpec{
		ID: id, ProfileID: TeamworkProfileID(), AgentRunID: runID,
		ClientKeyHash: Sum([]byte("client")), Kind: OperationTeamworkOffer,
		RequestDigest: Sum([]byte("request")), Status: OperationStarted,
		LeaseOwner: "worker-a", LeaseUntil: &lease, CreatedAt: now,
	}
	operation, err := NewOperation(spec)
	if err != nil {
		t.Fatalf("NewOperation(started) error = %v", err)
	}
	if operation.AgentRunID() != runID {
		t.Fatalf("AgentRunID() = %s, want %s", operation.AgentRunID().String(), runID.String())
	}
	missingRun := spec
	missingRun.AgentRunID = RunID{}
	if _, err := NewOperation(missingRun); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing AgentRun error = %v", err)
	}
	spec.LeaseOwner, spec.LeaseUntil = "", nil
	if _, err := NewOperation(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing lease error = %v", err)
	}
	result, _ := NewJSON([]byte(`{"status":"accepted"}`))
	finished := now.Add(time.Second)
	spec.Status, spec.Result, spec.FinishedAt = OperationCommitted, &result, &finished
	if _, err := NewOperation(spec); err != nil {
		t.Fatalf("NewOperation(committed) error = %v", err)
	}
	spec.Kind = OperationTeamworkAccept
	if _, err := NewOperation(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("contextless non-offer error = %v", err)
	}
}

func TestHandlingClaimAndDeadInvariants(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC)
	lease := now.Add(time.Minute)
	handlingID, _ := ParseHandlingID("handling-a")
	eventID, _ := ParseEventID("event-a")
	token := Sum([]byte("claim-token"))
	spec := HandlingSpec{handlingID, TeamworkProfileID(), eventID, HandlingClaimed, 0, now,
		"run-a", &token, &lease, 1, "", nil, "", 0, nil, now, now}
	handling, err := NewHandling(spec)
	if err != nil {
		t.Fatalf("NewHandling(claimed) error = %v", err)
	}
	if _, ok := handling.ClaimTokenHash(); !ok {
		t.Fatalf("claimed handling lost claim hash")
	}
	spec.Status, spec.ClaimOwner, spec.ClaimTokenHash, spec.LeaseUntil = HandlingPending, "", nil, nil
	if _, err := NewHandling(spec); err != nil {
		t.Fatalf("NewHandling(pending) error = %v", err)
	}
	spec.Status = HandlingDead
	if _, err := NewHandling(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("dead without dead_at error = %v", err)
	}
	deadBeforeCreation := now.Add(-time.Second)
	spec.DeadAt = &deadBeforeCreation
	if _, err := NewHandling(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("early dead_at error = %v", err)
	}
	deadAfterUpdate := now.Add(time.Second)
	spec.DeadAt = &deadAfterUpdate
	if _, err := NewHandling(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("late dead_at error = %v", err)
	}
}
