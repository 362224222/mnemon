package model

import (
	"errors"
	"testing"
	"time"
)

func TestAgentRunClaimSnapshotAndClosedStatus(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 12, 0, 0, 123, time.UTC)
	lease := now.Add(5 * time.Minute)
	runID, _ := ParseRunID("run-external-current")
	handlingID, _ := ParseHandlingID("handling-source")
	cause, _ := NewJSON([]byte(`{"kind":"external_current"}`))
	empty, _ := NewJSON([]byte(`{}`))
	fence := Sum([]byte("claim-secret"))

	run, err := NewAgentRun(AgentRunSpec{
		ID: runID, ProfileID: TeamworkProfileID(), HandlingID: &handlingID,
		Cause: cause, HandlingAttempt: 2, ClaimFenceHash: &fence, LeaseUntil: &lease,
		Launcher: "external", Runtime: RuntimeCodexAppServer,
		LauncherDiagnostic: empty, RuntimeIDs: empty, Status: AgentRunRunning, StartedAt: now,
	})
	if err != nil {
		t.Fatalf("NewAgentRun() error = %v", err)
	}
	gotHandling, hasHandling := run.HandlingID()
	gotFence, hasFence := run.ClaimFenceHash()
	gotLease, hasLease := run.LeaseUntil()
	if !hasHandling || gotHandling != handlingID || run.HandlingAttempt() != 2 ||
		!hasFence || gotFence != fence || !hasLease || !gotLease.Equal(lease) {
		t.Fatalf("claim snapshot was not preserved: %#v", run)
	}
	if !run.Status().OperationAuthority() || run.Status().Terminal() {
		t.Fatalf("running status classification is wrong")
	}
	if !AgentRunRuntimeFinished.OperationAuthority() || AgentRunRuntimeFinished.Terminal() {
		t.Fatalf("runtime_finished must remain nonterminal operation authority")
	}

	bad := AgentRunSpec{ID: runID, ProfileID: TeamworkProfileID(), Cause: cause,
		Launcher: "external", Runtime: RuntimeCodexAppServer, LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: AgentRunStatus("other"), StartedAt: now}
	if _, err := NewAgentRun(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("open status error = %v, want ErrInvalid", err)
	}
	bad.Status = AgentRunRunning
	bad.HandlingID = &handlingID
	if _, err := NewAgentRun(bad); !errors.Is(err, ErrInvariant) {
		t.Fatalf("partial claim snapshot error = %v, want ErrInvariant", err)
	}
}

func TestAgentRunTerminalEvidenceIsCanonical(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	finished := now.Add(time.Minute)
	runID, _ := ParseRunID("run-terminal")
	cause, _ := NewJSON([]byte(`{"kind":"external_initiate"}`))
	empty, _ := NewJSON([]byte(`{}`))
	receipt, _ := NewJSON([]byte(`{"result":"requeued"}`))
	spec := AgentRunSpec{ID: runID, ProfileID: TeamworkProfileID(), Cause: cause,
		Launcher: "external", Runtime: RuntimeCodexAppServer, LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: AgentRunRequeued, StartedAt: now, FinishedAt: &finished,
		CompletionReceipt: &receipt, Error: "claim lease expired"}
	run, err := NewAgentRun(spec)
	if err != nil {
		t.Fatalf("NewAgentRun() error = %v", err)
	}
	if !run.Status().Terminal() || run.Status().OperationAuthority() {
		t.Fatalf("terminal status classification is wrong")
	}
	if got, ok := run.CompletionReceipt(); !ok || got.String() != receipt.String() {
		t.Fatalf("completion receipt = (%s, %v)", got.String(), ok)
	}

	spec.FinishedAt = nil
	if _, err := NewAgentRun(spec); !errors.Is(err, ErrInvariant) {
		t.Fatalf("missing terminal finish error = %v, want ErrInvariant", err)
	}
	array, _ := NewJSON([]byte(`[]`))
	spec.FinishedAt, spec.Cause = &finished, array
	if _, err := NewAgentRun(spec); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-object cause error = %v, want ErrInvalid", err)
	}
}
