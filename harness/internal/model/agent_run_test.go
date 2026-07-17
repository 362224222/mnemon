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
		Cause: cause, HandlingAttempt: 2, HandlingRecovery: 3, ClaimFenceHash: &fence, LeaseUntil: &lease,
		Launcher: "external", Runtime: RuntimeCodexAppServer,
		LauncherDiagnostic: empty, RuntimeIDs: empty, Status: AgentRunRunning, StartedAt: now,
	})
	if err != nil {
		t.Fatalf("NewAgentRun() error = %v", err)
	}
	gotHandling, hasHandling := run.HandlingID()
	gotFence, hasFence := run.ClaimFenceHash()
	gotLease, hasLease := run.LeaseUntil()
	if !hasHandling || gotHandling != handlingID || run.HandlingAttempt() != 2 || run.HandlingRecovery() != 3 ||
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
	bad.HandlingID = nil
	bad.HandlingRecovery = 1
	if _, err := NewAgentRun(bad); !errors.Is(err, ErrInvariant) {
		t.Fatalf("contextless recovery snapshot error = %v, want ErrInvariant", err)
	}
}

func TestAgentRunTerminalEvidenceIsCanonical(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	finished := now.Add(time.Minute)
	completed := finished.Add(time.Minute)
	runID, _ := ParseRunID("run-terminal")
	cause, _ := NewJSON([]byte(`{"kind":"external_initiate"}`))
	empty, _ := NewJSON([]byte(`{}`))
	receipt, _ := NewJSON([]byte(`{"result":"requeued"}`))
	spec := AgentRunSpec{ID: runID, ProfileID: TeamworkProfileID(), Cause: cause,
		Launcher: "external", Runtime: RuntimeCodexAppServer, LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: AgentRunRequeued, StartedAt: now, FinishedAt: &finished,
		CompletionAt: &completed, CompletionReceipt: &receipt, Error: "claim lease expired"}
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
	if got, ok := run.CompletionAt(); !ok || !got.Equal(completed) {
		t.Fatalf("completion time = (%s, %v)", got, ok)
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

func TestAgentRunCompletionEvidenceIsPairedAndCausal(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	wake := now.Add(time.Second)
	finished := wake.Add(time.Second)
	completed := finished
	runID, _ := ParseRunID("run-completion-evidence")
	cause, _ := NewJSON([]byte(`{"kind":"managed"}`))
	empty, _ := NewJSON([]byte(`{}`))
	receipt, _ := NewJSON([]byte(`{"result":"finished"}`))
	base := AgentRunSpec{ID: runID, ProfileID: TeamworkProfileID(), Cause: cause,
		Launcher: "mnemond-wake", Runtime: RuntimeCodexAppServer, LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: AgentRunRuntimeFinished, WakeDeliveredAt: &wake,
		StartedAt: now, FinishedAt: &finished, CompletionAt: &completed, CompletionReceipt: &receipt}
	if _, err := NewAgentRun(base); err != nil {
		t.Fatalf("NewAgentRun() error = %v", err)
	}

	missingTime := base
	missingTime.CompletionAt = nil
	if _, err := NewAgentRun(missingTime); !errors.Is(err, ErrInvariant) {
		t.Fatalf("receipt without completion time error = %v, want ErrInvariant", err)
	}
	missingReceipt := base
	missingReceipt.CompletionReceipt = nil
	if _, err := NewAgentRun(missingReceipt); !errors.Is(err, ErrInvariant) {
		t.Fatalf("completion time without receipt error = %v, want ErrInvariant", err)
	}
	unequalFinish := base
	value := finished.Add(time.Nanosecond)
	unequalFinish.CompletionAt = &value
	if _, err := NewAgentRun(unequalFinish); !errors.Is(err, ErrInvariant) {
		t.Fatalf("runtime finish and completion differ error = %v, want ErrInvariant", err)
	}
	beforeWake := base
	value = wake.Add(-time.Nanosecond)
	beforeWake.CompletionAt = &value
	if _, err := NewAgentRun(beforeWake); !errors.Is(err, ErrInvariant) {
		t.Fatalf("completion before wake error = %v, want ErrInvariant", err)
	}
	beforeFinish := base
	value = finished.Add(-time.Nanosecond)
	beforeFinish.CompletionAt = &value
	if _, err := NewAgentRun(beforeFinish); !errors.Is(err, ErrInvariant) {
		t.Fatalf("completion before finish error = %v, want ErrInvariant", err)
	}
	finishBeforeWake := base
	value = wake.Add(-time.Nanosecond)
	finishBeforeWake.FinishedAt = &value
	if _, err := NewAgentRun(finishBeforeWake); !errors.Is(err, ErrInvariant) {
		t.Fatalf("finish before wake error = %v, want ErrInvariant", err)
	}
}
