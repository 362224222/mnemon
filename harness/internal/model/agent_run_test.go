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
	runtimeStarted := now.Add(500 * time.Millisecond)
	wake := now.Add(time.Second)
	finished := wake.Add(time.Second)
	completed := finished
	runID, _ := ParseRunID("run-completion-evidence")
	cause, _ := NewJSON([]byte(`{"kind":"managed"}`))
	diagnostic, _ := NewJSON([]byte(`{"adapter":"codex-app-server"}`))
	runtimeIDs, _ := NewJSON([]byte(`{"process_id":42}`))
	wakeReceipt, _ := NewJSON([]byte(`{"hook_id":"hook-completion-evidence"}`))
	receipt, _ := NewJSON([]byte(`{"result":"finished"}`))
	base := AgentRunSpec{ID: runID, ProfileID: TeamworkProfileID(), Cause: cause,
		Launcher: "mnemond-wake", Runtime: RuntimeCodexAppServer, LauncherDiagnostic: diagnostic,
		RuntimeIDs: runtimeIDs, Status: AgentRunRuntimeFinished, RuntimeStartedAt: &runtimeStarted,
		WakeDeliveredAt: &wake, WakeReceipt: &wakeReceipt,
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

func TestAgentRunManagedRuntimeStartEvidenceIsExplicitAndCausal(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	runtimeStarted := now.Add(time.Second)
	wake := runtimeStarted.Add(time.Second)
	runID, _ := ParseRunID("run-managed-launch-evidence")
	cause, _ := NewJSON([]byte(`{"kind":"managed"}`))
	empty, _ := NewJSON([]byte(`{}`))
	diagnostic, _ := NewJSON([]byte(`{"adapter":"codex-app-server"}`))
	runtimeIDs, _ := NewJSON([]byte(`{"process_id":42}`))
	wakeReceipt, _ := NewJSON([]byte(`{"hook_id":"hook-managed-launch"}`))
	base := AgentRunSpec{ID: runID, ProfileID: TeamworkProfileID(), Cause: cause,
		Launcher: "mnemond-wake", Runtime: RuntimeCodexAppServer, LauncherDiagnostic: diagnostic,
		RuntimeIDs: runtimeIDs, Status: AgentRunRunning, RuntimeStartedAt: &runtimeStarted,
		WakeDeliveredAt: &wake, WakeReceipt: &wakeReceipt, StartedAt: now}
	run, err := NewAgentRun(base)
	if err != nil {
		t.Fatalf("NewAgentRun() error = %v", err)
	}
	if got, ok := run.RuntimeStartedAt(); !ok || !got.Equal(runtimeStarted) {
		t.Fatalf("Runtime start = (%s, %v)", got, ok)
	}

	missingStart := base
	missingStart.RuntimeStartedAt = nil
	if _, err := NewAgentRun(missingStart); !errors.Is(err, ErrInvariant) {
		t.Fatalf("running without Runtime start error = %v, want ErrInvariant", err)
	}
	starting := base
	starting.Status = AgentRunStarting
	starting.WakeDeliveredAt = nil
	starting.WakeReceipt = nil
	if _, err := NewAgentRun(starting); !errors.Is(err, ErrInvariant) {
		t.Fatalf("starting with Runtime start error = %v, want ErrInvariant", err)
	}
	missingEvidence := base
	missingEvidence.LauncherDiagnostic = empty
	if _, err := NewAgentRun(missingEvidence); !errors.Is(err, ErrInvariant) {
		t.Fatalf("Runtime start without evidence error = %v, want ErrInvariant", err)
	}
	beforeRun := base
	value := now.Add(-time.Nanosecond)
	beforeRun.RuntimeStartedAt = &value
	if _, err := NewAgentRun(beforeRun); !errors.Is(err, ErrInvariant) {
		t.Fatalf("Runtime start before Run error = %v, want ErrInvariant", err)
	}
	wakeBeforeStart := base
	value = runtimeStarted.Add(-time.Nanosecond)
	wakeBeforeStart.WakeDeliveredAt = &value
	if _, err := NewAgentRun(wakeBeforeStart); !errors.Is(err, ErrInvariant) {
		t.Fatalf("wake before Runtime start error = %v, want ErrInvariant", err)
	}
	missingWakeReceipt := base
	missingWakeReceipt.WakeReceipt = nil
	if _, err := NewAgentRun(missingWakeReceipt); !errors.Is(err, ErrInvariant) {
		t.Fatalf("wake without receipt error = %v, want ErrInvariant", err)
	}
	receiptWithoutWake := base
	receiptWithoutWake.WakeDeliveredAt = nil
	if _, err := NewAgentRun(receiptWithoutWake); !errors.Is(err, ErrInvariant) {
		t.Fatalf("wake receipt without time error = %v, want ErrInvariant", err)
	}
	attachedBeforeStart := base
	attachmentHash := Sum([]byte("attachment"))
	attachedBeforeStart.AttachmentTokenHash = &attachmentHash
	expires := wake.Add(time.Minute)
	attachedBeforeStart.AttachmentExpiresAt = &expires
	value = runtimeStarted.Add(-time.Nanosecond)
	attachedBeforeStart.AttachedAt = &value
	if _, err := NewAgentRun(attachedBeforeStart); !errors.Is(err, ErrInvariant) {
		t.Fatalf("attachment before Runtime start error = %v, want ErrInvariant", err)
	}
}
