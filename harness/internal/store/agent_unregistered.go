package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrAgentUnregisteredRunInput     = errors.New("invalid unregistered Agent Run input")
	ErrAgentUnregisteredRunStale     = errors.New("unregistered Agent Run authority is stale")
	ErrAgentUnregisteredRunInvariant = errors.New("unregistered Agent Run durable invariant violated")
)

// AgentUnregisteredRunSpec identifies a wake preclaim whose process identity
// was never committed. It carries no process-exit receipt: this transition
// closes only the Run/Handling semantic authority and never claims that an OS
// process exited.
type AgentUnregisteredRunSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimFenceHash        model.Digest
	HandlingRecovery      uint32
	Error                 string
	At                    time.Time
}

// AbandonUnregisteredAgentRun closes the crash gap between preclaim and
// RecordAgentRuntimeLaunch. The adapter sends no protocol bytes before launch
// evidence commits, so this exact starting Run has never received managed-work
// authority. A possibly lingering idle child remains outside completion
// evidence and cannot justify another process mutation during restart.
func (s *Store) AbandonUnregisteredAgentRun(ctx context.Context,
	spec AgentUnregisteredRunSpec,
) (AgentRuntimeTransitionResult, error) {
	at, err := validateAgentUnregisteredRunSpec(s, ctx, spec)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("abandon unregistered Agent Run: begin: %w", err)
	}
	defer tx.Rollback()
	authoritySpec := agentRuntimeAuthoritySpec{profileID: spec.ProfileID,
		expectedAssetRevision: spec.ExpectedAssetRevision, runID: spec.RunID,
		claimFenceHash: spec.ClaimFenceHash, handlingRecovery: spec.HandlingRecovery, at: at}
	authority, err := readAgentRuntimeAuthority(ctx, tx, authoritySpec, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, mapAgentUnregisteredRunAuthorityError(err)
	}
	if err := requireUnregisteredAgentRunEvidence(authority.run); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if authority.run.Status().Terminal() {
		return replayUnregisteredAgentRun(tx, authority, spec, at)
	}
	if authority.run.Status() != model.AgentRunStarting {
		return AgentRuntimeTransitionResult{}, ErrAgentUnregisteredRunStale
	}
	if err := requireExactActiveOrphan(authority, spec.ClaimFenceHash, at); err != nil {
		return AgentRuntimeTransitionResult{}, mapAgentUnregisteredRunAuthorityError(err)
	}
	if err := requireNoUnregisteredAgentRunOperations(ctx, tx, authority.run, at); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}

	dead := authority.handling.Attempts() >= uint32(authority.budget.Spec().MaxAttempts)
	runStatus := model.AgentRunFailed
	handlingStatus, disposition := model.HandlingPending, "runtime_unregistered"
	availableAt, deadAt := authority.handling.AvailableAt(), any(nil)
	if dead {
		runStatus, handlingStatus, disposition = model.AgentRunDead, model.HandlingDead,
			"attempt_budget_exhausted"
		deadAt = storeTime(at)
	} else {
		availableAt, err = agentClaimRetryAt(at, authority.handling.Attempts(), authority.budget)
		if err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: retry backoff: %v",
				ErrAgentUnregisteredRunInvariant, err)
		}
	}
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	runResult, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status=?,finished_at=?,error=?
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
		AND launcher='mnemond-wake' AND runtime_kind=? AND status='starting'
		AND launcher_diagnostic_json=? AND runtime_ids_json=?
		AND runtime_started_at IS NULL AND attached_at IS NULL AND wake_delivered_at IS NULL
		AND wake_receipt_json IS NULL AND current_read_receipt_json IS NULL
		AND outcome_receipt_json IS NULL AND completion_at IS NULL
		AND completion_receipt_json IS NULL AND finished_at IS NULL AND error IS NULL`,
		string(runStatus), storeTime(at), spec.Error, authority.run.ID().String(),
		authority.profile.ID().String(), handlingID.String(), authority.run.HandlingAttempt(),
		authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(), storeTime(lease),
		string(authority.profile.Runtime()), []byte(`{}`), []byte(`{}`))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("abandon unregistered Agent Run: finish Run: %w", err)
	}
	if err := requireExactlyOneRow(runResult, "abandon unregistered Agent Run: Run fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentUnregisteredRunStale, err)
	}
	claimHash, _ := authority.handling.ClaimTokenHash()
	handlingLease, _ := authority.handling.LeaseUntil()
	handlingResult, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status=?,available_at=?,
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition=?,outcome_event_id=NULL,
		last_error=?,dead_at=?,updated_at=? WHERE handling_id=? AND profile_id=? AND event_id=?
		AND status='claimed' AND claim_owner=? AND claim_token_hash=? AND lease_until=? AND attempts=?
		AND recovery_count=? AND updated_at=?`, string(handlingStatus), storeTime(availableAt), disposition,
		spec.Error, deadAt, storeTime(at), authority.handling.ID().String(), authority.profile.ID().String(),
		authority.handling.EventID().String(), authority.handling.ClaimOwner(), claimHash.Bytes(),
		storeTime(handlingLease), authority.handling.Attempts(), authority.handling.RecoveryCount(),
		storeTime(authority.handling.UpdatedAt()))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("abandon unregistered Agent Run: settle Handling: %w", err)
	}
	if err := requireExactlyOneRow(handlingResult,
		"abandon unregistered Agent Run: Handling fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentUnregisteredRunStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: terminal Run cannot be read",
			ErrAgentUnregisteredRunInvariant)
	}
	authority.handling, err = readAgentHandling(ctx, tx, authority.handling.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: settled Handling cannot be read",
			ErrAgentUnregisteredRunInvariant)
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied,
		"abandon unregistered Agent Run: commit")
}

func validateAgentUnregisteredRunSpec(s *Store, ctx context.Context,
	spec AgentUnregisteredRunSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ProfileID != model.TeamworkProfileID() ||
		spec.ExpectedAssetRevision == "" || spec.RunID.IsZero() || spec.ClaimFenceHash.IsZero() {
		return time.Time{}, ErrAgentUnregisteredRunInput
	}
	if err := validateAgentRuntimeError(spec.Error); err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid failure diagnostic", ErrAgentUnregisteredRunInput)
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrAgentUnregisteredRunInput, err)
	}
	return at, nil
}

func requireUnregisteredAgentRunEvidence(run model.AgentRun) error {
	_, attached := run.AttachedAt()
	_, runtimeStarted := run.RuntimeStartedAt()
	_, wakeDelivered := run.WakeDeliveredAt()
	_, wakeReceipt := run.WakeReceipt()
	_, currentRead := run.CurrentReadReceipt()
	_, outcome := run.OutcomeReceipt()
	_, completionAt := run.CompletionAt()
	_, completion := run.CompletionReceipt()
	if run.Launcher() != "mnemond-wake" || run.LauncherDiagnostic().String() != `{}` ||
		run.RuntimeIDs().String() != `{}` || attached || runtimeStarted || wakeDelivered || wakeReceipt ||
		currentRead || outcome || completionAt || completion {
		return ErrAgentUnregisteredRunInvariant
	}
	return nil
}

func requireNoUnregisteredAgentRunOperations(ctx context.Context, tx *sql.Tx,
	run model.AgentRun, at time.Time,
) error {
	var operations int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations
		WHERE agent_run_id=? AND profile_id=?)`, run.ID().String(), run.ProfileID().String()).Scan(&operations); err != nil {
		return fmt.Errorf("abandon unregistered Agent Run: inspect operations: %w", err)
	}
	if operations == 1 {
		return fmt.Errorf("%w: unregistered Run already authorized an operation",
			ErrAgentUnregisteredRunInvariant)
	}
	if at.Before(run.StartedAt()) {
		return fmt.Errorf("%w: trusted time precedes Run", ErrAgentUnregisteredRunInvariant)
	}
	return nil
}

func replayUnregisteredAgentRun(tx *sql.Tx, authority agentRuntimeAuthority,
	spec AgentUnregisteredRunSpec, at time.Time,
) (AgentRuntimeTransitionResult, error) {
	finishedAt, finished := authority.run.FinishedAt()
	wantDead := authority.run.Status() == model.AgentRunDead
	wantFailed := authority.run.Status() == model.AgentRunFailed
	if !finished || finishedAt.After(at) || authority.run.Error() != spec.Error ||
		(!wantDead && !wantFailed) {
		return AgentRuntimeTransitionResult{}, ErrAgentUnregisteredRunInvariant
	}
	handlingID, hasHandling := authority.run.HandlingID()
	if !hasHandling || authority.handling.ID() != handlingID ||
		authority.handling.ProfileID() != authority.run.ProfileID() ||
		authority.handling.RecoveryCount() < authority.run.HandlingRecovery() ||
		(authority.handling.RecoveryCount() == authority.run.HandlingRecovery() &&
			authority.handling.Attempts() < authority.run.HandlingAttempt()) ||
		authority.handling.UpdatedAt().Before(finishedAt) {
		return AgentRuntimeTransitionResult{}, ErrAgentUnregisteredRunInvariant
	}
	// When the Handling is still the exact snapshot written by abandonment,
	// verify that pair in full. Once a later attempt, budget reseal, semantic
	// winner, or explicit dead recovery has advanced the Handling, the terminal
	// Run is the immutable replay record and the newer Handling must be returned
	// without being overwritten.
	if authority.handling.RecoveryCount() == authority.run.HandlingRecovery() &&
		authority.handling.Attempts() == authority.run.HandlingAttempt() &&
		authority.handling.UpdatedAt().Equal(finishedAt) {
		originalDead := wantDead && authority.handling.Status() == model.HandlingDead &&
			authority.handling.LastDisposition() == "attempt_budget_exhausted" &&
			authority.handling.LastError() == spec.Error
		originalFailed := wantFailed && authority.handling.Status() == model.HandlingPending &&
			authority.handling.LastDisposition() == "runtime_unregistered" &&
			authority.handling.LastError() == spec.Error
		if !originalDead && !originalFailed {
			return AgentRuntimeTransitionResult{}, ErrAgentUnregisteredRunInvariant
		}
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeReplayed,
		"abandon unregistered Agent Run: replay")
}

func mapAgentUnregisteredRunAuthorityError(err error) error {
	if errors.Is(err, ErrAgentRuntimeStale) || errors.Is(err, ErrAgentRuntimeOrphanStale) {
		return ErrAgentUnregisteredRunStale
	}
	return fmt.Errorf("%w: %v", ErrAgentUnregisteredRunInvariant, err)
}
