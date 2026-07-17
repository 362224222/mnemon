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
	ErrAgentRuntimeOrphanInput     = errors.New("invalid Agent Runtime orphan input")
	ErrAgentRuntimeOrphanStale     = errors.New("Agent Runtime orphan authority is stale")
	ErrAgentRuntimeOrphanInvariant = errors.New("Agent Runtime orphan durable invariant violated")
)

// MaxIncompleteManagedAgentRuns is the fail-closed startup recovery bound.
// A Node cannot normally accumulate this many unfinished T0 Runtime attempts;
// crossing the bound therefore requires operator inspection instead of a
// partial recovery that could silently leave an old process authoritative.
const MaxIncompleteManagedAgentRuns = 64

// AgentRuntimeOrphanSpec carries only Store authority and caller-produced
// process-exit evidence. The caller must prove that the exact Runtime process
// represented by RuntimeIDs has exited before invoking this transition; Store
// deliberately does not inspect PIDs or send signals.
type AgentRuntimeOrphanSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimFenceHash        model.Digest
	HandlingRecovery      uint32
	CompletionReceipt     model.JSON
	Error                 string
	At                    time.Time
}

// ListIncompleteManagedAgentRuns returns the complete bounded startup workset
// in deterministic creation order. It fails closed rather than returning a
// truncated prefix when durable unfinished Runtime evidence exceeds the fixed
// recovery bound.
func (s *Store) ListIncompleteManagedAgentRuns(ctx context.Context) ([]model.AgentRun, error) {
	if s == nil || s.db == nil || ctx == nil {
		return nil, ErrAgentRuntimeOrphanInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("list incomplete managed AgentRuns: begin: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT run_id FROM agent_runs
		WHERE launcher='mnemond-wake' AND completion_receipt_json IS NULL
		ORDER BY started_at,run_id LIMIT ?`, MaxIncompleteManagedAgentRuns+1)
	if err != nil {
		return nil, fmt.Errorf("list incomplete managed AgentRuns: query: %w", err)
	}
	ids := make([]model.RunID, 0, MaxIncompleteManagedAgentRuns+1)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: scan Run ID: %v", ErrAgentRuntimeOrphanInvariant, err)
		}
		id, err := model.ParseRunID(text)
		if err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("%w: invalid Run ID: %v", ErrAgentRuntimeOrphanInvariant, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("list incomplete managed AgentRuns: rows: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("list incomplete managed AgentRuns: close rows: %w", err)
	}
	if len(ids) > MaxIncompleteManagedAgentRuns {
		return nil, fmt.Errorf("%w: more than %d unfinished managed Runtime attempts",
			ErrAgentRuntimeOrphanInvariant, MaxIncompleteManagedAgentRuns)
	}
	runs := make([]model.AgentRun, 0, len(ids))
	for _, id := range ids {
		run, err := readAgentRun(ctx, tx, id)
		if err != nil || run.Launcher() != "mnemond-wake" {
			return nil, fmt.Errorf("%w: listed Run cannot be reconstructed",
				ErrAgentRuntimeOrphanInvariant)
		}
		if _, complete := run.CompletionReceipt(); complete {
			return nil, fmt.Errorf("%w: listed Run is already complete",
				ErrAgentRuntimeOrphanInvariant)
		}
		runs = append(runs, run)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("list incomplete managed AgentRuns: commit: %w", err)
	}
	return runs, nil
}

// SettleOrphanedAgentRuntime records the caller's exact process-exit receipt.
// An active durable claim is failed atomically. A claim already settled by
// lease expiry or by a semantic outcome keeps that winning lifecycle and only
// receives the independent Runtime completion/error evidence.
func (s *Store) SettleOrphanedAgentRuntime(ctx context.Context,
	spec AgentRuntimeOrphanSpec,
) (AgentRuntimeTransitionResult, error) {
	at, err := validateAgentRuntimeOrphanSpec(s, ctx, spec)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("settle orphaned Agent Runtime: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readAgentRuntimeOrphanAuthority(ctx, tx, spec, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if receipt, complete := authority.run.CompletionReceipt(); complete {
		completionAt, hasCompletionAt := authority.run.CompletionAt()
		if !hasCompletionAt || completionAt.After(at) ||
			receipt.String() != spec.CompletionReceipt.String() || authority.run.Error() != spec.Error {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completion replay evidence differs",
				ErrAgentRuntimeOrphanInvariant)
		}
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeReplayed,
			"settle orphaned Agent Runtime: replay")
	}
	if finishedAt, finished := authority.run.FinishedAt(); finished && at.Before(finishedAt) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completion precedes Run finish",
			ErrAgentRuntimeOrphanInvariant)
	}
	if runtimeStartedAt, started := authority.run.RuntimeStartedAt(); started && at.Before(runtimeStartedAt) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completion precedes Runtime start",
			ErrAgentRuntimeOrphanInvariant)
	}
	if wakeAt, delivered := authority.run.WakeDeliveredAt(); delivered && at.Before(wakeAt) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completion precedes wake delivery",
			ErrAgentRuntimeOrphanInvariant)
	}

	switch {
	case agentRuntimeSemanticSettled(authority.run):
		return appendOrphanCompletion(ctx, tx, authority, spec, at, true)
	case agentRuntimeLeaseSettled(authority.run):
		return appendOrphanCompletion(ctx, tx, authority, spec, at, false)
	case authority.run.Status() == model.AgentRunStarting ||
		authority.run.Status() == model.AgentRunRunning:
		return failActiveOrphan(ctx, tx, authority, spec, at)
	default:
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: unfinished Run has no recoverable lifecycle",
			ErrAgentRuntimeOrphanInvariant)
	}
}

func validateAgentRuntimeOrphanSpec(s *Store, ctx context.Context,
	spec AgentRuntimeOrphanSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ProfileID != model.TeamworkProfileID() ||
		spec.ExpectedAssetRevision == "" || spec.RunID.IsZero() || spec.ClaimFenceHash.IsZero() {
		return time.Time{}, ErrAgentRuntimeOrphanInput
	}
	if err := validateAgentRuntimeObject("completion receipt", spec.CompletionReceipt); err != nil ||
		spec.CompletionReceipt.String() == `{}` {
		return time.Time{}, fmt.Errorf("%w: completion receipt must be a non-empty canonical object",
			ErrAgentRuntimeOrphanInput)
	}
	if err := validateAgentRuntimeError(spec.Error); err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid Runtime error", ErrAgentRuntimeOrphanInput)
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrAgentRuntimeOrphanInput, err)
	}
	return at, nil
}

func readAgentRuntimeOrphanAuthority(ctx context.Context, tx *sql.Tx,
	spec AgentRuntimeOrphanSpec, at time.Time,
) (agentRuntimeAuthority, error) {
	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID,
		spec.ExpectedAssetRevision)
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: active Profile authority differs",
			ErrAgentRuntimeOrphanStale)
	}
	node, err := readNode(ctx, tx)
	if err != nil || at.Before(profile.UpdatedAt()) || at.Before(node.UpdatedAt()) {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: trusted time precedes active authority",
			ErrAgentRuntimeOrphanInvariant)
	}
	run, err := readAgentRun(ctx, tx, spec.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return agentRuntimeAuthority{}, ErrAgentRuntimeOrphanStale
	}
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: invalid AgentRun evidence",
			ErrAgentRuntimeOrphanInvariant)
	}
	handlingID, hasHandling := run.HandlingID()
	fence, hasFence := run.ClaimFenceHash()
	lease, hasLease := run.LeaseUntil()
	attachment, hasAttachment := run.AttachmentTokenHash()
	expiresAt, hasExpiry := run.AttachmentExpiresAt()
	if !hasHandling || !hasFence || !hasLease || !hasAttachment || !hasExpiry ||
		run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() ||
		run.Launcher() != "mnemond-wake" || run.HandlingRecovery() != spec.HandlingRecovery ||
		!sameCurrentDigest(fence, spec.ClaimFenceHash) || !sameCurrentDigest(attachment, fence) ||
		!expiresAt.Equal(lease) {
		return agentRuntimeAuthority{}, ErrAgentRuntimeOrphanStale
	}
	if at.Before(run.StartedAt()) {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: trusted time precedes Run start",
			ErrAgentRuntimeOrphanInvariant)
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if err != nil || handling.ProfileID() != profile.ID() || handling.ID() != handlingID ||
		handling.RecoveryCount() != run.HandlingRecovery() {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: Handling authority is unavailable",
			ErrAgentRuntimeOrphanStale)
	}
	return agentRuntimeAuthority{profile: profile, budget: budget, run: run, handling: handling}, nil
}

func appendOrphanCompletion(ctx context.Context, tx *sql.Tx, authority agentRuntimeAuthority,
	spec AgentRuntimeOrphanSpec, at time.Time, semantic bool,
) (AgentRuntimeTransitionResult, error) {
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	finishedAt, _ := authority.run.FinishedAt()
	outcomePredicate := "outcome_receipt_json IS NULL"
	if semantic {
		outcomePredicate = "outcome_receipt_json IS NOT NULL"
	}
	query := `UPDATE agent_runs SET completion_at=?,completion_receipt_json=?,error=?
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
		AND launcher='mnemond-wake' AND runtime_kind=? AND status=? AND finished_at=?
		AND completion_at IS NULL AND completion_receipt_json IS NULL
		AND ` + outcomePredicate
	result, err := tx.ExecContext(ctx, query, storeTime(at), spec.CompletionReceipt.Bytes(), spec.Error,
		authority.run.ID().String(), authority.profile.ID().String(), handlingID.String(),
		authority.run.HandlingAttempt(), authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(),
		storeTime(lease), string(authority.profile.Runtime()), string(authority.run.Status()),
		storeTime(finishedAt))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("settle orphaned Agent Runtime: append completion: %w", err)
	}
	if err := requireExactlyOneRow(result, "settle orphaned Agent Runtime: completion fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeOrphanStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completed Run cannot be read",
			ErrAgentRuntimeOrphanInvariant)
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied,
		"settle orphaned Agent Runtime: append completion")
}

func failActiveOrphan(ctx context.Context, tx *sql.Tx, authority agentRuntimeAuthority,
	spec AgentRuntimeOrphanSpec, at time.Time,
) (AgentRuntimeTransitionResult, error) {
	if err := requireExactActiveOrphan(authority, spec.ClaimFenceHash, at); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	operationReceipt, err := model.JSONFrom(struct {
		Code   string `json:"code"`
		Status string `json:"status"`
	}{"agent_runtime_orphaned", "rejected"})
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: operation receipt",
			ErrAgentRuntimeOrphanInvariant)
	}
	if err := rejectOrphanOperations(ctx, tx, authority.run.ID(), authority.profile.ID(),
		operationReceipt, at); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	dead := authority.handling.Attempts() >= uint32(authority.budget.Spec().MaxAttempts)
	runStatus := model.AgentRunFailed
	handlingStatus, disposition := model.HandlingPending, "runtime_orphaned"
	availableAt, deadAt := authority.handling.AvailableAt(), any(nil)
	if dead {
		runStatus, handlingStatus, disposition = model.AgentRunDead, model.HandlingDead,
			"attempt_budget_exhausted"
		deadAt = storeTime(at)
	} else {
		availableAt, err = agentClaimRetryAt(at, authority.handling.Attempts(), authority.budget)
		if err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: retry backoff: %v",
				ErrAgentRuntimeOrphanInvariant, err)
		}
	}
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	runResult, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status=?,finished_at=?,
		completion_at=?,completion_receipt_json=?,error=? WHERE run_id=? AND profile_id=?
		AND handling_id=? AND handling_attempt=? AND handling_recovery=? AND claim_fence_hash=?
		AND lease_until=? AND launcher='mnemond-wake' AND runtime_kind=? AND status=?
		AND finished_at IS NULL AND completion_at IS NULL AND completion_receipt_json IS NULL
		AND outcome_receipt_json IS NULL`, string(runStatus), storeTime(at), storeTime(at),
		spec.CompletionReceipt.Bytes(), spec.Error, authority.run.ID().String(), authority.profile.ID().String(),
		handlingID.String(), authority.run.HandlingAttempt(), authority.run.HandlingRecovery(),
		spec.ClaimFenceHash.Bytes(), storeTime(lease), string(authority.profile.Runtime()),
		string(authority.run.Status()))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("settle orphaned Agent Runtime: finish Run: %w", err)
	}
	if err := requireExactlyOneRow(runResult, "settle orphaned Agent Runtime: Run fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeOrphanStale, err)
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
		return AgentRuntimeTransitionResult{}, fmt.Errorf("settle orphaned Agent Runtime: settle Handling: %w", err)
	}
	if err := requireExactlyOneRow(handlingResult, "settle orphaned Agent Runtime: Handling fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeOrphanStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: failed Run cannot be read",
			ErrAgentRuntimeOrphanInvariant)
	}
	authority.handling, err = readAgentHandling(ctx, tx, authority.handling.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: settled Handling cannot be read",
			ErrAgentRuntimeOrphanInvariant)
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied,
		"settle orphaned Agent Runtime: commit")
}

func requireExactActiveOrphan(authority agentRuntimeAuthority, fence model.Digest, at time.Time) error {
	runHandling, hasRunHandling := authority.run.HandlingID()
	runFence, hasRunFence := authority.run.ClaimFenceHash()
	runLease, hasRunLease := authority.run.LeaseUntil()
	handlingFence, hasHandlingFence := authority.handling.ClaimTokenHash()
	handlingLease, hasHandlingLease := authority.handling.LeaseUntil()
	if !hasRunHandling || !hasRunFence || !hasRunLease || !hasHandlingFence || !hasHandlingLease ||
		authority.handling.Status() != model.HandlingClaimed || runHandling != authority.handling.ID() ||
		authority.run.ProfileID() != authority.handling.ProfileID() ||
		authority.run.HandlingAttempt() != authority.handling.Attempts() ||
		authority.run.HandlingRecovery() != authority.handling.RecoveryCount() ||
		!sameCurrentDigest(runFence, fence) || !sameCurrentDigest(handlingFence, fence) ||
		!runLease.Equal(handlingLease) {
		return ErrAgentRuntimeOrphanStale
	}
	// Startup recovery may run after the durable lease instant. The exact
	// claimed rows are still atomically fenced here; unlike normal Agent work,
	// this transition does not grant operation authority and need not extend the
	// expired lease.
	if at.Before(authority.run.StartedAt()) || at.Before(authority.handling.UpdatedAt()) {
		return fmt.Errorf("%w: trusted time precedes claim evidence",
			ErrAgentRuntimeOrphanInvariant)
	}
	return nil
}

func rejectOrphanOperations(ctx context.Context, tx *sql.Tx, runID model.RunID,
	profileID model.ProfileID, receipt model.JSON, at time.Time,
) error {
	var future int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations
		WHERE agent_run_id=? AND profile_id=? AND status='started' AND created_at>?)`,
		runID.String(), profileID.String(), storeTime(at)).Scan(&future); err != nil {
		return fmt.Errorf("settle orphaned Agent Runtime: inspect operations: %w", err)
	}
	if future == 1 {
		return fmt.Errorf("%w: trusted time precedes a started operation",
			ErrAgentRuntimeOrphanInvariant)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE agent_run_id=? AND profile_id=?
		AND status='started'`, receipt.Bytes(), storeTime(at), runID.String(), profileID.String()); err != nil {
		return fmt.Errorf("settle orphaned Agent Runtime: reject operations: %w", err)
	}
	return nil
}
