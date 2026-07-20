package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// RecoverDeadAgentHandlings starts a new explicit recovery generation after a
// successful setup self-check. Historical Runs remain immutable; attempts are
// reset only on the recovered Handling generation.
func (s *Store) RecoverDeadAgentHandlings(ctx context.Context,
	spec AgentDeadRecoverySpec,
) (AgentDeadRecoveryResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Profile.ID() != model.TeamworkProfileID() ||
		!spec.Profile.Enabled() || spec.Profile.ActiveAssetRevision() == "" {
		return AgentDeadRecoveryResult{}, ErrAgentDeadRecoveryInput
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: trusted time: %v", ErrAgentDeadRecoveryInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: begin: %w", err)
	}
	defer tx.Rollback()
	result, err := recoverDeadAgentHandlingsTx(ctx, tx, spec.Profile, at)
	if err != nil {
		return AgentDeadRecoveryResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: commit: %w", err)
	}
	return result, nil
}

func recoverDeadAgentHandlingsTx(ctx context.Context, tx *sql.Tx, expected model.Profile,
	at time.Time,
) (AgentDeadRecoveryResult, error) {
	profile, _, err := requireAgentClaimAuthority(ctx, tx, expected.ID(),
		expected.ActiveAssetRevision())
	if err != nil || !sameProfileIdentity(profile, expected) ||
		!sameProfileAuthority(profile, expected) || !profile.UpdatedAt().Equal(expected.UpdatedAt()) {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: exact setup generation is not active",
			ErrAgentDeadRecoveryAuthority)
	}
	if at.Before(profile.UpdatedAt()) {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: trusted time precedes Profile generation",
			ErrAgentDeadRecoveryInvariant)
	}
	var invalid int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_handlings
		WHERE profile_id=? AND status='dead' AND (updated_at>? OR claim_owner IS NOT NULL
		OR claim_token_hash IS NOT NULL OR lease_until IS NOT NULL OR outcome_event_id IS NOT NULL
		OR dead_at IS NULL OR attempts=0 OR recovery_count>=4294967295))`, profile.ID().String(),
		storeTime(at)).Scan(&invalid); err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: inspect: %w", err)
	}
	if invalid == 1 {
		return AgentDeadRecoveryResult{}, ErrAgentDeadRecoveryInvariant
	}
	var invalidRun int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM agent_handlings AS h
		LEFT JOIN agent_runs AS r ON r.profile_id=h.profile_id AND r.handling_id=h.handling_id
			AND r.handling_recovery=h.recovery_count AND r.handling_attempt=h.attempts
		WHERE h.profile_id=? AND h.status='dead' AND (
			r.run_id IS NULL
			OR NOT (
				r.status='dead'
				OR (h.last_disposition='attempt_budget_exhausted'
					AND h.last_error IS 'maximum handling attempts exhausted'
					AND r.status IN ('requeued','failed'))
			)
			OR (r.launcher='mnemond-wake' AND (
				NOT (r.completion_at IS NOT NULL AND r.completion_receipt_json IS NOT NULL
					AND r.completion_at<=?)
				AND NOT (r.runtime_started_at IS NULL
					AND r.launcher_diagnostic_json=X'7B7D' AND r.runtime_ids_json=X'7B7D'
					AND r.attached_at IS NULL AND r.wake_delivered_at IS NULL
					AND r.wake_receipt_json IS NULL AND r.current_read_receipt_json IS NULL
					AND r.outcome_receipt_json IS NULL AND r.completion_at IS NULL
					AND r.completion_receipt_json IS NULL)
			))
		)
	)`, profile.ID().String(), storeTime(at)).Scan(&invalidRun); err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: inspect terminal Run: %w", err)
	}
	if invalidRun == 1 {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: exact dead Run is not settled before recovery",
			ErrAgentDeadRecoveryInvariant)
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status='pending',available_at=?,
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,attempts=0,
		last_disposition='setup_recovered',outcome_event_id=NULL,last_error=NULL,
		recovery_count=recovery_count+1,dead_at=NULL,updated_at=?
		WHERE profile_id=? AND status='dead'`, storeTime(at), storeTime(at), profile.ID().String())
	if err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: update: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil || count < 0 || count > int64(^uint32(0)) {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: invalid recovered row count",
			ErrAgentDeadRecoveryInvariant)
	}
	return AgentDeadRecoveryResult{Recovered: uint32(count)}, nil
}
