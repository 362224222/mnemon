package selector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

type roundSettlement struct {
	state       SelectionState
	observation PreferenceObservation
	phase       SelectionPhase
	ready       bool
}

func deriveRoundSettlement(snapshot SelectionSnapshot, pending PendingRound,
	votes []AuthenticatedVote, now time.Time,
) (roundSettlement, error) {
	nextState := snapshot.state
	if now.Before(snapshot.descriptor.expiresAt) {
		if !now.Before(pending.deadline) {
			votes = nil
		}
		round, err := ApplyRound(snapshot.descriptor, snapshot.state, snapshot.self,
			pending.query, pending.sample, votes, now)
		if err != nil {
			return roundSettlement{}, err
		}
		nextState = round.state
	}
	observation, ready, err := Observe(snapshot.descriptor, nextState, now)
	if err != nil {
		return roundSettlement{}, err
	}
	phase := PhaseActive
	if ready {
		phase = PhaseObserved
	}
	return roundSettlement{state: nextState, observation: observation,
		phase: phase, ready: ready}, nil
}

func commitRoundSettlementTx(ctx context.Context, tx *sql.Tx, snapshot SelectionSnapshot,
	pending PendingRound, settlement roundSettlement, voteSetDigest agency.Digest, now time.Time,
) error {
	nextRevision := snapshot.revision + 1
	var observationDigest, observationJSON any
	if settlement.ready {
		observationDigest = settlement.observation.Digest().String()
		observationJSON = settlement.observation.CanonicalBytes()
	}
	result, err := tx.ExecContext(ctx, `UPDATE selections SET phase = ?, current_preference = ?,
		signed_margin = ?, completed_rounds = ?, revision = ?, observation_digest = ?,
		observation_json = ?, updated_at = ?
		WHERE selection_id = ? AND phase = 'active' AND revision = ?`, string(settlement.phase),
		settlement.state.preference.String(), settlement.state.margin, settlement.state.round,
		nextRevision, observationDigest, observationJSON, formatProviderTime(now),
		snapshot.descriptor.id.String(), snapshot.revision)
	if err != nil {
		return fmt.Errorf("apply selector observations: update: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return err
	}
	if err := insertRoundSettlementTx(ctx, tx, snapshot, pending, voteSetDigest,
		nextRevision, now); err != nil {
		return err
	}
	deleted, err := tx.ExecContext(ctx,
		"DELETE FROM pending_rounds WHERE selection_id = ? AND state_revision = ?",
		snapshot.descriptor.id.String(), snapshot.revision)
	if err != nil {
		return fmt.Errorf("apply selector observations: delete pending: %w", err)
	}
	return requireOneRow(deleted)
}

func insertRoundSettlementTx(ctx context.Context, tx *sql.Tx, snapshot SelectionSnapshot,
	pending PendingRound, voteSetDigest agency.Digest, resultRevision uint64, now time.Time,
) error {
	sampleJSON, err := canonicalSample(pending.sample)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO settled_rounds(
		selection_id, round, nonce_digest, sample_json, deadline, state_revision,
		vote_set_digest, result_revision, settled_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.descriptor.id.String(), pending.query.round, pending.query.nonce.String(),
		sampleJSON, formatProviderTime(pending.deadline), pending.stateRevision,
		voteSetDigest.String(), resultRevision, formatProviderTime(now))
	if err != nil {
		return fmt.Errorf("apply selector observations: record settlement: %w", err)
	}
	return nil
}
