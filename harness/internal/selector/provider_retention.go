package selector

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

// prepareSelectionCapacityTx reclaims only selector-private state. An
// unseeded expired descriptor has no observation to preserve; an active
// expired selection is first settled to a durable observation. Old observed
// selections are retained up to the fixed store bound and then removed in a
// deterministic oldest-first order.
func prepareSelectionCapacityTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	if err := settleDueSelectionsTx(ctx, tx, now); err != nil {
		return err
	}
	if err := retainSelectionCapacityTx(ctx, tx); err != nil {
		return err
	}
	var active int
	if err := tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM selections WHERE phase != 'observed'").Scan(&active); err != nil {
		return fmt.Errorf("create owner selection: count active: %w", err)
	}
	if active >= MaxActiveSelections {
		return ErrStoreCapacity
	}
	return nil
}

func settleDueSelectionsTx(ctx context.Context, tx *sql.Tx, now time.Time) error {
	rows, err := tx.QueryContext(ctx,
		"SELECT selection_id FROM selections WHERE phase != 'observed' ORDER BY selection_id")
	if err != nil {
		return fmt.Errorf("create owner selection: inspect expirable selections: %w", err)
	}
	var ids []SelectionID
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("create owner selection: scan expirable selection: %w", err)
		}
		id, err := ParseSelectionID(raw)
		if err != nil {
			_ = rows.Close()
			return fmt.Errorf("create owner selection: stored selection ID: %w", ErrState)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("create owner selection: inspect expirable selections: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("create owner selection: close expirable selections: %w", err)
	}
	for _, id := range ids {
		snapshot, err := loadSelectionTx(ctx, tx, id)
		if err != nil {
			return err
		}
		if !selectionSettlementDue(snapshot, now) {
			continue
		}
		if err := settleDueSelectionTx(ctx, tx, snapshot, now); err != nil {
			return err
		}
	}
	return nil
}

func selectionSettlementDue(snapshot SelectionSnapshot, now time.Time) bool {
	if !now.Before(snapshot.descriptor.expiresAt) {
		return true
	}
	pending, present := snapshot.PendingRound()
	return present && !now.Before(pending.deadline)
}

func settleDueSelectionTx(ctx context.Context, tx *sql.Tx, snapshot SelectionSnapshot,
	now time.Time,
) error {
	switch snapshot.phase {
	case PhaseAwaitingSeed:
		return discardAwaitingSelectionTx(ctx, tx, snapshot)
	case PhaseActive:
		if pending, present := snapshot.PendingRound(); present {
			emptyVotes, err := canonicalVoteSet(nil)
			if err != nil {
				return err
			}
			settlement, err := deriveRoundSettlement(snapshot, pending, nil, now)
			if err != nil {
				return err
			}
			return commitRoundSettlementTx(ctx, tx, snapshot, pending, settlement,
				agency.Sum(emptyVotes), now)
		}
		observation, ready, err := Observe(snapshot.descriptor, snapshot.state, now)
		if err != nil {
			return fmt.Errorf("expire selector selection: derive observation: %w", err)
		}
		if !ready {
			return fmt.Errorf("expire selector selection has no terminal observation: %w", ErrState)
		}
		result, err := tx.ExecContext(ctx, `UPDATE selections SET phase = 'observed',
			observation_digest = ?, observation_json = ?, revision = ?, updated_at = ?
			WHERE selection_id = ? AND phase = 'active' AND revision = ?`,
			observation.Digest().String(), observation.CanonicalBytes(), snapshot.revision+1,
			formatProviderTime(now), snapshot.descriptor.id.String(), snapshot.revision)
		if err != nil {
			return fmt.Errorf("expire selector selection: update: %w", err)
		}
		return requireOneRow(result)
	default:
		return fmt.Errorf("expire selector selection phase %q: %w", snapshot.phase, ErrState)
	}
}

func discardAwaitingSelectionTx(ctx context.Context, tx *sql.Tx,
	snapshot SelectionSnapshot,
) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM selections
		WHERE selection_id = ? AND phase = 'awaiting_seed' AND revision = ?`,
		snapshot.descriptor.id.String(), snapshot.revision)
	if err != nil {
		return fmt.Errorf("expire unseeded selection: delete: %w", err)
	}
	return requireOneRow(result)
}

func retainSelectionCapacityTx(ctx context.Context, tx *sql.Tx) error {
	var total int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM selections").Scan(&total); err != nil {
		return fmt.Errorf("create owner selection: count retained: %w", err)
	}
	remove := total - (MaxStoredSelections - 1)
	if remove <= 0 {
		return nil
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM selections WHERE selection_id IN (
		SELECT selection_id FROM selections WHERE phase = 'observed'
		ORDER BY updated_at, selection_id LIMIT ?
	)`, remove)
	if err != nil {
		return fmt.Errorf("create owner selection: prune observations: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("create owner selection: prune cardinality: %w", err)
	}
	if removed != int64(remove) {
		return ErrStoreCapacity
	}
	return nil
}
