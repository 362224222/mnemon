package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func materializePeerInboxSemanticTerminalOwnership(ctx context.Context, tx *sql.Tx,
	row peerInboxSemanticRow, snapshot peerInboxSemanticSnapshot,
	plan PeerInboxSemanticPlan, responses []model.Event,
) error {
	if handling, ok := plan.Handling(); ok {
		if err := materializePeerInboxSemanticHandling(ctx, tx, handling,
			snapshot.importedEvent, responses); err != nil {
			return err
		}
	}
	return handoffPeerInboxSemanticArtifactPins(ctx, tx, row.inboxID, row.requiredRoots)
}

func handoffPeerInboxSemanticArtifactPins(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, roots []model.Digest,
) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=?`, inboxID.String())
	if err != nil {
		return fmt.Errorf("%w: hand off Inbox Artifact pins: %v",
			ErrPeerInboxSemanticInvariant, err)
	}
	affected, err := result.RowsAffected()
	if err != nil || affected != int64(len(roots)) {
		return fmt.Errorf("%w: hand off Inbox Artifact pin cardinality",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}

func requireNoPeerInboxSemanticArtifactPins(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID,
) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_pins
		WHERE owner_kind='inbox' AND owner_id=?`, inboxID.String()).Scan(&count); err != nil ||
		count != 0 {
		return fmt.Errorf("%w: terminal Inbox Artifact pins remain",
			ErrPeerInboxSemanticInvariant)
	}
	return nil
}
