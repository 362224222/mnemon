package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// BeginPeerInboxArtifactStage registers an exact Inbox attempt before a
// manifest or block is fetched. A new attempt replaces only a still-staged
// generation; publishing recovery stays on its original physical generation.
func (s *Store) BeginPeerInboxArtifactStage(ctx context.Context,
	spec BeginPeerInboxArtifactStageSpec,
) (PeerInboxArtifactStageRegistration, error) {
	at, err := validatePeerInboxArtifactSettlementCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	defer tx.Rollback()
	row, err := readPeerInboxArtifactRow(ctx, tx, spec.Fence.inboxID)
	if err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	if _, _, err := readValidatedPeerInboxArtifactRenewReceipt(ctx, tx, row); err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	if err := requireLivePeerInboxArtifactFence(row, spec.Fence, at); err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	if err := requirePeerInboxArtifactAuthority(ctx, tx, row, at); err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	stage, found, err := readPeerInboxArtifactStage(ctx, tx, row.inboxID)
	if err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	stage, replayed, err := beginPeerInboxArtifactStageRow(ctx, tx, row, at,
		stage, found)
	if err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	owner, err := artifactdomain.NewInboxStageOwner(row.inboxID, stage.generation)
	if err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	if err := tx.Commit(); err != nil {
		return PeerInboxArtifactStageRegistration{}, err
	}
	return PeerInboxArtifactStageRegistration{owner: owner, state: stage.state,
		replayed: replayed}, nil
}

func beginPeerInboxArtifactStageRow(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, at time.Time, stage durableArtifactStage, found bool,
) (durableArtifactStage, bool, error) {
	if !found {
		stage, err := insertPeerInboxArtifactStage(ctx, tx, row, 1, at)
		return stage, false, err
	}
	if stage.semanticNonce != row.semanticNonce {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	switch stage.state {
	case ArtifactStagePublishing:
		return recoverPublishingPeerInboxArtifactStage(ctx, tx, row, at, stage)
	case ArtifactStageStaged:
		return replaceStagedPeerInboxArtifactStage(ctx, tx, row, at, stage)
	default:
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
}

func insertPeerInboxArtifactStage(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, generation uint64, at time.Time,
) (durableArtifactStage, error) {
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_inbox_artifact_stages(
		inbox_id,generation,state,attempt,lease_owner,lease_until,semantic_nonce,
		created_at,updated_at
	) VALUES(?,?,'staged',?,?,?,?,?,?)`, row.inboxID.String(), generation,
		row.attempts, row.leaseOwner, storeTime(row.leaseUntil), row.semanticNonce[:],
		storeTime(at), storeTime(at))
	if err != nil {
		return durableArtifactStage{}, err
	}
	return durableArtifactStage{generation: generation, state: ArtifactStageStaged,
		attempt: row.attempts, leaseOwner: row.leaseOwner, leaseUntil: row.leaseUntil,
		semanticNonce: row.semanticNonce, createdAt: at, updatedAt: at}, nil
}

func recoverPublishingPeerInboxArtifactStage(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, at time.Time, stage durableArtifactStage,
) (durableArtifactStage, bool, error) {
	if stage.cleanupClaimed {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	if peerInboxArtifactStageFenceMatches(stage, row) {
		return stage, true, nil
	}
	updated, err := tx.ExecContext(ctx, `UPDATE peer_inbox_artifact_stages
		SET attempt=?,lease_owner=?,lease_until=?,updated_at=?
		WHERE inbox_id=? AND generation=? AND state='publishing' AND attempt=?
		AND lease_owner=? AND lease_until=? AND semantic_nonce=? AND updated_at=?`,
		row.attempts, row.leaseOwner, storeTime(row.leaseUntil), storeTime(at),
		row.inboxID.String(), stage.generation, stage.attempt, stage.leaseOwner,
		storeTime(stage.leaseUntil), stage.semanticNonce[:], storeTime(stage.updatedAt))
	if err != nil || exactlyOne(updated) != nil {
		return durableArtifactStage{}, false,
			fmt.Errorf("%w: recover Inbox stage: %v", ErrArtifactStageFence, err)
	}
	stage.attempt, stage.leaseOwner, stage.leaseUntil = row.attempts, row.leaseOwner, row.leaseUntil
	stage.updatedAt = at
	return stage, false, nil
}

func replaceStagedPeerInboxArtifactStage(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, at time.Time, stage durableArtifactStage,
) (durableArtifactStage, bool, error) {
	if peerInboxArtifactStageFenceMatches(stage, row) && !stage.cleanupClaimed {
		return stage, true, nil
	}
	if stage.generation == model.MaxSQLiteInteger {
		return durableArtifactStage{}, false, ErrArtifactStageFence
	}
	next, err := insertPeerInboxArtifactStage(ctx, tx, row, stage.generation+1, at)
	if err != nil {
		return durableArtifactStage{}, false,
			fmt.Errorf("%w: replace Inbox stage: %v", ErrArtifactStageFence, err)
	}
	return next, false, nil
}

func peerInboxArtifactStageFenceMatches(stage durableArtifactStage,
	row peerInboxArtifactRow,
) bool {
	return stage.attempt == row.attempts &&
		stage.leaseOwner == row.leaseOwner &&
		stage.leaseUntil.Equal(row.leaseUntil)
}
