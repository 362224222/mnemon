package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// BeginOperationArtifactStage durably registers a physical owner before any
// bytes are staged. A reclaimed staged owner gets a new generation. Publishing
// is instead adopted in place so its exact directory remains recoverable.
func (s *Store) BeginOperationArtifactStage(ctx context.Context,
	spec BeginOperationArtifactStageSpec,
) (OperationArtifactStageResult, error) {
	at, leaseUntil, err := validateOperationArtifactStageCall(s, ctx, spec)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationArtifactStageResult{}, fmt.Errorf("begin operation Artifact stage: %w", err)
	}
	defer tx.Rollback()
	operation, err := readOperationByID(ctx, tx, spec.OperationID)
	if err != nil {
		return OperationArtifactStageResult{}, fmt.Errorf("begin operation Artifact stage: %w", err)
	}
	if err := requireExactOperationArtifactFence(operation, spec.LeaseOwner, leaseUntil, at); err != nil {
		return OperationArtifactStageResult{}, err
	}
	row, found, err := readOperationArtifactStage(ctx, tx, spec.OperationID)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	row, replayed, err := beginOperationArtifactStageRow(ctx, tx, spec, at,
		leaseUntil, row, found)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	owner, err := artifactdomain.NewOperationStageOwner(spec.OperationID, row.generation)
	if err != nil {
		return OperationArtifactStageResult{}, fmt.Errorf("begin operation Artifact stage owner: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return OperationArtifactStageResult{},
			fmt.Errorf("begin operation Artifact stage: commit: %w", err)
	}
	return OperationArtifactStageResult{fence: OperationArtifactStageFence{
		owner: owner, leaseOwner: spec.LeaseOwner, leaseUntil: leaseUntil,
	}, state: row.state, replayed: replayed}, nil
}

func beginOperationArtifactStageRow(ctx context.Context, tx *sql.Tx,
	spec BeginOperationArtifactStageSpec, at, leaseUntil time.Time,
	row durableArtifactStage, found bool,
) (durableArtifactStage, bool, error) {
	if !found {
		row, err := insertOperationArtifactStage(ctx, tx, spec.OperationID, 1,
			spec.LeaseOwner, leaseUntil, at)
		if err != nil {
			return durableArtifactStage{}, false,
				fmt.Errorf("begin operation Artifact stage: insert: %w", err)
		}
		return row, false, err
	}
	switch row.state {
	case ArtifactStagePublishing:
		return recoverPublishingOperationArtifactStage(ctx, tx, spec, at,
			leaseUntil, row)
	case ArtifactStageStaged:
		return replaceStagedOperationArtifactStage(ctx, tx, spec, at,
			leaseUntil, row)
	default:
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
}

func insertOperationArtifactStage(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, generation uint64, leaseOwner string,
	leaseUntil, at time.Time,
) (durableArtifactStage, error) {
	_, err := tx.ExecContext(ctx, `INSERT INTO operation_artifact_stages(
		operation_id,generation,state,lease_owner,lease_until,created_at,updated_at
	) VALUES(?,?,'staged',?,?,?,?)`, operationID.String(), generation, leaseOwner,
		storeTime(leaseUntil), storeTime(at), storeTime(at))
	if err != nil {
		return durableArtifactStage{}, err
	}
	return durableArtifactStage{generation: generation, state: ArtifactStageStaged,
		leaseOwner: leaseOwner, leaseUntil: leaseUntil, createdAt: at, updatedAt: at}, nil
}

func recoverPublishingOperationArtifactStage(ctx context.Context, tx *sql.Tx,
	spec BeginOperationArtifactStageSpec, at, leaseUntil time.Time,
	row durableArtifactStage,
) (durableArtifactStage, bool, error) {
	if row.cleanupClaimed {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	if operationArtifactStageLeaseMatches(row, spec.LeaseOwner, leaseUntil) {
		return row, true, nil
	}
	updated, err := tx.ExecContext(ctx, `UPDATE operation_artifact_stages
		SET lease_owner=?,lease_until=?,updated_at=?
		WHERE operation_id=? AND generation=? AND state='publishing'
		AND lease_owner=? AND lease_until=? AND updated_at=?`,
		spec.LeaseOwner, storeTime(leaseUntil), storeTime(at), spec.OperationID.String(),
		row.generation, row.leaseOwner, storeTime(row.leaseUntil), storeTime(row.updatedAt))
	if err != nil || exactlyOne(updated) != nil {
		return durableArtifactStage{}, false,
			fmt.Errorf("%w: recover operation stage: %v", ErrArtifactStageFence, err)
	}
	row.leaseOwner, row.leaseUntil, row.updatedAt = spec.LeaseOwner, leaseUntil, at
	return row, false, nil
}

func replaceStagedOperationArtifactStage(ctx context.Context, tx *sql.Tx,
	spec BeginOperationArtifactStageSpec, at, leaseUntil time.Time,
	row durableArtifactStage,
) (durableArtifactStage, bool, error) {
	if operationArtifactStageLeaseMatches(row, spec.LeaseOwner, leaseUntil) &&
		!row.cleanupClaimed {
		return row, true, nil
	}
	if row.generation == model.MaxSQLiteInteger {
		return durableArtifactStage{}, false, ErrArtifactStageFence
	}
	next, err := insertOperationArtifactStage(ctx, tx, spec.OperationID,
		row.generation+1, spec.LeaseOwner, leaseUntil, at)
	if err != nil {
		return durableArtifactStage{}, false,
			fmt.Errorf("%w: replace operation stage: %v", ErrArtifactStageFence, err)
	}
	return next, false, err
}

func operationArtifactStageLeaseMatches(stage durableArtifactStage,
	leaseOwner string, leaseUntil time.Time,
) bool {
	return stage.leaseOwner == leaseOwner && stage.leaseUntil.Equal(leaseUntil)
}
