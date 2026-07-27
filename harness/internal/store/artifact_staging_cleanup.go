package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
)

const artifactStagingMaxRoots = 256

// SweepArtifactStaging reclaims only relational metadata whose terminal
// unaccepted physical owner was already marked cleaned. Verified or otherwise
// owned Artifact roots are never candidates.
func (s *Store) SweepArtifactStaging(ctx context.Context,
	spec artifactdomain.StagingSweepSpec,
) (artifactdomain.StagingSweepResult, error) {
	cutoff, at, err := validateArtifactStagingSweepSpec(spec)
	if s == nil || s.db == nil || ctx == nil {
		return artifactdomain.StagingSweepResult{},
			artifactStagingError("sweep", errors.New("nil Store or context"))
	}
	if err != nil {
		return artifactdomain.StagingSweepResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return artifactdomain.StagingSweepResult{},
			fmt.Errorf("sweep Artifact staging: begin: %w", err)
	}
	defer tx.Rollback()

	// Publishing pins are recovery authority and ready pins are retention
	// authority. Only expired pins with no such durable owner may be removed.
	expired, err := tx.ExecContext(ctx, `DELETE FROM artifact_pins WHERE rowid IN (
		SELECT pin.rowid FROM artifact_pins pin
		WHERE pin.owner_kind='inbox' AND pin.expires_at IS NOT NULL
			AND pin.expires_at<=?
			AND NOT EXISTS (
				SELECT 1 FROM peer_inbox_artifact_stages stage
				WHERE stage.inbox_id=pin.owner_id
					AND stage.state IN ('publishing','ready')
					AND stage.cleaned_at IS NULL
			)
		ORDER BY pin.expires_at,pin.root_digest,pin.owner_id LIMIT ?
	)`, storeTime(at), spec.MaxRoots)
	if err != nil {
		return artifactdomain.StagingSweepResult{},
			artifactStagingError("remove expired Inbox pins", err)
	}
	count, err := expired.RowsAffected()
	if err != nil || count < 0 || count > int64(spec.MaxRoots) {
		return artifactdomain.StagingSweepResult{},
			artifactStagingError("count expired Inbox pins", err)
	}
	projections, err := deleteCleanedArtifactOwnerProjections(ctx, tx, spec.MaxRoots)
	if err != nil {
		return artifactdomain.StagingSweepResult{}, err
	}
	roots, err := deleteUnownedStagedArtifactRoots(ctx, tx, cutoff, spec.MaxRoots)
	if err != nil {
		return artifactdomain.StagingSweepResult{}, err
	}
	blocks, err := deleteUnmappedArtifactBlocks(ctx, tx, cutoff, spec.MaxRoots)
	if err != nil {
		return artifactdomain.StagingSweepResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return artifactdomain.StagingSweepResult{},
			fmt.Errorf("sweep Artifact staging: commit: %w", err)
	}
	return artifactdomain.StagingSweepResult{
		ExpiredPins: int(count), OwnerProjections: projections,
		Roots: roots, Blocks: blocks,
	}, nil
}

func deleteCleanedArtifactOwnerProjections(ctx context.Context, tx *sql.Tx,
	limit int,
) (int, error) {
	operation, err := boundedArtifactDelete(ctx, tx, `DELETE FROM operation_artifact_roots
		WHERE rowid IN (
			SELECT projection.rowid FROM operation_artifact_roots projection
			JOIN operations operation ON operation.operation_id=projection.operation_id
			WHERE operation.status='rejected' AND EXISTS (
				SELECT 1 FROM operation_artifact_stages stage
				WHERE stage.operation_id=projection.operation_id
					AND stage.cleaned_at IS NOT NULL
			) AND NOT EXISTS (
				SELECT 1 FROM operation_artifact_stages stage
				WHERE stage.operation_id=projection.operation_id
					AND stage.cleaned_at IS NULL
			)
			ORDER BY projection.operation_id,projection.root_digest LIMIT ?
		)`, limit, "remove cleaned operation Artifact projections")
	if err != nil {
		return 0, err
	}
	inbox, err := boundedArtifactDelete(ctx, tx, `DELETE FROM peer_inbox_artifact_roots
		WHERE rowid IN (
			SELECT projection.rowid FROM peer_inbox_artifact_roots projection
			JOIN peer_inbox inbox ON inbox.inbox_id=projection.inbox_id
			WHERE inbox.status IN ('quarantined','ignored') AND EXISTS (
				SELECT 1 FROM peer_inbox_artifact_stages stage
				WHERE stage.inbox_id=projection.inbox_id
					AND stage.cleaned_at IS NOT NULL
			) AND NOT EXISTS (
				SELECT 1 FROM peer_inbox_artifact_stages stage
				WHERE stage.inbox_id=projection.inbox_id
					AND stage.cleaned_at IS NULL
			) AND NOT EXISTS (
				SELECT 1 FROM artifact_pins pin
				WHERE pin.owner_kind='inbox' AND pin.owner_id=projection.inbox_id
					AND pin.expires_at IS NULL
			)
			ORDER BY projection.inbox_id,projection.root_digest LIMIT ?
		)`, limit, "remove cleaned Inbox Artifact projections")
	if err != nil {
		return 0, err
	}
	return operation + inbox, nil
}

func deleteUnownedStagedArtifactRoots(ctx context.Context, tx *sql.Tx,
	cutoff time.Time, limit int,
) (int, error) {
	return boundedArtifactDelete(ctx, tx, `DELETE FROM artifact_roots WHERE rowid IN (
		SELECT root.rowid FROM artifact_roots root
		WHERE root.state='staged' AND root.created_at<?
			AND NOT EXISTS (
				SELECT 1 FROM artifact_pins pin
				WHERE pin.root_digest=root.root_digest
			)
			AND NOT EXISTS (
				SELECT 1 FROM artifact_provenance provenance
				WHERE provenance.root_digest=root.root_digest
			)
			AND NOT EXISTS (
				SELECT 1 FROM operation_artifact_roots projection
				WHERE projection.root_digest=root.root_digest
			)
			AND NOT EXISTS (
				SELECT 1 FROM peer_inbox_artifact_roots projection
				WHERE projection.root_digest=root.root_digest
			)
		ORDER BY root.created_at,root.root_digest LIMIT ?
	)`, limit, "remove unowned staged Artifact roots", storeTime(cutoff))
}

func deleteUnmappedArtifactBlocks(ctx context.Context, tx *sql.Tx,
	cutoff time.Time, limit int,
) (int, error) {
	return boundedArtifactDelete(ctx, tx, `DELETE FROM artifact_blocks WHERE rowid IN (
		SELECT block.rowid FROM artifact_blocks block
		WHERE block.created_at<? AND NOT EXISTS (
			SELECT 1 FROM artifact_root_blocks mapping
			WHERE mapping.block_digest=block.block_digest
		)
		ORDER BY block.created_at,block.block_digest LIMIT ?
	)`, limit, "remove unmapped Artifact blocks", storeTime(cutoff))
}

func boundedArtifactDelete(ctx context.Context, tx *sql.Tx, query string,
	limit int, operation string, prefix ...any,
) (int, error) {
	args := append(append([]any(nil), prefix...), limit)
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, artifactStagingError(operation, err)
	}
	count, err := result.RowsAffected()
	if err != nil || count < 0 || count > int64(limit) {
		return 0, artifactStagingError("count "+operation, err)
	}
	return int(count), nil
}

func validateArtifactStagingSweepSpec(spec artifactdomain.StagingSweepSpec) (
	time.Time, time.Time, error,
) {
	cutoff, cutoffErr := canonicalStoreTime(spec.Cutoff)
	at, atErr := canonicalStoreTime(spec.At)
	if cutoffErr != nil || atErr != nil || cutoff != spec.Cutoff || at != spec.At ||
		!cutoff.Before(at) || spec.MaxRoots <= 0 || spec.MaxRoots > artifactStagingMaxRoots {
		return time.Time{}, time.Time{},
			artifactStagingError("validate input", errors.New("invalid bounded sweep"))
	}
	return cutoff, at, nil
}

func artifactStagingError(operation string, err error) error {
	if err == nil {
		err = errors.New("invalid durable cleanup result")
	}
	return fmt.Errorf("%w: %s: %v", artifactdomain.ErrStagingStoreInvariant, operation, err)
}
