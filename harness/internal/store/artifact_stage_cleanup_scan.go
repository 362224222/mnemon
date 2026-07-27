package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
)

type artifactStageCleanupRow struct {
	owner          artifactdomain.StageOwner
	state          ArtifactStageState
	updatedAt      time.Time
	cleanupStarted time.Time
	cleanupClaimed bool
}

func readArtifactStageCleanupPage(ctx context.Context, tx *sql.Tx,
	cutoff time.Time, after artifactdomain.StageOwner, limit int,
) ([]artifactStageCleanupRow, error) {
	query := `SELECT owner_kind,owner_id,generation,state,updated_at,cleanup_started_at
		FROM (
			SELECT 'operation' AS owner_kind,operation_id AS owner_id,generation,state,
				updated_at,cleanup_started_at
			FROM operation_artifact_stages
			WHERE cleaned_at IS NULL
				AND (cleanup_started_at IS NOT NULL OR updated_at<?)
			UNION ALL
			SELECT 'inbox' AS owner_kind,inbox_id AS owner_id,generation,state,
				updated_at,cleanup_started_at
			FROM peer_inbox_artifact_stages
			WHERE cleaned_at IS NULL
				AND (cleanup_started_at IS NOT NULL OR updated_at<?)
		) AS owners`
	args := []any{storeTime(cutoff), storeTime(cutoff)}
	if !after.IsZero() {
		switch after.Kind() {
		case artifactdomain.StageOwnerOperation:
			query += ` WHERE owner_kind='inbox' OR (
				owner_kind='operation' AND (
					owner_id>? OR (owner_id=? AND generation>?)
				)
			)`
		case artifactdomain.StageOwnerInbox:
			query += ` WHERE owner_kind='inbox' AND (
				owner_id>? OR (owner_id=? AND generation>?)
			)`
		default:
			return nil, ErrArtifactStageConflict
		}
		args = append(args, after.CanonicalID(), after.CanonicalID(),
			after.Generation())
	}
	query += ` ORDER BY CASE owner_kind WHEN 'operation' THEN 0 ELSE 1 END,
		owner_id,generation LIMIT ?`
	args = append(args, limit)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("scan Artifact stage cleanup: list owners: %w", err)
	}
	defer rows.Close()

	result := make([]artifactStageCleanupRow, 0, limit)
	for rows.Next() {
		row, err := scanArtifactStageCleanupRow(rows, cutoff)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan Artifact stage cleanup: iterate owners: %w", err)
	}
	return result, nil
}

func scanArtifactStageCleanupRow(rows *sql.Rows,
	cutoff time.Time,
) (artifactStageCleanupRow, error) {
	var kindText, ownerID, stateText, updatedText string
	var cleanupStarted sql.NullString
	var generation int64
	if err := rows.Scan(&kindText, &ownerID, &generation, &stateText,
		&updatedText, &cleanupStarted); err != nil {
		return artifactStageCleanupRow{},
			fmt.Errorf("scan Artifact stage cleanup: scan owner: %w", err)
	}
	if generation <= 0 {
		return artifactStageCleanupRow{}, ErrArtifactStageConflict
	}
	owner, err := newArtifactStageOwner(artifactdomain.StageOwnerKind(kindText),
		ownerID, uint64(generation))
	if err != nil {
		return artifactStageCleanupRow{}, ErrArtifactStageConflict
	}
	state := ArtifactStageState(stateText)
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil || !state.Valid() {
		return artifactStageCleanupRow{}, ErrArtifactStageConflict
	}
	row := artifactStageCleanupRow{owner: owner, state: state, updatedAt: updatedAt}
	if cleanupStarted.Valid {
		row.cleanupStarted, err = parseCanonicalStoreTime(cleanupStarted.String)
		if err != nil || !updatedAt.Before(row.cleanupStarted) {
			return artifactStageCleanupRow{}, ErrArtifactStageConflict
		}
		row.cleanupClaimed = true
	} else if !updatedAt.Before(cutoff) {
		return artifactStageCleanupRow{}, ErrArtifactStageConflict
	}
	return row, nil
}

func claimEligibleArtifactStages(ctx context.Context, tx *sql.Tx,
	rows []artifactStageCleanupRow, cutoff, at time.Time,
) ([]ArtifactStageCleanupCandidate, error) {
	candidates := make([]ArtifactStageCleanupCandidate, 0, len(rows))
	for _, row := range rows {
		if row.cleanupClaimed {
			candidates = append(candidates, ArtifactStageCleanupCandidate{
				owner: row.owner, state: row.state, updatedAt: row.updatedAt,
				claimStartedAt: row.cleanupStarted,
			})
			continue
		}
		eligible, err := artifactStageCleanupEligible(ctx, tx, row, cutoff, at)
		if err != nil {
			return nil, err
		}
		if !eligible {
			continue
		}
		candidate, err := claimArtifactStageCleanup(ctx, tx, row, at)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

func claimArtifactStageCleanup(ctx context.Context, tx *sql.Tx,
	row artifactStageCleanupRow, at time.Time,
) (ArtifactStageCleanupCandidate, error) {
	table, idColumn, err := artifactStageOwnerTable(row.owner.Kind())
	if err != nil {
		return ArtifactStageCleanupCandidate{}, err
	}
	query := fmt.Sprintf(`UPDATE %s SET cleanup_started_at=?
		WHERE %s=? AND generation=? AND state=? AND updated_at=?
		AND cleanup_started_at IS NULL AND cleaned_at IS NULL`, table, idColumn)
	updated, err := tx.ExecContext(ctx, query, storeTime(at),
		row.owner.CanonicalID(), row.owner.Generation(), string(row.state),
		storeTime(row.updatedAt))
	if err != nil || exactlyOne(updated) != nil {
		return ArtifactStageCleanupCandidate{}, fmt.Errorf(
			"%w: claim exact cleanup owner: %v", ErrArtifactStageFence, err)
	}
	return ArtifactStageCleanupCandidate{
		owner: row.owner, state: row.state, updatedAt: row.updatedAt,
		claimStartedAt: at,
	}, nil
}
