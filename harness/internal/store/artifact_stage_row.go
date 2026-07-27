package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrArtifactStageFence    = errors.New("Artifact stage fence rejected")
	ErrArtifactStageConflict = errors.New("Artifact stage conflicts with durable state")
)

type durableArtifactStage struct {
	generation     uint64
	state          ArtifactStageState
	attempt        uint32
	leaseOwner     string
	leaseUntil     time.Time
	semanticNonce  [32]byte
	payloadDigest  model.Digest
	createdAt      time.Time
	updatedAt      time.Time
	cleanupStarted time.Time
	cleanupClaimed bool
	cleanedAt      sql.NullString
}

func readOperationArtifactStage(ctx context.Context, tx *sql.Tx,
	id model.OperationID,
) (durableArtifactStage, bool, error) {
	return readOperationArtifactStageWhere(ctx, tx, id, 0)
}

func readOperationArtifactStageGeneration(ctx context.Context, tx *sql.Tx,
	id model.OperationID, generation uint64,
) (durableArtifactStage, bool, error) {
	if generation == 0 {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	return readOperationArtifactStageWhere(ctx, tx, id, generation)
}

func readOperationArtifactStageWhere(ctx context.Context, tx *sql.Tx,
	id model.OperationID, generation uint64,
) (durableArtifactStage, bool, error) {
	var row durableArtifactStage
	var state, leaseText, createdText, updatedText string
	var digest []byte
	var cleanupStartedText sql.NullString
	query := `SELECT generation,state,lease_owner,lease_until,
		capture_digest,created_at,updated_at,cleanup_started_at,cleaned_at
		FROM operation_artifact_stages WHERE operation_id=?`
	args := []any{id.String()}
	if generation == 0 {
		query += ` ORDER BY generation DESC LIMIT 1`
	} else {
		query += ` AND generation=?`
		args = append(args, generation)
	}
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&row.generation, &state, &row.leaseOwner, &leaseText, &digest,
		&createdText, &updatedText, &cleanupStartedText, &row.cleanedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return durableArtifactStage{}, false, nil
	}
	if err != nil {
		return durableArtifactStage{}, false, err
	}
	row.state = ArtifactStageState(state)
	row.leaseUntil, err = parseCanonicalStoreTime(leaseText)
	if err == nil {
		row.createdAt, err = parseCanonicalStoreTime(createdText)
	}
	if err == nil {
		row.updatedAt, err = parseCanonicalStoreTime(updatedText)
	}
	if err == nil && len(digest) != 0 {
		row.payloadDigest, err = model.DigestFromBytes(digest)
	}
	if err == nil {
		err = parseArtifactStageCleanupClaim(&row, cleanupStartedText)
	}
	if err != nil || !row.state.Valid() || row.generation == 0 ||
		row.updatedAt.Before(row.createdAt) ||
		(row.state == ArtifactStageStaged) != row.payloadDigest.IsZero() {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	return row, true, nil
}

func readPeerInboxArtifactStage(ctx context.Context, tx *sql.Tx,
	id model.InboxID,
) (durableArtifactStage, bool, error) {
	return readPeerInboxArtifactStageWhere(ctx, tx, id, 0)
}

func readPeerInboxArtifactStageGeneration(ctx context.Context, tx *sql.Tx,
	id model.InboxID, generation uint64,
) (durableArtifactStage, bool, error) {
	if generation == 0 {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	return readPeerInboxArtifactStageWhere(ctx, tx, id, generation)
}

func readPeerInboxArtifactStageWhere(ctx context.Context, tx *sql.Tx,
	id model.InboxID, generation uint64,
) (durableArtifactStage, bool, error) {
	var row durableArtifactStage
	var state, leaseText, createdText, updatedText string
	var nonce, digest []byte
	var cleanupStartedText sql.NullString
	var attempt int64
	query := `SELECT generation,state,attempt,lease_owner,lease_until,
		semantic_nonce,closure_digest,created_at,updated_at,cleanup_started_at,
		cleaned_at
		FROM peer_inbox_artifact_stages WHERE inbox_id=?`
	args := []any{id.String()}
	if generation == 0 {
		query += ` ORDER BY generation DESC LIMIT 1`
	} else {
		query += ` AND generation=?`
		args = append(args, generation)
	}
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&row.generation, &state, &attempt, &row.leaseOwner, &leaseText,
		&nonce, &digest, &createdText, &updatedText, &cleanupStartedText,
		&row.cleanedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return durableArtifactStage{}, false, nil
	}
	if err != nil {
		return durableArtifactStage{}, false, err
	}
	row.state = ArtifactStageState(state)
	row.leaseUntil, err = parseCanonicalStoreTime(leaseText)
	if err == nil {
		row.createdAt, err = parseCanonicalStoreTime(createdText)
	}
	if err == nil {
		row.updatedAt, err = parseCanonicalStoreTime(updatedText)
	}
	if len(nonce) == 32 {
		copy(row.semanticNonce[:], nonce)
	} else {
		err = ErrArtifactStageConflict
	}
	if err == nil && len(digest) != 0 {
		row.payloadDigest, err = model.DigestFromBytes(digest)
	}
	if err == nil {
		err = parseArtifactStageCleanupClaim(&row, cleanupStartedText)
	}
	if attempt <= 0 || uint64(attempt) > uint64(^uint32(0)) {
		err = ErrArtifactStageConflict
	} else {
		row.attempt = uint32(attempt)
	}
	if err != nil || !row.state.Valid() || row.generation == 0 || row.attempt == 0 ||
		row.updatedAt.Before(row.createdAt) ||
		(row.state == ArtifactStageStaged) != row.payloadDigest.IsZero() {
		return durableArtifactStage{}, false, ErrArtifactStageConflict
	}
	return row, true, nil
}

func parseArtifactStageCleanupClaim(row *durableArtifactStage,
	startedText sql.NullString,
) error {
	if startedText.Valid {
		started, err := parseCanonicalStoreTime(startedText.String)
		if err != nil || !row.updatedAt.Before(started) {
			return ErrArtifactStageConflict
		}
		row.cleanupStarted, row.cleanupClaimed = started, true
	}
	if row.cleanedAt.Valid {
		cleaned, err := parseCanonicalStoreTime(row.cleanedAt.String)
		if err != nil || !row.cleanupClaimed || cleaned.Before(row.cleanupStarted) {
			return ErrArtifactStageConflict
		}
	}
	return nil
}
