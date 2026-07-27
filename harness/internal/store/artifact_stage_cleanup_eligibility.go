package store

import (
	"context"
	"database/sql"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func artifactStageCleanupEligible(ctx context.Context, tx *sql.Tx,
	row artifactStageCleanupRow, cutoff, at time.Time,
) (bool, error) {
	if row.owner.IsZero() || !row.updatedAt.Before(cutoff) {
		return false, ErrArtifactStageConflict
	}
	switch row.owner.Kind() {
	case artifactdomain.StageOwnerOperation:
		return operationArtifactStageCleanupEligible(ctx, tx, row, cutoff)
	case artifactdomain.StageOwnerInbox:
		return peerInboxArtifactStageCleanupEligible(ctx, tx, row, cutoff, at)
	default:
		return false, ErrArtifactStageConflict
	}
}

func operationArtifactStageCleanupEligible(ctx context.Context, tx *sql.Tx,
	row artifactStageCleanupRow, cutoff time.Time,
) (bool, error) {
	id, err := model.ParseOperationID(row.owner.CanonicalID())
	if err != nil {
		return false, ErrArtifactStageConflict
	}
	stage, found, err := readOperationArtifactStageGeneration(ctx, tx, id,
		row.owner.Generation())
	if err != nil || !found || !exactArtifactStageCleanupRow(stage, row) {
		return false, ErrArtifactStageConflict
	}
	if stage.state == ArtifactStageReady {
		return operationArtifactStageFinalReady(ctx, tx, id, stage)
	}
	if stage.state == ArtifactStagePublishing {
		return operationArtifactStageTerminalUnaccepted(ctx, tx, id, stage, cutoff)
	}
	return operationArtifactStageExpired(ctx, tx, id, stage, cutoff)
}

func peerInboxArtifactStageCleanupEligible(ctx context.Context, tx *sql.Tx,
	row artifactStageCleanupRow, cutoff, at time.Time,
) (bool, error) {
	id, err := model.ParseInboxID(row.owner.CanonicalID())
	if err != nil {
		return false, ErrArtifactStageConflict
	}
	stage, found, err := readPeerInboxArtifactStageGeneration(ctx, tx, id,
		row.owner.Generation())
	if err != nil || !found || !exactArtifactStageCleanupRow(stage, row) {
		return false, ErrArtifactStageConflict
	}
	if stage.state == ArtifactStageReady {
		return peerInboxArtifactStageFinalReady(ctx, tx, id)
	}
	if stage.state == ArtifactStagePublishing {
		return peerInboxArtifactStageTerminalUnaccepted(
			ctx, tx, id, stage, row.owner, cutoff, at)
	}
	return peerInboxArtifactStageExpired(ctx, tx, id, stage, cutoff)
}

func operationArtifactStageTerminalUnaccepted(ctx context.Context, tx *sql.Tx,
	id model.OperationID, stage durableArtifactStage, cutoff time.Time,
) (bool, error) {
	var status string
	var finishedText sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT status,finished_at FROM operations
		WHERE operation_id=?`, id.String()).Scan(&status, &finishedText); err != nil {
		return false, ErrArtifactStageConflict
	}
	if status != "rejected" || !finishedText.Valid || stage.updatedAt.After(cutoff) {
		return false, nil
	}
	finishedAt, err := parseCanonicalStoreTime(finishedText.String)
	if err != nil || finishedAt.After(cutoff) {
		return false, err
	}
	var accepted int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM artifact_provenance
		WHERE operation_id=?`, id.String()).Scan(&accepted); err != nil {
		return false, err
	}
	return accepted == 0, nil
}

func exactArtifactStageCleanupRow(stage durableArtifactStage,
	row artifactStageCleanupRow,
) bool {
	return stage.state == row.state &&
		stage.updatedAt.Equal(row.updatedAt) &&
		!stage.cleanupClaimed
}

func operationArtifactStageExpired(ctx context.Context, tx *sql.Tx,
	id model.OperationID, stage durableArtifactStage, cutoff time.Time,
) (bool, error) {
	var status string
	var leaseOwner, leaseText, finished sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT status,lease_owner,lease_until,finished_at
		FROM operations WHERE operation_id=?`, id.String()).Scan(
		&status, &leaseOwner, &leaseText, &finished); err != nil {
		return false, ErrArtifactStageConflict
	}
	if status == "committed" {
		return !stage.leaseUntil.After(cutoff), nil
	}
	if status == "rejected" {
		if !finished.Valid {
			return false, ErrArtifactStageConflict
		}
		value, err := parseCanonicalStoreTime(finished.String)
		return err == nil && !value.After(cutoff), err
	}
	if status != "started" {
		return false, ErrArtifactStageConflict
	}
	if !leaseOwner.Valid || !leaseText.Valid {
		return false, ErrArtifactStageConflict
	}
	leaseUntil, err := parseCanonicalStoreTime(leaseText.String)
	if err != nil {
		return false, ErrArtifactStageConflict
	}
	if leaseOwner.String != stage.leaseOwner || !leaseUntil.Equal(stage.leaseUntil) {
		return !stage.leaseUntil.After(cutoff), nil
	}
	return !stage.leaseUntil.After(cutoff) && !leaseUntil.After(cutoff), nil
}

func operationArtifactStageFinalReady(ctx context.Context, tx *sql.Tx,
	id model.OperationID, stage durableArtifactStage,
) (bool, error) {
	var captureRaw []byte
	if err := tx.QueryRowContext(ctx, `SELECT capture_json FROM operations
		WHERE operation_id=?`, id.String()).Scan(&captureRaw); err != nil {
		return false, nil
	}
	capture, err := model.NewJSON(captureRaw)
	if err != nil || model.Sum(capture.Bytes()) != stage.payloadDigest {
		return false, nil
	}
	roots, err := parseOperationCapture(capture)
	if err != nil || len(roots) == 0 {
		return false, err
	}
	if err := requireOperationArtifactProjection(ctx, tx, id, roots); err != nil {
		return false, nil
	}
	for _, root := range roots {
		verified, err := requireVerifiedArtifactRoot(ctx, tx, root.RootDigest)
		if err != nil || verified.ManifestDigest != root.ManifestDigest {
			return false, nil
		}
	}
	return true, nil
}

func peerInboxArtifactStageExpired(ctx context.Context, tx *sql.Tx,
	id model.InboxID, stage durableArtifactStage, cutoff time.Time,
) (bool, error) {
	if stage.leaseUntil.After(cutoff) {
		return false, nil
	}
	var status string
	var attempt int64
	var leaseOwner, leaseText sql.NullString
	var nonce []byte
	if err := tx.QueryRowContext(ctx, `SELECT status,attempts,lease_owner,lease_until,
		semantic_nonce FROM peer_inbox WHERE inbox_id=?`, id.String()).Scan(
		&status, &attempt, &leaseOwner, &leaseText, &nonce); err != nil {
		return false, ErrArtifactStageConflict
	}
	if status != "waiting_artifact" {
		return true, nil
	}
	if attempt <= 0 || uint64(attempt) > uint64(^uint32(0)) ||
		len(nonce) != len(stage.semanticNonce) {
		return false, ErrArtifactStageConflict
	}
	var durableNonce [32]byte
	copy(durableNonce[:], nonce)
	if !leaseOwner.Valid || !leaseText.Valid {
		return false, ErrArtifactStageConflict
	}
	leaseUntil, err := parseCanonicalStoreTime(leaseText.String)
	if err != nil {
		return false, ErrArtifactStageConflict
	}
	exact := uint32(attempt) == stage.attempt && leaseOwner.String == stage.leaseOwner &&
		leaseUntil.Equal(stage.leaseUntil) && durableNonce == stage.semanticNonce
	return !exact || !leaseUntil.After(cutoff), nil
}

func peerInboxArtifactStageFinalReady(ctx context.Context, tx *sql.Tx,
	id model.InboxID,
) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM peer_inbox_artifact_roots
		WHERE inbox_id=?`, id.String()).Scan(&count); err != nil || count == 0 {
		return false, err
	}
	var valid int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*)
		FROM peer_inbox_artifact_roots p
		JOIN artifact_roots r ON r.root_digest=p.root_digest
			AND r.manifest_digest=p.manifest_digest AND r.state='verified'
			AND r.verified_at IS NOT NULL
		JOIN artifact_pins pin ON pin.root_digest=p.root_digest
			AND pin.owner_kind='inbox' AND pin.owner_id=p.inbox_id
			AND pin.expires_at IS NULL
		WHERE p.inbox_id=?`, id.String()).Scan(&valid); err != nil {
		return false, err
	}
	if valid == count {
		return true, nil
	}
	terminal, found, err := readPeerInboxSemanticTerminalRow(ctx, tx, id)
	if err != nil || !found || len(terminal.requiredRoots) != count {
		return false, err
	}
	if err := requireNoPeerInboxSemanticArtifactPins(ctx, tx, id); err != nil {
		return false, nil
	}
	if err := requirePeerInboxStageProjectionRoots(ctx, tx, id,
		terminal.requiredRoots); err != nil {
		return false, nil
	}
	if err := validatePeerInboxSemanticImportedArtifacts(ctx, tx,
		terminal.publication.Event()); err != nil {
		return false, nil
	}
	return true, nil
}

func readExactArtifactStage(ctx context.Context, tx *sql.Tx,
	owner artifactdomain.StageOwner,
) (durableArtifactStage, bool, error) {
	switch owner.Kind() {
	case artifactdomain.StageOwnerOperation:
		id, err := model.ParseOperationID(owner.CanonicalID())
		if err != nil {
			return durableArtifactStage{}, false, ErrArtifactStageFence
		}
		return readOperationArtifactStageGeneration(ctx, tx, id, owner.Generation())
	case artifactdomain.StageOwnerInbox:
		id, err := model.ParseInboxID(owner.CanonicalID())
		if err != nil {
			return durableArtifactStage{}, false, ErrArtifactStageFence
		}
		return readPeerInboxArtifactStageGeneration(ctx, tx, id, owner.Generation())
	default:
		return durableArtifactStage{}, false, ErrArtifactStageFence
	}
}

func newArtifactStageOwner(kind artifactdomain.StageOwnerKind, id string,
	generation uint64,
) (artifactdomain.StageOwner, error) {
	switch kind {
	case artifactdomain.StageOwnerOperation:
		parsed, err := model.ParseOperationID(id)
		if err != nil {
			return artifactdomain.StageOwner{}, err
		}
		return artifactdomain.NewOperationStageOwner(parsed, generation)
	case artifactdomain.StageOwnerInbox:
		parsed, err := model.ParseInboxID(id)
		if err != nil {
			return artifactdomain.StageOwner{}, err
		}
		return artifactdomain.NewInboxStageOwner(parsed, generation)
	default:
		return artifactdomain.StageOwner{}, ErrArtifactStageConflict
	}
}

func artifactStageOwnerTable(kind artifactdomain.StageOwnerKind) (string, string, error) {
	switch kind {
	case artifactdomain.StageOwnerOperation:
		return "operation_artifact_stages", "operation_id", nil
	case artifactdomain.StageOwnerInbox:
		return "peer_inbox_artifact_stages", "inbox_id", nil
	default:
		return "", "", ErrArtifactStageFence
	}
}
