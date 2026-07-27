package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func insertPeerInboxArtifactRootProjection(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, roots []VerifiedArtifactRoot,
) error {
	if err := requirePeerInboxArtifactRootProjection(ctx, tx, inboxID, nil); err != nil {
		return err
	}
	for _, root := range roots {
		if _, err := tx.ExecContext(ctx, `INSERT INTO peer_inbox_artifact_roots(
			inbox_id,root_digest,manifest_digest,verified_at) VALUES(?,?,?,?)`,
			inboxID.String(), root.RootDigest.String(), root.ManifestDigest.Bytes(),
			storeTime(root.VerifiedAt)); err != nil {
			return fmt.Errorf("insert Peer Inbox Artifact root projection: %w", err)
		}
	}
	return requirePeerInboxArtifactRootProjection(ctx, tx, inboxID, roots)
}

func requirePeerInboxArtifactRootProjection(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, expected []VerifiedArtifactRoot,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT root_digest,manifest_digest,verified_at
		FROM peer_inbox_artifact_roots WHERE inbox_id=? ORDER BY root_digest`,
		inboxID.String())
	if err != nil {
		return fmt.Errorf("%w: read Inbox root projection: %v", ErrArtifactStageConflict, err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var rootText string
		var manifestBytes []byte
		var verifiedText string
		if err := rows.Scan(&rootText, &manifestBytes, &verifiedText); err != nil ||
			index >= len(expected) {
			return ErrArtifactStageConflict
		}
		root, rootErr := model.ParseDigest(rootText)
		manifest, manifestErr := model.DigestFromBytes(manifestBytes)
		verifiedAt, verifiedErr := parseCanonicalStoreTime(verifiedText)
		if rootErr != nil || manifestErr != nil ||
			verifiedErr != nil || root != expected[index].RootDigest ||
			manifest != expected[index].ManifestDigest ||
			!verifiedAt.Equal(expected[index].VerifiedAt) {
			return ErrArtifactStageConflict
		}
		index++
	}
	if err := rows.Err(); err != nil || index != len(expected) {
		return ErrArtifactStageConflict
	}
	return nil
}

func requirePeerInboxStageProjectionRoots(ctx context.Context, tx *sql.Tx,
	inboxID model.InboxID, expected []model.Digest,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT p.root_digest,p.manifest_digest,r.manifest_digest
		FROM peer_inbox_artifact_roots p
		JOIN artifact_roots r ON r.root_digest=p.root_digest
		WHERE p.inbox_id=? ORDER BY p.root_digest`, inboxID.String())
	if err != nil {
		return ErrArtifactStageConflict
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var rootText string
		var projected, durable []byte
		if err := rows.Scan(&rootText, &projected, &durable); err != nil ||
			index >= len(expected) {
			return ErrArtifactStageConflict
		}
		root, rootErr := model.ParseDigest(rootText)
		projectedDigest, projectedErr := model.DigestFromBytes(projected)
		durableDigest, durableErr := model.DigestFromBytes(durable)
		if rootErr != nil || projectedErr != nil || durableErr != nil ||
			root != expected[index] || projectedDigest != durableDigest {
			return ErrArtifactStageConflict
		}
		index++
	}
	if err := rows.Err(); err != nil || index != len(expected) {
		return ErrArtifactStageConflict
	}
	return nil
}

func renewPeerInboxArtifactStageFence(ctx context.Context, tx *sql.Tx,
	row peerInboxArtifactRow, oldFence PeerInboxArtifactFence, leaseUntil, at time.Time,
) error {
	stage, found, err := readPeerInboxArtifactStage(ctx, tx, row.inboxID)
	if err != nil {
		return err
	}
	if !found || stage.cleanupClaimed {
		return nil
	}
	if stage.state == ArtifactStageReady || stage.semanticNonce != row.semanticNonce ||
		stage.attempt != oldFence.attempt || stage.leaseOwner != oldFence.leaseOwner {
		return ErrArtifactStageConflict
	}
	if stage.leaseUntil.Equal(leaseUntil) && !stage.updatedAt.Before(at) {
		return nil
	}
	if !stage.leaseUntil.Equal(oldFence.leaseUntil) || at.Before(stage.updatedAt) {
		return ErrArtifactStageFence
	}
	updated, err := tx.ExecContext(ctx, `UPDATE peer_inbox_artifact_stages
		SET lease_until=?,updated_at=? WHERE inbox_id=? AND generation=?
		AND state IN ('staged','publishing') AND attempt=? AND lease_owner=?
		AND lease_until=? AND semantic_nonce=? AND updated_at=?
		AND cleanup_started_at IS NULL`,
		storeTime(leaseUntil), storeTime(at), row.inboxID.String(), stage.generation,
		stage.attempt, stage.leaseOwner, storeTime(stage.leaseUntil), stage.semanticNonce[:],
		storeTime(stage.updatedAt))
	if err != nil || exactlyOne(updated) != nil {
		return fmt.Errorf("%w: renew Inbox stage: %v", ErrArtifactStageFence, err)
	}
	return nil
}
