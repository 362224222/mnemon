package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateOperationArtifactStageCall(s *Store, ctx context.Context,
	spec BeginOperationArtifactStageSpec,
) (time.Time, time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.OperationID.IsZero() ||
		!validPublicationIdentifier(spec.LeaseOwner) {
		return time.Time{}, time.Time{}, ErrArtifactStageFence
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at != spec.At {
		return time.Time{}, time.Time{}, ErrArtifactStageFence
	}
	leaseUntil, err := canonicalStoreTime(spec.LeaseUntil)
	if err != nil || leaseUntil != spec.LeaseUntil || !at.Before(leaseUntil) {
		return time.Time{}, time.Time{}, ErrArtifactStageFence
	}
	return at, leaseUntil, nil
}

func validateOperationArtifactFence(s *Store, ctx context.Context,
	fence OperationArtifactStageFence, value time.Time,
) (time.Time, error) {
	if fence.owner.IsZero() || fence.owner.Kind() != artifactdomain.StageOwnerOperation {
		return time.Time{}, ErrArtifactStageFence
	}
	id, err := model.ParseOperationID(fence.owner.CanonicalID())
	if err != nil {
		return time.Time{}, ErrArtifactStageFence
	}
	at, _, err := validateOperationArtifactStageCall(s, ctx, BeginOperationArtifactStageSpec{
		OperationID: id, LeaseOwner: fence.leaseOwner, LeaseUntil: fence.leaseUntil, At: value,
	})
	return at, err
}

func requireExactOperationArtifactFence(operation model.Operation, owner string,
	leaseUntil, at time.Time,
) error {
	durableUntil, ok := operation.LeaseUntil()
	if err := requireOperationFence(operation, owner, at); err != nil || !ok ||
		!durableUntil.Equal(leaseUntil) {
		return ErrOperationFence
	}
	return nil
}

func requireOperationStageFence(stage durableArtifactStage,
	fence OperationArtifactStageFence,
) error {
	if stage.generation != fence.owner.Generation() || stage.leaseOwner != fence.leaseOwner ||
		!stage.leaseUntil.Equal(fence.leaseUntil) {
		return ErrArtifactStageFence
	}
	return nil
}

func operationIDFromStageOwner(owner artifactdomain.StageOwner) (model.OperationID, error) {
	if owner.IsZero() || owner.Kind() != artifactdomain.StageOwnerOperation {
		return model.OperationID{}, ErrArtifactStageFence
	}
	id, err := model.ParseOperationID(owner.CanonicalID())
	if err != nil {
		return model.OperationID{}, ErrArtifactStageFence
	}
	return id, nil
}

func captureMatchesClosure(captured []captureRoot, roots []VerifiedArtifactRoot) bool {
	if len(captured) != len(roots) {
		return false
	}
	for index := range roots {
		if captured[index].RootDigest != roots[index].RootDigest ||
			captured[index].ManifestDigest != roots[index].ManifestDigest {
			return false
		}
	}
	return true
}

func verifiedClosureDigest(closure VerifiedArtifactClosure) (model.Digest, error) {
	digest := sha256.New()
	writeArtifactSemanticBytes(digest, []byte("mnemon/r5/artifact-semantic-closure/1"))
	writeArtifactSemanticUint64(digest, uint64(len(closure.Roots)))
	for _, root := range closure.Roots {
		writeArtifactSemanticBytes(digest, root.RootDigest.Bytes())
		writeArtifactSemanticBytes(digest, root.ManifestDigest.Bytes())
		writeArtifactSemanticUint64(digest, root.TotalBytes)
	}
	writeArtifactSemanticUint64(digest, uint64(len(closure.Blocks)))
	for _, block := range closure.Blocks {
		writeArtifactSemanticBytes(digest, block.Digest.Bytes())
		writeArtifactSemanticUint64(digest, block.SizeBytes)
	}
	writeArtifactSemanticUint64(digest, uint64(len(closure.RootBlocks)))
	for _, row := range closure.RootBlocks {
		writeArtifactSemanticBytes(digest, row.RootDigest.Bytes())
		writeArtifactSemanticUint64(digest, row.Ordinal)
		writeArtifactSemanticBytes(digest, []byte(row.LogicalPath))
		writeArtifactSemanticUint64(digest, row.OffsetBytes)
		writeArtifactSemanticUint64(digest, row.LengthBytes)
		writeArtifactSemanticBytes(digest, row.BlockDigest.Bytes())
		writeArtifactSemanticUint64(digest, uint64(row.Mode))
	}
	value, err := model.DigestFromBytes(digest.Sum(nil))
	if err != nil {
		return model.Digest{}, fmt.Errorf("digest verified Artifact closure: %w", err)
	}
	return value, nil
}

func writeArtifactSemanticBytes(target hash.Hash, value []byte) {
	writeArtifactSemanticUint64(target, uint64(len(value)))
	_, _ = target.Write(value)
}

func writeArtifactSemanticUint64(target hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = target.Write(encoded[:])
}

func stageArtifactClosureMetadata(ctx context.Context, tx *sql.Tx,
	closure VerifiedArtifactClosure, at time.Time,
) (VerifiedArtifactClosure, bool, error) {
	durable := cloneVerifiedArtifactClosureValue(closure)
	blocks, blocksReplayed, err := stageArtifactClosureBlocks(ctx, tx,
		closure.Blocks, at)
	if err != nil {
		return VerifiedArtifactClosure{}, false, err
	}
	durable.Blocks = blocks
	roots, rootStates, rootsReplayed, err := stageArtifactClosureRoots(ctx, tx,
		closure.Roots, at)
	if err != nil {
		return VerifiedArtifactClosure{}, false, err
	}
	durable.Roots = roots
	mapsReplayed, err := stageArtifactClosureRootMaps(ctx, tx, closure, rootStates)
	if err != nil {
		return VerifiedArtifactClosure{}, false, err
	}
	validated, err := validateVerifiedArtifactClosure(durable)
	if err != nil {
		return VerifiedArtifactClosure{}, false, ErrArtifactStageConflict
	}
	return validated, blocksReplayed && rootsReplayed && mapsReplayed, nil
}

func stageArtifactClosureBlocks(ctx context.Context, tx *sql.Tx,
	blocks []VerifiedArtifactBlock, at time.Time,
) ([]VerifiedArtifactBlock, bool, error) {
	durable := make([]VerifiedArtifactBlock, len(blocks))
	replayed := true
	for index, block := range blocks {
		if block.CreatedAt.After(at) {
			return nil, false, ErrArtifactStageConflict
		}
		stored, found, err := checkpointArtifactBlock(ctx, tx, block)
		if err != nil {
			return nil, false, err
		}
		if stored.CreatedAt.After(at) {
			return nil, false, ErrArtifactStageConflict
		}
		durable[index] = stored
		replayed = replayed && found
	}
	return durable, replayed, nil
}

func stageArtifactClosureRoots(ctx context.Context, tx *sql.Tx,
	roots []VerifiedArtifactRoot, at time.Time,
) ([]VerifiedArtifactRoot, map[model.Digest]string, bool, error) {
	durable := make([]VerifiedArtifactRoot, len(roots))
	states := make(map[model.Digest]string, len(roots))
	replayed := true
	for index, root := range roots {
		if root.CreatedAt.After(at) || root.VerifiedAt.After(at) {
			return nil, nil, false, ErrArtifactStageConflict
		}
		stored, state, found, err := stageArtifactClosureRoot(ctx, tx, root)
		if err != nil {
			return nil, nil, false, err
		}
		if stored.CreatedAt.After(at) || state == "verified" && stored.VerifiedAt.After(at) {
			return nil, nil, false, ErrArtifactStageConflict
		}
		stored.VerifiedAt = root.VerifiedAt
		if stored.VerifiedAt.Before(stored.CreatedAt) {
			stored.VerifiedAt = stored.CreatedAt
		}
		durable[index] = stored
		states[root.RootDigest] = state
		replayed = replayed && found
	}
	return durable, states, replayed, nil
}

func stageArtifactClosureRootMaps(ctx context.Context, tx *sql.Tx,
	closure VerifiedArtifactClosure, rootStates map[model.Digest]string,
) (bool, error) {
	replayed := true
	for _, root := range closure.Roots {
		state, err := checkpointArtifactRootBlockMap(ctx, tx, root.RootDigest,
			rootBlocksForDigest(closure.RootBlocks, root.RootDigest))
		if err != nil {
			return false, err
		}
		if state.rootState != rootStates[root.RootDigest] {
			return false, ErrArtifactStageConflict
		}
		replayed = replayed && !state.changed
	}
	return replayed, nil
}

func requireArtifactClosureProjection(ctx context.Context, tx *sql.Tx,
	closure VerifiedArtifactClosure,
) error {
	for _, root := range closure.Roots {
		stored, state, err := readArtifactRoot(ctx, tx, root.RootDigest)
		if err != nil || state != "staged" && state != "verified" ||
			!sameArtifactContent(stored, root) {
			return ErrArtifactStageConflict
		}
		mapping, err := readArtifactRootBlockMap(ctx, tx, root.RootDigest)
		if err != nil || !equalArtifactRootBlocks(mapping,
			rootBlocksForDigest(closure.RootBlocks, root.RootDigest)) {
			return ErrArtifactStageConflict
		}
	}
	for _, block := range closure.Blocks {
		var size uint64
		if err := tx.QueryRowContext(ctx, `SELECT size_bytes FROM artifact_blocks
			WHERE block_digest=?`, block.Digest.String()).Scan(&size); err != nil ||
			size != block.SizeBytes {
			return ErrArtifactStageConflict
		}
	}
	return nil
}

func readOperationArtifactDigests(ctx context.Context, tx *sql.Tx,
	id model.OperationID,
) ([]model.Digest, error) {
	rows, err := tx.QueryContext(ctx, `SELECT root_digest FROM operation_artifact_roots
		WHERE operation_id=? ORDER BY root_digest`, id.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []model.Digest
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return nil, err
		}
		root, err := model.ParseDigest(text)
		if err != nil {
			return nil, ErrArtifactStageConflict
		}
		result = append(result, root)
	}
	return result, rows.Err()
}

func promoteArtifactRoots(ctx context.Context, tx *sql.Tx, roots []model.Digest,
	at time.Time,
) error {
	if err := requirePromotablePeerInboxArtifactClosures(ctx, tx, roots, at); err != nil {
		return err
	}
	for _, root := range roots {
		result, err := tx.ExecContext(ctx, `UPDATE artifact_roots SET state='verified',verified_at=?
			WHERE root_digest=? AND state='staged' AND verified_at IS NULL`,
			storeTime(at), root.String())
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil || affected > 1 {
			return ErrArtifactStageConflict
		}
	}
	return requireReadyArtifactRoots(ctx, tx, roots, at)
}

func requireReadyArtifactRoots(ctx context.Context, tx *sql.Tx, roots []model.Digest,
	at time.Time,
) error {
	for _, root := range roots {
		value, err := requireVerifiedArtifactRoot(ctx, tx, root)
		if err != nil || value.VerifiedAt.After(at) {
			return ErrArtifactStageConflict
		}
	}
	return nil
}

func requireAcceptedPublishingArtifactRoots(ctx context.Context, tx *sql.Tx,
	roots []model.Digest, at time.Time,
) error {
	if err := requireReadyArtifactRoots(ctx, tx, roots, at); err == nil {
		return nil
	}
	return requirePromotablePeerInboxArtifactClosures(ctx, tx, roots, at)
}
