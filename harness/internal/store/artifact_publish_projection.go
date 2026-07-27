package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type artifactRootProjectionRow struct {
	root           model.Digest
	manifestDigest model.Digest
	verifiedAt     time.Time
}

func readOperationArtifactClosureProjection(ctx context.Context, tx *sql.Tx,
	id model.OperationID,
) (VerifiedArtifactClosure, error) {
	return readArtifactClosureProjection(ctx, tx,
		`SELECT root_digest,manifest_digest,verified_at
		 FROM operation_artifact_roots WHERE operation_id=? ORDER BY root_digest`,
		`SELECT m.root_digest,m.ordinal,m.logical_path,m.offset_bytes,m.length_bytes,
		        m.block_digest,m.mode
		 FROM operation_artifact_roots p
		 JOIN artifact_root_blocks m ON m.root_digest=p.root_digest
		 WHERE p.operation_id=? ORDER BY m.root_digest,m.ordinal`,
		`SELECT DISTINCT b.block_digest,b.size_bytes,b.created_at
		 FROM operation_artifact_roots p
		 JOIN artifact_root_blocks m ON m.root_digest=p.root_digest
		 JOIN artifact_blocks b ON b.block_digest=m.block_digest
		 WHERE p.operation_id=? ORDER BY b.block_digest`,
		id.String())
}

func readPeerInboxArtifactClosureProjection(ctx context.Context, tx *sql.Tx,
	id model.InboxID,
) (VerifiedArtifactClosure, error) {
	return readArtifactClosureProjection(ctx, tx,
		`SELECT root_digest,manifest_digest,verified_at
		 FROM peer_inbox_artifact_roots WHERE inbox_id=? ORDER BY root_digest`,
		`SELECT m.root_digest,m.ordinal,m.logical_path,m.offset_bytes,m.length_bytes,
		        m.block_digest,m.mode
		 FROM peer_inbox_artifact_roots p
		 JOIN artifact_root_blocks m ON m.root_digest=p.root_digest
		 WHERE p.inbox_id=? ORDER BY m.root_digest,m.ordinal`,
		`SELECT DISTINCT b.block_digest,b.size_bytes,b.created_at
		 FROM peer_inbox_artifact_roots p
		 JOIN artifact_root_blocks m ON m.root_digest=p.root_digest
		 JOIN artifact_blocks b ON b.block_digest=m.block_digest
		 WHERE p.inbox_id=? ORDER BY b.block_digest`,
		id.String())
}

func readArtifactClosureProjection(ctx context.Context, tx *sql.Tx,
	rootsQuery, rootBlocksQuery, blocksQuery, ownerID string,
) (VerifiedArtifactClosure, error) {
	projected, err := readArtifactRootProjectionRows(ctx, tx, rootsQuery, ownerID)
	if err != nil {
		return VerifiedArtifactClosure{}, err
	}
	if len(projected) == 0 || len(projected) > maxVerifiedClosureRoots {
		return VerifiedArtifactClosure{}, ErrArtifactStageConflict
	}
	closure := VerifiedArtifactClosure{
		Roots: make([]VerifiedArtifactRoot, 0, len(projected)),
	}
	for _, projection := range projected {
		root, state, err := readArtifactRoot(ctx, tx, projection.root)
		if err != nil || state != "staged" && state != "verified" ||
			root.ManifestDigest != projection.manifestDigest ||
			projection.verifiedAt.Before(root.CreatedAt) {
			return VerifiedArtifactClosure{}, ErrArtifactStageConflict
		}
		root.VerifiedAt = projection.verifiedAt
		closure.Roots = append(closure.Roots, root)
	}
	closure.RootBlocks, err = readProjectedArtifactRootBlocks(ctx, tx,
		rootBlocksQuery, ownerID)
	if err != nil {
		return VerifiedArtifactClosure{}, err
	}
	closure.Blocks, err = readProjectedArtifactBlocks(ctx, tx, blocksQuery, ownerID)
	if err != nil {
		return VerifiedArtifactClosure{}, err
	}
	validated, err := validateVerifiedArtifactClosure(closure)
	if err != nil {
		return VerifiedArtifactClosure{}, ErrArtifactStageConflict
	}
	return validated, nil
}

func readArtifactRootProjectionRows(ctx context.Context, tx *sql.Tx,
	query, ownerID string,
) ([]artifactRootProjectionRow, error) {
	rows, err := tx.QueryContext(ctx, query, ownerID)
	if err != nil {
		return nil, fmt.Errorf("%w: read root projection: %v", ErrArtifactStageConflict, err)
	}
	defer rows.Close()
	result := make([]artifactRootProjectionRow, 0)
	for rows.Next() {
		var rootText, verifiedText string
		var manifestBytes []byte
		if err := rows.Scan(&rootText, &manifestBytes, &verifiedText); err != nil {
			return nil, ErrArtifactStageConflict
		}
		root, rootErr := model.ParseDigest(rootText)
		manifest, manifestErr := model.DigestFromBytes(manifestBytes)
		verifiedAt, timeErr := parseCanonicalStoreTime(verifiedText)
		if rootErr != nil || manifestErr != nil || timeErr != nil {
			return nil, ErrArtifactStageConflict
		}
		result = append(result, artifactRootProjectionRow{
			root: root, manifestDigest: manifest, verifiedAt: verifiedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func readProjectedArtifactRootBlocks(ctx context.Context, tx *sql.Tx,
	query, ownerID string,
) ([]VerifiedArtifactRootBlock, error) {
	rows, err := tx.QueryContext(ctx, query, ownerID)
	if err != nil {
		return nil, fmt.Errorf("%w: read projected root map: %v", ErrArtifactStageConflict, err)
	}
	defer rows.Close()
	result := make([]VerifiedArtifactRootBlock, 0)
	for rows.Next() {
		var row VerifiedArtifactRootBlock
		var rootText, blockText string
		if err := rows.Scan(&rootText, &row.Ordinal, &row.LogicalPath,
			&row.OffsetBytes, &row.LengthBytes, &blockText, &row.Mode); err != nil {
			return nil, ErrArtifactStageConflict
		}
		var err error
		row.RootDigest, err = model.ParseDigest(rootText)
		if err == nil {
			row.BlockDigest, err = model.ParseDigest(blockText)
		}
		if err != nil {
			return nil, ErrArtifactStageConflict
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func readProjectedArtifactBlocks(ctx context.Context, tx *sql.Tx,
	query, ownerID string,
) ([]VerifiedArtifactBlock, error) {
	rows, err := tx.QueryContext(ctx, query, ownerID)
	if err != nil {
		return nil, fmt.Errorf("%w: read projected blocks: %v", ErrArtifactStageConflict, err)
	}
	defer rows.Close()
	result := make([]VerifiedArtifactBlock, 0)
	for rows.Next() {
		var digestText, createdText string
		var size int64
		if err := rows.Scan(&digestText, &size, &createdText); err != nil || size <= 0 {
			return nil, ErrArtifactStageConflict
		}
		digest, digestErr := model.ParseDigest(digestText)
		createdAt, timeErr := parseCanonicalStoreTime(createdText)
		if digestErr != nil || timeErr != nil {
			return nil, ErrArtifactStageConflict
		}
		result = append(result, VerifiedArtifactBlock{
			Digest: digest, SizeBytes: uint64(size), CreatedAt: createdAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func cloneVerifiedArtifactClosureValue(value VerifiedArtifactClosure) VerifiedArtifactClosure {
	result := VerifiedArtifactClosure{
		Roots:      append([]VerifiedArtifactRoot(nil), value.Roots...),
		Blocks:     append([]VerifiedArtifactBlock(nil), value.Blocks...),
		RootBlocks: append([]VerifiedArtifactRootBlock(nil), value.RootBlocks...),
	}
	for index := range result.Roots {
		if !result.Roots[index].Manifest.IsZero() {
			manifest, err := model.NewJSON(result.Roots[index].Manifest.Bytes())
			if err == nil {
				result.Roots[index].Manifest = manifest
			}
		}
	}
	return result
}
