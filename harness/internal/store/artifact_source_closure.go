package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readSealedArtifactSourceClosure(ctx context.Context, tx *sql.Tx,
	rootDigest model.Digest,
) (sealedArtifactSourceClosure, error) {
	root, manifest, expected, err := readSealedArtifactSourceRoot(
		ctx, tx, rootDigest)
	if err != nil {
		return sealedArtifactSourceClosure{}, err
	}
	blockSizes, err := readArtifactSourceBlockSizes(ctx, tx, expected)
	if err != nil {
		return sealedArtifactSourceClosure{}, err
	}
	if manifest.RootDigest() != root.RootDigest {
		return sealedArtifactSourceClosure{}, ErrArtifactSourceInvariant
	}
	return sealedArtifactSourceClosure{root: root, blockSizes: blockSizes}, nil
}

func readSealedArtifactSourceRoot(ctx context.Context, tx *sql.Tx,
	rootDigest model.Digest,
) (VerifiedArtifactRoot, artifactdomain.Manifest,
	[]VerifiedArtifactRootBlock, error,
) {
	root, state, err := readArtifactRoot(ctx, tx, rootDigest)
	if err != nil {
		return VerifiedArtifactRoot{}, artifactdomain.Manifest{}, nil,
			fmt.Errorf("%w: root metadata: %v", ErrArtifactSourceInvariant, err)
	}
	if state == "staged" && root.VerifiedAt.IsZero() {
		return VerifiedArtifactRoot{}, artifactdomain.Manifest{}, nil,
			ErrArtifactSourceUnavailable
	}
	if state != "verified" || root.VerifiedAt.IsZero() {
		return VerifiedArtifactRoot{}, artifactdomain.Manifest{}, nil,
			fmt.Errorf("%w: Event-pinned root is not verified",
				ErrArtifactSourceInvariant)
	}
	manifest, err := artifactdomain.ParseManifest(root.Manifest.Bytes())
	if err != nil || manifest.RootDigest() != root.RootDigest ||
		manifest.ManifestDigest() != root.ManifestDigest ||
		manifest.TotalBytes() != root.TotalBytes ||
		!bytes.Equal(manifest.CanonicalJSON().Bytes(), root.Manifest.Bytes()) {
		return VerifiedArtifactRoot{}, artifactdomain.Manifest{}, nil,
			fmt.Errorf("%w: sealed manifest projection: %v",
				ErrArtifactSourceInvariant, err)
	}
	expected := artifactSourceRootMap(manifest)
	actual, err := readArtifactRootBlockMap(ctx, tx, rootDigest)
	if err != nil || !equalArtifactRootBlocks(actual, expected) {
		return VerifiedArtifactRoot{}, artifactdomain.Manifest{}, nil,
			fmt.Errorf("%w: sealed root block map: %v",
				ErrArtifactSourceInvariant, err)
	}
	return root, manifest, expected, nil
}

func readArtifactSourceBlockSizes(ctx context.Context, tx *sql.Tx,
	rows []VerifiedArtifactRootBlock,
) (map[model.Digest]uint64, error) {
	result := make(map[model.Digest]uint64)
	for _, row := range rows {
		if previous, present := result[row.BlockDigest]; present {
			if previous != row.LengthBytes {
				return nil, fmt.Errorf("%w: inconsistent manifest block size",
					ErrArtifactSourceInvariant)
			}
			continue
		}
		size, err := readArtifactSourceBlockSize(ctx, tx, row)
		if err != nil {
			return nil, err
		}
		result[row.BlockDigest] = size
	}
	return result, nil
}

func readArtifactSourceBlockSize(ctx context.Context, tx *sql.Tx,
	row VerifiedArtifactRootBlock,
) (uint64, error) {
	var size uint64
	var createdText string
	err := tx.QueryRowContext(ctx, `SELECT size_bytes,created_at FROM artifact_blocks
		WHERE block_digest=?`, row.BlockDigest.String()).Scan(&size, &createdText)
	if err != nil || size != row.LengthBytes || size == 0 ||
		size > artifactdomain.BlockSize {
		return 0, fmt.Errorf("%w: reachable block metadata: %v",
			ErrArtifactSourceInvariant, err)
	}
	if _, err := parseCanonicalStoreTime(createdText); err != nil {
		return 0, fmt.Errorf("%w: reachable block creation time: %v",
			ErrArtifactSourceInvariant, err)
	}
	return size, nil
}

func artifactSourceRootMap(
	manifest artifactdomain.Manifest,
) []VerifiedArtifactRootBlock {
	entries := manifest.Entries()
	rows := make([]VerifiedArtifactRootBlock, 0)
	for _, entry := range entries {
		for _, block := range entry.Blocks {
			rows = append(rows, VerifiedArtifactRootBlock{
				RootDigest: manifest.RootDigest(),
				Ordinal:    uint64(len(rows)), LogicalPath: entry.LogicalPath,
				OffsetBytes: block.OffsetBytes, LengthBytes: block.LengthBytes,
				BlockDigest: block.Digest, Mode: entry.Mode,
			})
		}
	}
	return rows
}
