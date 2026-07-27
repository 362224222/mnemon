package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type ArtifactRootCheckpoint struct {
	Root     VerifiedArtifactRoot
	Replayed bool
}

func (s *Store) CheckpointVerifiedArtifactRoot(ctx context.Context,
	requested VerifiedArtifactRoot,
) (ArtifactRootCheckpoint, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ArtifactRootCheckpoint{}, errors.New("checkpoint Artifact root: nil store or context")
	}
	root, err := validateVerifiedArtifactRoot(requested)
	if err != nil {
		return ArtifactRootCheckpoint{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactRootCheckpoint{}, fmt.Errorf("checkpoint Artifact root: begin: %w", err)
	}
	defer tx.Rollback()

	checkpoint, err := checkpointVerifiedArtifactRootTx(ctx, tx, root)
	if err != nil {
		return ArtifactRootCheckpoint{}, err
	}
	commitKind := "commit"
	if checkpoint.Replayed {
		commitKind = "replay commit"
	}
	if err := tx.Commit(); err != nil {
		return ArtifactRootCheckpoint{},
			fmt.Errorf("checkpoint Artifact root: %s: %w", commitKind, err)
	}
	return checkpoint, nil
}

func checkpointVerifiedArtifactRootTx(ctx context.Context, tx *sql.Tx,
	root VerifiedArtifactRoot,
) (ArtifactRootCheckpoint, error) {
	existing, state, err := readArtifactRoot(ctx, tx, root.RootDigest)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO artifact_roots(
			root_digest, manifest_json, manifest_digest, total_bytes, state, created_at, verified_at
		) VALUES(?, ?, ?, ?, 'verified', ?, ?)`, root.RootDigest.String(), root.Manifest.Bytes(),
			root.ManifestDigest.Bytes(), root.TotalBytes, storeTime(root.CreatedAt),
			storeTime(root.VerifiedAt))
		if err != nil {
			return ArtifactRootCheckpoint{},
				fmt.Errorf("checkpoint Artifact root: insert: %w", err)
		}
		return ArtifactRootCheckpoint{Root: root}, nil
	}
	if err != nil {
		return ArtifactRootCheckpoint{},
			fmt.Errorf("checkpoint Artifact root: inspect: %w", err)
	}
	return checkpointExistingVerifiedArtifactRoot(ctx, tx, existing, state, root)
}

func checkpointExistingVerifiedArtifactRoot(ctx context.Context, tx *sql.Tx,
	existing VerifiedArtifactRoot, state string, requested VerifiedArtifactRoot,
) (ArtifactRootCheckpoint, error) {
	if !sameArtifactContent(existing, requested) {
		return ArtifactRootCheckpoint{}, ErrArtifactConflict
	}
	if state == "verified" {
		return ArtifactRootCheckpoint{Root: existing, Replayed: true}, nil
	}
	if state != "staged" || requested.VerifiedAt.Before(existing.CreatedAt) {
		return ArtifactRootCheckpoint{}, ErrArtifactConflict
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE artifact_roots SET state = 'verified', verified_at = ? WHERE root_digest = ? AND state = 'staged'",
		storeTime(requested.VerifiedAt), requested.RootDigest.String()); err != nil {
		return ArtifactRootCheckpoint{},
			fmt.Errorf("checkpoint Artifact root: promote: %w", err)
	}
	requested.CreatedAt = existing.CreatedAt
	return ArtifactRootCheckpoint{Root: requested}, nil
}

type VerifiedArtifactClosureCheckpoint struct {
	Closure  VerifiedArtifactClosure
	Replayed bool
}

func (s *Store) CheckpointVerifiedArtifactClosure(ctx context.Context,
	requested VerifiedArtifactClosure,
) (VerifiedArtifactClosureCheckpoint, error) {
	if s == nil || s.db == nil || ctx == nil {
		return VerifiedArtifactClosureCheckpoint{},
			errors.New("checkpoint Artifact closure: nil store or context")
	}
	closure, err := validateVerifiedArtifactClosure(requested)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{},
			fmt.Errorf("checkpoint Artifact closure: begin: %w", err)
	}
	defer tx.Rollback()

	checkpoint, err := checkpointVerifiedArtifactClosureTx(ctx, tx, closure)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	if err := tx.Commit(); err != nil {
		return VerifiedArtifactClosureCheckpoint{},
			fmt.Errorf("checkpoint Artifact closure: commit: %w", err)
	}
	return checkpoint, nil
}

func checkpointVerifiedArtifactClosureTx(ctx context.Context, tx *sql.Tx,
	closure VerifiedArtifactClosure,
) (VerifiedArtifactClosureCheckpoint, error) {
	durableBlocks, blocksReplayed, err := checkpointArtifactClosureBlocks(ctx, tx, closure.Blocks)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	durableRoots, rootStates, rootsReplayed, err := checkpointArtifactClosureRoots(
		ctx, tx, closure.Roots)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	if err := raiseArtifactClosureVerificationTimes(
		durableRoots, rootStates, durableBlocks, closure.RootBlocks); err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	mapsReplayed, err := checkpointArtifactClosureMaps(
		ctx, tx, durableRoots, closure.RootBlocks)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	durableRoots, err = readVerifiedArtifactClosureRoots(
		ctx, tx, durableRoots, closure.RootBlocks)
	if err != nil {
		return VerifiedArtifactClosureCheckpoint{}, err
	}
	return VerifiedArtifactClosureCheckpoint{Closure: VerifiedArtifactClosure{
		Roots: durableRoots, Blocks: durableBlocks,
		RootBlocks: append([]VerifiedArtifactRootBlock(nil), closure.RootBlocks...),
	}, Replayed: blocksReplayed && rootsReplayed && mapsReplayed}, nil
}

func checkpointArtifactClosureBlocks(ctx context.Context, tx *sql.Tx,
	blocks []VerifiedArtifactBlock,
) ([]VerifiedArtifactBlock, bool, error) {
	replayed := true
	durable := make([]VerifiedArtifactBlock, len(blocks))
	for index, block := range blocks {
		stored, found, err := checkpointArtifactBlock(ctx, tx, block)
		if err != nil {
			return nil, false, err
		}
		durable[index] = stored
		replayed = replayed && found
	}
	return durable, replayed, nil
}

func checkpointArtifactClosureRoots(ctx context.Context, tx *sql.Tx,
	roots []VerifiedArtifactRoot,
) ([]VerifiedArtifactRoot, []string, bool, error) {
	replayed := true
	durable := make([]VerifiedArtifactRoot, len(roots))
	states := make([]string, len(roots))
	for index, root := range roots {
		stored, state, found, err := stageArtifactClosureRoot(ctx, tx, root)
		if err != nil {
			return nil, nil, false, err
		}
		durable[index] = stored
		states[index] = state
		replayed = replayed && found && state == "verified"
	}
	return durable, states, replayed, nil
}

func raiseArtifactClosureVerificationTimes(roots []VerifiedArtifactRoot, states []string,
	blocks []VerifiedArtifactBlock, mappings []VerifiedArtifactRootBlock,
) error {
	blockTimes := make(map[model.Digest]time.Time, len(blocks))
	for _, block := range blocks {
		blockTimes[block.Digest] = block.CreatedAt
	}
	rootIndexes := make(map[model.Digest]int, len(roots))
	for index, root := range roots {
		rootIndexes[root.RootDigest] = index
	}
	for _, row := range mappings {
		rootIndex := rootIndexes[row.RootDigest]
		blockCreatedAt := blockTimes[row.BlockDigest]
		if !roots[rootIndex].VerifiedAt.Before(blockCreatedAt) {
			continue
		}
		if states[rootIndex] == "verified" {
			return fmt.Errorf("%w: verified root precedes a durable block", ErrArtifactConflict)
		}
		roots[rootIndex].VerifiedAt = blockCreatedAt
	}
	return nil
}

func checkpointArtifactClosureMaps(ctx context.Context, tx *sql.Tx,
	roots []VerifiedArtifactRoot, mappings []VerifiedArtifactRootBlock,
) (bool, error) {
	replayed := true
	for _, root := range roots {
		expected := rootBlocksForDigest(mappings, root.RootDigest)
		state, err := checkpointArtifactRootBlockMap(ctx, tx, root.RootDigest, expected)
		if err != nil {
			return false, err
		}
		replayed = replayed && !state.changed
		if state.rootState != "staged" {
			continue
		}
		result, err := tx.ExecContext(ctx, `UPDATE artifact_roots SET state='verified',verified_at=?
			WHERE root_digest=? AND state='staged' AND verified_at IS NULL`,
			storeTime(root.VerifiedAt), root.RootDigest.String())
		if err != nil || exactlyOne(result) != nil {
			return false, fmt.Errorf("%w: promote root %s",
				ErrArtifactConflict, root.RootDigest.String())
		}
		replayed = false
	}
	return replayed, nil
}

func readVerifiedArtifactClosureRoots(ctx context.Context, tx *sql.Tx,
	roots []VerifiedArtifactRoot, mappings []VerifiedArtifactRootBlock,
) ([]VerifiedArtifactRoot, error) {
	durable := make([]VerifiedArtifactRoot, len(roots))
	for index, root := range roots {
		verified, err := requireVerifiedArtifactRoot(ctx, tx, root.RootDigest)
		if err != nil {
			return nil, err
		}
		storedMap, err := readArtifactRootBlockMap(ctx, tx, root.RootDigest)
		if err != nil || !equalArtifactRootBlocks(storedMap,
			rootBlocksForDigest(mappings, root.RootDigest)) {
			return nil, fmt.Errorf("%w: verified root map differs", ErrArtifactConflict)
		}
		durable[index] = verified
	}
	return durable, nil
}
