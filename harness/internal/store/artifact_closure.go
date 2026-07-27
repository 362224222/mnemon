package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	maxVerifiedClosureRoots      = 16
	maxVerifiedClosureEntries    = 4096
	maxVerifiedClosureBytes      = 256 << 20
	maxVerifiedClosureBlockBytes = 1 << 20
	maxVerifiedClosureBlocks     = maxVerifiedClosureEntries + maxVerifiedClosureBytes/maxVerifiedClosureBlockBytes
	maxVerifiedLogicalPathBytes  = 512
	maxVerifiedManifestBytes     = 4 << 20
)

type VerifiedArtifactBlock struct {
	Digest    model.Digest
	SizeBytes uint64
	CreatedAt time.Time
}

type VerifiedArtifactRootBlock struct {
	RootDigest  model.Digest
	Ordinal     uint64
	LogicalPath string
	OffsetBytes uint64
	LengthBytes uint64
	BlockDigest model.Digest
	Mode        uint32
}

// VerifiedArtifactClosure is the typed SQLite projection of a closure whose
// immutable CAS bytes have already been fully rehashed by the Artifact layer.
// The Store independently validates its bounded relational shape before any
// row becomes verified authority.
type VerifiedArtifactClosure struct {
	Roots      []VerifiedArtifactRoot
	Blocks     []VerifiedArtifactBlock
	RootBlocks []VerifiedArtifactRootBlock
}

func validateVerifiedArtifactClosure(requested VerifiedArtifactClosure) (VerifiedArtifactClosure, error) {
	closure := VerifiedArtifactClosure{Roots: append([]VerifiedArtifactRoot(nil), requested.Roots...),
		Blocks:     append([]VerifiedArtifactBlock(nil), requested.Blocks...),
		RootBlocks: append([]VerifiedArtifactRootBlock(nil), requested.RootBlocks...)}
	if len(closure.Roots) == 0 || len(closure.Roots) > maxVerifiedClosureRoots ||
		len(closure.Blocks) > maxVerifiedClosureBlocks || len(closure.RootBlocks) > maxVerifiedClosureBlocks {
		return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: collection bound exceeded")
	}
	rootSet := make(map[model.Digest]VerifiedArtifactRoot, len(closure.Roots))
	var aggregateBytes uint64
	for index, requestedRoot := range closure.Roots {
		root, err := validateVerifiedArtifactRoot(requestedRoot)
		if err != nil || len(root.Manifest.Bytes()) > maxVerifiedManifestBytes ||
			(index > 0 && closure.Roots[index-1].RootDigest.String() >= root.RootDigest.String()) {
			return VerifiedArtifactClosure{}, fmt.Errorf("checkpoint Artifact closure: invalid canonical root %d: %w",
				index, err)
		}
		if root.TotalBytes > maxVerifiedClosureBytes ||
			aggregateBytes > maxVerifiedClosureBytes-root.TotalBytes {
			return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: aggregate bytes exceed 256 MiB")
		}
		aggregateBytes += root.TotalBytes
		closure.Roots[index] = root
		rootSet[root.RootDigest] = root
	}

	blockSet := make(map[model.Digest]VerifiedArtifactBlock, len(closure.Blocks))
	for index, block := range closure.Blocks {
		createdAt, err := canonicalStoreTime(block.CreatedAt)
		if block.Digest.IsZero() || block.SizeBytes == 0 || block.SizeBytes > maxVerifiedClosureBlockBytes ||
			err != nil || (index > 0 && closure.Blocks[index-1].Digest.String() >= block.Digest.String()) {
			return VerifiedArtifactClosure{}, fmt.Errorf("checkpoint Artifact closure: invalid canonical block %d", index)
		}
		block.CreatedAt = createdAt
		closure.Blocks[index] = block
		blockSet[block.Digest] = block
	}

	totals := make(map[model.Digest]uint64, len(rootSet))
	blockUses := make(map[model.Digest]uint64, len(blockSet))
	lastOrdinal := make(map[model.Digest]uint64, len(rootSet))
	seenRoot := make(map[model.Digest]bool, len(rootSet))
	lastPath := make(map[model.Digest]string, len(rootSet))
	lastOffset := make(map[model.Digest]uint64, len(rootSet))
	lastMode := make(map[model.Digest]uint32, len(rootSet))
	for index, row := range closure.RootBlocks {
		root, rootExists := rootSet[row.RootDigest]
		block, blockExists := blockSet[row.BlockDigest]
		if !rootExists || !blockExists || row.LengthBytes == 0 || row.LengthBytes != block.SizeBytes ||
			row.LengthBytes > maxVerifiedClosureBlockBytes || row.Mode > 0o777 ||
			!validVerifiedLogicalPath(row.LogicalPath) || row.Ordinal > model.MaxSQLiteInteger ||
			row.OffsetBytes > model.MaxSQLiteInteger || row.LengthBytes > model.MaxSQLiteInteger {
			return VerifiedArtifactClosure{}, fmt.Errorf("checkpoint Artifact closure: invalid root block %d", index)
		}
		if index > 0 {
			previous := closure.RootBlocks[index-1]
			if previous.RootDigest.String() > row.RootDigest.String() ||
				(previous.RootDigest == row.RootDigest && previous.Ordinal >= row.Ordinal) {
				return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: root blocks are not canonical")
			}
		}
		if !seenRoot[row.RootDigest] {
			if row.Ordinal != 0 {
				return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: root block ordinal does not start at zero")
			}
			seenRoot[row.RootDigest] = true
		} else if row.Ordinal != lastOrdinal[row.RootDigest]+1 {
			return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: root block ordinal has a gap")
		}
		if lastPath[row.RootDigest] != row.LogicalPath {
			if prior := lastPath[row.RootDigest]; prior != "" && prior >= row.LogicalPath {
				return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: logical paths are not ordered")
			}
			if row.OffsetBytes != 0 {
				return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: file block offset does not start at zero")
			}
			lastPath[row.RootDigest] = row.LogicalPath
			lastOffset[row.RootDigest] = 0
			lastMode[row.RootDigest] = row.Mode
		} else if row.OffsetBytes != lastOffset[row.RootDigest] || row.Mode != lastMode[row.RootDigest] {
			return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: file block offset or mode changed")
		}
		if totals[row.RootDigest] > root.TotalBytes ||
			row.LengthBytes > root.TotalBytes-totals[row.RootDigest] {
			return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: root block bytes overflow root total")
		}
		totals[row.RootDigest] += row.LengthBytes
		blockUses[row.BlockDigest]++
		lastOffset[row.RootDigest] = row.OffsetBytes + row.LengthBytes
		lastOrdinal[row.RootDigest] = row.Ordinal
	}
	for digest, root := range rootSet {
		if totals[digest] != root.TotalBytes {
			return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: root block bytes differ from root total")
		}
	}
	for digest := range blockSet {
		if blockUses[digest] == 0 {
			return VerifiedArtifactClosure{}, errors.New("checkpoint Artifact closure: unreferenced block row")
		}
	}
	return closure, nil
}

func checkpointArtifactBlock(ctx context.Context, tx *sql.Tx,
	requested VerifiedArtifactBlock,
) (VerifiedArtifactBlock, bool, error) {
	var size uint64
	var createdText string
	err := tx.QueryRowContext(ctx, `SELECT size_bytes,created_at FROM artifact_blocks
		WHERE block_digest=?`, requested.Digest.String()).Scan(&size, &createdText)
	if err == nil {
		createdAt, parseErr := parseCanonicalStoreTime(createdText)
		if parseErr != nil || size != requested.SizeBytes {
			return VerifiedArtifactBlock{}, true, ErrArtifactConflict
		}
		return VerifiedArtifactBlock{Digest: requested.Digest, SizeBytes: size, CreatedAt: createdAt}, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return VerifiedArtifactBlock{}, false, fmt.Errorf("checkpoint Artifact closure: inspect block: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_blocks(block_digest,size_bytes,created_at)
		VALUES(?,?,?)`, requested.Digest.String(), requested.SizeBytes, storeTime(requested.CreatedAt)); err != nil {
		return VerifiedArtifactBlock{}, false, fmt.Errorf("checkpoint Artifact closure: insert block: %w", err)
	}
	return requested, false, nil
}

func stageArtifactClosureRoot(ctx context.Context, tx *sql.Tx,
	requested VerifiedArtifactRoot,
) (VerifiedArtifactRoot, string, bool, error) {
	existing, state, err := readArtifactRoot(ctx, tx, requested.RootDigest)
	if err == nil {
		if !sameArtifactContent(existing, requested) || (state != "staged" && state != "verified") {
			return VerifiedArtifactRoot{}, "", true, ErrArtifactConflict
		}
		if state == "staged" && requested.VerifiedAt.Before(existing.CreatedAt) {
			// A same-content closure may have been staged first by a later
			// concurrent observer. Its durable creation time is the earliest
			// valid verification instant for this shared staged row.
			requested.VerifiedAt = existing.CreatedAt
		}
		if state == "verified" {
			requested.VerifiedAt = existing.VerifiedAt
		}
		requested.CreatedAt = existing.CreatedAt
		return requested, state, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return VerifiedArtifactRoot{}, "", false, fmt.Errorf("checkpoint Artifact closure: inspect root: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_roots(root_digest,manifest_json,
		manifest_digest,total_bytes,state,created_at) VALUES(?,?,?,?,'staged',?)`,
		requested.RootDigest.String(), requested.Manifest.Bytes(), requested.ManifestDigest.Bytes(),
		requested.TotalBytes, storeTime(requested.CreatedAt)); err != nil {
		return VerifiedArtifactRoot{}, "", false, fmt.Errorf("checkpoint Artifact closure: insert staged root: %w", err)
	}
	return requested, "staged", false, nil
}

type artifactRootMapState struct {
	rootState string
	changed   bool
}

func checkpointArtifactRootBlockMap(ctx context.Context, tx *sql.Tx, root model.Digest,
	expected []VerifiedArtifactRootBlock,
) (artifactRootMapState, error) {
	var state string
	if err := tx.QueryRowContext(ctx, "SELECT state FROM artifact_roots WHERE root_digest=?",
		root.String()).Scan(&state); err != nil {
		return artifactRootMapState{}, fmt.Errorf("checkpoint Artifact closure: read root state: %w", err)
	}
	existing, err := readArtifactRootBlockMap(ctx, tx, root)
	if err != nil {
		return artifactRootMapState{}, err
	}
	if len(existing) != 0 {
		if !equalArtifactRootBlocks(existing, expected) {
			return artifactRootMapState{}, ErrArtifactConflict
		}
		return artifactRootMapState{rootState: state}, nil
	}
	if len(expected) == 0 {
		return artifactRootMapState{rootState: state}, nil
	}
	if state != "staged" {
		return artifactRootMapState{}, ErrArtifactConflict
	}
	for _, row := range expected {
		if _, err := tx.ExecContext(ctx, `INSERT INTO artifact_root_blocks(root_digest,ordinal,
			logical_path,offset_bytes,length_bytes,block_digest,mode) VALUES(?,?,?,?,?,?,?)`,
			row.RootDigest.String(), row.Ordinal, row.LogicalPath, row.OffsetBytes,
			row.LengthBytes, row.BlockDigest.String(), row.Mode); err != nil {
			return artifactRootMapState{}, fmt.Errorf("checkpoint Artifact closure: insert root block: %w", err)
		}
	}
	return artifactRootMapState{rootState: state, changed: true}, nil
}

func readArtifactRootBlockMap(ctx context.Context, tx *sql.Tx,
	root model.Digest,
) ([]VerifiedArtifactRootBlock, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,logical_path,offset_bytes,length_bytes,
		block_digest,mode FROM artifact_root_blocks WHERE root_digest=? ORDER BY ordinal`, root.String())
	if err != nil {
		return nil, fmt.Errorf("checkpoint Artifact closure: read root map: %w", err)
	}
	defer rows.Close()
	result := make([]VerifiedArtifactRootBlock, 0)
	for rows.Next() {
		var row VerifiedArtifactRootBlock
		var digestText string
		if err := rows.Scan(&row.Ordinal, &row.LogicalPath, &row.OffsetBytes,
			&row.LengthBytes, &digestText, &row.Mode); err != nil {
			return nil, fmt.Errorf("checkpoint Artifact closure: scan root map: %w", err)
		}
		row.RootDigest = root
		row.BlockDigest, err = model.ParseDigest(digestText)
		if err != nil {
			return nil, ErrArtifactConflict
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("checkpoint Artifact closure: iterate root map: %w", err)
	}
	return result, nil
}

func rootBlocksForDigest(rows []VerifiedArtifactRootBlock,
	root model.Digest,
) []VerifiedArtifactRootBlock {
	start := sort.Search(len(rows), func(index int) bool {
		return rows[index].RootDigest.String() >= root.String()
	})
	end := start
	for end < len(rows) && rows[end].RootDigest == root {
		end++
	}
	return rows[start:end]
}

func equalArtifactRootBlocks(left, right []VerifiedArtifactRootBlock) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func validVerifiedLogicalPath(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > maxVerifiedLogicalPathBytes ||
		strings.HasPrefix(value, "/") || strings.ContainsRune(value, 0) {
		return false
	}
	components := strings.Split(value, "/")
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return value != ".mnemon/harness" && !strings.HasPrefix(value, ".mnemon/harness/")
}
