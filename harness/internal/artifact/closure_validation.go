package artifact

import (
	"context"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateClosureMetadata(ctx context.Context, closure Closure) error {
	if ctx == nil || closure.IsZero() || len(closure.roots) == 0 ||
		len(closure.roots) > MaxRoots {
		return fmt.Errorf("%w: empty or oversized closure", ErrClosureMismatch)
	}
	capturedAt, err := canonicalCaptureTime(closure.capturedAt)
	if err != nil || !capturedAt.Equal(closure.capturedAt) {
		return fmt.Errorf("%w: invalid capture time", ErrClosureMismatch)
	}
	metadata, err := buildClosureMetadata(ctx, closure, capturedAt)
	if err != nil {
		return err
	}
	if err := validateClosureBlockMap(closure, metadata); err != nil {
		return err
	}
	if err := validateClosureBlocks(ctx, closure, capturedAt, metadata); err != nil {
		return err
	}
	return validateClosureCheckpoint(closure)
}

type closureMetadata struct {
	expectedBlocks map[model.Digest]uint64
	expectedMap    []RootBlock
	entryCount     int
	totalBytes     uint64
}

func buildClosureMetadata(ctx context.Context, closure Closure,
	capturedAt time.Time,
) (closureMetadata, error) {
	metadata := closureMetadata{expectedBlocks: make(map[model.Digest]uint64)}
	for index, root := range closure.roots {
		if err := ctx.Err(); err != nil {
			return closureMetadata{}, err
		}
		manifest, err := validateCapturedRoot(closure.roots, index, capturedAt)
		if err != nil {
			return closureMetadata{}, err
		}
		if err := metadata.addRoot(root, manifest); err != nil {
			return closureMetadata{}, err
		}
	}
	return metadata, nil
}

func validateCapturedRoot(roots []CapturedRoot, index int,
	capturedAt time.Time,
) (Manifest, error) {
	root := roots[index]
	if root.CreatedAt != capturedAt || root.VerifiedAt != capturedAt ||
		(index > 0 && roots[index-1].RootDigest.String() >= root.RootDigest.String()) {
		return Manifest{}, fmt.Errorf("%w: root metadata is not canonical", ErrClosureMismatch)
	}
	manifest, err := ParseManifest(root.Manifest.Bytes())
	if err != nil || manifest.RootDigest() != root.RootDigest ||
		manifest.ManifestDigest() != root.ManifestDigest || manifest.TotalBytes() != root.TotalBytes {
		return Manifest{}, fmt.Errorf("%w: root metadata differs from manifest", ErrClosureMismatch)
	}
	return manifest, nil
}

func (metadata *closureMetadata) addRoot(root CapturedRoot, manifest Manifest) error {
	entries := manifest.Entries()
	if metadata.entryCount > MaxEntries-len(entries) ||
		metadata.totalBytes > MaxTotalBytes-root.TotalBytes {
		return fmt.Errorf("%w: aggregate closure budget exceeded", ErrClosureMismatch)
	}
	metadata.entryCount += len(entries)
	metadata.totalBytes += root.TotalBytes
	return metadata.addManifestBlocks(root.RootDigest, entries)
}

func (metadata *closureMetadata) addManifestBlocks(root model.Digest,
	entries []ManifestEntry,
) error {
	ordinal := uint64(0)
	for _, entry := range entries {
		for _, block := range entry.Blocks {
			if size, present := metadata.expectedBlocks[block.Digest]; present &&
				size != block.LengthBytes {
				return fmt.Errorf("%w: one block digest has conflicting lengths",
					ErrClosureMismatch)
			}
			metadata.expectedBlocks[block.Digest] = block.LengthBytes
			metadata.expectedMap = append(metadata.expectedMap, RootBlock{
				RootDigest: root, Ordinal: ordinal, LogicalPath: entry.LogicalPath,
				OffsetBytes: block.OffsetBytes, LengthBytes: block.LengthBytes,
				BlockDigest: block.Digest, Mode: entry.Mode,
			})
			ordinal++
		}
	}
	return nil
}

func validateClosureBlockMap(closure Closure, metadata closureMetadata) error {
	if len(metadata.expectedBlocks) != len(closure.blocks) ||
		len(metadata.expectedMap) != len(closure.blockMap) {
		return fmt.Errorf("%w: typed block closure has missing or extra rows", ErrClosureMismatch)
	}
	for index, expected := range metadata.expectedMap {
		if closure.blockMap[index] != expected {
			return fmt.Errorf("%w: root block map differs", ErrClosureMismatch)
		}
	}
	return nil
}

func validateClosureBlocks(ctx context.Context, closure Closure, capturedAt time.Time,
	metadata closureMetadata,
) error {
	for index, block := range closure.blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if block.CreatedAt != capturedAt ||
			(index > 0 && closure.blocks[index-1].Digest.String() >= block.Digest.String()) {
			return fmt.Errorf("%w: block metadata is not canonical", ErrClosureMismatch)
		}
		length, present := metadata.expectedBlocks[block.Digest]
		if !present || length != block.SizeBytes {
			return fmt.Errorf("%w: block metadata differs", ErrClosureMismatch)
		}
	}
	return nil
}

func validateClosureCheckpoint(closure Closure) error {
	rebuilt := captureState{roots: append([]CapturedRoot{}, closure.roots...),
		blocks: make(map[model.Digest]CapturedBlock), blockMap: append([]RootBlock{}, closure.blockMap...),
		capturedAt: closure.capturedAt}
	for _, block := range closure.blocks {
		rebuilt.blocks[block.Digest] = block
	}
	rebuiltClosure, err := rebuilt.closure()
	if err != nil || rebuiltClosure.checkpoint.String() != closure.checkpoint.String() {
		return fmt.Errorf("%w: checkpoint differs", ErrClosureMismatch)
	}
	return nil
}
