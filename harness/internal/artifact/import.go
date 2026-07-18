package artifact

import (
	"context"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// BuildImportedClosure builds the canonical typed closure for manifests whose
// immutable bytes have already crossed a trust boundary. It intentionally does
// not read or write the CAS: callers must durably store every referenced object
// and use CAS.VerifyClosure before the closure becomes local authority.
func BuildImportedClosure(ctx context.Context, manifests []Manifest,
	verifiedAt time.Time,
) (Closure, error) {
	if ctx == nil {
		return Closure{}, fmt.Errorf("%w: nil import context", ErrClosureMismatch)
	}
	if err := ctx.Err(); err != nil {
		return Closure{}, err
	}
	if len(manifests) == 0 || len(manifests) > MaxRoots {
		return Closure{}, fmt.Errorf("%w: import requires 1..%d manifests",
			ErrArtifactLimit, MaxRoots)
	}
	canonicalAt, err := canonicalCaptureTime(verifiedAt)
	if err != nil || canonicalAt != verifiedAt {
		return Closure{}, fmt.Errorf("%w: verification time is not canonical UTC",
			ErrClosureMismatch)
	}

	state := captureState{capturedAt: verifiedAt,
		blocks: make(map[model.Digest]CapturedBlock)}
	rootDigests := make(map[model.Digest]struct{}, len(manifests))
	for index, candidate := range manifests {
		if err := ctx.Err(); err != nil {
			return Closure{}, err
		}
		manifest, err := reparseImportedManifest(candidate)
		if err != nil {
			return Closure{}, fmt.Errorf("%w: manifest %d is not an immutable parsed manifest: %v",
				ErrClosureMismatch, index, err)
		}
		if _, duplicate := rootDigests[manifest.RootDigest()]; duplicate {
			return Closure{}, fmt.Errorf("%w: duplicate imported root %s",
				ErrClosureMismatch, manifest.RootDigest())
		}

		entries := manifest.Entries()
		if len(entries) > MaxEntries-state.entryCount {
			return Closure{}, fmt.Errorf("%w: imported closure exceeds %d entries",
				ErrArtifactLimit, MaxEntries)
		}
		if manifest.TotalBytes() > MaxTotalBytes-state.totalBytes {
			return Closure{}, fmt.Errorf("%w: imported closure exceeds %d bytes",
				ErrArtifactLimit, MaxTotalBytes)
		}
		state.entryCount += len(entries)
		state.totalBytes += manifest.TotalBytes()
		rootDigests[manifest.RootDigest()] = struct{}{}
		state.roots = append(state.roots, CapturedRoot{
			RootDigest:     manifest.RootDigest(),
			Manifest:       manifest.CanonicalJSON(),
			ManifestDigest: manifest.ManifestDigest(),
			TotalBytes:     manifest.TotalBytes(),
			CreatedAt:      verifiedAt,
			VerifiedAt:     verifiedAt,
		})

		ordinal := uint64(0)
		for _, entry := range entries {
			for _, block := range entry.Blocks {
				if err := ctx.Err(); err != nil {
					return Closure{}, err
				}
				if existing, present := state.blocks[block.Digest]; present {
					if existing.SizeBytes != block.LengthBytes {
						return Closure{}, fmt.Errorf("%w: block %s has conflicting imported lengths",
							ErrClosureMismatch, block.Digest)
					}
				} else {
					state.blocks[block.Digest] = CapturedBlock{Digest: block.Digest,
						SizeBytes: block.LengthBytes, CreatedAt: verifiedAt}
				}
				state.blockMap = append(state.blockMap, RootBlock{
					RootDigest:  manifest.RootDigest(),
					Ordinal:     ordinal,
					LogicalPath: entry.LogicalPath,
					OffsetBytes: block.OffsetBytes,
					LengthBytes: block.LengthBytes,
					BlockDigest: block.Digest,
					Mode:        entry.Mode,
				})
				ordinal++
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return Closure{}, err
	}
	closure, err := state.closure()
	if err != nil {
		return Closure{}, fmt.Errorf("%w: build imported checkpoint: %v",
			ErrClosureMismatch, err)
	}
	return closure, nil
}

func reparseImportedManifest(candidate Manifest) (Manifest, error) {
	if candidate.IsZero() {
		return Manifest{}, ErrInvalidManifest
	}
	parsed, err := ParseManifest(candidate.CanonicalJSON().Bytes())
	if err != nil {
		return Manifest{}, err
	}
	if !sameImportedManifest(candidate, parsed) {
		return Manifest{}, fmt.Errorf("%w: typed projection differs from canonical bytes",
			ErrInvalidManifest)
	}
	return parsed, nil
}

func sameImportedManifest(left, right Manifest) bool {
	if left.RootKind() != right.RootKind() || left.RootPath() != right.RootPath() ||
		left.TotalBytes() != right.TotalBytes() || left.ManifestDigest() != right.ManifestDigest() ||
		left.RootDigest() != right.RootDigest() ||
		left.CanonicalJSON().String() != right.CanonicalJSON().String() {
		return false
	}
	leftEntries, rightEntries := left.Entries(), right.Entries()
	if len(leftEntries) != len(rightEntries) {
		return false
	}
	for index := range leftEntries {
		leftEntry, rightEntry := leftEntries[index], rightEntries[index]
		if leftEntry.Kind != rightEntry.Kind || leftEntry.LogicalPath != rightEntry.LogicalPath ||
			leftEntry.Mode != rightEntry.Mode || leftEntry.SizeBytes != rightEntry.SizeBytes ||
			len(leftEntry.Blocks) != len(rightEntry.Blocks) {
			return false
		}
		for blockIndex := range leftEntry.Blocks {
			if leftEntry.Blocks[blockIndex] != rightEntry.Blocks[blockIndex] {
				return false
			}
		}
	}
	return true
}
