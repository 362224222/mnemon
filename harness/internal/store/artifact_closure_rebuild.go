package store

import (
	"context"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
)

// RebuildArtifactClosure validates and reconstructs the exact typed closure
// projected by the Store. Filesystem byte presence remains the caller's CAS
// responsibility.
func RebuildArtifactClosure(ctx context.Context, durable VerifiedArtifactClosure,
	at time.Time,
) (artifactdomain.Closure, error) {
	if ctx == nil {
		return artifactdomain.Closure{}, ErrArtifactStageConflict
	}
	manifests := make([]artifactdomain.Manifest, 0, len(durable.Roots))
	for _, root := range durable.Roots {
		manifest, err := artifactdomain.ParseManifest(root.Manifest.Bytes())
		if err != nil || manifest.RootDigest() != root.RootDigest ||
			manifest.ManifestDigest() != root.ManifestDigest ||
			manifest.TotalBytes() != root.TotalBytes {
			return artifactdomain.Closure{}, fmt.Errorf(
				"%w: projected root differs from manifest", ErrArtifactStageConflict)
		}
		manifests = append(manifests, manifest)
	}
	closure, err := artifactdomain.BuildImportedClosure(ctx, manifests, at)
	if err != nil {
		return artifactdomain.Closure{}, err
	}
	if !sameRebuiltArtifactClosure(closure, durable) {
		return artifactdomain.Closure{}, fmt.Errorf(
			"%w: rebuilt Artifact closure differs", ErrArtifactStageConflict)
	}
	return closure, nil
}

func sameRebuiltArtifactClosure(closure artifactdomain.Closure,
	durable VerifiedArtifactClosure,
) bool {
	roots, blocks, mappings := closure.Roots(), closure.Blocks(), closure.BlockMap()
	if len(roots) != len(durable.Roots) || len(blocks) != len(durable.Blocks) ||
		len(mappings) != len(durable.RootBlocks) {
		return false
	}
	for index, root := range roots {
		stored := durable.Roots[index]
		if root.RootDigest != stored.RootDigest ||
			root.Manifest.String() != stored.Manifest.String() ||
			root.ManifestDigest != stored.ManifestDigest ||
			root.TotalBytes != stored.TotalBytes {
			return false
		}
	}
	for index, block := range blocks {
		stored := durable.Blocks[index]
		if block.Digest != stored.Digest || block.SizeBytes != stored.SizeBytes {
			return false
		}
	}
	for index, mapping := range mappings {
		stored := durable.RootBlocks[index]
		if mapping.RootDigest != stored.RootDigest ||
			mapping.Ordinal != stored.Ordinal ||
			mapping.LogicalPath != stored.LogicalPath ||
			mapping.OffsetBytes != stored.OffsetBytes ||
			mapping.LengthBytes != stored.LengthBytes ||
			mapping.BlockDigest != stored.BlockDigest ||
			mapping.Mode != stored.Mode {
			return false
		}
	}
	return true
}
