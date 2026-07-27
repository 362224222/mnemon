package peer

import (
	"bytes"
	"context"
	"errors"
	"os"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (receiver *ArtifactReceiver) receiveStagedClaim(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage,
) (*artifactReceiverClaimFailure, error) {
	refs, failure, err := receiver.collectManifestRefs(ctx, claim, stage)
	if err != nil || failure != nil {
		return failure, err
	}
	closure, failure, err := receiver.buildCollectedClosure(ctx, claim, stage, refs)
	if err != nil || failure != nil || closure.IsZero() {
		return failure, err
	}
	failure, err = receiver.materializeBlocks(ctx, claim, stage, closure, refs)
	if err != nil || failure != nil {
		return failure, err
	}
	return receiver.prepareAndPublishArtifact(ctx, claim, stage, closure)
}

func (receiver *ArtifactReceiver) collectManifestRefs(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage,
) ([]artifactReceiverManifestRef, *artifactReceiverClaimFailure, error) {
	refs := make([]artifactReceiverManifestRef, 0, len(claim.requiredRoots))
	for _, rootDigest := range claim.requiredRoots {
		if ctx.Err() != nil {
			return nil, nil, nil
		}
		ref, failure, err := receiver.collectManifestRef(ctx, claim, stage, rootDigest)
		if err != nil || failure != nil {
			return nil, failure, err
		}
		refs = append(refs, ref)
	}
	return refs, nil, nil
}

func (receiver *ArtifactReceiver) collectManifestRef(ctx context.Context,
	claim *artifactReceiverClaim, stage *artifactdomain.Stage, rootDigest model.Digest,
) (artifactReceiverManifestRef, *artifactReceiverClaimFailure, error) {
	live, err := receiver.ensureLease(ctx, claim)
	if err != nil || !live {
		return artifactReceiverManifestRef{}, nil, err
	}
	at, err := receiver.now()
	if err != nil {
		return artifactReceiverManifestRef{}, nil, artifactReceiverFatal(
			ArtifactReceiverFatalWorkerInvariant, "read cached-root clock", err)
	}
	cached, found, err := receiver.backend.readRoot(ctx, claim.fence, rootDigest, at)
	if err != nil {
		failure, classified := receiver.classifyStoreClaimFailure(
			"read cached Artifact root", err)
		return artifactReceiverManifestRef{}, failure, classified
	}
	if found {
		ref, usable, failure, err := receiver.validateCachedManifest(
			stage, cached, rootDigest, at, claim.fence.attempt)
		if err != nil || failure != nil || usable {
			if usable {
				receiver.recordManifestCacheHit()
			}
			return ref, failure, err
		}
	}
	return receiver.fetchManifest(ctx, claim, stage, rootDigest)
}

func (receiver *ArtifactReceiver) validateCachedManifest(stage *artifactdomain.Stage,
	root artifactReceiverCachedRoot, expected model.Digest, at time.Time, attempt uint32,
) (artifactReceiverManifestRef, bool, *artifactReceiverClaimFailure, error) {
	if !validArtifactReceiverCachedRoot(root, expected, at) {
		return artifactReceiverManifestRef{}, false, nil, artifactReceiverFatal(
			ArtifactReceiverFatalStoreInvariant, "invalid cached Artifact root", nil)
	}
	manifest, err := artifactdomain.ParseManifest(root.manifest.Bytes())
	if err != nil || manifest.RootDigest() != expected ||
		manifest.ManifestDigest() != root.manifestDigest || manifest.TotalBytes() != root.totalBytes {
		return artifactReceiverManifestRef{}, false, nil, artifactReceiverFatal(
			ArtifactReceiverFatalStoreInvariant, "cached Artifact manifest differs", err)
	}
	return receiver.readCachedManifest(stage, root, expected, attempt)
}

func validArtifactReceiverCachedRoot(root artifactReceiverCachedRoot,
	expected model.Digest, at time.Time,
) bool {
	if root.rootDigest != expected || root.manifest.IsZero() || root.manifestDigest.IsZero() ||
		root.createdAt.IsZero() || root.createdAt.After(at) ||
		model.Sum(root.manifest.Bytes()) != root.manifestDigest {
		return false
	}
	if root.verified {
		return !root.verifiedAt.IsZero() && !root.verifiedAt.Before(root.createdAt) &&
			!root.verifiedAt.After(at)
	}
	return root.verifiedAt.IsZero()
}

func (receiver *ArtifactReceiver) readCachedManifest(stage *artifactdomain.Stage,
	root artifactReceiverCachedRoot, expected model.Digest, attempt uint32,
) (artifactReceiverManifestRef, bool, *artifactReceiverClaimFailure, error) {
	stored, err := stage.ReadAvailable(root.manifestDigest, artifactdomain.MaxManifestBytes)
	if err != nil {
		if artifactReceiverResourceFailure(err) {
			return artifactReceiverManifestRef{}, false, retryArtifactReceiverClaim(
				store.PeerInboxArtifactRetryResourceExhausted, artifactReceiverBackoff(attempt)), nil
		}
		if errors.Is(err, os.ErrNotExist) && !root.verified {
			return artifactReceiverManifestRef{}, false, nil, nil
		}
		return artifactReceiverManifestRef{}, false, nil, receiver.casFatal(
			"verified Store root has no readable CAS manifest", err)
	}
	if !bytes.Equal(stored, root.manifest.Bytes()) {
		return artifactReceiverManifestRef{}, false, nil, receiver.casFatal(
			"verified Store root CAS manifest differs", artifactdomain.ErrCASCorruption)
	}
	return artifactReceiverManifestRef{rootDigest: expected,
		manifestDigest: root.manifestDigest, verified: root.verified}, true, nil, nil
}
