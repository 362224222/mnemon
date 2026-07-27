package agent

import (
	"context"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (coordinator *ArtifactCaptureCoordinator) checkpointNonempty(ctx context.Context,
	reservation store.ManagedOperationReservation, paths []string, replay bool,
) (ArtifactCaptureResult, *ControlError) {
	operation := reservation.Operation
	now, apiErr := coordinator.fencedNow(reservation)
	if apiErr != nil {
		if replay {
			return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
		}
		return ArtifactCaptureResult{}, apiErr
	}
	leaseUntil, _ := operation.LeaseUntil()
	begun, err := coordinator.store.BeginOperationArtifactStage(ctx,
		store.BeginOperationArtifactStageSpec{OperationID: operation.ID(),
			LeaseOwner: operation.LeaseOwner(), LeaseUntil: leaseUntil, At: now})
	if err != nil {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	switch begun.State() {
	case store.ArtifactStagePublishing:
		return coordinator.resumeOperationArtifactPublish(ctx, reservation, begun)
	case store.ArtifactStageStaged:
	default:
		return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
			"durable Artifact stage has an invalid state", operation.ID())
	}
	if replay {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}

	stage, err := coordinator.stages.OpenStage(begun.Fence().Owner())
	if err != nil {
		return ArtifactCaptureResult{}, mapArtifactVerificationError(err, operation.ID())
	}
	closure, err := coordinator.capturer.Capture(ctx, append([]string(nil), paths...), stage)
	if err != nil {
		return ArtifactCaptureResult{}, mapLiveArtifactError(err, operation.ID())
	}
	if err := stage.VerifyClosure(ctx, closure); err != nil {
		return ArtifactCaptureResult{}, mapArtifactVerificationError(err, operation.ID())
	}
	now, apiErr = coordinator.fencedNow(reservation)
	if apiErr != nil {
		return ArtifactCaptureResult{}, apiErr
	}
	prepared, err := coordinator.store.PrepareOperationArtifactPublish(ctx,
		store.PrepareOperationArtifactPublishSpec{Fence: begun.Fence(),
			Capture: closure.Checkpoint(), Closure: storeClosureFromArtifact(closure), At: now})
	if err != nil {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	if prepared.State() != store.ArtifactStagePublishing {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	return coordinator.captureResult(closure.Checkpoint(), prepared.Replayed(),
		operation.ID())
}

func (coordinator *ArtifactCaptureCoordinator) resumeOperationArtifactPublish(ctx context.Context,
	reservation store.ManagedOperationReservation, begun store.OperationArtifactStageResult,
) (ArtifactCaptureResult, *ControlError) {
	operation := reservation.Operation
	now, apiErr := coordinator.fencedNow(reservation)
	if apiErr != nil {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	durable, err := coordinator.store.ReadOperationArtifactPublish(ctx,
		store.ReadOperationArtifactPublishSpec{Fence: begun.Fence(), At: now})
	if err != nil {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	if checkpoint, exists := operation.Capture(); exists &&
		checkpoint.String() != durable.Capture().String() {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	if _, err := artifactClosureFromStore(ctx, durable.Closure(),
		durable.Capture(), now); err != nil {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	if durable.State() != store.ArtifactStagePublishing {
		return ArtifactCaptureResult{}, mapArtifactPublicationError(operation.ID())
	}
	return coordinator.captureResult(durable.Capture(), true, operation.ID())
}

// PublishAccepted completes only a committed operation's durable publication
// obligation. It never reads the live workspace or requires the expired
// operation lease.
func (coordinator *ArtifactCaptureCoordinator) PublishAccepted(ctx context.Context,
	operation model.OperationID,
) *ControlError {
	if coordinator == nil || coordinator.stages == nil || coordinator.store == nil ||
		coordinator.clock == nil || ctx == nil || operation.IsZero() {
		return captureAPIError(CodeInternal,
			"Artifact publication coordinator is unavailable", operation)
	}
	now, apiErr := coordinator.freshNow(operation)
	if apiErr != nil {
		return apiErr
	}
	durable, found, err := coordinator.store.ReadCommittedOperationArtifactPublish(ctx,
		store.ReadCommittedOperationArtifactPublishSpec{
			OperationID: operation, At: now,
		})
	if err != nil {
		return mapAcceptedArtifactPublicationError(err, operation)
	}
	if !found {
		return acceptedArtifactPublicationInvariant(operation)
	}
	closure, err := artifactClosureFromStore(ctx, durable.Closure(),
		durable.Capture(), now)
	if err != nil {
		return mapAcceptedArtifactPublicationError(err, operation)
	}
	stage, err := coordinator.stages.OpenStage(durable.Fence().Owner())
	if err != nil {
		return mapAcceptedArtifactPublicationError(err, operation)
	}
	if err := stage.Publish(ctx, closure); err != nil {
		return mapAcceptedArtifactPublicationError(err, operation)
	}
	now, apiErr = coordinator.freshNow(operation)
	if apiErr != nil {
		return apiErr
	}
	ready, err := coordinator.store.MarkOperationArtifactReady(ctx,
		store.MarkOperationArtifactReadySpec{Fence: durable.Fence(), At: now})
	if err != nil {
		return mapAcceptedArtifactPublicationError(err, operation)
	}
	if ready.State() != store.ArtifactStageReady {
		return acceptedArtifactPublicationInvariant(operation)
	}
	return nil
}

func (coordinator *ArtifactCaptureCoordinator) captureResult(checkpoint model.JSON, replayed bool,
	operation model.OperationID,
) (ArtifactCaptureResult, *ControlError) {
	roots, err := parseArtifactCaptureCheckpoint(checkpoint)
	if err != nil {
		return ArtifactCaptureResult{}, captureAPIError(CodeInternal,
			"Artifact checkpoint projection is invalid", operation)
	}
	return ArtifactCaptureResult{Checkpoint: checkpoint, Roots: roots, Replayed: replayed}, nil
}

func storeClosureFromArtifact(closure artifact.Closure) store.VerifiedArtifactClosure {
	roots := closure.Roots()
	blocks := closure.Blocks()
	blockMap := closure.BlockMap()
	result := store.VerifiedArtifactClosure{
		Roots:      make([]store.VerifiedArtifactRoot, len(roots)),
		Blocks:     make([]store.VerifiedArtifactBlock, len(blocks)),
		RootBlocks: make([]store.VerifiedArtifactRootBlock, len(blockMap)),
	}
	for index, root := range roots {
		result.Roots[index] = store.VerifiedArtifactRoot{RootDigest: root.RootDigest,
			Manifest: root.Manifest, ManifestDigest: root.ManifestDigest, TotalBytes: root.TotalBytes,
			CreatedAt: root.CreatedAt, VerifiedAt: root.VerifiedAt}
	}
	for index, block := range blocks {
		result.Blocks[index] = store.VerifiedArtifactBlock{Digest: block.Digest,
			SizeBytes: block.SizeBytes, CreatedAt: block.CreatedAt}
	}
	for index, row := range blockMap {
		result.RootBlocks[index] = store.VerifiedArtifactRootBlock{RootDigest: row.RootDigest,
			Ordinal: row.Ordinal, LogicalPath: row.LogicalPath, OffsetBytes: row.OffsetBytes,
			LengthBytes: row.LengthBytes, BlockDigest: row.BlockDigest, Mode: row.Mode}
	}
	return result
}

func artifactClosureFromStore(ctx context.Context, durable store.VerifiedArtifactClosure,
	checkpoint model.JSON, capturedAt time.Time,
) (artifact.Closure, error) {
	closure, err := store.RebuildArtifactClosure(ctx, durable, capturedAt)
	if err != nil {
		return artifact.Closure{}, err
	}
	if closure.Checkpoint().String() != checkpoint.String() {
		return artifact.Closure{}, artifact.ErrClosureMismatch
	}
	return closure, nil
}

func mapArtifactPublicationError(operation model.OperationID) *ControlError {
	return captureAPIError(CodeOperationPending,
		"Artifact publication remains pending", operation)
}

func mapAcceptedArtifactPublicationError(err error,
	operation model.OperationID,
) *ControlError {
	switch {
	case errors.Is(err, artifact.ErrCASCorruption),
		errors.Is(err, artifact.ErrCASInput),
		errors.Is(err, artifact.ErrInvalidManifest),
		errors.Is(err, artifact.ErrClosureMismatch),
		errors.Is(err, store.ErrArtifactStageConflict),
		errors.Is(err, store.ErrArtifactStageFence):
		return acceptedArtifactPublicationInvariant(operation)
	default:
		return mapArtifactPublicationError(operation)
	}
}

func acceptedArtifactPublicationInvariant(operation model.OperationID) *ControlError {
	return captureAPIError(CodeInternal,
		"Committed Artifact publication evidence is invalid", operation)
}
