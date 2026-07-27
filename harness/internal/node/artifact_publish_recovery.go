package node

import (
	"context"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// recoverAcceptedArtifactPublishes completes only obligations whose semantic
// owner is already durable. It finishes the bounded page before reporting any
// candidate failure to the worker supervisor.
func (worker *artifactStageCleanupWorker) recoverAcceptedArtifactPublishes(
	ctx context.Context, at time.Time,
) error {
	page, err := worker.store.ScanAcceptedArtifactPublishes(ctx,
		store.ScanAcceptedArtifactPublishesSpec{
			At: at, MaxExamined: worker.maxExamined, After: worker.recoveryAt,
		})
	if err != nil {
		return artifactStageCleanupFailure("scan accepted Artifact publishes", err)
	}
	if page.Examined() < 0 || page.Examined() > worker.maxExamined ||
		len(page.Candidates()) != page.Examined() {
		return artifactStageCleanupFailure("validate accepted Artifact publish page", nil)
	}
	if page.Examined() == 0 {
		worker.recoveryAt = store.AcceptedArtifactPublishCursor{}
		return nil
	}
	worker.recoveryAt = page.Cursor()
	var recoveryErr error
	for _, candidate := range page.Candidates() {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch candidate.Kind() {
		case artifact.StageOwnerOperation:
			err = worker.recoverAcceptedOperationPublish(ctx, candidate, at)
		case artifact.StageOwnerInbox:
			err = worker.recoverAcceptedPeerInboxPublish(ctx, candidate, at)
		default:
			err = artifactStageCleanupFailure(
				"validate accepted Artifact publish candidate", nil)
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			recoveryErr = errors.Join(recoveryErr, err)
		}
	}
	if recoveryErr != nil {
		return artifactStageCleanupFailure(
			"recover accepted Artifact publish candidate", recoveryErr)
	}
	return nil
}

func (worker *artifactStageCleanupWorker) recoverAcceptedOperationPublish(
	ctx context.Context, candidate store.AcceptedArtifactPublishCandidate, at time.Time,
) error {
	checkpoint, found, err := worker.store.ReadCommittedOperationArtifactPublish(ctx,
		store.ReadCommittedOperationArtifactPublishSpec{
			OperationID: candidate.OperationID(), At: at,
		})
	if err != nil || !found {
		if err == nil {
			err = store.ErrArtifactStageConflict
		}
		return err
	}
	closure, err := store.RebuildArtifactClosure(ctx, checkpoint.Closure(), at)
	if err != nil || closure.Checkpoint().String() != checkpoint.Capture().String() {
		return store.ErrArtifactStageConflict
	}
	stage, err := worker.cas.OpenStage(checkpoint.Fence().Owner())
	if err != nil {
		return err
	}
	if err := stage.Publish(ctx, closure); err != nil {
		return err
	}
	ready, err := worker.store.MarkOperationArtifactReady(ctx,
		store.MarkOperationArtifactReadySpec{
			Fence: checkpoint.Fence(), At: at,
		})
	if err != nil {
		return err
	}
	if ready.State() != store.ArtifactStageReady {
		return store.ErrArtifactStageConflict
	}
	return nil
}

func (worker *artifactStageCleanupWorker) recoverAcceptedPeerInboxPublish(
	ctx context.Context, candidate store.AcceptedArtifactPublishCandidate, at time.Time,
) error {
	checkpoint, err := worker.store.ReadPeerInboxArtifactPublish(ctx,
		store.ReadPeerInboxArtifactPublishSpec{
			Fence: candidate.PeerInboxFence(), Owner: candidate.Owner(), At: at,
		})
	if err != nil {
		return err
	}
	closure, err := store.RebuildArtifactClosure(ctx, checkpoint.Closure(), at)
	if err != nil {
		return err
	}
	stage, err := worker.cas.OpenStage(candidate.Owner())
	if err != nil {
		return err
	}
	if err := stage.Publish(ctx, closure); err != nil {
		return err
	}
	ready, err := worker.store.MarkPeerInboxArtifactReady(ctx,
		store.MarkPeerInboxArtifactReadySpec{
			Fence: candidate.PeerInboxFence(), Owner: candidate.Owner(), At: at,
		})
	if err != nil {
		return err
	}
	if ready.Status() != "ready" {
		return store.ErrArtifactStageConflict
	}
	return nil
}
