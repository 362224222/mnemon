package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func acceptOperationArtifactStage(ctx context.Context, tx *sql.Tx,
	operation model.Operation, capture model.JSON, at time.Time,
) error {
	stage, found, err := readOperationArtifactStage(ctx, tx, operation.ID())
	if err != nil || !found ||
		stage.state != ArtifactStagePublishing ||
		stage.cleanupClaimed ||
		stage.payloadDigest != model.Sum(capture.Bytes()) {
		return ErrArtifactStageConflict
	}
	if err := requireExactOperationArtifactFence(operation, stage.leaseOwner,
		stage.leaseUntil, at); err != nil {
		return err
	}
	roots, err := readOperationArtifactDigests(ctx, tx, operation.ID())
	if err != nil {
		return err
	}
	return requireAcceptedPublishingArtifactRoots(ctx, tx, roots, at)
}
