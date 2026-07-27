package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readCommittedOperationArtifactCapture(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID,
) (model.Operation, model.JSON, []captureRoot, error) {
	operation, err := readOperationByID(ctx, tx, operationID)
	if err != nil || operation.Status() != model.OperationCommitted {
		return model.Operation{}, model.JSON{}, nil, ErrArtifactStageFence
	}
	capture, ok := operation.Capture()
	if !ok || capture.IsZero() {
		return model.Operation{}, model.JSON{}, nil, ErrArtifactStageConflict
	}
	captured, err := parseOperationCapture(capture)
	if err != nil {
		return model.Operation{}, model.JSON{}, nil, ErrArtifactStageConflict
	}
	return operation, capture, captured, nil
}

func readCommittedOperationArtifactPublishStage(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, capture model.JSON, at time.Time,
) (durableArtifactStage, error) {
	stage, found, err := readOperationArtifactStage(ctx, tx, operationID)
	if err != nil || !found || at.Before(stage.updatedAt) ||
		(stage.state != ArtifactStagePublishing && stage.state != ArtifactStageReady) ||
		stage.payloadDigest != model.Sum(capture.Bytes()) ||
		(stage.state == ArtifactStagePublishing && stage.cleanupClaimed) {
		return durableArtifactStage{}, ErrArtifactStageFence
	}
	return stage, nil
}
