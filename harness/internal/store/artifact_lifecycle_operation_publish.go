package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// PrepareOperationArtifactPublish installs the entire closure, exact capture
// projection, and publishing obligation in one fenced transaction.
func (s *Store) PrepareOperationArtifactPublish(ctx context.Context,
	spec PrepareOperationArtifactPublishSpec,
) (OperationArtifactStageResult, error) {
	at, closure, captured, err := validatePrepareOperationArtifactPublish(s, ctx, spec)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationArtifactStageResult{},
			fmt.Errorf("prepare operation Artifact publish: begin: %w", err)
	}
	defer tx.Rollback()
	operationID, err := operationIDFromStageOwner(spec.Fence.owner)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	operation, err := readOperationByID(ctx, tx, operationID)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	if err := requireExactOperationArtifactFence(operation, spec.Fence.leaseOwner,
		spec.Fence.leaseUntil, at); err != nil {
		return OperationArtifactStageResult{}, err
	}
	stage, found, err := readOperationArtifactStage(ctx, tx, operationID)
	if err != nil || !found {
		return OperationArtifactStageResult{}, ErrArtifactStageFence
	}
	if err := requireOperationStageFence(stage, spec.Fence); err != nil {
		return OperationArtifactStageResult{}, err
	}
	captureDigest := model.Sum(spec.Capture.Bytes())
	if stage.state != ArtifactStageStaged {
		if err := requireReplayedOperationArtifactPublish(ctx, tx, operationID,
			operation, stage, spec.Capture, captured, closure, captureDigest); err != nil {
			return OperationArtifactStageResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return OperationArtifactStageResult{}, err
		}
		return OperationArtifactStageResult{fence: spec.Fence, state: stage.state,
			replayed: true}, nil
	}
	if stage.cleanupClaimed {
		return OperationArtifactStageResult{}, ErrArtifactStageFence
	}
	if err := installOperationArtifactPublish(ctx, tx, operationID, spec,
		captured, closure, captureDigest, at); err != nil {
		return OperationArtifactStageResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return OperationArtifactStageResult{},
			fmt.Errorf("prepare operation Artifact publish: commit: %w", err)
	}
	return OperationArtifactStageResult{fence: spec.Fence, state: ArtifactStagePublishing}, nil
}

func validatePrepareOperationArtifactPublish(s *Store, ctx context.Context,
	spec PrepareOperationArtifactPublishSpec,
) (time.Time, VerifiedArtifactClosure, []captureRoot, error) {
	at, err := validateOperationArtifactFence(s, ctx, spec.Fence, spec.At)
	if err != nil || spec.Capture.IsZero() {
		if err == nil {
			err = errors.New("missing capture")
		}
		return time.Time{}, VerifiedArtifactClosure{}, nil,
			fmt.Errorf("prepare operation Artifact publish: %w", err)
	}
	closure, err := validateVerifiedArtifactClosure(spec.Closure)
	if err != nil {
		return time.Time{}, VerifiedArtifactClosure{}, nil, err
	}
	captured, err := parseOperationCapture(spec.Capture)
	if err != nil || !captureMatchesClosure(captured, closure.Roots) {
		return time.Time{}, VerifiedArtifactClosure{}, nil,
			fmt.Errorf("%w: capture and closure roots differ", ErrCaptureMismatch)
	}
	return at, closure, captured, nil
}

func requireReplayedOperationArtifactPublish(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, operation model.Operation, stage durableArtifactStage,
	capture model.JSON, captured []captureRoot, closure VerifiedArtifactClosure,
	captureDigest model.Digest,
) error {
	if stage.state != ArtifactStagePublishing || stage.payloadDigest != captureDigest {
		return ErrArtifactStageConflict
	}
	if err := requireOperationArtifactProjection(ctx, tx, operationID, captured); err != nil {
		return err
	}
	projected, err := readOperationArtifactClosureProjection(ctx, tx, operationID)
	if err != nil {
		return err
	}
	if !sameVerifiedArtifactClosureDigest(closure, projected) {
		return ErrArtifactStageConflict
	}
	if existing, ok := operation.Capture(); !ok || existing.String() != capture.String() {
		return ErrArtifactStageConflict
	}
	return nil
}

func sameVerifiedArtifactClosureDigest(left, right VerifiedArtifactClosure) bool {
	leftDigest, leftErr := verifiedClosureDigest(left)
	rightDigest, rightErr := verifiedClosureDigest(right)
	return leftErr == nil && rightErr == nil && leftDigest == rightDigest
}

func installOperationArtifactPublish(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, spec PrepareOperationArtifactPublishSpec,
	captured []captureRoot, closure VerifiedArtifactClosure,
	captureDigest model.Digest, at time.Time,
) error {
	durableClosure, _, err := stageArtifactClosureMetadata(ctx, tx, closure, at)
	if err != nil {
		return err
	}
	if err := requireOperationArtifactProjection(ctx, tx, operationID, nil); err != nil {
		return err
	}
	if err := insertOperationArtifactRootProjection(ctx, tx, operationID,
		captured, durableClosure); err != nil {
		return err
	}
	if err := installOperationArtifactCapture(ctx, tx, operationID, spec, at); err != nil {
		return err
	}
	return markOperationArtifactPublishing(ctx, tx, operationID, spec.Fence,
		captureDigest, at)
}

func insertOperationArtifactRootProjection(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, captured []captureRoot,
	closure VerifiedArtifactClosure,
) error {
	for index, root := range captured {
		if _, err := tx.ExecContext(ctx, `INSERT INTO operation_artifact_roots(
			operation_id,root_digest,manifest_digest,verified_at) VALUES(?,?,?,?)`,
			operationID.String(), root.RootDigest.String(), root.ManifestDigest.Bytes(),
			storeTime(closure.Roots[index].VerifiedAt)); err != nil {
			return err
		}
	}
	return nil
}

func installOperationArtifactCapture(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, spec PrepareOperationArtifactPublishSpec,
	at time.Time,
) error {
	updated, err := tx.ExecContext(ctx, `UPDATE operations SET capture_json=?
		WHERE operation_id=? AND status='started' AND lease_owner=? AND lease_until=?
		AND lease_until>? AND capture_json IS NULL`, spec.Capture.Bytes(), operationID.String(),
		spec.Fence.leaseOwner, storeTime(spec.Fence.leaseUntil), storeTime(at))
	if err != nil || exactlyOne(updated) != nil {
		return fmt.Errorf("%w: install operation capture: %v", ErrOperationFence, err)
	}
	return nil
}

func markOperationArtifactPublishing(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, fence OperationArtifactStageFence,
	captureDigest model.Digest, at time.Time,
) error {
	updated, err := tx.ExecContext(ctx, `UPDATE operation_artifact_stages
		SET state='publishing',capture_digest=?,updated_at=?
		WHERE operation_id=? AND generation=? AND state='staged' AND lease_owner=?
		AND lease_until=? AND cleanup_started_at IS NULL`, captureDigest.Bytes(), storeTime(at),
		operationID.String(), fence.owner.Generation(), fence.leaseOwner,
		storeTime(fence.leaseUntil))
	if err != nil || exactlyOne(updated) != nil {
		return fmt.Errorf("%w: publish operation stage: %v", ErrArtifactStageFence, err)
	}
	return nil
}

// MarkOperationArtifactReady is the sole operation publishing-to-ready
// boundary. It runs only after filesystem publication, so promoting roots and
// exposing the durable ready state preserves verified => final CAS.
func (s *Store) MarkOperationArtifactReady(ctx context.Context,
	spec MarkOperationArtifactReadySpec,
) (OperationArtifactStageResult, error) {
	at, err := validateOperationArtifactCompletionCall(s, ctx, spec.Fence, spec.At)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	operationID, err := operationIDFromStageOwner(spec.Fence.owner)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	defer tx.Rollback()
	stage, roots, err := readOperationArtifactCompletionState(
		ctx, tx, operationID, spec.Fence, at)
	if err != nil {
		return OperationArtifactStageResult{}, err
	}
	if stage.state == ArtifactStageReady {
		if err := requireReadyArtifactRoots(ctx, tx, roots, at); err != nil {
			return OperationArtifactStageResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return OperationArtifactStageResult{}, err
		}
		return OperationArtifactStageResult{fence: spec.Fence, state: ArtifactStageReady,
			replayed: true}, nil
	}
	if stage.state != ArtifactStagePublishing || stage.cleanupClaimed {
		return OperationArtifactStageResult{}, ErrArtifactStageFence
	}
	if err := promoteArtifactRoots(ctx, tx, roots, at); err != nil {
		return OperationArtifactStageResult{}, err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE operation_artifact_stages
		SET state='ready',updated_at=? WHERE operation_id=? AND generation=?
		AND state='publishing' AND lease_owner=? AND lease_until=?`,
		storeTime(at), operationID.String(), spec.Fence.owner.Generation(),
		spec.Fence.leaseOwner, storeTime(spec.Fence.leaseUntil))
	if err != nil || exactlyOne(updated) != nil {
		return OperationArtifactStageResult{},
			fmt.Errorf("%w: ready operation stage: %v", ErrArtifactStageFence, err)
	}
	if err := tx.Commit(); err != nil {
		return OperationArtifactStageResult{}, err
	}
	return OperationArtifactStageResult{fence: spec.Fence, state: ArtifactStageReady}, nil
}

func readOperationArtifactCompletionState(ctx context.Context, tx *sql.Tx,
	operationID model.OperationID, fence OperationArtifactStageFence, at time.Time,
) (durableArtifactStage, []model.Digest, error) {
	operation, err := readOperationByID(ctx, tx, operationID)
	if err != nil {
		return durableArtifactStage{}, nil, err
	}
	capture, ok := operation.Capture()
	if operation.Status() != model.OperationCommitted || !ok || capture.IsZero() {
		return durableArtifactStage{}, nil, ErrArtifactStageFence
	}
	stage, found, err := readOperationArtifactStage(ctx, tx, operationID)
	if err != nil || !found {
		return durableArtifactStage{}, nil, ErrArtifactStageFence
	}
	if err := requireOperationStageFence(stage, fence); err != nil {
		return durableArtifactStage{}, nil, err
	}
	if at.Before(stage.updatedAt) || stage.payloadDigest != model.Sum(capture.Bytes()) {
		return durableArtifactStage{}, nil, ErrArtifactStageFence
	}
	roots, err := readOperationArtifactDigests(ctx, tx, operationID)
	if err != nil {
		return durableArtifactStage{}, nil, err
	}
	return stage, roots, nil
}

func validateOperationArtifactCompletionCall(s *Store, ctx context.Context,
	fence OperationArtifactStageFence, value time.Time,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || fence.owner.IsZero() ||
		fence.owner.Kind() != artifactdomain.StageOwnerOperation ||
		!validPublicationIdentifier(fence.leaseOwner) {
		return time.Time{}, ErrArtifactStageFence
	}
	if _, err := model.ParseOperationID(fence.owner.CanonicalID()); err != nil {
		return time.Time{}, ErrArtifactStageFence
	}
	leaseUntil, leaseErr := canonicalStoreTime(fence.leaseUntil)
	at, atErr := canonicalStoreTime(value)
	if leaseErr != nil || atErr != nil || leaseUntil != fence.leaseUntil || at != value {
		return time.Time{}, ErrArtifactStageFence
	}
	return at, nil
}
