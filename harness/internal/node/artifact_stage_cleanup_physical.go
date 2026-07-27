package node

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
)

func (worker *artifactStageCleanupWorker) reconcilePhysicalStageDirectories(
	ctx context.Context, cutoff time.Time,
) error {
	page, err := worker.cas.ScanStageDirectories(
		worker.directoryAt, worker.maxExamined)
	if err != nil {
		return artifactStageCleanupFailure(
			"scan physical Artifact stages", err)
	}
	if page.Examined() < 0 || page.Examined() > worker.maxExamined ||
		len(page.Candidates()) != page.Examined() {
		return artifactStageCleanupFailure(
			"validate physical Artifact stage page", nil)
	}
	if page.Done() {
		worker.directoryAt = artifact.StageDirectoryCursor{}
	} else {
		worker.directoryAt = page.Next()
	}
	var reconcileErr error
	for _, candidate := range page.Candidates() {
		if err := ctx.Err(); err != nil {
			return err
		}
		reconcileErr = errors.Join(reconcileErr,
			worker.reconcilePhysicalStageDirectory(ctx, candidate, cutoff))
	}
	return reconcileErr
}

func (worker *artifactStageCleanupWorker) reconcilePhysicalStageDirectory(
	ctx context.Context, candidate artifact.StageDirectoryCandidate,
	cutoff time.Time,
) error {
	switch candidate.Status() {
	case artifact.StageDirectoryOwned:
		return worker.reconcileOwnedPhysicalStage(ctx, candidate, cutoff)
	case artifact.StageDirectoryEmptyUnmarked:
		return worker.reconcileUnmarkedPhysicalStage(candidate, cutoff)
	default:
		return artifactStageCleanupFailure("inspect physical stage diagnostic",
			fmt.Errorf("%w: status %s",
				artifact.ErrCASCorruption, candidate.Status()))
	}
}

func (worker *artifactStageCleanupWorker) reconcileOwnedPhysicalStage(
	ctx context.Context, candidate artifact.StageDirectoryCandidate,
	cutoff time.Time,
) error {
	expired, err := physicalStageExpired(candidate, cutoff)
	if err != nil {
		return artifactStageCleanupFailure(
			"validate physical owner age", err)
	}
	if !expired {
		return nil
	}
	probe, err := worker.store.ProbeArtifactStageOwner(ctx, candidate.Owner())
	if err != nil {
		return artifactStageCleanupFailure("probe physical owner", err)
	}
	if probe.Found() && !probe.Cleaned() {
		return nil
	}
	if err := worker.cas.RemoveScannedStage(candidate); err != nil {
		return artifactStageCleanupFailure("remove physical owner", err)
	}
	return nil
}

func (worker *artifactStageCleanupWorker) reconcileUnmarkedPhysicalStage(
	candidate artifact.StageDirectoryCandidate, cutoff time.Time,
) error {
	expired, err := physicalStageExpired(candidate, cutoff)
	if err != nil {
		return artifactStageCleanupFailure(
			"validate unmarked stage age", err)
	}
	if !expired {
		return nil
	}
	if err := worker.cas.RemoveScannedStage(candidate); err != nil {
		return artifactStageCleanupFailure("remove unmarked stage", err)
	}
	return nil
}

func physicalStageExpired(candidate artifact.StageDirectoryCandidate,
	cutoff time.Time,
) (bool, error) {
	modifiedAt, err := artifactStageCleanupTime(candidate.ModifiedAt())
	if err != nil {
		return false, err
	}
	return modifiedAt.Before(cutoff), nil
}
