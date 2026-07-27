package node

import (
	"context"
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// runCycle never enumerates or removes final CAS objects. Its mutation order is
// durable recovery, exact owner cleanup, physical reconciliation, relational
// sweep, then recognizable temp pruning.
func (worker *artifactStageCleanupWorker) runCycle(ctx context.Context) error {
	if err := worker.usable(ctx); err != nil {
		return err
	}
	worker.cycle.Lock()
	defer worker.cycle.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	now, cutoff, err := worker.cleanupTimes()
	if err != nil {
		return err
	}
	var cycleErr error
	for _, phase := range []func(context.Context) error{
		func(ctx context.Context) error {
			return worker.recoverAcceptedArtifactPublishes(ctx, now)
		},
		func(ctx context.Context) error {
			return worker.cleanupDurableArtifactStages(ctx, cutoff, now)
		},
		func(ctx context.Context) error {
			return worker.reconcilePhysicalStageDirectories(ctx, cutoff)
		},
		func(ctx context.Context) error {
			return worker.sweepArtifactStaging(ctx, cutoff, now)
		},
		func(ctx context.Context) error {
			return worker.pruneArtifactTemps(ctx, cutoff)
		},
	} {
		if err := phase(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			cycleErr = errors.Join(cycleErr, err)
		}
	}
	return cycleErr
}

func (worker *artifactStageCleanupWorker) cleanupTimes() (time.Time, time.Time, error) {
	now, err := artifactStageCleanupTime(worker.clock.Now())
	if err != nil {
		return time.Time{}, time.Time{},
			artifactStageCleanupFailure("read trusted clock", err)
	}
	cutoff, err := artifactStageCleanupTime(now.Add(-worker.ttl))
	if err != nil {
		return time.Time{}, time.Time{},
			artifactStageCleanupFailure("derive cleanup cutoff", err)
	}
	return now, cutoff, nil
}

func (worker *artifactStageCleanupWorker) cleanupDurableArtifactStages(
	ctx context.Context, cutoff, now time.Time,
) error {
	page, err := worker.store.ScanArtifactStageCleanupCandidates(ctx,
		store.ScanArtifactStageCleanupSpec{
			Cutoff: cutoff, At: now, MaxExamined: worker.maxExamined,
			After: worker.cleanupAt,
		})
	if err != nil {
		return artifactStageCleanupFailure("claim durable owner stages", err)
	}
	if err := validateArtifactStageCleanupPage(
		page, cutoff, now, worker.maxExamined); err != nil {
		return err
	}
	if page.Examined() == 0 || page.Done() {
		worker.cleanupAt = artifact.StageOwner{}
	} else {
		worker.cleanupAt = page.Next()
	}
	var cleanupErr error
	for _, candidate := range page.Candidates() {
		if err := ctx.Err(); err != nil {
			return err
		}
		cleanupErr = errors.Join(cleanupErr,
			worker.cleanupDurableArtifactStage(ctx, candidate, now))
	}
	return cleanupErr
}

func (worker *artifactStageCleanupWorker) cleanupDurableArtifactStage(
	ctx context.Context, candidate store.ArtifactStageCleanupCandidate,
	now time.Time,
) error {
	if err := worker.cas.RemoveStage(candidate.Owner()); err != nil {
		return artifactStageCleanupFailure("remove claimed owner stage", err)
	}
	if _, err := worker.store.MarkArtifactStageCleaned(ctx,
		store.MarkArtifactStageCleanedSpec{
			Candidate: candidate, At: now,
		}); err != nil {
		return artifactStageCleanupFailure(
			"mark claimed owner stage cleaned", err)
	}
	return nil
}

func (worker *artifactStageCleanupWorker) sweepArtifactStaging(
	ctx context.Context, cutoff, now time.Time,
) error {
	// Failed owner-local cleanup remains recoverable. Sweep only reclaims
	// relational owners that were already marked physically cleaned.
	result, err := worker.store.SweepArtifactStaging(ctx, artifact.StagingSweepSpec{
		Cutoff: cutoff, At: now, MaxRoots: worker.maxExamined,
	})
	if err != nil {
		return artifactStageCleanupFailure(
			"sweep expired relational staging", err)
	}
	if result.ExpiredPins < 0 || result.ExpiredPins > worker.maxExamined ||
		result.OwnerProjections < 0 ||
		result.OwnerProjections > 2*worker.maxExamined ||
		result.Roots < 0 || result.Roots > worker.maxExamined ||
		result.Blocks < 0 || result.Blocks > worker.maxExamined {
		return artifactStageCleanupFailure(
			"validate relational sweep result", nil)
	}
	return nil
}

func (worker *artifactStageCleanupWorker) pruneArtifactTemps(
	_ context.Context, cutoff time.Time,
) error {
	removed, err := worker.cas.PruneTempsBefore(cutoff, worker.maxTemps)
	if err != nil {
		return artifactStageCleanupFailure(
			"prune recognizable CAS temps", err)
	}
	if len(removed) > worker.maxTemps {
		return artifactStageCleanupFailure(
			"validate CAS temp prune result", nil)
	}
	return nil
}
