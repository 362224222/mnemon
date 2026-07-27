package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const (
	defaultArtifactStageCleanupPeriod      = time.Minute
	defaultArtifactStageCleanupTTL         = time.Hour
	defaultArtifactStageCleanupMaxExamined = 32
	defaultArtifactStageCleanupMaxTemps    = 32
	maximumArtifactStageCleanupPeriod      = time.Hour
	maximumArtifactStageCleanupTTL         = 365 * 24 * time.Hour
	maximumArtifactStageCleanupExamined    = 256
	maximumArtifactStageCleanupTemps       = 256
)

var (
	ErrArtifactStageCleanup        = errors.New("Artifact stage cleanup")
	ErrArtifactStageCleanupRunning = fmt.Errorf(
		"%w: worker has already run", ErrArtifactStageCleanup)
)

type artifactStageCleanupOptions struct {
	Period      time.Duration
	TTL         time.Duration
	MaxExamined int
	MaxTemps    int
}

// artifactStageCleanupWorker owns the prepare -> filesystem I/O -> fenced
// settlement protocol for expired owner-scoped staging directories.
type artifactStageCleanupWorker struct {
	store *store.Store
	cas   *artifact.CAS
	clock Clock

	period      time.Duration
	ttl         time.Duration
	maxExamined int
	maxTemps    int
	recoveryAt  store.AcceptedArtifactPublishCursor
	cleanupAt   artifact.StageOwner
	directoryAt artifact.StageDirectoryCursor

	cycle sync.Mutex
	mu    sync.Mutex
	run   bool
}

func newArtifactStageCleanupWorker(st *store.Store, cas *artifact.CAS, clock Clock,
	options artifactStageCleanupOptions,
) (*artifactStageCleanupWorker, error) {
	applyArtifactStageCleanupDefaults(&options)
	if st == nil || cas == nil || clock == nil ||
		options.Period <= 0 || options.Period > maximumArtifactStageCleanupPeriod ||
		options.TTL <= 0 || options.TTL > maximumArtifactStageCleanupTTL ||
		options.MaxExamined <= 0 ||
		options.MaxExamined > maximumArtifactStageCleanupExamined ||
		options.MaxTemps <= 0 || options.MaxTemps > maximumArtifactStageCleanupTemps {
		return nil, fmt.Errorf("%w: complete bounded configuration is required",
			ErrArtifactStageCleanup)
	}
	return &artifactStageCleanupWorker{
		store: st, cas: cas, clock: clock,
		period: options.Period, ttl: options.TTL,
		maxExamined: options.MaxExamined, maxTemps: options.MaxTemps,
	}, nil
}

func applyArtifactStageCleanupDefaults(options *artifactStageCleanupOptions) {
	if options.Period == 0 {
		options.Period = defaultArtifactStageCleanupPeriod
	}
	if options.TTL == 0 {
		options.TTL = defaultArtifactStageCleanupTTL
	}
	if options.MaxExamined == 0 {
		options.MaxExamined = defaultArtifactStageCleanupMaxExamined
	}
	if options.MaxTemps == 0 {
		options.MaxTemps = defaultArtifactStageCleanupMaxTemps
	}
}

// Run performs one immediate bounded cycle and then repeats periodically.
func (worker *artifactStageCleanupWorker) Run(ctx context.Context) error {
	if err := worker.usable(ctx); err != nil {
		return err
	}
	worker.mu.Lock()
	if worker.run {
		worker.mu.Unlock()
		return ErrArtifactStageCleanupRunning
	}
	worker.run = true
	worker.mu.Unlock()

	ticker := time.NewTicker(worker.period)
	defer ticker.Stop()
	for {
		if err := worker.runCycle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func validateArtifactStageCleanupPage(page store.ArtifactStageCleanupPage,
	requestedCutoff, now time.Time, maximum int,
) error {
	if page.Examined() < 0 || page.Examined() > maximum ||
		(page.Examined() == 0) != page.Next().IsZero() ||
		(!page.Done() && page.Examined() != maximum) {
		return artifactStageCleanupFailure("validate durable cleanup page", nil)
	}
	candidates := page.Candidates()
	if len(candidates) > page.Examined() || len(candidates) > maximum {
		return artifactStageCleanupFailure("validate durable cleanup page", nil)
	}
	for _, candidate := range candidates {
		owner := candidate.Owner()
		updatedAt, updatedErr := artifactStageCleanupTime(candidate.UpdatedAt())
		claimedAt, claimedErr := artifactStageCleanupTime(candidate.ClaimStartedAt())
		if owner.IsZero() ||
			(owner.Kind() != artifact.StageOwnerOperation &&
				owner.Kind() != artifact.StageOwnerInbox) ||
			candidate.State() != store.ArtifactStageStaged &&
				candidate.State() != store.ArtifactStagePublishing &&
				candidate.State() != store.ArtifactStageReady ||
			updatedErr != nil || claimedErr != nil ||
			!updatedAt.Before(requestedCutoff) || !updatedAt.Before(claimedAt) ||
			claimedAt.After(now) {
			return artifactStageCleanupFailure("validate durable cleanup candidate",
				errors.Join(updatedErr, claimedErr))
		}
	}
	return nil
}

func artifactStageCleanupTime(value time.Time) (time.Time, error) {
	value = value.Round(0).UTC()
	if value.IsZero() || value.Year() < 1 || value.Year() > 9999 ||
		!time.Unix(0, value.UnixNano()).UTC().Equal(value) {
		return time.Time{}, errors.New("unsupported Artifact stage cleanup time")
	}
	return value, nil
}

func artifactStageCleanupFailure(operation string, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s", ErrArtifactStageCleanup, operation)
	}
	return fmt.Errorf("%w: %s: %w", ErrArtifactStageCleanup, operation, cause)
}

func (worker *artifactStageCleanupWorker) usable(ctx context.Context) error {
	if worker == nil || worker.store == nil || worker.cas == nil ||
		worker.clock == nil || ctx == nil {
		return fmt.Errorf("%w: worker is unavailable", ErrArtifactStageCleanup)
	}
	return nil
}
