package store

import (
	"context"
	"time"
)

func validateArtifactStageCleanupScan(s *Store, ctx context.Context,
	spec ScanArtifactStageCleanupSpec,
) (time.Time, time.Time, error) {
	if s == nil || s.db == nil || ctx == nil ||
		spec.MaxExamined <= 0 || spec.MaxExamined > maxArtifactStageCleanupExamined {
		return time.Time{}, time.Time{}, ErrArtifactStageConflict
	}
	cutoff, cutoffErr := canonicalStoreTime(spec.Cutoff)
	at, atErr := canonicalStoreTime(spec.At)
	if cutoffErr != nil || atErr != nil || cutoff != spec.Cutoff || at != spec.At ||
		!cutoff.Before(at) {
		return time.Time{}, time.Time{}, ErrArtifactStageConflict
	}
	if !spec.After.IsZero() {
		owner, err := newArtifactStageOwner(spec.After.Kind(),
			spec.After.CanonicalID(), spec.After.Generation())
		if err != nil || owner != spec.After {
			return time.Time{}, time.Time{}, ErrArtifactStageConflict
		}
	}
	return cutoff, at, nil
}

func validateArtifactStageCleanupCandidate(s *Store, ctx context.Context,
	spec MarkArtifactStageCleanedSpec,
) (time.Time, error) {
	candidate := spec.Candidate
	if s == nil || s.db == nil || ctx == nil ||
		!validArtifactStageCleanupCandidateShape(candidate) {
		return time.Time{}, ErrArtifactStageFence
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at != spec.At || at.Before(candidate.claimStartedAt) ||
		!canonicalArtifactStageCleanupCandidateTimes(candidate) {
		return time.Time{}, ErrArtifactStageFence
	}
	owner, err := newArtifactStageOwner(candidate.owner.Kind(),
		candidate.owner.CanonicalID(), candidate.owner.Generation())
	if err != nil || owner != candidate.owner {
		return time.Time{}, ErrArtifactStageFence
	}
	return at, nil
}

func validArtifactStageCleanupCandidateShape(
	candidate ArtifactStageCleanupCandidate,
) bool {
	return !candidate.owner.IsZero() &&
		candidate.state.Valid() &&
		!candidate.updatedAt.IsZero() &&
		!candidate.claimStartedAt.IsZero() &&
		candidate.updatedAt.Before(candidate.claimStartedAt)
}

func canonicalArtifactStageCleanupCandidateTimes(
	candidate ArtifactStageCleanupCandidate,
) bool {
	updatedAt, updatedErr := canonicalStoreTime(candidate.updatedAt)
	startedAt, startedErr := canonicalStoreTime(candidate.claimStartedAt)
	return updatedErr == nil && startedErr == nil &&
		updatedAt == candidate.updatedAt &&
		startedAt == candidate.claimStartedAt
}
