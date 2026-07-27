package store

import (
	"context"
	"fmt"
	"time"

	artifactdomain "github.com/mnemon-dev/mnemon/harness/internal/artifact"
)

const maxArtifactStageCleanupExamined = 256

// ScanArtifactStageCleanupSpec bounds owner metadata examined by one
// transaction. After is an optional process-local keyset position; it is
// never durable cleanup authority.
type ScanArtifactStageCleanupSpec struct {
	Cutoff      time.Time
	At          time.Time
	MaxExamined int
	After       artifactdomain.StageOwner
}

// ArtifactStageCleanupCandidate is an opaque exact-CAS permit for one physical
// owner directory.
type ArtifactStageCleanupCandidate struct {
	owner          artifactdomain.StageOwner
	state          ArtifactStageState
	updatedAt      time.Time
	claimStartedAt time.Time
}

func (candidate ArtifactStageCleanupCandidate) Owner() artifactdomain.StageOwner {
	return candidate.owner
}

func (candidate ArtifactStageCleanupCandidate) State() ArtifactStageState {
	return candidate.state
}

func (candidate ArtifactStageCleanupCandidate) UpdatedAt() time.Time {
	return candidate.updatedAt
}

func (candidate ArtifactStageCleanupCandidate) ClaimStartedAt() time.Time {
	return candidate.claimStartedAt
}

type ArtifactStageCleanupPage struct {
	next       artifactdomain.StageOwner
	done       bool
	examined   int
	candidates []ArtifactStageCleanupCandidate
}

func (page ArtifactStageCleanupPage) Examined() int { return page.examined }
func (page ArtifactStageCleanupPage) Next() artifactdomain.StageOwner {
	return page.next
}
func (page ArtifactStageCleanupPage) Done() bool { return page.done }

func (page ArtifactStageCleanupPage) Candidates() []ArtifactStageCleanupCandidate {
	return append([]ArtifactStageCleanupCandidate(nil), page.candidates...)
}

type MarkArtifactStageCleanedSpec struct {
	Candidate ArtifactStageCleanupCandidate
	At        time.Time
}

type ArtifactStageCleanedResult struct {
	replayed bool
}

func (result ArtifactStageCleanedResult) Replayed() bool { return result.replayed }

// ArtifactStageOwnerProbe is the exact relational status used by the
// filesystem orphan scanner. Absence or a durable cleaned mark permits only
// the scanner's separate age-and-snapshot-checked removal path.
type ArtifactStageOwnerProbe struct {
	found   bool
	cleaned bool
}

func (probe ArtifactStageOwnerProbe) Found() bool   { return probe.found }
func (probe ArtifactStageOwnerProbe) Cleaned() bool { return probe.cleaned }

func (s *Store) ProbeArtifactStageOwner(ctx context.Context,
	owner artifactdomain.StageOwner,
) (ArtifactStageOwnerProbe, error) {
	if s == nil || s.db == nil || ctx == nil || owner.IsZero() {
		return ArtifactStageOwnerProbe{}, ErrArtifactStageFence
	}
	rebuilt, err := newArtifactStageOwner(owner.Kind(), owner.CanonicalID(),
		owner.Generation())
	if err != nil || rebuilt != owner {
		return ArtifactStageOwnerProbe{}, ErrArtifactStageFence
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactStageOwnerProbe{}, err
	}
	defer tx.Rollback()
	stage, found, err := readExactArtifactStage(ctx, tx, owner)
	if err != nil {
		return ArtifactStageOwnerProbe{}, err
	}
	if err := tx.Commit(); err != nil {
		return ArtifactStageOwnerProbe{}, err
	}
	return ArtifactStageOwnerProbe{found: found,
		cleaned: found && stage.cleanedAt.Valid}, nil
}

// ScanArtifactStageCleanupCandidates advances across every old owner it
// examines. The caller may retain Next only in memory so a protected or
// repeatedly failing low key cannot starve later owners.
func (s *Store) ScanArtifactStageCleanupCandidates(ctx context.Context,
	spec ScanArtifactStageCleanupSpec,
) (ArtifactStageCleanupPage, error) {
	cutoff, at, err := validateArtifactStageCleanupScan(s, ctx, spec)
	if err != nil {
		return ArtifactStageCleanupPage{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactStageCleanupPage{}, fmt.Errorf("scan Artifact stage cleanup: begin: %w", err)
	}
	defer tx.Rollback()

	raw, err := readArtifactStageCleanupPage(ctx, tx, cutoff, spec.After,
		spec.MaxExamined)
	if err != nil {
		return ArtifactStageCleanupPage{}, err
	}
	candidates, err := claimEligibleArtifactStages(ctx, tx, raw, cutoff, at)
	if err != nil {
		return ArtifactStageCleanupPage{}, err
	}
	var next artifactdomain.StageOwner
	if len(raw) != 0 {
		next = raw[len(raw)-1].owner
	}
	if err := tx.Commit(); err != nil {
		return ArtifactStageCleanupPage{}, fmt.Errorf("scan Artifact stage cleanup: commit: %w", err)
	}
	return ArtifactStageCleanupPage{next: next, done: len(raw) < spec.MaxExamined,
		examined: len(raw), candidates: candidates}, nil
}

// MarkArtifactStageCleaned records successful physical directory removal with
// an exact owner/state/time CAS. It never changes or deletes final Artifact
// metadata.
func (s *Store) MarkArtifactStageCleaned(ctx context.Context,
	spec MarkArtifactStageCleanedSpec,
) (ArtifactStageCleanedResult, error) {
	at, err := validateArtifactStageCleanupCandidate(s, ctx, spec)
	if err != nil {
		return ArtifactStageCleanedResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ArtifactStageCleanedResult{}, fmt.Errorf("mark Artifact stage cleaned: begin: %w", err)
	}
	defer tx.Rollback()
	stage, found, err := readExactArtifactStage(ctx, tx, spec.Candidate.owner)
	if err != nil || !found {
		return ArtifactStageCleanedResult{}, ErrArtifactStageFence
	}
	if stage.state != spec.Candidate.state ||
		!stage.updatedAt.Equal(spec.Candidate.updatedAt) {
		return ArtifactStageCleanedResult{}, ErrArtifactStageFence
	}
	if !stage.cleanupClaimed ||
		!stage.cleanupStarted.Equal(spec.Candidate.claimStartedAt) {
		return ArtifactStageCleanedResult{}, ErrArtifactStageFence
	}
	if stage.cleanedAt.Valid {
		if _, err := parseCanonicalStoreTime(stage.cleanedAt.String); err != nil {
			return ArtifactStageCleanedResult{}, ErrArtifactStageConflict
		}
		if err := tx.Commit(); err != nil {
			return ArtifactStageCleanedResult{}, err
		}
		return ArtifactStageCleanedResult{replayed: true}, nil
	}
	table, idColumn, err := artifactStageOwnerTable(spec.Candidate.owner.Kind())
	if err != nil {
		return ArtifactStageCleanedResult{}, err
	}
	query := fmt.Sprintf(`UPDATE %s SET cleaned_at=?
		WHERE %s=? AND generation=? AND state=? AND updated_at=?
		AND cleanup_started_at=? AND cleaned_at IS NULL`,
		table, idColumn)
	updated, err := tx.ExecContext(ctx, query, storeTime(at),
		spec.Candidate.owner.CanonicalID(), spec.Candidate.owner.Generation(),
		string(spec.Candidate.state), storeTime(spec.Candidate.updatedAt),
		storeTime(spec.Candidate.claimStartedAt))
	if err != nil || exactlyOne(updated) != nil {
		return ArtifactStageCleanedResult{}, fmt.Errorf(
			"%w: mark exact owner cleaned: %v", ErrArtifactStageFence, err)
	}
	if err := tx.Commit(); err != nil {
		return ArtifactStageCleanedResult{}, fmt.Errorf("mark Artifact stage cleaned: commit: %w", err)
	}
	return ArtifactStageCleanedResult{}, nil
}
