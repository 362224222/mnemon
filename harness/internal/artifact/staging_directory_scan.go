package artifact

import (
	"fmt"
	"os"
	"time"
)

const (
	maxStageDirectoryScan   = 256
	stageDirectoryScanBytes = 4096
)

// StageDirectoryStatus is a closed physical observation. Only Owned and
// EmptyUnmarked candidates can be passed to RemoveScannedStage.
type StageDirectoryStatus string

const (
	StageDirectoryOwned         StageDirectoryStatus = "owned"
	StageDirectoryEmptyUnmarked StageDirectoryStatus = "empty_unmarked"
	StageDirectoryUnsafe        StageDirectoryStatus = "unsafe"
	StageDirectoryMissingMarker StageDirectoryStatus = "missing_marker"
	StageDirectoryInvalidMarker StageDirectoryStatus = "invalid_marker"
	StageDirectoryOwnerMismatch StageDirectoryStatus = "owner_mismatch"
	StageDirectoryChanged       StageDirectoryStatus = "changed"
)

// StageDirectoryCursor is an opaque process-local directory-stream position.
// It is deliberately not durable authority; a zero value starts a fresh pass.
type StageDirectoryCursor struct {
	offset    int64
	pending   []string
	directory os.FileInfo
	terminal  bool
	valid     bool
}

// StageDirectoryCandidate carries the exact snapshots needed for a later
// remove. A non-zero Owner is returned only after validating the canonical
// marker and its hashed directory name.
type StageDirectoryCandidate struct {
	status     StageDirectoryStatus
	owner      StageOwner
	name       string
	modifiedAt time.Time
	parent     os.FileInfo
	directory  os.FileInfo
	marker     os.FileInfo
}

func (candidate StageDirectoryCandidate) Status() StageDirectoryStatus {
	return candidate.status
}

func (candidate StageDirectoryCandidate) Owner() StageOwner {
	return candidate.owner
}

func (candidate StageDirectoryCandidate) ModifiedAt() time.Time {
	return candidate.modifiedAt
}

// StageDirectoryPage reports only the bounded names examined by this call.
type StageDirectoryPage struct {
	examined   int
	candidates []StageDirectoryCandidate
	next       StageDirectoryCursor
	done       bool
}

func (page StageDirectoryPage) Examined() int { return page.examined }

func (page StageDirectoryPage) Candidates() []StageDirectoryCandidate {
	return append([]StageDirectoryCandidate(nil), page.candidates...)
}

func (page StageDirectoryPage) Next() StageDirectoryCursor {
	return cloneStageDirectoryCursor(page.next)
}

func (page StageDirectoryPage) Done() bool { return page.done }

// ScanStageDirectories examines at most maximum entries from the .staging
// directory stream. Active stage I/O and this scan share the staging mutex, so
// every returned candidate binds a stable directory identity and mtime.
func (cas *CAS) ScanStageDirectories(cursor StageDirectoryCursor,
	maximum int,
) (StageDirectoryPage, error) {
	if err := cas.validate(); err != nil {
		return StageDirectoryPage{}, err
	}
	if maximum <= 0 || maximum > maxStageDirectoryScan {
		return StageDirectoryPage{}, fmt.Errorf(
			"%w: Artifact stage directory scan limit", ErrCASInput)
	}
	cas.coordination.staging.Lock()
	defer cas.coordination.staging.Unlock()

	parent, err := os.Lstat(cas.staging)
	if err != nil || validateCASDirectoryInfo(parent) != nil {
		return StageDirectoryPage{}, fmt.Errorf(
			"%w: inspect Artifact staging directory", ErrCASCorruption)
	}
	cursor, err = normalizeStageDirectoryCursor(cursor, parent)
	if err != nil {
		return StageDirectoryPage{}, err
	}
	names, next, done, err := readStageDirectoryNames(
		cas.staging, parent, cursor, maximum)
	if err != nil {
		return StageDirectoryPage{}, err
	}
	candidates := make([]StageDirectoryCandidate, 0, len(names))
	for _, name := range names {
		candidates = append(candidates,
			cas.inspectStageDirectoryCandidateLocked(name, parent))
	}
	after, err := os.Lstat(cas.staging)
	if err != nil || !sameCASDirectorySnapshot(parent, after) {
		return StageDirectoryPage{}, fmt.Errorf(
			"%w: Artifact staging directory changed while scanning",
			ErrCASCorruption)
	}
	if !done {
		next.directory = after
		next.valid = true
	}
	return StageDirectoryPage{
		examined: len(names), candidates: candidates, next: next, done: done,
	}, nil
}
