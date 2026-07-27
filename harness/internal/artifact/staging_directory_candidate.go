package artifact

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// RemoveScannedStage removes only a still-exact owned directory or a still
// empty unmarked directory. The caller owns Store and TTL policy; this method
// owns only physical snapshot validation and durable removal.
func (cas *CAS) RemoveScannedStage(candidate StageDirectoryCandidate) error {
	if err := cas.validate(); err != nil {
		return err
	}
	if candidate.status != StageDirectoryOwned &&
		candidate.status != StageDirectoryEmptyUnmarked {
		return fmt.Errorf("%w: unsafe Artifact stage directory candidate", ErrCASInput)
	}
	cas.coordination.staging.Lock()
	defer cas.coordination.staging.Unlock()
	parent, err := os.Lstat(cas.staging)
	if err != nil || !sameCASDirectoryIdentity(candidate.parent, parent) {
		return fmt.Errorf("%w: Artifact staging directory identity changed",
			ErrCASCorruption)
	}
	path := filepath.Join(cas.staging, candidate.name)
	current, err := os.Lstat(path)
	if err != nil || !sameCASDirectorySnapshot(candidate.directory, current) {
		return fmt.Errorf("%w: scanned Artifact stage directory changed",
			ErrCASCorruption)
	}
	if candidate.status == StageDirectoryEmptyUnmarked {
		return cas.removeEmptyUnmarkedStageLocked(path, candidate.directory)
	}
	return cas.removeOwnedScannedStageLocked(candidate)
}

func (cas *CAS) inspectStageDirectoryCandidateLocked(name string,
	parent os.FileInfo,
) StageDirectoryCandidate {
	candidate := StageDirectoryCandidate{
		status: StageDirectoryChanged, name: name, parent: parent,
	}
	path := filepath.Join(cas.staging, name)
	info, err := os.Lstat(path)
	if err != nil {
		return candidate
	}
	candidate.modifiedAt = info.ModTime()
	if !validStageDirectoryName(name) || validateCASDirectoryInfo(info) != nil {
		candidate.status = StageDirectoryUnsafe
		return candidate
	}
	candidate.directory = info
	markerPath := filepath.Join(path, stageOwnerMarkerName)
	owner, marker, err := readStageOwnerMarker(markerPath)
	if errors.Is(err, os.ErrNotExist) {
		return inspectUnmarkedStageDirectory(path, candidate)
	}
	if err != nil {
		candidate.status = StageDirectoryInvalidMarker
		return candidate
	}
	expected, err := owner.directoryName()
	if err != nil || expected != name {
		candidate.status = StageDirectoryOwnerMismatch
		return candidate
	}
	after, err := os.Lstat(path)
	if err != nil || !sameCASDirectorySnapshot(info, after) {
		candidate.status = StageDirectoryChanged
		candidate.owner = StageOwner{}
		candidate.marker = nil
		return candidate
	}
	candidate.status = StageDirectoryOwned
	candidate.owner = owner
	candidate.marker = marker
	candidate.directory = after
	candidate.modifiedAt = after.ModTime()
	return candidate
}

func inspectUnmarkedStageDirectory(path string,
	candidate StageDirectoryCandidate,
) StageDirectoryCandidate {
	handle, err := os.Open(path)
	if err != nil {
		candidate.status = StageDirectoryChanged
		return candidate
	}
	entries, readErr := handle.ReadDir(1)
	afterFD, statErr := handle.Stat()
	closeErr := handle.Close()
	afterPath, pathErr := os.Lstat(path)
	if statErr != nil || closeErr != nil || pathErr != nil ||
		!sameCASDirectorySnapshot(candidate.directory, afterFD) ||
		!sameCASDirectorySnapshot(candidate.directory, afterPath) {
		candidate.status = StageDirectoryChanged
		return candidate
	}
	candidate.directory = afterPath
	candidate.modifiedAt = afterPath.ModTime()
	if len(entries) == 0 && errors.Is(readErr, io.EOF) {
		candidate.status = StageDirectoryEmptyUnmarked
		return candidate
	}
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		candidate.status = StageDirectoryChanged
		return candidate
	}
	candidate.status = StageDirectoryMissingMarker
	return candidate
}

func (cas *CAS) removeOwnedScannedStageLocked(
	candidate StageDirectoryCandidate,
) error {
	if candidate.owner.IsZero() || candidate.marker == nil {
		return fmt.Errorf("%w: incomplete owned Artifact stage candidate", ErrCASInput)
	}
	markerPath := filepath.Join(cas.staging, candidate.name, stageOwnerMarkerName)
	currentMarker, err := os.Lstat(markerPath)
	if err != nil || !sameCASObjectSnapshot(candidate.marker, currentMarker) {
		return fmt.Errorf("%w: scanned Artifact stage marker changed",
			ErrCASCorruption)
	}
	owner, _, err := readStageOwnerMarker(markerPath)
	if err != nil || owner != candidate.owner {
		return fmt.Errorf("%w: scanned Artifact stage owner changed",
			ErrCASCorruption)
	}
	stage, err := cas.OpenStage(owner)
	if err != nil {
		return err
	}
	layout, err := stage.scanLocked(false)
	if err != nil {
		return err
	}
	if !layout.exists || layout.markerMissing {
		return fmt.Errorf("%w: scanned owned Artifact stage disappeared",
			ErrCASCorruption)
	}
	return stage.removeLayoutLocked(layout)
}

func (cas *CAS) removeEmptyUnmarkedStageLocked(path string,
	expected os.FileInfo,
) error {
	handle, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open empty unmarked Artifact stage: %w", err)
	}
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(expected, opened) {
		_ = handle.Close()
		return fmt.Errorf("%w: empty unmarked Artifact stage changed",
			ErrCASCorruption)
	}
	entries, readErr := handle.ReadDir(1)
	if len(entries) != 0 || !errors.Is(readErr, io.EOF) {
		_ = handle.Close()
		return fmt.Errorf("%w: unmarked Artifact stage is no longer empty",
			ErrCASCorruption)
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		return fmt.Errorf("fsync empty unmarked Artifact stage: %w", err)
	}
	afterFD, statErr := handle.Stat()
	closeErr := handle.Close()
	afterPath, pathErr := os.Lstat(path)
	if statErr != nil || closeErr != nil || pathErr != nil ||
		!sameCASDirectorySnapshot(expected, afterFD) ||
		!sameCASDirectorySnapshot(expected, afterPath) {
		return fmt.Errorf("%w: empty unmarked Artifact stage changed before removal",
			ErrCASCorruption)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove empty unmarked Artifact stage: %w", err)
	}
	return syncDirectory(cas.staging)
}

func validStageDirectoryName(name string) bool {
	if len(name) != 64 || strings.ToLower(name) != name ||
		filepath.Base(name) != name {
		return false
	}
	for _, character := range name {
		if character < '0' ||
			character > '9' && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
