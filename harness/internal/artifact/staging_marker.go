package artifact

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (stage *Stage) scanOwnerMarkerLocked(
	entries []os.DirEntry,
) (os.FileInfo, bool, error) {
	found := false
	for _, entry := range entries {
		if entry.Name() != stageOwnerMarkerName {
			continue
		}
		if found {
			return nil, false, fmt.Errorf("%w: duplicate Artifact stage owner marker",
				ErrCASCorruption)
		}
		found = true
	}
	if !found {
		return nil, false, nil
	}
	owner, snapshot, err := readStageOwnerMarker(
		filepath.Join(stage.path, stageOwnerMarkerName))
	if err != nil {
		return nil, true, err
	}
	if owner != stage.owner {
		return nil, true, fmt.Errorf("%w: Artifact stage owner marker differs from handle",
			ErrCASCorruption)
	}
	name, err := owner.directoryName()
	if err != nil || filepath.Base(stage.path) != name {
		return nil, true, fmt.Errorf("%w: Artifact stage owner marker path differs",
			ErrCASCorruption)
	}
	return snapshot, true, nil
}

func (stage *Stage) writeOwnerMarkerLocked(expectedDirectory os.FileInfo) (os.FileInfo, error) {
	current, err := os.Lstat(stage.path)
	if err != nil || !sameCASDirectoryIdentity(expectedDirectory, current) {
		return nil, fmt.Errorf("%w: Artifact stage directory changed before marker write",
			ErrCASCorruption)
	}
	content, err := stage.owner.markerBytes()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(stage.path, stageOwnerMarkerName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, casObjectMode)
	if err != nil {
		return nil, fmt.Errorf("create Artifact stage owner marker: %w", err)
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(path)
		}
	}()
	if err := writeFull(file, content); err != nil {
		return nil, fmt.Errorf("write Artifact stage owner marker: %w", err)
	}
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("fsync Artifact stage owner marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close Artifact stage owner marker: %w", err)
	}
	owner, snapshot, err := readStageOwnerMarker(path)
	if err != nil || owner != stage.owner {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%w: written Artifact stage owner marker differs",
			ErrCASCorruption)
	}
	if err := syncDirectory(stage.path); err != nil {
		return nil, err
	}
	after, err := os.Lstat(stage.path)
	if err != nil || !sameCASDirectoryIdentity(expectedDirectory, after) {
		return nil, fmt.Errorf("%w: Artifact stage directory changed after marker write",
			ErrCASCorruption)
	}
	complete = true
	return snapshot, nil
}

func readStageOwnerMarker(path string) (StageOwner, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return StageOwner{}, nil, fmt.Errorf("inspect Artifact stage owner marker: %w", err)
	}
	if err := validateCASRegular(before, maxStageOwnerMarkerSize); err != nil {
		return StageOwner{}, nil, err
	}
	if err := requireCASLinkCount(before, 1); err != nil {
		return StageOwner{}, nil, err
	}
	content, err := readCASRegularBytes(path, maxStageOwnerMarkerSize, 1)
	if err != nil {
		return StageOwner{}, nil, err
	}
	owner, err := parseStageOwnerMarker(content)
	if err != nil {
		return StageOwner{}, nil, err
	}
	after, err := os.Lstat(path)
	if err != nil || !sameCASObjectSnapshot(before, after) {
		return StageOwner{}, nil, fmt.Errorf(
			"%w: Artifact stage owner marker changed while reading", ErrCASCorruption)
	}
	return owner, after, nil
}

func (stage *Stage) scanUnmarkedLayoutLocked(create bool, entries int,
	before os.FileInfo,
) (stageLayout, error) {
	if entries != 0 {
		return stageLayout{}, fmt.Errorf(
			"%w: nonempty Artifact stage directory has no owner marker",
			ErrCASCorruption)
	}
	if create {
		if _, err := stage.writeOwnerMarkerLocked(before); err != nil {
			return stageLayout{}, err
		}
		return stage.scanLocked(false)
	}
	after, err := os.Lstat(stage.path)
	if err != nil || !sameCASDirectorySnapshot(before, after) {
		return stageLayout{}, fmt.Errorf(
			"%w: unmarked Artifact stage directory changed while scanning",
			ErrCASCorruption)
	}
	return stageLayout{
		exists: true, directory: after, markerMissing: true,
		objects: make(map[model.Digest]stagedObjectSnapshot),
	}, nil
}
