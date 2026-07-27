package artifact

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type stagedObjectSnapshot struct {
	digest   model.Digest
	name     string
	snapshot os.FileInfo
}

type stageLayout struct {
	exists        bool
	directory     os.FileInfo
	marker        os.FileInfo
	markerMissing bool
	objects       map[model.Digest]stagedObjectSnapshot
	bytes         uint64
}

func (stage *Stage) directory(create bool) (string, bool, error) {
	if err := stage.validate(); err != nil {
		return "", false, err
	}
	if err := requireCASDirectory(stage.cas.staging); err != nil {
		return "", false, err
	}
	info, err := os.Lstat(stage.path)
	if err == nil {
		if err := validateCASDirectoryInfo(info); err != nil {
			return "", false, err
		}
		return stage.path, true, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", false, fmt.Errorf("inspect Artifact stage directory: %w", err)
	}
	if !create {
		return stage.path, false, nil
	}
	if err := os.Mkdir(stage.path, casDirectoryMode); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", false, fmt.Errorf("create Artifact stage directory: %w", err)
		}
		info, statErr := os.Lstat(stage.path)
		if statErr != nil {
			return "", false, fmt.Errorf("inspect raced Artifact stage directory: %w", statErr)
		}
		if err := validateCASDirectoryInfo(info); err != nil {
			return "", false, err
		}
		return stage.path, true, nil
	}
	info, err = os.Lstat(stage.path)
	if err != nil || validateCASDirectoryInfo(info) != nil {
		return "", false, fmt.Errorf("%w: Artifact stage directory verification failed",
			ErrCASCorruption)
	}
	if _, err := stage.writeOwnerMarkerLocked(info); err != nil {
		if removeErr := os.Remove(stage.path); removeErr == nil {
			_ = syncDirectory(stage.cas.staging)
		}
		return "", false, err
	}
	if err := syncDirectory(stage.cas.staging); err != nil {
		return "", false, err
	}
	return stage.path, true, nil
}

func (stage *Stage) scanLocked(create bool) (stageLayout, error) {
	_, exists, err := stage.directory(create)
	if err != nil || !exists {
		return stageLayout{exists: exists}, err
	}
	entries, before, err := stage.listEntriesLocked()
	if err != nil {
		return stageLayout{}, err
	}
	marker, markerFound, err := stage.scanOwnerMarkerLocked(entries)
	if err != nil {
		return stageLayout{}, err
	}
	if !markerFound {
		return stage.scanUnmarkedLayoutLocked(create, len(entries), before)
	}
	objects, stagedBytes, recovered, err := stage.scanEntriesLocked(entries)
	if err != nil {
		return stageLayout{}, err
	}
	if recovered {
		return stage.scanLocked(false)
	}
	after, err := os.Lstat(stage.path)
	if err != nil || !sameCASDirectorySnapshot(before, after) {
		return stageLayout{}, fmt.Errorf("%w: Artifact stage directory changed while scanning",
			ErrCASCorruption)
	}
	return stageLayout{
		exists: true, directory: after, marker: marker,
		objects: objects, bytes: stagedBytes,
	}, nil
}

func (stage *Stage) listEntriesLocked() ([]os.DirEntry, os.FileInfo, error) {
	before, err := os.Lstat(stage.path)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect Artifact stage directory: %w", err)
	}
	if err := validateCASDirectoryInfo(before); err != nil {
		return nil, nil, err
	}
	handle, err := os.Open(stage.path)
	if err != nil {
		return nil, nil, fmt.Errorf("open Artifact stage directory: %w", err)
	}
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(before, opened) {
		_ = handle.Close()
		return nil, nil, fmt.Errorf("%w: Artifact stage directory changed while opening",
			ErrCASCorruption)
	}
	entries, readErr := handle.ReadDir(MaxClosureMembers + 2)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		_ = handle.Close()
		return nil, nil, fmt.Errorf("list Artifact stage directory: %w", readErr)
	}
	if len(entries) > MaxClosureMembers+1 {
		_ = handle.Close()
		return nil, nil, fmt.Errorf("%w: Artifact stage member bound exceeded", ErrArtifactLimit)
	}
	afterFD, fdErr := handle.Stat()
	closeErr := handle.Close()
	afterPath, pathErr := os.Lstat(stage.path)
	if fdErr != nil || closeErr != nil || pathErr != nil ||
		!sameCASDirectorySnapshot(before, afterFD) ||
		!sameCASDirectorySnapshot(before, afterPath) {
		return nil, nil, fmt.Errorf("%w: Artifact stage directory identity changed",
			ErrCASCorruption)
	}
	return entries, afterPath, nil
}

type stageTempSnapshot struct {
	digest   model.Digest
	name     string
	snapshot os.FileInfo
}

func (stage *Stage) recoverTempsLocked(temps []stageTempSnapshot) error {
	for _, temp := range temps {
		if err := stage.recoverTempLocked(temp); err != nil {
			return err
		}
	}
	if len(temps) > 0 {
		return syncDirectory(stage.path)
	}
	return nil
}

func (stage *Stage) readObjectLocked(digest model.Digest, maximum int) ([]byte, bool, error) {
	path := filepath.Join(stage.path, stagedObjectName(digest))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect Artifact staged object: %w", err)
	}
	if err := validateCASRegular(info, maximum); err != nil {
		return nil, true, err
	}
	if err := requireCASLinkCount(info, 1); err != nil {
		return nil, true, err
	}
	content, err := readCASObject(path, digest, maximum, 1)
	return content, true, err
}

func inspectStagedObject(path string, digest model.Digest, expected []byte) (PutResult, bool, error) {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return PutResult{}, false, nil
	}
	if err != nil {
		return PutResult{}, false, fmt.Errorf("inspect Artifact staged object: %w", err)
	}
	content, err := readCASObject(path, digest, len(expected), 1)
	if err != nil || !bytes.Equal(content, expected) {
		return PutResult{}, true, fmt.Errorf("%w: staged digest has different bytes",
			ErrCASCorruption)
	}
	return PutResult{Digest: digest, Size: uint64(len(content))}, true, nil
}

func (stage *Stage) newTempLocked(digest model.Digest) (string, *os.File, error) {
	digestName := stagedObjectName(digest)
	for attempt := 0; attempt < 8; attempt++ {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", nil, fmt.Errorf("allocate Artifact stage temp: %w", err)
		}
		name := "put-" + digestName + "-" + hex.EncodeToString(random) + ".tmp"
		path := filepath.Join(stage.path, name)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, casObjectMode)
		if err == nil {
			return path, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, fmt.Errorf("create Artifact stage temp: %w", err)
		}
	}
	return "", nil, errors.New("allocate Artifact stage temp: collision budget exhausted")
}

func (stage *Stage) removeLayoutLocked(layout stageLayout) error {
	current, err := os.Lstat(stage.path)
	if err != nil || !sameCASDirectorySnapshot(layout.directory, current) {
		return fmt.Errorf("%w: Artifact stage directory changed before cleanup",
			ErrCASCorruption)
	}
	names := make([]string, 0, len(layout.objects))
	for _, object := range layout.objects {
		names = append(names, object.name)
	}
	sort.Strings(names)
	for _, name := range names {
		object := layout.objects[mustParseStagedObjectName(name)]
		if err := removeStageFile(filepath.Join(stage.path, name), object.snapshot); err != nil {
			return err
		}
	}
	if !layout.markerMissing {
		if layout.marker == nil {
			return fmt.Errorf("%w: Artifact stage layout has no owner marker",
				ErrCASCorruption)
		}
		if err := removeStageFile(
			filepath.Join(stage.path, stageOwnerMarkerName), layout.marker); err != nil {
			return err
		}
	}
	if err := stage.syncEmptyLayoutDirectoryLocked(layout.directory); err != nil {
		return err
	}
	if err := os.Remove(stage.path); err != nil {
		return fmt.Errorf("remove Artifact stage directory: %w", err)
	}
	return syncDirectory(stage.cas.staging)
}

func (stage *Stage) syncEmptyLayoutDirectoryLocked(expected os.FileInfo) error {
	before, err := os.Lstat(stage.path)
	if err != nil || !sameCASDirectoryIdentity(expected, before) {
		return fmt.Errorf("%w: Artifact stage directory changed before removal",
			ErrCASCorruption)
	}
	handle, err := os.Open(stage.path)
	if err != nil {
		return fmt.Errorf("open Artifact stage directory before removal: %w", err)
	}
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectoryIdentity(expected, opened) {
		_ = handle.Close()
		return fmt.Errorf("%w: Artifact stage directory changed while opening for removal",
			ErrCASCorruption)
	}
	entries, readErr := handle.ReadDir(1)
	if len(entries) != 0 || !errors.Is(readErr, io.EOF) {
		_ = handle.Close()
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return fmt.Errorf("check empty Artifact stage directory: %w", readErr)
		}
		return fmt.Errorf("%w: Artifact stage directory is not empty", ErrCASCorruption)
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		return fmt.Errorf("fsync Artifact stage directory before removal: %w", err)
	}
	afterFD, fdErr := handle.Stat()
	closeErr := handle.Close()
	afterPath, pathErr := os.Lstat(stage.path)
	if fdErr != nil || closeErr != nil || pathErr != nil ||
		!sameCASDirectoryIdentity(expected, afterFD) ||
		!sameCASDirectoryIdentity(expected, afterPath) {
		return fmt.Errorf("%w: Artifact stage directory identity changed before removal",
			ErrCASCorruption)
	}
	return nil
}

func removeStageFile(path string, snapshot os.FileInfo) error {
	current, err := os.Lstat(path)
	if err != nil || !sameCASObjectSnapshot(snapshot, current) {
		return fmt.Errorf("%w: Artifact stage entry changed before removal", ErrCASCorruption)
	}
	if err := requireCASLinkCount(current, 1); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove Artifact stage entry: %w", err)
	}
	return nil
}

func (stage *Stage) validate() error {
	if stage == nil || stage.cas == nil || stage.path == "" {
		return fmt.Errorf("%w: unavailable Artifact stage", ErrCASInput)
	}
	if err := stage.cas.validate(); err != nil {
		return err
	}
	name, err := stage.owner.directoryName()
	if err != nil {
		return err
	}
	expected := filepath.Join(stage.cas.staging, name)
	if stage.path != expected || filepath.Clean(stage.path) != stage.path {
		return fmt.Errorf("%w: Artifact stage path does not match owner", ErrCASInput)
	}
	return nil
}

func (stage *Stage) validateRead(digest model.Digest, maximum int) error {
	if err := stage.validate(); err != nil {
		return err
	}
	if digest.IsZero() || maximum < 0 || maximum > maxCASObjectSize {
		return fmt.Errorf("%w: staged read digest or limit", ErrCASInput)
	}
	return nil
}
