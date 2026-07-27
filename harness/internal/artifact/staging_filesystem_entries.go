package artifact

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type stageEntryScan struct {
	digest   model.Digest
	snapshot os.FileInfo
	bytes    uint64
	object   bool
}

func (stage *Stage) scanEntriesLocked(entries []os.DirEntry) (
	map[model.Digest]stagedObjectSnapshot, uint64, bool, error,
) {
	temps := make([]stageTempSnapshot, 0, 1)
	objects := make(map[model.Digest]stagedObjectSnapshot, len(entries)-1)
	var stagedBytes uint64
	for _, entry := range entries {
		if entry.Name() == stageOwnerMarkerName {
			continue
		}
		scanned, err := stage.scanEntryLocked(entry, stagedBytes)
		if err != nil {
			return nil, 0, false, err
		}
		stagedBytes = scanned.bytes
		if scanned.object {
			objects[scanned.digest] = stagedObjectSnapshot{
				digest: scanned.digest, name: entry.Name(), snapshot: scanned.snapshot,
			}
			continue
		}
		temps = append(temps, stageTempSnapshot{
			digest: scanned.digest, name: entry.Name(), snapshot: scanned.snapshot,
		})
	}
	if len(temps) == 0 {
		return objects, stagedBytes, false, nil
	}
	if err := stage.recoverTempsLocked(temps); err != nil {
		return nil, 0, false, err
	}
	return nil, 0, true, nil
}

func (stage *Stage) scanEntryLocked(entry os.DirEntry, currentBytes uint64) (stageEntryScan, error) {
	path := filepath.Join(stage.path, entry.Name())
	info, err := os.Lstat(path)
	if err != nil {
		return stageEntryScan{}, fmt.Errorf("inspect Artifact stage entry: %w", err)
	}
	if err := validateCASRegular(info, maxCASObjectSize); err != nil {
		return stageEntryScan{}, fmt.Errorf("%w: unsafe stage entry %q", err, entry.Name())
	}
	if err := requireCASLinkCount(info, 1); err != nil {
		return stageEntryScan{}, err
	}
	stagedBytes, err := boundedStageBytes(currentBytes, uint64(info.Size()))
	if err != nil {
		return stageEntryScan{}, err
	}
	if digest, ok := parseStagedObjectName(entry.Name()); ok {
		if _, err := readCASObject(path, digest, maxCASObjectSize, 1); err != nil {
			return stageEntryScan{}, err
		}
		return stageEntryScan{
			digest: digest, snapshot: info, bytes: stagedBytes, object: true,
		}, nil
	}
	if digest, ok := parseStageTempName(entry.Name()); ok {
		return stageEntryScan{
			digest: digest, snapshot: info, bytes: stagedBytes,
		}, nil
	}
	return stageEntryScan{}, fmt.Errorf("%w: unknown Artifact stage entry %q",
		ErrCASCorruption, entry.Name())
}

func (stage *Stage) recoverTempLocked(temp stageTempSnapshot) error {
	path := filepath.Join(stage.path, temp.name)
	content, err := readCASRegularBytes(path, maxCASObjectSize, 1)
	if err != nil {
		return err
	}
	if model.Sum(content) != temp.digest {
		return removeStageFile(path, temp.snapshot)
	}
	final := filepath.Join(stage.path, stagedObjectName(temp.digest))
	if err := renameStageNoReplace(path, final); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("recover Artifact stage temp: %w", err)
		}
		result, found, inspectErr := inspectStagedObject(final, temp.digest, content)
		if inspectErr != nil {
			return inspectErr
		}
		if !found {
			return fmt.Errorf("%w: recovered stage winner disappeared", ErrCASCorruption)
		}
		if result.Size != uint64(len(content)) {
			return fmt.Errorf("%w: recovered stage winner size changed", ErrCASCorruption)
		}
		return removeStageFile(path, temp.snapshot)
	}
	return nil
}

func stagedObjectName(digest model.Digest) string {
	return strings.TrimPrefix(digest.String(), "sha256:")
}

func parseStagedObjectName(name string) (model.Digest, bool) {
	if len(name) != 64 || strings.ToLower(name) != name {
		return model.Digest{}, false
	}
	digest, err := model.ParseDigest("sha256:" + name)
	return digest, err == nil
}

func mustParseStagedObjectName(name string) model.Digest {
	digest, ok := parseStagedObjectName(name)
	if !ok {
		panic("validated Artifact stage object name became invalid")
	}
	return digest
}

func parseStageTempName(name string) (model.Digest, bool) {
	if len(name) != len("put-")+64+1+32+len(".tmp") ||
		!strings.HasPrefix(name, "put-") || !strings.HasSuffix(name, ".tmp") {
		return model.Digest{}, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, "put-"), ".tmp")
	if body[64] != '-' {
		return model.Digest{}, false
	}
	digest, ok := parseStagedObjectName(body[:64])
	if !ok || strings.ToLower(body[65:]) != body[65:] {
		return model.Digest{}, false
	}
	random, err := hex.DecodeString(body[65:])
	return digest, err == nil && len(random) == 16
}
