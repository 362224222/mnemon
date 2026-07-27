package artifact

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	// MaxClosureMembers is a defensive upper bound for unique block and
	// manifest objects reachable from one valid closure.
	MaxClosureMembers = MaxRoots + MaxEntries + MaxTotalBytes/BlockSize
	// MaxStagingBytes bounds one owner generation to the maximum captured
	// content plus one maximum-sized manifest for every root.
	MaxStagingBytes = MaxTotalBytes + MaxRoots*MaxManifestBytes
)

// Stage is one exact owner generation under CAS .staging. It carries no Store
// authority; callers must durably register its owner before the first Put.
type Stage struct {
	cas   *CAS
	owner StageOwner
	path  string
}

// OpenStage returns an owner-bound handle without creating its directory.
func (cas *CAS) OpenStage(owner StageOwner) (*Stage, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	name, err := owner.directoryName()
	if err != nil {
		return nil, err
	}
	return &Stage{cas: cas, owner: owner, path: filepath.Join(cas.staging, name)}, nil
}

func (stage *Stage) Owner() StageOwner {
	if stage == nil {
		return StageOwner{}
	}
	return stage.owner
}

// Put writes only to this owner generation. It never creates final CAS
// authority; replay validates the existing staged bytes before succeeding.
func (stage *Stage) Put(digest model.Digest, content []byte) (PutResult, error) {
	if err := stage.validate(); err != nil {
		return PutResult{}, err
	}
	if digest.IsZero() || len(content) > maxCASObjectSize {
		return PutResult{}, fmt.Errorf("%w: staged digest or object size", ErrCASInput)
	}
	if model.Sum(content) != digest {
		return PutResult{}, fmt.Errorf("%w: staged bytes do not match digest", ErrCASCorruption)
	}

	stage.cas.coordination.staging.Lock()
	defer stage.cas.coordination.staging.Unlock()
	return stage.putLocked(digest, content, stage.cas.staging)
}

func (stage *Stage) putLocked(digest model.Digest, content []byte,
	parentSyncPath string,
) (PutResult, error) {
	layout, err := stage.preparePutLayoutLocked(parentSyncPath)
	if err != nil {
		return PutResult{}, err
	}
	final := filepath.Join(stage.path, stagedObjectName(digest))
	if result, found, err := stage.replayPutLocked(final, digest, content); found || err != nil {
		return result, err
	}
	if len(layout.objects) >= MaxClosureMembers {
		return PutResult{}, fmt.Errorf("%w: Artifact stage member bound exceeded", ErrArtifactLimit)
	}
	if _, err := boundedStageBytes(layout.bytes, uint64(len(content))); err != nil {
		return PutResult{}, err
	}
	tempPath, err := stage.writePutTempLocked(digest, content)
	if err != nil {
		return PutResult{}, err
	}
	defer os.Remove(tempPath)
	if err := syncDirectory(stage.path); err != nil {
		return PutResult{}, err
	}
	return stage.promotePutTempLocked(tempPath, final, digest, content)
}

func (stage *Stage) preparePutLayoutLocked(parentSyncPath string) (stageLayout, error) {
	layout, err := stage.scanLocked(true)
	if err != nil {
		return stageLayout{}, err
	}
	// A previous mkdir may have survived only in the page cache while its
	// parent fsync failed. Re-establish parent durability on every Put before
	// admitting either a temp or a replayed object.
	if err := syncDirectory(parentSyncPath); err != nil {
		return stageLayout{}, err
	}
	return layout, nil
}

func (stage *Stage) replayPutLocked(path string, digest model.Digest,
	content []byte,
) (PutResult, bool, error) {
	result, found, err := inspectStagedObject(path, digest, content)
	if err != nil || !found {
		return PutResult{}, found, err
	}
	result.Replayed = true
	if err := syncDirectory(stage.path); err != nil {
		return PutResult{}, true, err
	}
	return result, true, nil
}

func (stage *Stage) writePutTempLocked(digest model.Digest, content []byte) (string, error) {
	tempPath, file, err := stage.newTempLocked(digest)
	if err != nil {
		return "", err
	}
	defer file.Close()
	complete := false
	defer func() {
		if !complete {
			_ = os.Remove(tempPath)
		}
	}()
	if err := writeFull(file, content); err != nil {
		return "", fmt.Errorf("write Artifact stage temp: %w", err)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("fsync Artifact stage temp: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close Artifact stage temp: %w", err)
	}
	verified, err := readCASRegularBytes(tempPath, maxCASObjectSize, 1)
	if err != nil || !bytes.Equal(verified, content) || model.Sum(verified) != digest {
		return "", fmt.Errorf("%w: Artifact stage temp changed", ErrCASCorruption)
	}
	complete = true
	return tempPath, nil
}

func (stage *Stage) promotePutTempLocked(tempPath, final string, digest model.Digest,
	content []byte,
) (PutResult, error) {
	if err := renameStageNoReplace(tempPath, final); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return PutResult{}, fmt.Errorf("promote Artifact staged object: %w", err)
		}
		result, found, inspectErr := inspectStagedObject(final, digest, content)
		if inspectErr != nil || !found {
			if inspectErr != nil {
				return PutResult{}, inspectErr
			}
			return PutResult{}, fmt.Errorf("%w: staged promotion winner disappeared",
				ErrCASCorruption)
		}
		tempInfo, statErr := os.Lstat(tempPath)
		if statErr != nil {
			return PutResult{}, fmt.Errorf("inspect losing Artifact stage temp: %w", statErr)
		}
		if err := removeStageFile(tempPath, tempInfo); err != nil {
			return PutResult{}, err
		}
		if err := syncDirectory(stage.path); err != nil {
			return PutResult{}, err
		}
		result.Replayed = true
		return result, nil
	}
	if err := syncDirectory(stage.path); err != nil {
		return PutResult{}, err
	}
	return PutResult{Digest: digest, Size: uint64(len(content))}, nil
}

// Read returns a verified staged object. It does not fall back to final CAS.
func (stage *Stage) Read(digest model.Digest, maximum int) ([]byte, error) {
	if err := stage.validateRead(digest, maximum); err != nil {
		return nil, err
	}
	stage.cas.coordination.staging.Lock()
	defer stage.cas.coordination.staging.Unlock()
	layout, err := stage.scanLocked(false)
	if err != nil {
		return nil, err
	}
	if !layout.exists {
		return nil, fmt.Errorf("read Artifact staged object: %w", os.ErrNotExist)
	}
	content, found, err := stage.readObjectLocked(digest, maximum)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("read Artifact staged object: %w", os.ErrNotExist)
	}
	return content, nil
}

// ReadAvailable returns the owner-local staged object when present and falls
// back to final CAS authority only when that exact staged object is absent. A
// corrupt staged copy is never hidden by a valid final copy.
func (stage *Stage) ReadAvailable(digest model.Digest, maximum int) ([]byte, error) {
	if err := stage.validateRead(digest, maximum); err != nil {
		return nil, err
	}
	stage.cas.coordination.staging.Lock()
	defer stage.cas.coordination.staging.Unlock()
	layout, err := stage.scanLocked(false)
	if err != nil {
		return nil, err
	}
	if layout.exists {
		if _, present := layout.objects[digest]; present {
			content, found, err := stage.readObjectLocked(digest, maximum)
			if err != nil || !found {
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("%w: staged object disappeared", ErrCASCorruption)
			}
			return content, nil
		}
	}
	return stage.cas.Read(digest, maximum)
}

// VerifyClosure reads staged bytes first and falls back to already-published
// final objects. A corrupt staged copy is never hidden by a valid final copy.
func (stage *Stage) VerifyClosure(ctx context.Context, closure Closure) error {
	if err := stage.validate(); err != nil {
		return err
	}
	stage.cas.coordination.staging.Lock()
	defer stage.cas.coordination.staging.Unlock()
	layout, err := stage.scanLocked(false)
	if err != nil {
		return err
	}
	_, err = stage.verifyMixedLocked(ctx, closure, layout)
	return err
}

// Publish verifies the complete staged/final union before any promotion,
// promotes with final CAS no-overwrite semantics, verifies the final closure,
// and only then removes this exact owner generation. Replaying after removal
// succeeds from the complete final closure.
func (stage *Stage) Publish(ctx context.Context, closure Closure) error {
	if err := stage.validate(); err != nil {
		return err
	}
	stage.cas.coordination.staging.Lock()
	defer stage.cas.coordination.staging.Unlock()
	layout, err := stage.scanLocked(false)
	if err != nil {
		return err
	}
	requested, err := stage.verifyMixedLocked(ctx, closure, layout)
	if err != nil {
		return err
	}
	digests := make([]model.Digest, 0, len(requested))
	for digest := range requested {
		digests = append(digests, digest)
	}
	sort.Slice(digests, func(left, right int) bool {
		return digests[left].String() < digests[right].String()
	})
	for _, digest := range digests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, present := layout.objects[digest]; !present {
			continue
		}
		content, found, err := stage.readObjectLocked(digest, requested[digest])
		if err != nil || !found {
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: verified staged object disappeared", ErrCASCorruption)
		}
		if _, err := stage.cas.Put(digest, content); err != nil {
			return err
		}
	}
	if err := stage.cas.VerifyClosure(ctx, closure); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !layout.exists {
		return syncDirectory(stage.cas.staging)
	}
	current, err := stage.scanLocked(false)
	if err != nil {
		return err
	}
	return stage.removeLayoutLocked(current)
}

// RemoveStage validates and removes only the exact owner generation. Missing
// directories are an idempotent success; final CAS objects are never touched.
func (cas *CAS) RemoveStage(owner StageOwner) error {
	stage, err := cas.OpenStage(owner)
	if err != nil {
		return err
	}
	cas.coordination.staging.Lock()
	defer cas.coordination.staging.Unlock()
	layout, err := stage.scanLocked(false)
	if err != nil {
		return err
	}
	if !layout.exists {
		return syncDirectory(cas.staging)
	}
	return stage.removeLayoutLocked(layout)
}

func boundedStageBytes(current, added uint64) (uint64, error) {
	if current > MaxStagingBytes || added > MaxStagingBytes-current {
		return 0, fmt.Errorf("%w: Artifact stage byte bound exceeded", ErrArtifactLimit)
	}
	return current + added, nil
}

func sameCASDirectoryIdentity(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) &&
		validateCASDirectoryInfo(left) == nil && validateCASDirectoryInfo(right) == nil &&
		left.Mode() == right.Mode()
}

func (stage *Stage) verifyMixedLocked(ctx context.Context, closure Closure,
	layout stageLayout,
) (map[model.Digest]int, error) {
	requested := make(map[model.Digest]int)
	read := func(digest model.Digest, maximum int) ([]byte, error) {
		if previous := requested[digest]; maximum > previous {
			requested[digest] = maximum
		}
		if layout.exists {
			if _, present := layout.objects[digest]; present {
				content, found, err := stage.readObjectLocked(digest, maximum)
				if err != nil || !found {
					if err != nil {
						return nil, err
					}
					return nil, fmt.Errorf("%w: staged object disappeared", ErrCASCorruption)
				}
				return content, nil
			}
		}
		return stage.cas.Read(digest, maximum)
	}
	if err := verifyClosureWithReader(ctx, closure, read); err != nil {
		return nil, err
	}
	if err := stage.requireLayoutStable(layout); err != nil {
		return nil, err
	}
	return requested, nil
}

func (stage *Stage) requireLayoutStable(layout stageLayout) error {
	current, err := os.Lstat(stage.path)
	if !layout.exists && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !layout.exists || !sameCASDirectorySnapshot(layout.directory, current) {
		return fmt.Errorf("%w: Artifact stage changed during closure verification",
			ErrCASCorruption)
	}
	return nil
}
