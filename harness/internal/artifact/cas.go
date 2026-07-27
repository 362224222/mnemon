package artifact

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	casDirectoryMode = 0o700
	casObjectMode    = 0o600
	maxCASObjectSize = MaxManifestBytes
	casDigestShards  = 256
	maxCASPruneScan  = 256
)

var (
	ErrCASInput      = errors.New("invalid Artifact CAS input")
	ErrCASCorruption = errors.New("Artifact CAS corruption")
)

type CAS struct {
	root         string
	temp         string
	staging      string
	coordination *casCoordination
}

type casCoordination struct {
	temp       sync.Mutex
	tempOffset int64
	staging    sync.Mutex
	digests    [casDigestShards]sync.RWMutex
}

var casCoordinationRegistry = struct {
	sync.Mutex
	roots map[string]*casCoordination
}{roots: make(map[string]*casCoordination)}

type PutResult struct {
	Digest   model.Digest
	Size     uint64
	Replayed bool
}

// NewCAS creates or validates an owner-only sha256 object directory. The root
// is expected to be the Node's objects/sha256 path, not a digest-specific path.
func NewCAS(root string) (*CAS, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("%w: CAS root must be an absolute canonical path", ErrCASInput)
	}
	if err := ensureCASDirectory(root); err != nil {
		return nil, err
	}
	temp := filepath.Join(root, ".tmp")
	if err := ensureCASDirectory(temp); err != nil {
		return nil, err
	}
	staging := filepath.Join(root, ".staging")
	if err := ensureCASDirectory(staging); err != nil {
		return nil, err
	}
	if err := syncDirectory(root); err != nil {
		return nil, err
	}
	registryRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve Artifact CAS root: %w", err)
	}
	casCoordinationRegistry.Lock()
	coordination := casCoordinationRegistry.roots[registryRoot]
	if coordination == nil {
		coordination = &casCoordination{}
		casCoordinationRegistry.roots[registryRoot] = coordination
	}
	casCoordinationRegistry.Unlock()
	return &CAS{root: root, temp: temp, staging: staging, coordination: coordination}, nil
}

func (cas *CAS) Root() string {
	if cas == nil {
		return ""
	}
	return cas.root
}

// Put verifies digest == SHA-256(bytes), fsyncs an owner-only recognizable
// temp, then promotes with hard-link no-overwrite semantics. A losing writer
// validates the winner byte-for-byte before reporting replay.
func (cas *CAS) Put(digest model.Digest, content []byte) (PutResult, error) {
	if cas == nil || digest.IsZero() || len(content) > maxCASObjectSize {
		return PutResult{}, fmt.Errorf("%w: digest or object size", ErrCASInput)
	}
	if model.Sum(content) != digest {
		return PutResult{}, fmt.Errorf("%w: supplied bytes do not match digest", ErrCASCorruption)
	}
	digestLock, err := cas.digestLock(digest)
	if err != nil {
		return PutResult{}, err
	}
	digestLock.Lock()
	defer digestLock.Unlock()
	return cas.putLocked(digest, content)
}

func (cas *CAS) putLocked(digest model.Digest, content []byte) (PutResult, error) {
	final, err := cas.objectPath(digest, true)
	if err != nil {
		return PutResult{}, err
	}
	if err := cas.recoverPromotedTemp(final, digest, content); err != nil {
		return PutResult{}, err
	}
	if result, found, err := inspectCASObject(final, digest, content); found || err != nil {
		if err != nil {
			return PutResult{}, err
		}
		result.Replayed = true
		return result, nil
	}

	cas.coordination.temp.Lock()
	defer cas.coordination.temp.Unlock()
	if err := requireCASDirectory(cas.temp); err != nil {
		return PutResult{}, err
	}
	tempPath, file, err := cas.newTemp()
	if err != nil {
		return PutResult{}, err
	}
	removeTemp := true
	defer func() {
		_ = file.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := writeFull(file, content); err != nil {
		return PutResult{}, fmt.Errorf("write Artifact CAS temp: %w", err)
	}
	if err := file.Sync(); err != nil {
		return PutResult{}, fmt.Errorf("fsync Artifact CAS temp: %w", err)
	}
	if err := file.Close(); err != nil {
		return PutResult{}, fmt.Errorf("close Artifact CAS temp: %w", err)
	}
	verified, err := os.ReadFile(tempPath)
	if err != nil || !bytes.Equal(verified, content) || model.Sum(verified) != digest {
		return PutResult{}, fmt.Errorf("%w: staged bytes changed", ErrCASCorruption)
	}
	if err := syncDirectory(cas.temp); err != nil {
		return PutResult{}, err
	}
	if err := os.Link(tempPath, final); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return PutResult{}, fmt.Errorf("promote Artifact CAS object: %w", err)
		}
		result, found, inspectErr := inspectCASObject(final, digest, content)
		if inspectErr != nil || !found {
			if inspectErr != nil {
				return PutResult{}, inspectErr
			}
			return PutResult{}, fmt.Errorf("%w: promotion winner disappeared", ErrCASCorruption)
		}
		result.Replayed = true
		return result, nil
	}
	// Once final exists the temp is recovery evidence until final's directory
	// has been synced. Do not let an error-path defer discard that evidence.
	removeTemp = false
	if err := syncDirectory(filepath.Dir(final)); err != nil {
		return PutResult{}, err
	}
	if err := os.Remove(tempPath); err != nil {
		return PutResult{}, fmt.Errorf("remove promoted Artifact CAS temp: %w", err)
	}
	if err := syncDirectory(cas.temp); err != nil {
		return PutResult{}, err
	}
	return PutResult{Digest: digest, Size: uint64(len(content))}, nil
}

func (cas *CAS) recoverPromotedTemp(final string, digest model.Digest, expected []byte) error {
	finalInfo, err := os.Lstat(final)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect Artifact CAS promotion recovery: %w", err)
	}
	if err := validateCASRegular(finalInfo, len(expected)); err != nil {
		return err
	}
	links, err := casLinkCount(finalInfo)
	if err != nil {
		return err
	}
	if links == 1 {
		return nil
	}
	if links != 2 {
		return fmt.Errorf("%w: final object has unexplained hard links", ErrCASCorruption)
	}

	cas.coordination.temp.Lock()
	defer cas.coordination.temp.Unlock()
	if err := requireCASDirectory(cas.temp); err != nil {
		return err
	}
	handle, err := os.Open(cas.temp)
	if err != nil {
		return fmt.Errorf("list Artifact CAS promotion temps: %w", err)
	}
	matched := ""
	for {
		entries, readErr := handle.ReadDir(128)
		for _, entry := range entries {
			if !recognizableCASTemp(entry.Name()) {
				continue
			}
			path := filepath.Join(cas.temp, entry.Name())
			info, err := os.Lstat(path)
			if err != nil {
				_ = handle.Close()
				return fmt.Errorf("inspect Artifact CAS promotion temp: %w", err)
			}
			if !os.SameFile(finalInfo, info) {
				continue
			}
			if matched != "" {
				_ = handle.Close()
				return fmt.Errorf("%w: final object has multiple promotion temps", ErrCASCorruption)
			}
			if err := validateCASRegular(info, len(expected)); err != nil {
				_ = handle.Close()
				return err
			}
			if err := requireCASLinkCount(info, 2); err != nil {
				_ = handle.Close()
				return err
			}
			matched = path
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = handle.Close()
			return fmt.Errorf("list Artifact CAS promotion temps: %w", readErr)
		}
	}
	if err := handle.Close(); err != nil {
		return fmt.Errorf("close Artifact CAS promotion temp directory: %w", err)
	}
	if matched == "" {
		return fmt.Errorf("%w: final object's second link is not a CAS promotion temp", ErrCASCorruption)
	}
	content, err := readCASObject(final, digest, len(expected), 2)
	if err != nil || !bytes.Equal(content, expected) {
		return fmt.Errorf("%w: promotion recovery bytes do not match", ErrCASCorruption)
	}
	if err := syncDirectory(filepath.Dir(final)); err != nil {
		return err
	}
	if err := os.Remove(matched); err != nil {
		return fmt.Errorf("remove recovered Artifact CAS promotion temp: %w", err)
	}
	if err := syncDirectory(cas.temp); err != nil {
		return err
	}
	return nil
}

func (cas *CAS) validatePromotedTemp(path string, tempInfo os.FileInfo) (string, error) {
	content, err := readCASRegularBytes(path, maxCASObjectSize, 2)
	if err != nil {
		return "", err
	}
	digest := model.Sum(content)
	final, err := cas.objectPath(digest, false)
	if err != nil {
		return "", err
	}
	finalInfo, err := os.Lstat(final)
	if err != nil {
		return "", fmt.Errorf("inspect promoted Artifact CAS final: %w", err)
	}
	if !os.SameFile(tempInfo, finalInfo) {
		return "", fmt.Errorf("%w: temp hard link does not name its digest final", ErrCASCorruption)
	}
	if err := requireCASLinkCount(finalInfo, 2); err != nil {
		return "", err
	}
	if _, err := readCASObject(final, digest, maxCASObjectSize, 2); err != nil {
		return "", err
	}
	return final, nil
}

func (cas *CAS) Read(digest model.Digest, maximum int) ([]byte, error) {
	if cas == nil || digest.IsZero() || maximum < 0 || maximum > maxCASObjectSize {
		return nil, fmt.Errorf("%w: read digest or limit", ErrCASInput)
	}
	digestLock, err := cas.digestLock(digest)
	if err != nil {
		return nil, err
	}
	digestLock.RLock()
	defer digestLock.RUnlock()
	path, err := cas.objectPath(digest, false)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read Artifact CAS object: %w", err)
	}
	if err := validateCASRegular(info, maximum); err != nil {
		return nil, err
	}
	if err := requireCASLinkCount(info, 1); err != nil {
		return nil, err
	}
	return readCASObject(path, digest, maximum, 1)
}

func readCASObject(path string, digest model.Digest, maximum int, expectedLinks uint64) ([]byte, error) {
	content, err := readCASRegularBytes(path, maximum, expectedLinks)
	if err != nil {
		return nil, err
	}
	if model.Sum(content) != digest {
		return nil, fmt.Errorf("%w: object bytes do not match its digest", ErrCASCorruption)
	}
	return content, nil
}

func readCASRegularBytes(path string, maximum int, expectedLinks uint64) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read Artifact CAS object: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm() != casObjectMode {
		return nil, fmt.Errorf("%w: object is not an owner-only regular file", ErrCASCorruption)
	}
	if err := requireCASLinkCount(before, expectedLinks); err != nil {
		return nil, err
	}
	if before.Size() < 0 || before.Size() > int64(maximum) {
		return nil, fmt.Errorf("%w: object exceeds read budget", ErrCASCorruption)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read Artifact CAS object: %w", err)
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !sameCASObjectSnapshot(before, opened) || requireCASLinkCount(opened, expectedLinks) != nil {
		return nil, fmt.Errorf("%w: object changed while opening", ErrCASCorruption)
	}
	content, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil || len(content) > maximum {
		return nil, fmt.Errorf("%w: object read failed or exceeded budget", ErrCASCorruption)
	}
	afterFD, fdErr := file.Stat()
	afterPath, pathErr := os.Lstat(path)
	if fdErr != nil || pathErr != nil || !sameCASObjectSnapshot(before, afterFD) ||
		!sameCASObjectSnapshot(before, afterPath) || int64(len(content)) != before.Size() ||
		requireCASLinkCount(afterFD, expectedLinks) != nil ||
		requireCASLinkCount(afterPath, expectedLinks) != nil {
		return nil, fmt.Errorf("%w: object identity changed", ErrCASCorruption)
	}
	return content, nil
}

// TempFiles lists recognizable crash leftovers. They are never considered by
// Read and therefore cannot become content authority merely by existing.
func (cas *CAS) TempFiles() ([]string, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	cas.coordination.temp.Lock()
	defer cas.coordination.temp.Unlock()
	if err := requireCASDirectory(cas.temp); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(cas.temp)
	if err != nil {
		return nil, fmt.Errorf("list Artifact CAS temps: %w", err)
	}
	result := make([]string, 0)
	for _, entry := range entries {
		if recognizableCASTemp(entry.Name()) {
			result = append(result, entry.Name())
		}
	}
	sort.Strings(result)
	return result, nil
}

// PruneTempsBefore removes at most limit recognizable owner-only CAS temps
// whose modification time is strictly before cutoff. Active Put staging and
// pruning share one mutex, so a live temp cannot be selected underneath Put.
func (cas *CAS) PruneTempsBefore(cutoff time.Time, limit int) ([]string, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	if cutoff.IsZero() || limit <= 0 || limit > maxCASPruneScan {
		return nil, fmt.Errorf("%w: temp pruning cutoff or limit", ErrCASInput)
	}
	cutoff = cutoff.Round(0)
	cas.coordination.temp.Lock()
	defer cas.coordination.temp.Unlock()
	if err := requireCASDirectory(cas.temp); err != nil {
		return nil, err
	}
	cursor := cas.coordination.tempOffset
	names, nextCursor, err := cas.readTempBatchLocked(cursor)
	if err != nil {
		return nil, err
	}
	candidates := make([]casTempCandidate, 0, limit)
	eligible := 0
	for _, name := range names {
		if !recognizableCASTemp(name) {
			continue
		}
		path := filepath.Join(cas.temp, name)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("inspect Artifact CAS temp: %w", err)
		}
		if err := validateCASRegular(info, maxCASObjectSize); err != nil {
			return nil, fmt.Errorf("%w: unsafe recognizable temp %q", err, name)
		}
		links, err := casLinkCount(info)
		if err != nil || (links != 1 && links != 2) {
			return nil, fmt.Errorf("%w: unsafe recognizable temp %q", ErrCASCorruption, name)
		}
		promotedFinal := ""
		if links == 2 {
			promotedFinal, err = cas.validatePromotedTemp(path, info)
			if err != nil {
				return nil, err
			}
		}
		if !info.ModTime().Before(cutoff) {
			continue
		}
		eligible++
		candidates = insertCASTempCandidate(candidates,
			casTempCandidate{name: name, snapshot: info,
				expectedLinks: links, promotedFinal: promotedFinal}, limit)
	}
	if eligible > limit {
		// Revisit this bounded batch after removing its first page. Advancing
		// past unremoved eligible entries would postpone them until wrap.
		nextCursor = cursor
	}
	removed := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		path := filepath.Join(cas.temp, candidate.name)
		current, err := os.Lstat(path)
		if err != nil || !sameCASObjectSnapshot(candidate.snapshot, current) {
			return nil, fmt.Errorf("%w: temp changed before pruning", ErrCASCorruption)
		}
		if err := requireCASLinkCount(current, candidate.expectedLinks); err != nil {
			return nil, err
		}
		if candidate.promotedFinal != "" {
			finalInfo, err := os.Lstat(candidate.promotedFinal)
			if err != nil || !os.SameFile(current, finalInfo) {
				return nil, fmt.Errorf("%w: promoted final changed before temp pruning", ErrCASCorruption)
			}
			if err := requireCASLinkCount(finalInfo, 2); err != nil {
				return nil, err
			}
			if err := syncDirectory(filepath.Dir(candidate.promotedFinal)); err != nil {
				return nil, err
			}
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove Artifact CAS temp: %w", err)
		}
		removed = append(removed, candidate.name)
	}
	if len(removed) > 0 {
		if err := syncDirectory(cas.temp); err != nil {
			return nil, err
		}
	}
	cas.coordination.tempOffset = nextCursor
	return removed, nil
}

func (cas *CAS) validate() error {
	if cas == nil || cas.root == "" || cas.temp == "" || cas.staging == "" ||
		cas.coordination == nil {
		return fmt.Errorf("%w: nil or incomplete CAS", ErrCASInput)
	}
	for _, directory := range []string{cas.root, cas.temp, cas.staging} {
		if err := requireCASDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func (cas *CAS) digestLock(digest model.Digest) (*sync.RWMutex, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	if digest.IsZero() {
		return nil, fmt.Errorf("%w: zero CAS digest", ErrCASInput)
	}
	return &cas.coordination.digests[casDigestShard(digest)], nil
}

func casDigestShard(digest model.Digest) int {
	bytes := digest.Bytes()
	return int(bytes[0])
}

type casTempCandidate struct {
	name          string
	snapshot      os.FileInfo
	expectedLinks uint64
	promotedFinal string
}

func insertCASTempCandidate(candidates []casTempCandidate, candidate casTempCandidate,
	limit int,
) []casTempCandidate {
	index := sort.Search(len(candidates), func(index int) bool {
		return candidates[index].name >= candidate.name
	})
	if len(candidates) == limit && index == len(candidates) {
		return candidates
	}
	candidates = append(candidates, casTempCandidate{})
	copy(candidates[index+1:], candidates[index:])
	candidates[index] = candidate
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func recognizableCASTemp(name string) bool {
	if len(name) != len("cas-")+32+len(".tmp") || !strings.HasPrefix(name, "cas-") ||
		!strings.HasSuffix(name, ".tmp") {
		return false
	}
	hexValue := strings.TrimSuffix(strings.TrimPrefix(name, "cas-"), ".tmp")
	if strings.ToLower(hexValue) != hexValue {
		return false
	}
	decoded, err := hex.DecodeString(hexValue)
	return err == nil && len(decoded) == 16
}
