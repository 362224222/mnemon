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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	casDirectoryMode = 0o700
	casObjectMode    = 0o600
	maxCASObjectSize = MaxManifestBytes
	casDigestShards  = 256
	// Object collection is an internal bounded worker surface. A hard page
	// limit prevents a caller-controlled allocation from weakening PX-11.
	maxCASObjectPageSize = 256
)

var (
	ErrCASInput      = errors.New("invalid Artifact CAS input")
	ErrCASCorruption = errors.New("Artifact CAS corruption")
)

type CAS struct {
	root      string
	temp      string
	trash     string
	lifecycle sync.RWMutex
	tempMu    sync.Mutex
	digests   [casDigestShards]sync.RWMutex
}

// CASLease fences the complete filesystem/Store lifecycle of a CAS user or
// collector. Release is safe to call more than once. CAS byte methods do not
// acquire this lease themselves: callers that compose CAS and Store effects
// must hold a use lease, while collection holds an exclusive lease.
type CASLease struct {
	once    sync.Once
	release func()
}

// CASObjectCandidate is a defensive snapshot of one canonical final object.
// Object scan pages return candidates in Digest order.
type CASObjectCandidate struct {
	Digest     model.Digest
	Size       uint64
	ModifiedAt time.Time
}

// CASObjectScanCursor is the restartable exclusive lower bound for one
// object-collection pass. cutoff is carried by every successor so a caller
// cannot accidentally move the eligibility boundary between pages. A zero
// after digest denotes the beginning of the canonical digest keyspace.
type CASObjectScanCursor struct {
	cutoff time.Time
	after  model.Digest
}

// CASObjectPage is one bounded, deterministic object-collection page. The
// next cursor advances past every returned candidate even when the Store later
// decides that candidate is protected. Done explicitly marks the terminal
// state observed by this scan. Replaying its cursor cannot return an existing
// candidate at or below that digest; ordinary later writes fall outside the
// frozen cutoff.
type CASObjectPage struct {
	Candidates []CASObjectCandidate
	NextCursor CASObjectScanCursor
	Done       bool
}

// NewCASObjectScanCursor constructs a canonical restart cursor. Callers may
// durably persist Cutoff and After and reconstruct the same scan after restart.
func NewCASObjectScanCursor(cutoff time.Time, after model.Digest) (CASObjectScanCursor, error) {
	if cutoff.IsZero() {
		return CASObjectScanCursor{}, fmt.Errorf("%w: object scan cutoff", ErrCASInput)
	}
	return CASObjectScanCursor{cutoff: cutoff.Round(0).UTC(), after: after}, nil
}

func (cursor CASObjectScanCursor) Cutoff() time.Time   { return cursor.cutoff }
func (cursor CASObjectScanCursor) After() model.Digest { return cursor.after }

type CASTombstoneState string

const (
	CASTombstoneFinalOnly     CASTombstoneState = "final_only"
	CASTombstoneFinalAndTrash CASTombstoneState = "final_and_tombstone"
	CASTombstoneTrashOnly     CASTombstoneState = "tombstone_only"
	CASTombstoneAbsent        CASTombstoneState = "neither"
)

// CASTombstoneStatus distinguishes every crash-recovery state. Closed means
// that the canonical final path is absent; the Store queue state decides
// whether a closed "neither" state is expected or corruption.
type CASTombstoneStatus struct {
	State  CASTombstoneState
	Closed bool
}

// CASTombstoneDescriptor is a defensive startup-reconciliation snapshot of
// one canonical .trash entry and its corresponding final-path state.
type CASTombstoneDescriptor struct {
	Digest model.Digest
	Token  [32]byte
	State  CASTombstoneState
	Closed bool
}

type PutResult struct {
	Digest   model.Digest
	Size     uint64
	Replayed bool
}

// NewCAS creates or validates an owner-only sha256 object directory. The root
// is expected to be the Node's objects/sha256 path, not a digest-specific path.
// The caller owns the unique live CAS instance for root and must inject that
// same pointer into every concurrent user; lifecycle, temp, and digest barriers
// are deliberately instance-local rather than process-global state.
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
	trash := filepath.Join(root, ".trash")
	if err := ensureCASDirectory(trash); err != nil {
		return nil, err
	}
	if err := syncDirectory(root); err != nil {
		return nil, err
	}
	return &CAS{root: root, temp: temp, trash: trash}, nil
}

func (cas *CAS) Root() string {
	if cas == nil {
		return ""
	}
	return cas.root
}

// AcquireUse holds the shared lifecycle barrier until Release. It is intended
// to cover all CAS operations and the Store checkpoint that makes them owned.
func (cas *CAS) AcquireUse() (*CASLease, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	cas.lifecycle.RLock()
	return &CASLease{release: cas.lifecycle.RUnlock}, nil
}

// AcquireExclusive holds the lifecycle barrier exclusively. A collector keeps
// it across durable enqueue, tombstoning, Store mark-renamed, purge, and queue
// completion.
func (cas *CAS) AcquireExclusive() (*CASLease, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	cas.lifecycle.Lock()
	return &CASLease{release: cas.lifecycle.Unlock}, nil
}

func (lease *CASLease) Release() {
	if lease == nil {
		return
	}
	lease.once.Do(func() {
		if lease.release != nil {
			lease.release()
		}
	})
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

	cas.tempMu.Lock()
	defer cas.tempMu.Unlock()
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

	cas.tempMu.Lock()
	defer cas.tempMu.Unlock()
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
	cas.tempMu.Lock()
	defer cas.tempMu.Unlock()
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

// ListObjectsBefore returns the first bounded page of canonical final objects
// strictly older than cutoff. It remains as a compatibility surface; a
// collector must use ListObjectsPage and persist its returned cursor so a
// protected low-digest prefix cannot starve later orphan candidates.
func (cas *CAS) ListObjectsBefore(cutoff time.Time, limit int) ([]CASObjectCandidate, error) {
	cursor, err := NewCASObjectScanCursor(cutoff, model.Digest{})
	if err != nil {
		return nil, err
	}
	page, err := cas.ListObjectsPage(cursor, limit)
	if err != nil {
		return nil, err
	}
	return page.Candidates, nil
}

// ListObjectsPage returns at most limit canonical final objects strictly older
// than the cursor's frozen cutoff and strictly after its digest lower bound.
//
// Filesystem directory order is not portable or stable across restart. To
// preserve canonical digest order with bounded memory, the scanner completely
// validates each visited shard while retaining only its smallest limit+1
// matches. Once lookahead is full it stops before opening another shard, so a
// nonterminal page does not perform a fixed traversal of all 256 object shards.
// The root namespace remains a bounded full validation (two reserved names plus
// at most 256 shards) so unknown, uppercase, file, and symlink root entries
// still fail closed.
//
// A very large single shard is necessarily rescanned on each page within that
// shard. File.ReadDir's directory-order cursor is neither a canonical digest
// cursor nor reliable across restart/layout changes; using it would trade away
// deterministic recovery. The one-byte on-disk sharding bounds neither that
// I/O nor latency, but this conservative scan bounds retained memory/results
// and closes protected-prefix starvation without changing the CAS layout.
func (cas *CAS) ListObjectsPage(cursor CASObjectScanCursor, limit int) (CASObjectPage, error) {
	if err := cas.validate(); err != nil {
		return CASObjectPage{}, err
	}
	if err := validateCASObjectScanCursor(cursor); err != nil {
		return CASObjectPage{}, err
	}
	if limit <= 0 || limit > maxCASObjectPageSize {
		return CASObjectPage{}, fmt.Errorf("%w: object page limit", ErrCASInput)
	}
	rootSnapshot, err := cas.validateObjectRootLayout()
	if err != nil {
		return CASObjectPage{}, err
	}

	lookahead := limit + 1
	candidates := make([]CASObjectCandidate, 0, lookahead)
	scanned := make([]casObjectShardSnapshot, 0, 4)
	start := 0
	if !cursor.after.IsZero() {
		start = casDigestShard(cursor.after)
	}
	done := true
	for prefix := start; prefix < casDigestShards; prefix++ {
		if !rootSnapshot.shards[prefix] {
			continue
		}
		lock := &cas.digests[prefix]
		lock.RLock()
		shardSnapshot, scanErr := cas.scanObjectShard(byte(prefix), cursor, lookahead, &candidates)
		lock.RUnlock()
		if scanErr != nil {
			return CASObjectPage{}, scanErr
		}
		scanned = append(scanned, casObjectShardSnapshot{prefix: byte(prefix), info: shardSnapshot})
		if len(candidates) == lookahead {
			done = false
			break
		}
	}
	if err := cas.validateObjectScanSnapshots(rootSnapshot, scanned); err != nil {
		return CASObjectPage{}, err
	}

	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	next := cursor
	if len(candidates) > 0 {
		next.after = candidates[len(candidates)-1].Digest
	}
	return CASObjectPage{Candidates: candidates, NextCursor: next, Done: done}, nil
}

// ListTombstones returns at most limit canonical tombstones in deterministic
// digest/token order. Every entry encountered is validated before any result
// is returned, so an unsafe orphan cannot be silently skipped at startup.
func (cas *CAS) ListTombstones(limit int) ([]CASTombstoneDescriptor, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%w: tombstone listing limit", ErrCASInput)
	}
	before, err := os.Lstat(cas.trash)
	if err != nil {
		return nil, fmt.Errorf("inspect Artifact CAS tombstone directory: %w", err)
	}
	if err := validateCASDirectoryInfo(before); err != nil {
		return nil, err
	}
	handle, err := os.Open(cas.trash)
	if err != nil {
		return nil, fmt.Errorf("open Artifact CAS tombstones: %w", err)
	}
	defer handle.Close()
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(before, opened) {
		return nil, fmt.Errorf("%w: tombstone directory changed while opening", ErrCASCorruption)
	}
	descriptors := make([]CASTombstoneDescriptor, 0, limit)
	for {
		entries, readErr := handle.ReadDir(128)
		for _, entry := range entries {
			digest, token, parseErr := parseCASTombstoneName(entry.Name())
			if parseErr != nil {
				return nil, parseErr
			}
			lock, lockErr := cas.digestLock(digest)
			if lockErr != nil {
				return nil, lockErr
			}
			lock.RLock()
			status, statErr := cas.inspectTombstoneLocked(digest, token)
			lock.RUnlock()
			if statErr != nil {
				return nil, statErr
			}
			descriptors = insertCASTombstoneDescriptor(descriptors,
				CASTombstoneDescriptor{Digest: digest, Token: token,
					State: status.State, Closed: status.Closed}, limit)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("list Artifact CAS tombstones: %w", readErr)
		}
	}
	afterFD, fdErr := handle.Stat()
	afterPath, pathErr := os.Lstat(cas.trash)
	if fdErr != nil || pathErr != nil || !sameCASDirectorySnapshot(before, afterFD) ||
		!sameCASDirectorySnapshot(before, afterPath) {
		return nil, fmt.Errorf("%w: tombstone directory changed while listing", ErrCASCorruption)
	}
	return descriptors, nil
}

// PruneTempsBefore removes at most limit recognizable owner-only CAS temps
// whose modification time is strictly before cutoff. Active Put staging and
// pruning share one mutex, so a live temp cannot be selected underneath Put.
func (cas *CAS) PruneTempsBefore(cutoff time.Time, limit int) ([]string, error) {
	if err := cas.validate(); err != nil {
		return nil, err
	}
	if cutoff.IsZero() || limit <= 0 {
		return nil, fmt.Errorf("%w: temp pruning cutoff or limit", ErrCASInput)
	}
	cutoff = cutoff.Round(0)
	cas.tempMu.Lock()
	defer cas.tempMu.Unlock()
	if err := requireCASDirectory(cas.temp); err != nil {
		return nil, err
	}
	handle, err := os.Open(cas.temp)
	if err != nil {
		return nil, fmt.Errorf("list Artifact CAS temps: %w", err)
	}
	candidates := make([]casTempCandidate, 0, limit)
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
				return nil, fmt.Errorf("inspect Artifact CAS temp: %w", err)
			}
			if err := validateCASRegular(info, maxCASObjectSize); err != nil {
				_ = handle.Close()
				return nil, fmt.Errorf("%w: unsafe recognizable temp %q", err, entry.Name())
			}
			links, err := casLinkCount(info)
			if err != nil || (links != 1 && links != 2) {
				_ = handle.Close()
				return nil, fmt.Errorf("%w: unsafe recognizable temp %q", ErrCASCorruption, entry.Name())
			}
			promotedFinal := ""
			if links == 2 {
				promotedFinal, err = cas.validatePromotedTemp(path, info)
				if err != nil {
					_ = handle.Close()
					return nil, err
				}
			}
			if !info.ModTime().Before(cutoff) {
				continue
			}
			candidates = insertCASTempCandidate(candidates,
				casTempCandidate{name: entry.Name(), snapshot: info,
					expectedLinks: links, promotedFinal: promotedFinal}, limit)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = handle.Close()
			return nil, fmt.Errorf("list Artifact CAS temps: %w", readErr)
		}
	}
	if err := handle.Close(); err != nil {
		return nil, fmt.Errorf("close Artifact CAS temp directory: %w", err)
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
	return removed, nil
}

// InspectTombstone returns the exact portable hard-link tombstone state for
// digest and token without mutating either path.
func (cas *CAS) InspectTombstone(digest model.Digest, token [32]byte) (CASTombstoneStatus, error) {
	lock, err := cas.tombstoneLock(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	lock.RLock()
	defer lock.RUnlock()
	return cas.inspectTombstoneLocked(digest, token)
}

// Tombstone durably closes a final object by linking it under its operation
// token, fsyncing .trash, unlinking the final path, and fsyncing its shard.
// Replaying a tombstone-only state succeeds; an absent object is fail-closed.
func (cas *CAS) Tombstone(digest model.Digest, token [32]byte) (CASTombstoneStatus, error) {
	lock, err := cas.tombstoneLock(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	lock.Lock()
	defer lock.Unlock()
	status, err := cas.inspectTombstoneLocked(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	final, err := cas.objectPath(digest, false)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	tombstone := cas.tombstonePath(digest, token)
	switch status.State {
	case CASTombstoneFinalOnly:
		if err := os.Link(final, tombstone); err != nil {
			return CASTombstoneStatus{}, fmt.Errorf("link Artifact CAS tombstone: %w", err)
		}
		if err := syncDirectory(cas.trash); err != nil {
			return CASTombstoneStatus{}, err
		}
		status, err = cas.inspectTombstoneLocked(digest, token)
		if err != nil || status.State != CASTombstoneFinalAndTrash {
			if err != nil {
				return CASTombstoneStatus{}, err
			}
			return CASTombstoneStatus{}, fmt.Errorf("%w: linked tombstone state did not close", ErrCASCorruption)
		}
		fallthrough
	case CASTombstoneFinalAndTrash:
		// A replay after the link but before the directory sync must establish
		// link durability before it removes the final name.
		if err := syncDirectory(cas.trash); err != nil {
			return CASTombstoneStatus{}, err
		}
		if err := os.Remove(final); err != nil {
			return CASTombstoneStatus{}, fmt.Errorf("unlink Artifact CAS final object: %w", err)
		}
		if err := syncDirectory(filepath.Dir(final)); err != nil {
			return CASTombstoneStatus{}, err
		}
	case CASTombstoneTrashOnly:
		// Replay the final-directory durability boundary before reporting a
		// physically closed object.
		if err := syncDirectory(filepath.Dir(final)); err != nil {
			return CASTombstoneStatus{}, err
		}
		if err := syncDirectory(cas.trash); err != nil {
			return CASTombstoneStatus{}, err
		}
	case CASTombstoneAbsent:
		return status, fmt.Errorf("%w: object and tombstone are both absent", ErrCASCorruption)
	default:
		return CASTombstoneStatus{}, fmt.Errorf("%w: unknown tombstone state", ErrCASCorruption)
	}
	status, err = cas.inspectTombstoneLocked(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	if status.State != CASTombstoneTrashOnly || !status.Closed {
		return CASTombstoneStatus{}, fmt.Errorf("%w: tombstone did not reach its closed state", ErrCASCorruption)
	}
	return status, nil
}

// PurgeTombstone removes a closed tombstone after the Store has durably marked
// its queue record renamed and before that record is completed. Tombstone-only
// and neither are idempotent states; an open final path is always refused.
func (cas *CAS) PurgeTombstone(digest model.Digest, token [32]byte) (CASTombstoneStatus, error) {
	lock, err := cas.tombstoneLock(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	lock.Lock()
	defer lock.Unlock()
	status, err := cas.inspectTombstoneLocked(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	switch status.State {
	case CASTombstoneTrashOnly:
		if err := syncDirectory(filepath.Dir(cas.tombstonePath(digest, token))); err != nil {
			return CASTombstoneStatus{}, err
		}
		final, pathErr := cas.objectPath(digest, false)
		if pathErr != nil {
			return CASTombstoneStatus{}, pathErr
		}
		if err := syncDirectory(filepath.Dir(final)); err != nil {
			return CASTombstoneStatus{}, err
		}
		if err := os.Remove(cas.tombstonePath(digest, token)); err != nil {
			return CASTombstoneStatus{}, fmt.Errorf("purge Artifact CAS tombstone: %w", err)
		}
		if err := syncDirectory(cas.trash); err != nil {
			return CASTombstoneStatus{}, err
		}
	case CASTombstoneAbsent:
		if err := syncDirectory(cas.trash); err != nil {
			return CASTombstoneStatus{}, err
		}
		return status, nil
	case CASTombstoneFinalOnly, CASTombstoneFinalAndTrash:
		return status, fmt.Errorf("%w: refuse to purge an open CAS object", ErrCASCorruption)
	default:
		return CASTombstoneStatus{}, fmt.Errorf("%w: unknown tombstone state", ErrCASCorruption)
	}
	status, err = cas.inspectTombstoneLocked(digest, token)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	if status.State != CASTombstoneAbsent || !status.Closed {
		return CASTombstoneStatus{}, fmt.Errorf("%w: tombstone purge did not close", ErrCASCorruption)
	}
	return status, nil
}

func (cas *CAS) validate() error {
	if cas == nil || cas.root == "" || cas.temp == "" || cas.trash == "" {
		return fmt.Errorf("%w: nil or incomplete CAS", ErrCASInput)
	}
	for _, directory := range []string{cas.root, cas.temp, cas.trash} {
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
	return &cas.digests[casDigestShard(digest)], nil
}

func (cas *CAS) tombstoneLock(digest model.Digest, token [32]byte) (*sync.RWMutex, error) {
	if token == ([32]byte{}) {
		return nil, fmt.Errorf("%w: zero CAS tombstone token", ErrCASInput)
	}
	return cas.digestLock(digest)
}

func casDigestShard(digest model.Digest) int {
	bytes := digest.Bytes()
	return int(bytes[0])
}

type casObjectRootSnapshot struct {
	info   os.FileInfo
	shards [casDigestShards]bool
}

type casObjectShardSnapshot struct {
	prefix byte
	info   os.FileInfo
}

func validateCASObjectScanCursor(cursor CASObjectScanCursor) error {
	if cursor.cutoff.IsZero() || cursor.cutoff != cursor.cutoff.Round(0).UTC() {
		return fmt.Errorf("%w: noncanonical object scan cursor", ErrCASInput)
	}
	return nil
}

func (cas *CAS) validateObjectRootLayout() (casObjectRootSnapshot, error) {
	before, err := os.Lstat(cas.root)
	if err != nil {
		return casObjectRootSnapshot{}, fmt.Errorf("inspect Artifact CAS object root: %w", err)
	}
	if err := validateCASDirectoryInfo(before); err != nil {
		return casObjectRootSnapshot{}, err
	}
	handle, err := os.Open(cas.root)
	if err != nil {
		return casObjectRootSnapshot{}, fmt.Errorf("open Artifact CAS object root: %w", err)
	}
	defer handle.Close()
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(before, opened) {
		return casObjectRootSnapshot{}, fmt.Errorf("%w: object root changed while opening", ErrCASCorruption)
	}
	snapshot := casObjectRootSnapshot{info: before}
	foundTemp := false
	foundTrash := false
	for {
		entries, readErr := handle.ReadDir(128)
		for _, entry := range entries {
			name := entry.Name()
			path := filepath.Join(cas.root, name)
			info, statErr := os.Lstat(path)
			if statErr != nil {
				return casObjectRootSnapshot{}, fmt.Errorf("inspect Artifact CAS root entry: %w", statErr)
			}
			switch name {
			case ".tmp":
				foundTemp = true
			case ".trash":
				foundTrash = true
			default:
				if len(name) != 2 || strings.ToLower(name) != name {
					return casObjectRootSnapshot{}, fmt.Errorf("%w: noncanonical object root entry %q", ErrCASCorruption, name)
				}
				decoded, decodeErr := hex.DecodeString(name)
				if decodeErr != nil || len(decoded) != 1 {
					return casObjectRootSnapshot{}, fmt.Errorf("%w: noncanonical object root entry %q", ErrCASCorruption, name)
				}
				snapshot.shards[int(decoded[0])] = true
			}
			if err := validateCASDirectoryInfo(info); err != nil {
				return casObjectRootSnapshot{}, fmt.Errorf("%w: unsafe object root entry %q", err, name)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return casObjectRootSnapshot{}, fmt.Errorf("list Artifact CAS object root: %w", readErr)
		}
	}
	if !foundTemp || !foundTrash {
		return casObjectRootSnapshot{}, fmt.Errorf("%w: object root is missing a reserved directory", ErrCASCorruption)
	}
	afterFD, fdErr := handle.Stat()
	afterPath, pathErr := os.Lstat(cas.root)
	if fdErr != nil || pathErr != nil || !sameCASDirectorySnapshot(before, afterFD) ||
		!sameCASDirectorySnapshot(before, afterPath) {
		return casObjectRootSnapshot{}, fmt.Errorf("%w: object root changed while validating", ErrCASCorruption)
	}
	return snapshot, nil
}

func (cas *CAS) scanObjectShard(prefix byte, cursor CASObjectScanCursor, limit int,
	candidates *[]CASObjectCandidate,
) (os.FileInfo, error) {
	directory := filepath.Join(cas.root, fmt.Sprintf("%02x", prefix))
	before, err := os.Lstat(directory)
	if err != nil {
		return nil, fmt.Errorf("%w: expected object shard is unavailable: %v", ErrCASCorruption, err)
	}
	if err := validateCASDirectoryInfo(before); err != nil {
		return nil, err
	}
	handle, err := os.Open(directory)
	if err != nil {
		return nil, fmt.Errorf("open Artifact CAS object shard: %w", err)
	}
	defer handle.Close()
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(before, opened) {
		return nil, fmt.Errorf("%w: object shard changed while opening", ErrCASCorruption)
	}
	afterName := ""
	if !cursor.after.IsZero() {
		afterName = strings.TrimPrefix(cursor.after.String(), "sha256:")
	}
	for {
		entries, readErr := handle.ReadDir(128)
		for _, entry := range entries {
			name := entry.Name()
			digest, parseErr := model.ParseDigest("sha256:" + name)
			if parseErr != nil || digest.IsZero() || len(name) != 64 ||
				!strings.HasPrefix(name, fmt.Sprintf("%02x", prefix)) {
				return nil, fmt.Errorf("%w: noncanonical object entry %q", ErrCASCorruption, name)
			}
			path := filepath.Join(directory, name)
			info, statErr := os.Lstat(path)
			if statErr != nil {
				return nil, fmt.Errorf("inspect Artifact CAS object candidate: %w", statErr)
			}
			if err := validateCASRegular(info, maxCASObjectSize); err != nil {
				return nil, err
			}
			if err := requireCASLinkCount(info, 1); err != nil {
				return nil, err
			}
			if (afterName != "" && name <= afterName) ||
				!info.ModTime().Before(cursor.cutoff) {
				continue
			}
			*candidates = insertCASObjectCandidate(*candidates, CASObjectCandidate{
				Digest: digest, Size: uint64(info.Size()), ModifiedAt: info.ModTime().Round(0).UTC(),
			}, limit)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("list Artifact CAS object shard: %w", readErr)
		}
	}
	afterFD, fdErr := handle.Stat()
	afterPath, pathErr := os.Lstat(directory)
	if fdErr != nil || pathErr != nil || !sameCASDirectorySnapshot(before, afterFD) ||
		!sameCASDirectorySnapshot(before, afterPath) {
		return nil, fmt.Errorf("%w: object shard changed while listing", ErrCASCorruption)
	}
	return before, nil
}

func (cas *CAS) validateObjectScanSnapshots(root casObjectRootSnapshot,
	shards []casObjectShardSnapshot,
) error {
	for _, snapshot := range shards {
		path := filepath.Join(cas.root, fmt.Sprintf("%02x", snapshot.prefix))
		current, err := os.Lstat(path)
		if err != nil || !sameCASDirectorySnapshot(snapshot.info, current) {
			return fmt.Errorf("%w: object shard changed after listing", ErrCASCorruption)
		}
	}
	rootAfter, err := os.Lstat(cas.root)
	if err != nil || !sameCASDirectorySnapshot(root.info, rootAfter) {
		return fmt.Errorf("%w: object root changed while listing", ErrCASCorruption)
	}
	return nil
}

func insertCASObjectCandidate(candidates []CASObjectCandidate, candidate CASObjectCandidate,
	limit int,
) []CASObjectCandidate {
	key := candidate.Digest.String()
	index := sort.Search(len(candidates), func(index int) bool {
		return candidates[index].Digest.String() >= key
	})
	if index < len(candidates) && candidates[index].Digest == candidate.Digest {
		candidates[index] = candidate
		return candidates
	}
	if len(candidates) == limit && index == len(candidates) {
		return candidates
	}
	candidates = append(candidates, CASObjectCandidate{})
	copy(candidates[index+1:], candidates[index:])
	candidates[index] = candidate
	if len(candidates) > limit {
		candidates = candidates[:limit]
	}
	return candidates
}

func insertCASTombstoneDescriptor(descriptors []CASTombstoneDescriptor,
	descriptor CASTombstoneDescriptor, limit int,
) []CASTombstoneDescriptor {
	key := descriptor.Digest.String() + hex.EncodeToString(descriptor.Token[:])
	index := sort.Search(len(descriptors), func(index int) bool {
		current := descriptors[index].Digest.String() + hex.EncodeToString(descriptors[index].Token[:])
		return current >= key
	})
	if len(descriptors) == limit && index == len(descriptors) {
		return descriptors
	}
	descriptors = append(descriptors, CASTombstoneDescriptor{})
	copy(descriptors[index+1:], descriptors[index:])
	descriptors[index] = descriptor
	if len(descriptors) > limit {
		descriptors = descriptors[:limit]
	}
	return descriptors
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

func (cas *CAS) tombstonePath(digest model.Digest, token [32]byte) string {
	digestHex := strings.TrimPrefix(digest.String(), "sha256:")
	return filepath.Join(cas.trash, digestHex+"-"+hex.EncodeToString(token[:])+".trash")
}

func (cas *CAS) inspectTombstoneLocked(digest model.Digest,
	token [32]byte,
) (CASTombstoneStatus, error) {
	if err := requireCASDirectory(cas.trash); err != nil {
		return CASTombstoneStatus{}, err
	}
	if err := cas.rejectForeignTombstone(digest, token); err != nil {
		return CASTombstoneStatus{}, err
	}
	final, err := cas.objectPath(digest, false)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	tombstone := cas.tombstonePath(digest, token)
	finalInfo, finalFound, err := lstatCASPath(final)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	tombstoneInfo, tombstoneFound, err := lstatCASPath(tombstone)
	if err != nil {
		return CASTombstoneStatus{}, err
	}
	if finalFound {
		if err := validateCASRegular(finalInfo, maxCASObjectSize); err != nil {
			return CASTombstoneStatus{}, err
		}
	}
	if tombstoneFound {
		if err := validateCASRegular(tombstoneInfo, maxCASObjectSize); err != nil {
			return CASTombstoneStatus{}, err
		}
		if err := requireCASDirectory(filepath.Dir(final)); err != nil {
			return CASTombstoneStatus{}, err
		}
	}
	switch {
	case finalFound && tombstoneFound:
		if !os.SameFile(finalInfo, tombstoneInfo) {
			return CASTombstoneStatus{}, fmt.Errorf("%w: final and tombstone are different files", ErrCASCorruption)
		}
		if err := requireCASLinkCount(finalInfo, 2); err != nil {
			return CASTombstoneStatus{}, err
		}
		if _, err := readCASObject(final, digest, maxCASObjectSize, 2); err != nil {
			return CASTombstoneStatus{}, err
		}
		return CASTombstoneStatus{State: CASTombstoneFinalAndTrash}, nil
	case finalFound:
		if err := requireCASLinkCount(finalInfo, 1); err != nil {
			return CASTombstoneStatus{}, err
		}
		if _, err := readCASObject(final, digest, maxCASObjectSize, 1); err != nil {
			return CASTombstoneStatus{}, err
		}
		return CASTombstoneStatus{State: CASTombstoneFinalOnly}, nil
	case tombstoneFound:
		if err := requireCASLinkCount(tombstoneInfo, 1); err != nil {
			return CASTombstoneStatus{}, err
		}
		if _, err := readCASObject(tombstone, digest, maxCASObjectSize, 1); err != nil {
			return CASTombstoneStatus{}, err
		}
		return CASTombstoneStatus{State: CASTombstoneTrashOnly, Closed: true}, nil
	default:
		return CASTombstoneStatus{State: CASTombstoneAbsent, Closed: true}, nil
	}
}

func (cas *CAS) rejectForeignTombstone(digest model.Digest, token [32]byte) error {
	handle, err := os.Open(cas.trash)
	if err != nil {
		return fmt.Errorf("list Artifact CAS tombstones: %w", err)
	}
	defer handle.Close()
	for {
		entries, readErr := handle.ReadDir(128)
		for _, entry := range entries {
			entryDigest, entryToken, err := parseCASTombstoneName(entry.Name())
			if err != nil {
				return err
			}
			if entryDigest == digest && entryToken != token {
				return fmt.Errorf("%w: digest has a foreign tombstone token", ErrCASCorruption)
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("list Artifact CAS tombstones: %w", readErr)
		}
	}
}

func parseCASTombstoneName(name string) (model.Digest, [32]byte, error) {
	const digestLength = 64
	const tokenLength = 64
	var token [32]byte
	if len(name) != digestLength+1+tokenLength+len(".trash") || name[digestLength] != '-' ||
		!strings.HasSuffix(name, ".trash") {
		return model.Digest{}, token, fmt.Errorf("%w: noncanonical tombstone entry %q", ErrCASCorruption, name)
	}
	digestHex := name[:digestLength]
	tokenHex := name[digestLength+1 : digestLength+1+tokenLength]
	if strings.ToLower(digestHex) != digestHex || strings.ToLower(tokenHex) != tokenHex {
		return model.Digest{}, token, fmt.Errorf("%w: noncanonical tombstone entry %q", ErrCASCorruption, name)
	}
	digest, err := model.ParseDigest("sha256:" + digestHex)
	if err != nil || digest.IsZero() {
		return model.Digest{}, token, fmt.Errorf("%w: noncanonical tombstone digest", ErrCASCorruption)
	}
	rawToken, err := hex.DecodeString(tokenHex)
	if err != nil || len(rawToken) != len(token) {
		return model.Digest{}, token, fmt.Errorf("%w: noncanonical tombstone token", ErrCASCorruption)
	}
	copy(token[:], rawToken)
	if token == ([32]byte{}) {
		return model.Digest{}, token, fmt.Errorf("%w: zero tombstone token", ErrCASCorruption)
	}
	return digest, token, nil
}

func lstatCASPath(path string) (os.FileInfo, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("inspect Artifact CAS path: %w", err)
	}
	return info, true, nil
}

func (cas *CAS) objectPath(digest model.Digest, create bool) (string, error) {
	if cas == nil || digest.IsZero() {
		return "", fmt.Errorf("%w: zero CAS digest", ErrCASInput)
	}
	hexDigest := strings.TrimPrefix(digest.String(), "sha256:")
	if len(hexDigest) != 64 {
		return "", fmt.Errorf("%w: malformed CAS digest", ErrCASInput)
	}
	directory := filepath.Join(cas.root, hexDigest[:2])
	if create {
		_, inspectErr := os.Lstat(directory)
		created := errors.Is(inspectErr, os.ErrNotExist)
		if inspectErr != nil && !created {
			return "", fmt.Errorf("inspect Artifact CAS object shard: %w", inspectErr)
		}
		if err := ensureCASDirectory(directory); err != nil {
			return "", err
		}
		if created {
			if err := syncDirectory(cas.root); err != nil {
				return "", err
			}
		}
	} else if info, err := os.Lstat(directory); err == nil {
		if err := validateCASDirectoryInfo(info); err != nil {
			return "", err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect Artifact CAS object shard: %w", err)
	}
	return filepath.Join(directory, hexDigest), nil
}

func (cas *CAS) newTemp() (string, *os.File, error) {
	for attempt := 0; attempt < 8; attempt++ {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", nil, fmt.Errorf("allocate Artifact CAS temp: %w", err)
		}
		path := filepath.Join(cas.temp, "cas-"+hex.EncodeToString(random)+".tmp")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, casObjectMode)
		if err == nil {
			return path, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, fmt.Errorf("create Artifact CAS temp: %w", err)
		}
	}
	return "", nil, errors.New("allocate Artifact CAS temp: collision budget exhausted")
}

func inspectCASObject(path string, digest model.Digest, expected []byte) (PutResult, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return PutResult{}, false, nil
	}
	if err != nil {
		return PutResult{}, false, fmt.Errorf("inspect Artifact CAS object: %w", err)
	}
	if err := validateCASRegular(info, len(expected)); err != nil {
		return PutResult{}, true, err
	}
	if err := requireCASLinkCount(info, 1); err != nil {
		return PutResult{}, true, err
	}
	content, err := readCASObject(path, digest, len(expected), 1)
	if err != nil || !bytes.Equal(content, expected) || model.Sum(content) != digest {
		return PutResult{}, true, fmt.Errorf("%w: same digest has different bytes", ErrCASCorruption)
	}
	return PutResult{Digest: digest, Size: uint64(len(content))}, true, nil
}

func sameCASObjectSnapshot(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.Mode() == right.Mode() &&
		left.Mode().IsRegular() && left.Mode()&os.ModeSymlink == 0 && left.Mode().Perm() == casObjectMode &&
		left.Size() == right.Size() && left.ModTime().Equal(right.ModTime())
}

func sameCASDirectorySnapshot(left, right os.FileInfo) bool {
	return left != nil && right != nil && os.SameFile(left, right) && left.Mode() == right.Mode() &&
		left.IsDir() && right.IsDir() && left.Mode()&os.ModeSymlink == 0 &&
		left.Mode().Perm() == casDirectoryMode && left.ModTime().Equal(right.ModTime())
}

func validateCASRegular(info os.FileInfo, maximum int) error {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != casObjectMode {
		return fmt.Errorf("%w: CAS path is not an owner-only regular file", ErrCASCorruption)
	}
	if info.Size() < 0 || info.Size() > int64(maximum) {
		return fmt.Errorf("%w: CAS object size is outside its bound", ErrCASCorruption)
	}
	return nil
}

func requireCASLinkCount(info os.FileInfo, expected uint64) error {
	actual, err := casLinkCount(info)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("%w: CAS object has an unexpected hard-link count", ErrCASCorruption)
	}
	return nil
}

func casLinkCount(info os.FileInfo) (uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("%w: CAS object has unavailable hard-link metadata", ErrCASCorruption)
	}
	return uint64(stat.Nlink), nil
}

func validateCASDirectoryInfo(info os.FileInfo) error {
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != casDirectoryMode {
		return fmt.Errorf("%w: CAS path is not an owner-only real directory", ErrCASCorruption)
	}
	return nil
}

func requireCASDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect Artifact CAS directory: %w", err)
	}
	return validateCASDirectoryInfo(info)
}

func ensureCASDirectory(path string) error {
	if info, err := os.Lstat(path); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: CAS path is not a real directory", ErrCASCorruption)
		}
		if err := os.Chmod(path, casDirectoryMode); err != nil {
			return fmt.Errorf("protect Artifact CAS directory: %w", err)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect Artifact CAS directory: %w", err)
	}
	if err := os.MkdirAll(path, casDirectoryMode); err != nil {
		return fmt.Errorf("create Artifact CAS directory: %w", err)
	}
	if err := os.Chmod(path, casDirectoryMode); err != nil {
		return fmt.Errorf("protect Artifact CAS directory: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != casDirectoryMode {
		return fmt.Errorf("%w: CAS directory verification failed", ErrCASCorruption)
	}
	return nil
}

func writeFull(writer io.Writer, content []byte) error {
	for len(content) > 0 {
		written, err := writer.Write(content)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		content = content[written:]
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open Artifact CAS directory for fsync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("fsync Artifact CAS directory: %w", err)
	}
	return nil
}
