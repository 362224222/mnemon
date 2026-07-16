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

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	casDirectoryMode = 0o700
	casObjectMode    = 0o600
	maxCASObjectSize = MaxManifestBytes
)

var (
	ErrCASInput      = errors.New("invalid Artifact CAS input")
	ErrCASCorruption = errors.New("Artifact CAS corruption")
)

type CAS struct {
	root string
	temp string
}

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
	return &CAS{root: root, temp: temp}, nil
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
	final, err := cas.objectPath(digest, true)
	if err != nil {
		return PutResult{}, err
	}
	if result, found, err := inspectCASObject(final, digest, content); found || err != nil {
		if err != nil {
			return PutResult{}, err
		}
		result.Replayed = true
		return result, nil
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
	if err := syncDirectory(filepath.Dir(final)); err != nil {
		return PutResult{}, err
	}
	if err := os.Remove(tempPath); err != nil {
		return PutResult{}, fmt.Errorf("remove promoted Artifact CAS temp: %w", err)
	}
	removeTemp = false
	if err := syncDirectory(cas.temp); err != nil {
		return PutResult{}, err
	}
	return PutResult{Digest: digest, Size: uint64(len(content))}, nil
}

func (cas *CAS) Read(digest model.Digest, maximum int) ([]byte, error) {
	if cas == nil || digest.IsZero() || maximum < 0 || maximum > maxCASObjectSize {
		return nil, fmt.Errorf("%w: read digest or limit", ErrCASInput)
	}
	path, err := cas.objectPath(digest, false)
	if err != nil {
		return nil, err
	}
	return readCASObject(path, digest, maximum)
}

func readCASObject(path string, digest model.Digest, maximum int) ([]byte, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read Artifact CAS object: %w", err)
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Mode().Perm() != casObjectMode {
		return nil, fmt.Errorf("%w: object is not an owner-only regular file", ErrCASCorruption)
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
	if err != nil || !sameCASObjectSnapshot(before, opened) {
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
		model.Sum(content) != digest {
		return nil, fmt.Errorf("%w: object bytes or identity changed", ErrCASCorruption)
	}
	return content, nil
}

// TempFiles lists recognizable crash leftovers. They are never considered by
// Read and therefore cannot become content authority merely by existing.
func (cas *CAS) TempFiles() ([]string, error) {
	if cas == nil {
		return nil, fmt.Errorf("%w: nil CAS", ErrCASInput)
	}
	entries, err := os.ReadDir(cas.temp)
	if err != nil {
		return nil, fmt.Errorf("list Artifact CAS temps: %w", err)
	}
	result := make([]string, 0)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "cas-") && strings.HasSuffix(entry.Name(), ".tmp") {
			result = append(result, entry.Name())
		}
	}
	sort.Strings(result)
	return result, nil
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
		if err := ensureCASDirectory(directory); err != nil {
			return "", err
		}
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
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return PutResult{}, false, nil
	}
	if err != nil {
		return PutResult{}, false, fmt.Errorf("inspect Artifact CAS object: %w", err)
	}
	content, err := readCASObject(path, digest, len(expected))
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
