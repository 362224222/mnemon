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
	"strings"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

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
