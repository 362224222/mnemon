package artifact

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

const casTempScanBytes = 4096

func (cas *CAS) readTempBatchLocked(offset int64) ([]string, int64, error) {
	names, next, empty, err := readCASTempBatchAt(cas.temp, offset)
	if err != nil {
		return nil, 0, err
	}
	if !empty && offset != 0 && next == offset {
		// Some filesystems return a terminal entry again at their end cookie.
		// Process that bounded batch once, then wrap on the following cycle.
		next = 0
	} else if empty && offset != 0 {
		// Reopen before wrapping. Overlay filesystems can retain an exhausted
		// directory stream even after a successful seek back to zero.
		names, next, _, err = readCASTempBatchAt(cas.temp, 0)
		if err != nil {
			return nil, 0, err
		}
	} else if empty {
		next = 0
	}
	return names, next, nil
}

func readCASTempBatchAt(path string, offset int64) ([]string, int64, bool, error) {
	handle, before, err := openCASTempDirectory(path)
	if err != nil {
		return nil, 0, false, err
	}
	defer handle.Close()
	names, next, empty, err := readCASTempDirentBatch(handle, offset)
	if err != nil {
		return nil, 0, false, err
	}
	afterFD, fdErr := handle.Stat()
	afterPath, pathErr := os.Lstat(path)
	if fdErr != nil || pathErr != nil || !sameCASDirectorySnapshot(before, afterFD) ||
		!sameCASDirectorySnapshot(before, afterPath) {
		return nil, 0, false,
			fmt.Errorf("%w: Artifact CAS temp directory changed while scanning",
				ErrCASCorruption)
	}
	return names, next, empty, nil
}

func openCASTempDirectory(path string) (*os.File, os.FileInfo, error) {
	before, err := os.Lstat(path)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect Artifact CAS temp directory: %w", err)
	}
	if err := validateCASDirectoryInfo(before); err != nil {
		return nil, nil, err
	}
	handle, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open Artifact CAS temp directory: %w", err)
	}
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(before, opened) {
		_ = handle.Close()
		return nil, nil, fmt.Errorf("%w: Artifact CAS temp directory changed while opening",
			ErrCASCorruption)
	}
	return handle, before, nil
}

func readCASTempDirentBatch(handle *os.File, offset int64) ([]string, int64, bool, error) {
	if offset < 0 {
		return nil, 0, false,
			fmt.Errorf("%w: negative Artifact CAS temp offset", ErrCASCorruption)
	}
	if _, err := unix.Seek(int(handle.Fd()), offset, io.SeekStart); err != nil {
		return nil, 0, false,
			fmt.Errorf("seek Artifact CAS temp directory: %w", err)
	}
	buffer := make([]byte, casTempScanBytes)
	count, err := unix.ReadDirent(int(handle.Fd()), buffer)
	if err != nil {
		return nil, 0, false, fmt.Errorf("scan Artifact CAS temp directory: %w", err)
	}
	next, err := unix.Seek(int(handle.Fd()), 0, io.SeekCurrent)
	if err != nil {
		return nil, 0, false, fmt.Errorf("read Artifact CAS temp offset: %w", err)
	}
	if count == 0 {
		return nil, next, true, nil
	}
	consumed, parsed, names := unix.ParseDirent(buffer[:count], -1, nil)
	if consumed != count || parsed != len(names) || len(names) > maxCASPruneScan {
		return nil, 0, false, fmt.Errorf(
			"%w: malformed or oversized Artifact CAS temp batch", ErrCASCorruption)
	}
	return names, next, false, nil
}
