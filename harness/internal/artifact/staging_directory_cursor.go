package artifact

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

func normalizeStageDirectoryCursor(cursor StageDirectoryCursor,
	current os.FileInfo,
) (StageDirectoryCursor, error) {
	if !cursor.valid {
		return StageDirectoryCursor{}, nil
	}
	if cursor.offset < 0 || len(cursor.pending) > maxStageDirectoryScan ||
		cursor.directory == nil {
		return StageDirectoryCursor{}, fmt.Errorf(
			"%w: malformed Artifact stage directory cursor", ErrCASInput)
	}
	if !sameCASDirectoryIdentity(cursor.directory, current) {
		return StageDirectoryCursor{}, fmt.Errorf(
			"%w: Artifact staging directory identity changed", ErrCASCorruption)
	}
	if !sameCASDirectorySnapshot(cursor.directory, current) {
		// A completed remove or a newly-created owner invalidates filesystem
		// cookies. Restarting the process-local pass is safe and bounded.
		return StageDirectoryCursor{}, nil
	}
	return cloneStageDirectoryCursor(cursor), nil
}

func cloneStageDirectoryCursor(cursor StageDirectoryCursor) StageDirectoryCursor {
	cursor.pending = append([]string(nil), cursor.pending...)
	return cursor
}

func readStageDirectoryNames(path string, parent os.FileInfo,
	cursor StageDirectoryCursor, maximum int,
) ([]string, StageDirectoryCursor, bool, error) {
	names := append([]string(nil), cursor.pending...)
	cursor.pending = nil
	done := cursor.terminal
	cursor.terminal = false
	if len(names) < maximum && !done {
		previous := cursor.offset
		batch, offset, empty, err := readStageDirectoryBatchAt(
			path, parent, previous)
		if err != nil {
			return nil, StageDirectoryCursor{}, false, err
		}
		if !empty && previous == 0 && offset == previous {
			return nil, StageDirectoryCursor{}, false, fmt.Errorf(
				"%w: Artifact stage directory cursor made no progress",
				ErrCASCorruption)
		}
		cursor.offset = offset
		names = append(names, batch...)
		done = empty || !empty && previous != 0 && offset == previous
	}
	if len(names) > maximum {
		cursor.pending = append([]string(nil), names[maximum:]...)
		names = names[:maximum]
		cursor.terminal = done
		done = false
	}
	if done {
		return names, StageDirectoryCursor{}, true, nil
	}
	return names, cursor, false, nil
}

func readStageDirectoryBatchAt(path string, expected os.FileInfo, offset int64) (
	[]string, int64, bool, error,
) {
	if offset < 0 {
		return nil, 0, false, fmt.Errorf(
			"%w: negative Artifact stage directory cursor", ErrCASInput)
	}
	handle, err := os.Open(path)
	if err != nil {
		return nil, 0, false, fmt.Errorf(
			"open Artifact staging directory: %w", err)
	}
	opened, err := handle.Stat()
	if err != nil || !sameCASDirectorySnapshot(expected, opened) {
		_ = handle.Close()
		return nil, 0, false, fmt.Errorf(
			"%w: Artifact staging directory changed while opening",
			ErrCASCorruption)
	}
	if _, err := unix.Seek(int(handle.Fd()), offset, io.SeekStart); err != nil {
		_ = handle.Close()
		return nil, 0, false, fmt.Errorf(
			"seek Artifact staging directory: %w", err)
	}
	buffer := make([]byte, stageDirectoryScanBytes)
	count, err := unix.ReadDirent(int(handle.Fd()), buffer)
	if err != nil {
		_ = handle.Close()
		return nil, 0, false, fmt.Errorf(
			"scan Artifact staging directory: %w", err)
	}
	next, err := unix.Seek(int(handle.Fd()), 0, io.SeekCurrent)
	if err != nil {
		_ = handle.Close()
		return nil, 0, false, fmt.Errorf(
			"read Artifact stage directory cursor: %w", err)
	}
	if next < 0 {
		_ = handle.Close()
		return nil, 0, false, fmt.Errorf(
			"%w: negative Artifact stage directory cursor", ErrCASCorruption)
	}
	afterFD, fdErr := handle.Stat()
	closeErr := handle.Close()
	afterPath, pathErr := os.Lstat(path)
	if fdErr != nil || closeErr != nil || pathErr != nil ||
		!sameCASDirectorySnapshot(expected, afterFD) ||
		!sameCASDirectorySnapshot(expected, afterPath) {
		return nil, 0, false, fmt.Errorf(
			"%w: Artifact staging directory changed while scanning",
			ErrCASCorruption)
	}
	if count == 0 {
		return nil, next, true, nil
	}
	consumed, parsed, names := unix.ParseDirent(buffer[:count], -1, nil)
	if consumed != count || parsed != len(names) ||
		len(names) > maxStageDirectoryScan {
		return nil, 0, false, fmt.Errorf(
			"%w: malformed or oversized Artifact stage directory batch",
			ErrCASCorruption)
	}
	return names, next, false, nil
}
