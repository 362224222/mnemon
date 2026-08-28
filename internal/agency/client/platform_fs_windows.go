//go:build windows

package agencyclient

import "os"

// On Windows the R7 durable-journal filesystem primitives are unavailable:
// there is no POSIX owner uid, hard-link count, openat directory flag, flock,
// or peercred. Every helper fails closed so the existing !ok / error branches
// keep the durable journal disabled rather than trusting a file. The agency
// command tree is excluded from Windows builds, so these paths are not reached
// at runtime; they exist only so `go build ./...` succeeds on Windows.

func fsStatOf(_ os.FileInfo) (*fsStat, bool) { return nil, false }

func fsStatAt(_ int, _ string) (*fsStat, error) { return nil, errWindowsUnsupported }

func openDirReadOnly(_ string) (int, error) { return -1, errWindowsUnsupported }

func openAtReadOnly(_ int, _ string) (int, error) { return -1, errWindowsUnsupported }

func openAtReadOnlyName(_ int, _ string) (int, error) { return -1, errWindowsUnsupported }

func openAtWriteStage(_ int, _ string) (int, error) { return -1, errWindowsUnsupported }

func openAtLock(_ int, _ string, _ bool) (int, error) { return -1, errWindowsUnsupported }

func closeFD(_ int) error { return errWindowsUnsupported }

func flockEx(_ int) error { return errWindowsUnsupported }

func flockUn(_ int) error { return errWindowsUnsupported }

func mkdirAt(_ int, _ string) error { return errWindowsUnsupported }

func unlinkAt(_ int, _ string) error { return errWindowsUnsupported }

func renameAt(_ int, _ string, _ int, _ string) error { return errWindowsUnsupported }

func fileTypeMatches(_ uint32, _ bool) bool { return false }
