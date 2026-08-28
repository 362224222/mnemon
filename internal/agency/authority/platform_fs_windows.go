//go:build windows

package authority

import "os"

// On Windows the authority-store filesystem primitives are unavailable: there
// is no POSIX owner uid, hard-link count, openat no-follow flag, or flock.
// Every helper fails closed so the existing !ok / error branches keep the
// authority store disabled rather than trusting a file.

func fsStatOf(_ os.FileInfo) (*fsStat, bool) { return nil, false }

func openLockGuard(_ string, _ bool) (int, error) { return -1, errWindowsUnsupported }

func openDBFile(_ string) (int, error) { return -1, errWindowsUnsupported }

func closeFD(_ int) error { return errWindowsUnsupported }

func flockExNB(_ int) error { return errWindowsUnsupported }

func flockUn(_ int) error { return errWindowsUnsupported }

func isLockBusy(_ error) bool { return false }
