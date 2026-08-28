//go:build !windows

package authority

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func fsStatOf(info os.FileInfo) (*fsStat, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, false
	}
	return &fsStat{Uid: uint32(s.Uid), Nlink: uint64(s.Nlink)}, true
}

// openLockGuard opens (or atomically creates) the writer guard file with
// owner-only, no-follow flags.
func openLockGuard(path string, create bool) (int, error) {
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if create {
		flags |= unix.O_CREAT | unix.O_EXCL
	}
	return unix.Open(path, flags, uint32(privateFileMode))
}

// openDBFile atomically creates the SQLite database file with owner-only,
// no-follow flags.
func openDBFile(path string) (int, error) {
	return unix.Open(path,
		unix.O_RDWR|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(privateFileMode))
}

func closeFD(fd int) error { return unix.Close(fd) }

func flockExNB(fd int) error { return unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB) }

func flockUn(fd int) error { return unix.Flock(fd, unix.LOCK_UN) }

// isLockBusy reports whether a flock failure means "already held by another
// writer" rather than an unexpected error.
func isLockBusy(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}
