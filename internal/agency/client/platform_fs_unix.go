//go:build !windows

package agencyclient

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// fsStatOf fills fsStat from a Go os.FileInfo on Unix.
func fsStatOf(info os.FileInfo) (*fsStat, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, false
	}
	return &fsStat{
		Dev:   uint64(s.Dev),
		Ino:   uint64(s.Ino),
		Uid:   uint32(s.Uid),
		Nlink: uint64(s.Nlink),
		Mode:  uint32(s.Mode),
	}, true
}

// fsStatAt is the Unix fstatat(AT_SYMLINK_NOFOLLOW) equivalent.
func fsStatAt(dirfd int, name string) (*fsStat, error) {
	var st unix.Stat_t
	if err := unix.Fstatat(dirfd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, err
	}
	return &fsStat{
		Dev:   uint64(st.Dev),
		Ino:   uint64(st.Ino),
		Uid:   uint32(st.Uid),
		Nlink: uint64(st.Nlink),
		Mode:  uint32(st.Mode),
	}, nil
}

func openDirReadOnly(path string) (int, error) {
	return unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
}

func openAtReadOnly(dirfd int, name string) (int, error) {
	return unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
}

func openAtReadOnlyName(dirfd int, name string) (int, error) {
	return unix.Openat(dirfd, name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func openAtWriteStage(dirfd int, name string) (int, error) {
	return unix.Openat(dirfd, name,
		unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_CREAT|unix.O_EXCL,
		uint32(ownerFileMode))
}

func openAtLock(dirfd int, name string, create bool) (int, error) {
	if create {
		return unix.Openat(dirfd, name,
			unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_CREAT, uint32(ownerFileMode))
	}
	return unix.Openat(dirfd, name, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
}

func closeFD(fd int) error { return unix.Close(fd) }

func flockEx(fd int) error { return unix.Flock(fd, unix.LOCK_EX) }

func flockUn(fd int) error { return unix.Flock(fd, unix.LOCK_UN) }

func mkdirAt(dirfd int, name string) error {
	return unix.Mkdirat(dirfd, name, uint32(ownerDirectoryMode))
}

func unlinkAt(dirfd int, name string) error { return unix.Unlinkat(dirfd, name, 0) }

func renameAt(oldDir int, oldName string, newDir int, newName string) error {
	return unix.Renameat(oldDir, oldName, newDir, newName)
}

// fileTypeMatches reports whether the mode encodes a directory or regular file
// using the Unix S_IFMT macros.
func fileTypeMatches(mode uint32, directory bool) bool {
	t := mode & unix.S_IFMT
	if directory {
		return t == unix.S_IFDIR
	}
	return t == unix.S_IFREG
}
