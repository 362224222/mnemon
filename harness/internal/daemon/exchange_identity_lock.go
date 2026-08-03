package daemon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

const transportIdentityLock = ".peer-identity.lock"

func acquireTransportIdentityLock(directory string) (*os.File, error) {
	path := filepath.Join(directory, transportIdentityLock)
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(ownerFileMode))
	if err != nil {
		return nil, fmt.Errorf("provision transport identity: open setup lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("provision transport identity: setup lock is unavailable")
	}
	fail := func(cause error) (*os.File, error) {
		_ = file.Close()
		return nil, cause
	}
	if err := file.Chmod(ownerFileMode); err != nil {
		return fail(fmt.Errorf("provision transport identity: protect setup lock: %w", err))
	}
	info, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("provision transport identity: inspect setup lock: %w", err))
	}
	if err := requireOwnerRegularFile(info); err != nil {
		return fail(err)
	}
	if err := unix.Flock(fd, unix.LOCK_EX); err != nil {
		return fail(fmt.Errorf("provision transport identity: acquire setup lock: %w", err))
	}
	return file, nil
}

func releaseTransportIdentityLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}
