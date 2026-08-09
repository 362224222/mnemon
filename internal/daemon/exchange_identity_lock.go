package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const (
	provisionLockFile = ".provision.lock"
	ensureLockFile    = ".ensure.lock"
	provisionLockWait = 5 * time.Second
	provisionLockPoll = 10 * time.Millisecond
)

func acquireProvisionLock(ctx context.Context, directory string) (*os.File, error) {
	return openProvisionLock(ctx, directory, true)
}

func acquireExistingProvisionLock(ctx context.Context, directory string) (*os.File, error) {
	return openProvisionLock(ctx, directory, false)
}

func acquireExistingEnsureLock(ctx context.Context, directory string) (*os.File, error) {
	return openNamedLock(ctx, filepath.Join(directory, ensureLockFile), false)
}

func provisionEnsureLock(directory string) error {
	file, err := openProvisionLockFile(filepath.Join(directory, ensureLockFile), true)
	if err != nil {
		return err
	}
	return file.Close()
}

func openProvisionLock(ctx context.Context, directory string, create bool) (*os.File, error) {
	return openNamedLock(ctx, filepath.Join(directory, provisionLockFile), create)
}

func openNamedLock(ctx context.Context, path string, create bool) (*os.File, error) {
	if ctx == nil {
		return nil, errors.New("daemon provision: lock context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := openProvisionLockFile(path, create)
	if err != nil {
		return nil, err
	}
	if err := waitForProvisionLock(ctx, file); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := verifyProvisionLockStillCurrent(file); err != nil {
		_ = releaseProvisionLock(file)
		return nil, err
	}
	return file, nil
}

func openProvisionLockFile(path string, create bool) (*os.File, error) {
	expected, exists, err := inspectProvisionLock(path)
	if err != nil {
		return nil, err
	}
	if exists {
		return openExistingProvisionLockFile(path, expected)
	}
	if !create {
		return nil, errors.New("daemon provision: lock is not provisioned")
	}
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW|
		unix.O_CREAT|unix.O_EXCL, uint32(ownerFileMode))
	if errors.Is(err, unix.EEXIST) {
		expected, exists, err = inspectProvisionLock(path)
		if err != nil || !exists {
			return nil, errors.Join(errors.New("daemon provision: concurrent lock is unavailable"), err)
		}
		return openExistingProvisionLockFile(path, expected)
	}
	if err != nil {
		return nil, fmt.Errorf("daemon provision: create lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("daemon provision: created lock is unavailable")
	}
	if err := file.Chmod(ownerFileMode); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("daemon provision: protect lock: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("daemon provision: sync lock: %w", err)
	}
	if err := syncProvisionDirectory(filepath.Dir(path)); err != nil {
		_ = file.Close()
		return nil, err
	}
	return validateOpenedProvisionLock(path, file, nil)
}

func openExistingProvisionLockFile(path string, expected os.FileInfo) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		uint32(ownerFileMode))
	if err != nil {
		return nil, fmt.Errorf("daemon provision: open lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("daemon provision: lock is unavailable")
	}
	return validateOpenedProvisionLock(path, file, expected)
}

func validateOpenedProvisionLock(path string, file *os.File, expected os.FileInfo) (*os.File, error) {
	fail := func(cause error) (*os.File, error) {
		_ = file.Close()
		return nil, cause
	}
	info, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("daemon provision: inspect lock: %w", err))
	}
	if err := requireOwnerRegularFile(info); err != nil {
		return fail(err)
	}
	if expected != nil && !os.SameFile(expected, info) {
		return fail(errors.New("daemon provision: lock identity changed while opening"))
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(current, info) {
		return fail(errors.New("daemon provision: lock identity changed after opening"))
	}
	return file, nil
}

func verifyProvisionLockStillCurrent(file *os.File) error {
	if file == nil {
		return errors.New("daemon provision: lock is unavailable")
	}
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("daemon provision: inspect acquired lock: %w", err)
	}
	current, err := os.Lstat(file.Name())
	if err != nil || !os.SameFile(current, info) {
		return errors.New("daemon provision: acquired lock is no longer current")
	}
	return requireOwnerRegularFile(info)
}

func waitForProvisionLock(ctx context.Context, file *os.File) error {
	waitContext, cancel := context.WithTimeout(ctx, provisionLockWait)
	defer cancel()
	poll := time.NewTicker(provisionLockPoll)
	defer poll.Stop()
	for {
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return fmt.Errorf("daemon provision: acquire lock: %w", err)
		}
		select {
		case <-waitContext.Done():
			return fmt.Errorf("daemon provision: acquire lock: %w", waitContext.Err())
		case <-poll.C:
		}
	}
}

func inspectProvisionLock(path string) (os.FileInfo, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("daemon provision: inspect lock: %w", err)
	}
	if err := requireOwnerRegularFile(info); err != nil {
		return nil, false, err
	}
	return info, true, nil
}

func releaseProvisionLock(file *os.File) error {
	if file == nil {
		return nil
	}
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}
