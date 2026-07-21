package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	ensureLockName = "ensure.lock"
	ensureLockMode = os.FileMode(0o600)
)

type ensureLock struct {
	state *identityNodeState
	file  *os.File
	held  bool
}

func acquireEnsureLock(ctx context.Context, nodeState string,
	poll time.Duration,
) (*ensureLock, error) {
	state, err := openIdentityNodeState(nodeState)
	if err != nil {
		return nil, err
	}
	file, err := openEnsureLockFile(state)
	if err != nil {
		state.close()
		return nil, err
	}
	lock := &ensureLock{state: state, file: file}
	for {
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			lock.held = true
			if err := validateEnsureLockFile(lock); err != nil {
				_ = lock.close()
				return nil, err
			}
			return lock, nil
		} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = lock.close()
			return nil, err
		}
		if err := waitEnsurePoll(ctx, poll); err != nil {
			_ = lock.close()
			return nil, err
		}
	}
}

func validateHeldEnsureLock(lock *ensureLock, nodeState string) error {
	if lock == nil || !lock.held || lock.state == nil || lock.state.path != nodeState {
		return errors.New("ensure lock is not held for this Node")
	}
	if err := validateEnsureLockFile(lock); err != nil {
		return err
	}
	if err := unix.Flock(int(lock.file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("reassert exclusive ensure lock: %w", err)
	}
	return validateEnsureLockFile(lock)
}

func openEnsureLockFile(state *identityNodeState) (*os.File, error) {
	if state == nil || state.dir == nil {
		return nil, errors.New("Node state directory is unavailable")
	}
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_CREAT | unix.O_EXCL
	fd, err := unix.Openat(int(state.dir.Fd()), ensureLockName, flags, uint32(ensureLockMode))
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		fd, err = unix.Openat(int(state.dir.Fd()), ensureLockName,
			unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), filepath.Join(state.path, ensureLockName))
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("open ensure lock returned no file")
	}
	if created {
		if err := file.Chmod(ensureLockMode); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := state.dir.Sync(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	if err := validateEnsureLockFile(&ensureLock{state: state, file: file}); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func validateEnsureLockFile(lock *ensureLock) error {
	if lock == nil || lock.state == nil || lock.state.root == nil || lock.file == nil {
		return errors.New("ensure lock is unavailable")
	}
	if err := lock.state.validateLive(); err != nil {
		return err
	}
	opened, err := lock.file.Stat()
	if err != nil {
		return err
	}
	ownerUID, err := validateIdentityOwnerPath(opened, ensureLockMode, false)
	if err != nil || ownerUID != lock.state.ownerUID {
		if err == nil {
			err = errors.New("ensure lock owner differs from Node state owner")
		}
		return err
	}
	stat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("ensure lock must have exactly one filesystem link")
	}
	current, err := lock.state.root.Lstat(ensureLockName)
	if err != nil || !os.SameFile(opened, current) {
		return errors.New("ensure lock identity changed")
	}
	return nil
}

func (lock *ensureLock) close() error {
	return lock.closeValidated(true)
}

// closeAfterRename releases a lock whose complete Node directory was moved
// while the descriptor remained held. Name-based validation is intentionally
// impossible after that successful atomic rename; the live descriptors still
// pin the exact authority being released.
func (lock *ensureLock) closeAfterRename() error {
	return lock.closeValidated(false)
}

func (lock *ensureLock) closeValidated(validate bool) error {
	if lock == nil {
		return nil
	}
	var err error
	if validate && lock.file != nil && lock.state != nil {
		err = validateEnsureLockFile(lock)
	}
	if lock.file != nil {
		if lock.held {
			err = errors.Join(err, unix.Flock(int(lock.file.Fd()), unix.LOCK_UN))
			lock.held = false
		}
		err = errors.Join(err, lock.file.Close())
		lock.file = nil
	}
	if lock.state != nil {
		lock.state.close()
		lock.state = nil
	}
	return err
}
