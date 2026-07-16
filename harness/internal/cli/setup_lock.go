package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	setupLockName = "setup.lock"
	setupLockMode = os.FileMode(0o600)
	setupNodeMode = os.FileMode(0o700)
	setupLockPoll = 10 * time.Millisecond
)

var errSetupLock = errors.New("mnemon harness setup lock")

// setupLock serializes the complete setup transaction for one initialized
// Node. The open Node directory and lock descriptors pin the authority that
// was validated at acquisition time; Close revalidates them before release.
type setupLock struct {
	mu    sync.Mutex
	state *setupLockNodeState
	file  *os.File
}

type setupLockNodeState struct {
	path     string
	identity os.FileInfo
	dir      *os.File
	ownerUID uint32
}

// acquireSetupLock acquires <node-state>/setup.lock without creating the Node
// directory. The caller must initialize the canonical owner-only Node state
// first, then hold this handle across every remaining setup mutation.
func acquireSetupLock(ctx context.Context, nodeState string) (*setupLock, error) {
	if ctx == nil {
		return nil, setupLockError("acquire", errors.New("context is unavailable"))
	}
	if err := ctx.Err(); err != nil {
		return nil, setupLockError("acquire", err)
	}
	state, err := openSetupLockNodeState(nodeState)
	if err != nil {
		return nil, err
	}
	file, err := openSetupLockFile(state)
	if err != nil {
		_ = state.close()
		return nil, err
	}
	lock := &setupLock{state: state, file: file}
	for {
		if err := ctx.Err(); err != nil {
			_ = lock.close(false)
			return nil, setupLockError("acquire", err)
		}
		err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			if err := ctx.Err(); err != nil {
				_ = lock.close(false)
				return nil, setupLockError("acquire", err)
			}
			if err := validateSetupLock(lock); err != nil {
				_ = lock.close(false)
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				_ = lock.close(false)
				return nil, setupLockError("acquire", err)
			}
			return lock, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = lock.close(false)
			return nil, setupLockError("acquire", err)
		}
		if err := waitSetupLock(ctx); err != nil {
			_ = lock.close(false)
			return nil, setupLockError("acquire", err)
		}
	}
}

func openSetupLockNodeState(path string) (*setupLockNodeState, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, setupLockError("open Node state",
			errors.New("path must be absolute and canonical"))
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, setupLockError("inspect Node state", err)
	}
	ownerUID, err := validateSetupOwnerPath(before, setupNodeMode, true)
	if err != nil {
		return nil, setupLockError("inspect Node state", err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_DIRECTORY, 0)
	if err != nil {
		return nil, setupLockError("open Node state", err)
	}
	dir := os.NewFile(uintptr(fd), path)
	if dir == nil {
		_ = unix.Close(fd)
		return nil, setupLockError("open Node state", errors.New("open returned no directory"))
	}
	state := &setupLockNodeState{path: path, identity: before, dir: dir, ownerUID: ownerUID}
	if err := state.validateLive(); err != nil {
		_ = state.close()
		return nil, err
	}
	return state, nil
}

func openSetupLockFile(state *setupLockNodeState) (*os.File, error) {
	if state == nil || state.dir == nil {
		return nil, setupLockError("open lock", errors.New("Node state is unavailable"))
	}
	if err := state.validateLive(); err != nil {
		return nil, err
	}
	flags := unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_CREAT | unix.O_EXCL
	fd, err := unix.Openat(int(state.dir.Fd()), setupLockName, flags, uint32(setupLockMode))
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		fd, err = unix.Openat(int(state.dir.Fd()), setupLockName,
			unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, setupLockError("open lock", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(state.path, setupLockName))
	if file == nil {
		_ = unix.Close(fd)
		return nil, setupLockError("open lock", errors.New("open returned no file"))
	}
	fail := func(err error) (*os.File, error) {
		_ = file.Close()
		return nil, err
	}
	if created {
		if err := file.Chmod(setupLockMode); err != nil {
			return fail(setupLockError("protect lock", err))
		}
		if err := file.Sync(); err != nil {
			return fail(setupLockError("persist lock", err))
		}
		if err := state.dir.Sync(); err != nil {
			return fail(setupLockError("persist lock directory", err))
		}
	}
	lock := &setupLock{state: state, file: file}
	if err := validateSetupLock(lock); err != nil {
		return fail(err)
	}
	return file, nil
}

func validateSetupLock(lock *setupLock) error {
	if lock == nil || lock.state == nil || lock.state.dir == nil || lock.file == nil {
		return setupLockError("validate", errors.New("lock is unavailable"))
	}
	if err := lock.state.validateLive(); err != nil {
		return err
	}
	opened, err := lock.file.Stat()
	if err != nil {
		return setupLockError("inspect opened lock", err)
	}
	ownerUID, err := validateSetupOwnerPath(opened, setupLockMode, false)
	if err != nil {
		return setupLockError("inspect opened lock", err)
	}
	if ownerUID != lock.state.ownerUID {
		return setupLockError("inspect opened lock",
			errors.New("owner differs from Node state owner"))
	}
	openedStat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || openedStat.Nlink != 1 {
		return setupLockError("inspect opened lock",
			errors.New("lock must have exactly one filesystem link"))
	}
	var current unix.Stat_t
	if err := unix.Fstatat(int(lock.state.dir.Fd()), setupLockName, &current,
		unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return setupLockError("inspect live lock", err)
	}
	if uint64(openedStat.Dev) != uint64(current.Dev) ||
		uint64(openedStat.Ino) != uint64(current.Ino) {
		return setupLockError("inspect live lock", errors.New("lock identity changed"))
	}
	confirmed, err := lock.file.Stat()
	if err != nil || !os.SameFile(opened, confirmed) {
		if err == nil {
			err = errors.New("opened lock identity changed")
		}
		return setupLockError("confirm opened lock", err)
	}
	if _, err := validateSetupOwnerPath(confirmed, setupLockMode, false); err != nil {
		return setupLockError("confirm opened lock", err)
	}
	confirmedStat, ok := confirmed.Sys().(*syscall.Stat_t)
	if !ok || confirmedStat.Nlink != 1 {
		return setupLockError("confirm opened lock",
			errors.New("lock must have exactly one filesystem link"))
	}
	if err := lock.state.validateLive(); err != nil {
		return err
	}
	return nil
}

func (state *setupLockNodeState) validateLive() error {
	if state == nil || state.dir == nil || state.identity == nil {
		return setupLockError("validate Node state", errors.New("Node state is unavailable"))
	}
	opened, err := state.dir.Stat()
	if err != nil || !os.SameFile(state.identity, opened) {
		if err == nil {
			err = errors.New("opened directory identity changed")
		}
		return setupLockError("validate Node state", err)
	}
	ownerUID, err := validateSetupOwnerPath(opened, setupNodeMode, true)
	if err != nil || ownerUID != state.ownerUID {
		if err == nil {
			err = errors.New("opened directory owner changed")
		}
		return setupLockError("validate Node state", err)
	}
	live, err := os.Lstat(state.path)
	if err != nil || !os.SameFile(state.identity, live) {
		if err == nil {
			err = errors.New("live directory identity changed")
		}
		return setupLockError("validate Node state", err)
	}
	ownerUID, err = validateSetupOwnerPath(live, setupNodeMode, true)
	if err != nil || ownerUID != state.ownerUID {
		if err == nil {
			err = errors.New("live directory owner changed")
		}
		return setupLockError("validate Node state", err)
	}
	return nil
}

func validateSetupOwnerPath(info os.FileInfo, mode os.FileMode, directory bool) (uint32, error) {
	if info == nil || info.Mode()&os.ModeSymlink != 0 {
		return 0, errors.New("path must not be a symlink")
	}
	if directory {
		if !info.IsDir() {
			return 0, errors.New("path must be a directory")
		}
	} else if !info.Mode().IsRegular() {
		return 0, errors.New("path must be a regular file")
	}
	if info.Mode().Perm() != mode {
		return 0, fmt.Errorf("path mode is %04o, want %04o", info.Mode().Perm(), mode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, errors.New("path owner is unavailable")
	}
	ownerUID := uint32(stat.Uid)
	if ownerUID != uint32(os.Geteuid()) {
		return 0, errors.New("path is not owned by the current effective user")
	}
	return ownerUID, nil
}

// Close releases the setup transaction lock. Replacement or permission drift
// is reported while the descriptor is still locked, then every descriptor is
// closed so a failed validation cannot leak the authority.
func (lock *setupLock) Close() error {
	return lock.close(true)
}

func (lock *setupLock) close(validate bool) error {
	if lock == nil {
		return nil
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.file == nil && lock.state == nil {
		return nil
	}
	var err error
	if validate && lock.file != nil && lock.state != nil {
		err = validateSetupLock(lock)
	}
	if lock.file != nil {
		err = errors.Join(err, unix.Flock(int(lock.file.Fd()), unix.LOCK_UN), lock.file.Close())
		lock.file = nil
	}
	if lock.state != nil {
		err = errors.Join(err, lock.state.close())
		lock.state = nil
	}
	return err
}

func (state *setupLockNodeState) close() error {
	if state == nil || state.dir == nil {
		return nil
	}
	err := state.dir.Close()
	state.dir = nil
	return err
}

func waitSetupLock(ctx context.Context) error {
	timer := time.NewTimer(setupLockPoll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func setupLockError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", errSetupLock, operation, err)
}
