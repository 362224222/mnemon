package node

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	daemonLaunchPermitEnvironment = "MNEMON_HARNESS_INTERNAL_MNEMOND_ENSURE_FD"
	daemonLaunchPermitChildFD     = 3
	daemonLaunchPermitMaximumFD   = 1 << 20
)

var ErrDaemonLaunchPermit = errors.New("mnemond launch permit is invalid")

// inheritedDaemonLaunchPermit owns only the child-side duplicate of the
// parent's ensure.lock open-file description. Closing this duplicate must not
// issue LOCK_UN: the parent retains its original lock through authenticated
// readiness, and an explicit unlock on a duplicated description could release
// that fence on some supported kernels.
type inheritedDaemonLaunchPermit struct {
	mu     sync.Mutex
	state  *identityNodeState
	file   *os.File
	closed bool
}

func openInheritedDaemonLaunchPermit(nodeState string) (*inheritedDaemonLaunchPermit, error) {
	raw, present := os.LookupEnv(daemonLaunchPermitEnvironment)
	if present {
		if err := os.Unsetenv(daemonLaunchPermitEnvironment); err != nil {
			return nil, launchPermitError("consume inherited descriptor", err)
		}
	}
	if !present || raw == "" {
		return nil, launchPermitError("read inherited descriptor",
			errors.New("launch permit environment is unavailable"))
	}
	fd, err := strconv.Atoi(raw)
	if err != nil || fd < daemonLaunchPermitChildFD || fd > daemonLaunchPermitMaximumFD ||
		strconv.Itoa(fd) != raw {
		return nil, launchPermitError("read inherited descriptor",
			errors.New("launch permit descriptor is invalid"))
	}
	return openDaemonLaunchPermitFD(nodeState, fd)
}

func openDaemonLaunchPermitFD(nodeState string, fd int) (*inheritedDaemonLaunchPermit, error) {
	if fd < daemonLaunchPermitChildFD || fd > daemonLaunchPermitMaximumFD {
		return nil, launchPermitError("open inherited descriptor",
			errors.New("launch permit descriptor is outside its closed range"))
	}
	state, err := openIdentityNodeState(nodeState)
	if err != nil {
		_ = unix.Close(fd)
		return nil, launchPermitError("open Node state", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(nodeState, ensureLockName))
	if file == nil {
		state.close()
		_ = unix.Close(fd)
		return nil, launchPermitError("open inherited descriptor",
			errors.New("descriptor did not produce a file"))
	}
	permit := &inheritedDaemonLaunchPermit{state: state, file: file}
	fail := func(cause error) (*inheritedDaemonLaunchPermit, error) {
		_ = permit.close()
		return nil, cause
	}
	if err := permit.validate(); err != nil {
		return fail(launchPermitError("validate inherited descriptor", err))
	}
	// Reasserting the nonblocking exclusive lock proves this descriptor either
	// shares the parent's inherited lock or can itself acquire the lock. An
	// independently opened, unlocked descriptor cannot bypass a lifecycle
	// holder: that case fails with EWOULDBLOCK before Store is opened.
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fail(launchPermitError("reassert inherited descriptor", err))
	}
	if err := permit.validate(); err != nil {
		return fail(launchPermitError("revalidate inherited descriptor", err))
	}
	return permit, nil
}

func (permit *inheritedDaemonLaunchPermit) validate() error {
	if permit == nil || permit.state == nil || permit.file == nil || permit.closed {
		return errors.New("inherited ensure lock is unavailable")
	}
	if err := permit.state.validateLive(); err != nil {
		return err
	}
	opened, err := permit.file.Stat()
	if err != nil {
		return err
	}
	ownerUID, err := validateIdentityOwnerPath(opened, ensureLockMode, false)
	if err != nil || ownerUID != permit.state.ownerUID {
		if err == nil {
			err = errors.New("ensure lock owner differs from Node state owner")
		}
		return err
	}
	stat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("ensure lock must have exactly one filesystem link")
	}
	current, err := permit.state.root.Lstat(ensureLockName)
	if err != nil || !os.SameFile(opened, current) {
		return errors.New("inherited descriptor is not the live ensure lock")
	}
	return nil
}

func (permit *inheritedDaemonLaunchPermit) close() error {
	if permit == nil {
		return nil
	}
	permit.mu.Lock()
	defer permit.mu.Unlock()
	if permit.closed {
		return nil
	}
	permit.closed = true
	var err error
	if permit.file != nil {
		err = permit.file.Close()
		permit.file = nil
	}
	if permit.state != nil {
		permit.state.close()
		permit.state = nil
	}
	return err
}

func launchPermitError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrDaemonLaunchPermit, operation, err)
}
