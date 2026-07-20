package node

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// pinControlSocket opens the live control socket with O_PATH relative to the
// held Node-state directory. The returned handle pins the socket inode for the
// whole removal wait: while it stays open the kernel cannot recycle the inode
// number, so os.SameFile reliably distinguishes the observed daemon socket
// from any replacement bound at the same path.
func (lease *DaemonLifecycleLease) pinControlSocket() (*os.File, os.FileInfo, bool, error) {
	if err := lease.validateHeld(); err != nil {
		return nil, nil, false, err
	}
	fd, err := unix.Openat(int(lease.lock.state.dir.Fd()), controlSocketName,
		unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	for errors.Is(err, unix.EINTR) {
		fd, err = unix.Openat(int(lease.lock.state.dir.Fd()), controlSocketName,
			unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	}
	if errors.Is(err, unix.ENOENT) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	pin := os.NewFile(uintptr(fd), controlSocketName)
	info, err := pin.Stat()
	if err != nil {
		_ = pin.Close()
		return nil, nil, false, err
	}
	if err := validateLifecycleSocket(info, lease.lock.state.ownerUID); err != nil {
		_ = pin.Close()
		return nil, nil, false, err
	}
	return pin, info, true, nil
}

// drainOnlineDaemon requests graceful shutdown from the pinned online daemon
// and, when the controller acknowledges with a canonical stopping response,
// waits until exactly that pinned control socket is removed. The caller must
// keep the pin backing socketIdentity open until this method returns.
func (lease *DaemonLifecycleLease) drainOnlineDaemon(bounded context.Context,
	client DaemonLifecycleClient, expected localapi.AuthorityResponse,
	socketIdentity os.FileInfo, poll time.Duration,
) error {
	response, shutdownErr := client.ShutdownForMutation(bounded, expected)
	if err := bounded.Err(); err != nil {
		return lifecycleError("request graceful shutdown", err)
	}
	if shutdownErr != nil && shutdownErr.Code != localapi.CodeMnemondUnavailable {
		return lifecycleError("request graceful shutdown", shutdownErr)
	}
	if shutdownErr == nil {
		expectedDigest, digestErr := localapi.AuthorityDigest(expected)
		actualDigest, parseErr := model.ParseDigest(response.AuthorityDigest)
		if digestErr != nil || parseErr != nil || response.SchemaVersion != localapi.SchemaVersion ||
			response.Status != "stopping" || actualDigest != expectedDigest {
			return lifecycleError("request graceful shutdown",
				errors.New("shutdown response is noncanonical"))
		}
		if err := lease.waitForExactSocketRemoval(bounded, socketIdentity, poll); err != nil {
			return lifecycleError("wait for control socket removal", err)
		}
	}
	return nil
}

// waitForExactSocketRemoval requires the caller to keep the pin backing
// expected open; only that pin makes the SameFile comparison replacement-proof
// against immediate inode recycling.
func (lease *DaemonLifecycleLease) waitForExactSocketRemoval(ctx context.Context,
	expected os.FileInfo, poll time.Duration,
) error {
	if expected == nil {
		return errors.New("control socket identity is unavailable")
	}
	for {
		if err := lease.validateHeld(); err != nil {
			return err
		}
		current, err := lease.lock.state.root.Lstat(controlSocketName)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := validateLifecycleSocket(current, lease.lock.state.ownerUID); err != nil {
			return err
		}
		if !os.SameFile(expected, current) {
			return errors.New("control socket was replaced during graceful shutdown")
		}
		if err := waitEnsurePoll(ctx, poll); err != nil {
			return err
		}
	}
}

func (lease *DaemonLifecycleLease) confirmControlSocketAbsent() error {
	if err := lease.validateHeld(); err != nil {
		return err
	}
	_, err := lease.lock.state.root.Lstat(controlSocketName)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("control socket exists while offline authority is required")
}

func validateLifecycleSocket(info os.FileInfo, ownerUID uint32) error {
	if info == nil || info.Mode()&os.ModeType != os.ModeSocket ||
		info.Mode().Perm() != 0o600 {
		return errors.New("control socket is not an owner-only socket")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != ownerUID || stat.Nlink != 1 {
		return errors.New("control socket owner or link count is unsafe")
	}
	return nil
}
