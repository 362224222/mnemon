package node

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type controlSocketObservation struct {
	identity os.FileInfo
	pin      *os.File
}

func (observation *controlSocketObservation) Close() error {
	if observation == nil || observation.pin == nil {
		return nil
	}
	err := observation.pin.Close()
	observation.pin = nil
	return err
}

// observeControlSocket captures the strongest control-socket identity offered
// by the host while the lifecycle lease excludes authorized replacement.
// Linux retains an O_PATH inode pin; Darwin, which cannot open a Unix-domain
// socket as a file, uses a validated Lstat snapshot. Both paths still require
// replacement detection, offline writer proof, and final socket absence.
func (lease *DaemonLifecycleLease) observeControlSocket() (
	*controlSocketObservation, bool, error,
) {
	if err := lease.validateHeld(); err != nil {
		return nil, false, err
	}
	return observeLifecycleSocket(lease.lock.state)
}

// drainOnlineDaemon requests graceful shutdown from the observed online daemon
// and, when the controller acknowledges with a canonical stopping response,
// waits until that control socket is removed. A different object at the same
// path is rejected rather than treated as the acknowledged daemon.
func (lease *DaemonLifecycleLease) drainOnlineDaemon(bounded context.Context,
	client DaemonLifecycleClient, expected AuthorityResponse,
	socketIdentity os.FileInfo, poll time.Duration,
) error {
	response, shutdownErr := client.ShutdownForMutation(bounded, expected)
	if err := bounded.Err(); err != nil {
		return lifecycleError("request graceful shutdown", err)
	}
	if shutdownErr != nil && shutdownErr.Code != CodeMnemondUnavailable {
		return lifecycleError("request graceful shutdown", shutdownErr)
	}
	if shutdownErr == nil {
		expectedDigest, digestErr := AuthorityDigest(expected)
		actualDigest, parseErr := model.ParseDigest(response.AuthorityDigest)
		if digestErr != nil || parseErr != nil || response.SchemaVersion != SchemaVersion ||
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

// waitForExactSocketRemoval rejects a replacement at the observed path. Linux
// keeps the expected inode pinned against recycling. On Darwin the lifecycle
// lease prevents an authorized launch while the later offline-writer proof and
// final absence check close the fallback snapshot boundary.
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
