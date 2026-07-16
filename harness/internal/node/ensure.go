package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"golang.org/x/sys/unix"
)

const (
	ensureLockName        = "ensure.lock"
	ensureLockMode        = os.FileMode(0o600)
	daemonEnsureDeadline  = 3 * time.Second
	daemonEnsurePoll      = 20 * time.Millisecond
	daemonCleanupDeadline = time.Second
)

var (
	ErrDaemonEnsure          = errors.New("ensure mnemond readiness")
	ErrDaemonHealthAuthority = errors.New("authenticated mnemond health authority is invalid")
)

// DaemonHealthProbe is the authenticated local health boundary used by the
// zero-touch ensure path. A localapi.Client satisfies this interface directly.
// Implementations must honor cancellation of the supplied bounded context.
type DaemonHealthProbe interface {
	ProbeHealth(context.Context) (localapi.HealthResponse, *localapi.APIError)
}

// DaemonEnsurePreflight strictly validates the existing Node schema,
// identity, Profile credential and exact managed assets. It must not repair or
// initialize authority and must honor cancellation of the supplied context.
type DaemonEnsurePreflight interface {
	Verify(context.Context) error
}

type DaemonEnsurePreflightFunc func(context.Context) error

func (verify DaemonEnsurePreflightFunc) Verify(ctx context.Context) error {
	if verify == nil {
		return errors.New("daemon ensure preflight is unavailable")
	}
	return verify(ctx)
}

// DaemonReadyGate performs an optional caller-specific post-health check while
// Ensure still owns a newly launched child. Setup uses it for the actual
// projected Hook self-check; ordinary Agent ensure leaves it nil.
type DaemonReadyGate interface {
	VerifyReady(context.Context, localapi.HealthResponse) error
}

type DaemonReadyGateFunc func(context.Context, localapi.HealthResponse) error

func (verify DaemonReadyGateFunc) VerifyReady(ctx context.Context,
	health localapi.HealthResponse,
) error {
	if verify == nil {
		return errors.New("daemon ready gate is unavailable")
	}
	return verify(ctx, health)
}

// DaemonLaunch is the unique child process ownership returned by one launch.
// Release detaches that exact child only after authenticated exact-ready
// health. Terminate stops that exact child and removes its launch-owned process
// state after every post-launch failure. Both methods must return promptly and
// Terminate must honor its bounded cleanup context.
type DaemonLaunch interface {
	Release() error
	Terminate(context.Context) error
}

// DaemonLauncher starts one managed mnemond serve child. The launch
// implementation owns process, log and pid publication and must honor context
// cancellation until it returns a non-nil ownership handle. Readiness remains
// the authority of the authenticated health probe.
type DaemonLauncher interface {
	Launch(context.Context) (DaemonLaunch, error)
}

type DaemonLauncherFunc func(context.Context) (DaemonLaunch, error)

func (launch DaemonLauncherFunc) Launch(ctx context.Context) (DaemonLaunch, error) {
	if launch == nil {
		return nil, errors.New("daemon launcher is unavailable")
	}
	return launch(ctx)
}

type DaemonEnsureOptions struct {
	NodeState     string
	AssetRevision string
	Probe         DaemonHealthProbe
	Preflight     DaemonEnsurePreflight
	Launcher      DaemonLauncher
	ReadyGate     DaemonReadyGate
}

type DaemonEnsureResult struct {
	Health  localapi.HealthResponse
	Started bool
}

type daemonEnsureTiming struct {
	deadline time.Duration
	poll     time.Duration
}

// EnsureDaemon returns only after the authenticated daemon health is ready at
// the exact requested asset revision. A reachable daemon is authoritative: a
// not_ready state, protocol error or different revision fails closed and is
// never shadowed by launching another process. Only an unavailable transport
// may acquire the owner-only launch lock and start one daemon.
func EnsureDaemon(ctx context.Context, options DaemonEnsureOptions) (DaemonEnsureResult, error) {
	return ensureDaemon(ctx, options, daemonEnsureTiming{
		deadline: daemonEnsureDeadline,
		poll:     daemonEnsurePoll,
	})
}

func ensureDaemon(ctx context.Context, options DaemonEnsureOptions,
	timing daemonEnsureTiming,
) (result DaemonEnsureResult, err error) {
	if ctx == nil || options.Probe == nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: health boundary is unavailable", ErrDaemonEnsure)
	}
	if options.NodeState == "" || !filepath.IsAbs(options.NodeState) ||
		filepath.Clean(options.NodeState) != options.NodeState {
		return DaemonEnsureResult{}, fmt.Errorf("%w: Node state path is invalid", ErrDaemonEnsure)
	}
	if _, parseErr := model.ParseDigest(options.AssetRevision); parseErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: asset revision is invalid", ErrDaemonEnsure)
	}
	if timing.deadline <= 0 || timing.poll <= 0 || timing.poll > timing.deadline {
		return DaemonEnsureResult{}, fmt.Errorf("%w: bounded timing is invalid", ErrDaemonEnsure)
	}

	bounded, cancel := context.WithTimeout(ctx, timing.deadline)
	defer cancel()
	if health, unavailable, probeErr := probeDaemonHealth(bounded, options.Probe,
		options.AssetRevision); !unavailable {
		if probeErr != nil {
			return DaemonEnsureResult{}, probeErr
		}
		if gateErr := verifyDaemonReadyGate(bounded, options.ReadyGate, health); gateErr != nil {
			return DaemonEnsureResult{}, gateErr
		}
		return DaemonEnsureResult{Health: health}, nil
	}
	if options.Preflight == nil || options.Launcher == nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: launch boundary is unavailable", ErrDaemonEnsure)
	}

	lock, lockErr := acquireEnsureLock(bounded, options.NodeState, timing.poll)
	if lockErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: acquire launch lock: %w", ErrDaemonEnsure, lockErr)
	}
	defer func() {
		if closeErr := lock.close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("%w: release launch lock: %v", ErrDaemonEnsure, closeErr))
		}
	}()
	var launched DaemonLaunch
	defer func() {
		if launched == nil {
			return
		}
		cleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), daemonCleanupDeadline)
		defer cancelCleanup()
		if terminateErr := launched.Terminate(cleanup); terminateErr != nil {
			err = errors.Join(err,
				fmt.Errorf("%w: terminate launched mnemond: %w", ErrDaemonEnsure, terminateErr))
		}
	}()

	if health, unavailable, probeErr := probeDaemonHealth(bounded, options.Probe,
		options.AssetRevision); !unavailable {
		if probeErr != nil {
			return DaemonEnsureResult{}, probeErr
		}
		if gateErr := verifyDaemonReadyGate(bounded, options.ReadyGate, health); gateErr != nil {
			return DaemonEnsureResult{}, gateErr
		}
		return DaemonEnsureResult{Health: health}, nil
	}
	if verifyErr := options.Preflight.Verify(bounded); verifyErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: strict preflight: %w", ErrDaemonEnsure, verifyErr)
	}
	if err := bounded.Err(); err != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: bounded deadline: %w", ErrDaemonEnsure, err)
	}
	launch, launchErr := options.Launcher.Launch(bounded)
	if launch != nil {
		launched = launch
		result.Started = true
	}
	if launchErr != nil {
		return result, fmt.Errorf("%w: launch mnemond: %w", ErrDaemonEnsure, launchErr)
	}
	if launched == nil {
		return result, fmt.Errorf("%w: launcher returned no child ownership", ErrDaemonEnsure)
	}

	for {
		health, unavailable, probeErr := probeDaemonHealth(bounded, options.Probe,
			options.AssetRevision)
		if !unavailable {
			if probeErr != nil {
				return result, probeErr
			}
			result.Health = health
			if gateErr := verifyDaemonReadyGate(bounded, options.ReadyGate, health); gateErr != nil {
				return result, gateErr
			}
			if releaseErr := launched.Release(); releaseErr != nil {
				return result, fmt.Errorf("%w: release ready mnemond: %w",
					ErrDaemonEnsure, releaseErr)
			}
			launched = nil
			return result, nil
		}
		if waitErr := waitEnsurePoll(bounded, timing.poll); waitErr != nil {
			return result, fmt.Errorf("%w: daemon did not become ready: %w",
				ErrDaemonEnsure, waitErr)
		}
	}
}

func verifyDaemonReadyGate(ctx context.Context, gate DaemonReadyGate,
	health localapi.HealthResponse,
) error {
	if gate == nil {
		return nil
	}
	if err := gate.VerifyReady(ctx, health); err != nil {
		return fmt.Errorf("%w: daemon post-ready gate: %w", ErrDaemonEnsure, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: daemon post-ready gate exceeded its bound: %w", ErrDaemonEnsure, err)
	}
	return nil
}

func probeDaemonHealth(ctx context.Context, probe DaemonHealthProbe,
	expectedRevision string,
) (localapi.HealthResponse, bool, error) {
	if err := ctx.Err(); err != nil {
		return localapi.HealthResponse{}, false,
			fmt.Errorf("%w: bounded deadline: %w", ErrDaemonEnsure, err)
	}
	health, apiErr := probe.ProbeHealth(ctx)
	if apiErr != nil {
		if apiErr.Code == localapi.CodeMnemondUnavailable {
			return localapi.HealthResponse{}, true, nil
		}
		return localapi.HealthResponse{}, false,
			fmt.Errorf("%w: authenticated health failed: %w", ErrDaemonEnsure, apiErr)
	}
	if health.SchemaVersion != localapi.SchemaVersion ||
		(health.Status != "ready" && health.Status != "not_ready") {
		return localapi.HealthResponse{}, false,
			fmt.Errorf("%w: %w: response is noncanonical", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
	if _, err := model.ParseDigest(health.AssetRevision); err != nil {
		return localapi.HealthResponse{}, false,
			fmt.Errorf("%w: %w: revision is invalid", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
	if health.AssetRevision != expectedRevision {
		return localapi.HealthResponse{}, false,
			fmt.Errorf("%w: %w: revision differs", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
	if health.Status != "ready" {
		return localapi.HealthResponse{}, false,
			fmt.Errorf("%w: %w: daemon is not ready", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
	return health, false, nil
}

type ensureLock struct {
	state *identityNodeState
	file  *os.File
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
	if lock == nil {
		return nil
	}
	var err error
	if lock.file != nil {
		err = errors.Join(unix.Flock(int(lock.file.Fd()), unix.LOCK_UN), lock.file.Close())
		lock.file = nil
	}
	if lock.state != nil {
		lock.state.close()
		lock.state = nil
	}
	return err
}

func waitEnsurePoll(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
