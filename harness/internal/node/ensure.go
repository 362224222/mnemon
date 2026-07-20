package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

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
	ErrDaemonEnsure             = errors.New("ensure mnemond readiness")
	ErrDaemonHealthAuthority    = errors.New("authenticated mnemond health authority is invalid")
	ErrDaemonControlUnavailable = errors.New("mnemond local control is unavailable")
)

// DaemonHealthProbe is the authenticated local health boundary used by the
// zero-touch ensure path. Transport adapters must validate their wire before
// returning this identity-free Node value and honor cancellation.
type DaemonHealthProbe interface {
	ProbeDaemonHealth(context.Context) (DaemonHealth, error)
}

type DaemonHealth struct {
	AssetRevision string
	Ready         bool
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
	VerifyReady(context.Context, DaemonHealth) error
}

type DaemonReadyGateFunc func(context.Context, DaemonHealth) error

func (verify DaemonReadyGateFunc) VerifyReady(ctx context.Context,
	health DaemonHealth,
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
	Launch(context.Context, DaemonLaunchPermit) (DaemonLaunch, error)
}

// DaemonLaunchPermit is an opaque capability for the exact ensure.lock held by
// the caller. Only EnsureDaemon or a Node lifecycle lease can construct one.
// Launchers must pass it through unchanged; the production process launcher
// inherits its descriptor into mnemond.
type DaemonLaunchPermit struct {
	lock *ensureLock
}

type DaemonLauncherFunc func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error)

func (launch DaemonLauncherFunc) Launch(ctx context.Context,
	permit DaemonLaunchPermit,
) (DaemonLaunch, error) {
	if launch == nil {
		return nil, errors.New("daemon launcher is unavailable")
	}
	return launch(ctx, permit)
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
	Health         DaemonHealth
	Started        bool
	FailureOutcome DaemonEnsureFailureOutcome
}

// DaemonEnsureFailureOutcome is the closed compensation proof returned with
// every failed ensure. Setup may attempt exact authority compensation only for
// the two fenced outcomes; Unproven includes reachable existing/concurrent
// daemons, lock acquisition failures and failed child cleanup.
type DaemonEnsureFailureOutcome string

const (
	DaemonEnsureFailureNone               DaemonEnsureFailureOutcome = ""
	DaemonEnsureFailureUnproven           DaemonEnsureFailureOutcome = "unproven"
	DaemonEnsureFailureCompensationFenced DaemonEnsureFailureOutcome = "compensation_fenced"
	DaemonEnsureFailureOwnedChildCleaned  DaemonEnsureFailureOutcome = "owned_child_cleaned"
)

func (outcome DaemonEnsureFailureOutcome) AllowsCompensation() bool {
	return outcome == DaemonEnsureFailureCompensationFenced ||
		outcome == DaemonEnsureFailureOwnedChildCleaned
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
	defer func() {
		if err == nil {
			result.FailureOutcome = DaemonEnsureFailureNone
			return
		}
		if result.FailureOutcome == DaemonEnsureFailureNone {
			result.FailureOutcome = DaemonEnsureFailureUnproven
		}
	}()
	if validateErr := validateDaemonEnsure(options, timing); validateErr != nil {
		return DaemonEnsureResult{}, validateErr
	}
	if ctx == nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: context is unavailable", ErrDaemonEnsure)
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
	if isNilNodeInterface(options.Preflight) || isNilNodeInterface(options.Launcher) {
		return DaemonEnsureResult{}, fmt.Errorf("%w: launch boundary is unavailable", ErrDaemonEnsure)
	}

	lock, lockErr := acquireEnsureLock(bounded, options.NodeState, timing.poll)
	if lockErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: acquire launch lock: %w", ErrDaemonEnsure, lockErr)
	}
	defer func() {
		if closeErr := lock.close(); closeErr != nil {
			result.FailureOutcome = DaemonEnsureFailureUnproven
			err = errors.Join(err, fmt.Errorf("%w: release launch lock: %v", ErrDaemonEnsure, closeErr))
		}
	}()
	return ensureDaemonLocked(bounded, options, lock, timing.poll)
}

// ensureDaemonLocked is the single launch core shared by ordinary Ensure and
// a caller that already owns the Node lifecycle lease. It never acquires or
// releases ensure.lock itself.
func ensureDaemonLocked(ctx context.Context, options DaemonEnsureOptions,
	lock *ensureLock, poll time.Duration,
) (result DaemonEnsureResult, err error) {
	compensationFenced := false
	defer func() {
		if err == nil {
			result.FailureOutcome = DaemonEnsureFailureNone
			return
		}
		if result.FailureOutcome == DaemonEnsureFailureNone {
			if compensationFenced {
				result.FailureOutcome = DaemonEnsureFailureCompensationFenced
			} else {
				result.FailureOutcome = DaemonEnsureFailureUnproven
			}
		}
	}()
	if lockErr := validateHeldEnsureLock(lock, options.NodeState); lockErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: launch permit: %w", ErrDaemonEnsure, lockErr)
	}
	var launched DaemonLaunch
	defer func() {
		if isNilNodeInterface(launched) {
			return
		}
		cleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), daemonCleanupDeadline)
		defer cancelCleanup()
		if terminateErr := launched.Terminate(cleanup); terminateErr != nil {
			result.FailureOutcome = DaemonEnsureFailureUnproven
			err = errors.Join(err,
				fmt.Errorf("%w: terminate launched mnemond: %w", ErrDaemonEnsure, terminateErr))
			return
		}
		result.FailureOutcome = DaemonEnsureFailureOwnedChildCleaned
	}()

	if health, unavailable, probeErr := probeDaemonHealth(ctx, options.Probe,
		options.AssetRevision); !unavailable {
		if probeErr != nil {
			return DaemonEnsureResult{}, probeErr
		}
		if gateErr := verifyDaemonReadyGate(ctx, options.ReadyGate, health); gateErr != nil {
			return DaemonEnsureResult{}, gateErr
		}
		return DaemonEnsureResult{Health: health}, nil
	}
	// The second authenticated probe is unavailable while this caller owns the
	// managed launch lock. From here until a child is returned, exact Store-side
	// compensation is permitted to make the final writer/busy decision.
	compensationFenced = true
	if isNilNodeInterface(options.Preflight) || isNilNodeInterface(options.Launcher) {
		return DaemonEnsureResult{}, fmt.Errorf("%w: launch boundary is unavailable", ErrDaemonEnsure)
	}
	if verifyErr := options.Preflight.Verify(ctx); verifyErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: strict preflight: %w", ErrDaemonEnsure, verifyErr)
	}
	if err := ctx.Err(); err != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: bounded deadline: %w", ErrDaemonEnsure, err)
	}
	if lockErr := validateHeldEnsureLock(lock, options.NodeState); lockErr != nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: revalidate launch permit: %w",
			ErrDaemonEnsure, lockErr)
	}
	launch, launchErr := options.Launcher.Launch(ctx, DaemonLaunchPermit{lock: lock})
	if !isNilNodeInterface(launch) {
		launched = launch
		result.Started = true
	}
	if launchErr != nil {
		return result, fmt.Errorf("%w: launch mnemond: %w", ErrDaemonEnsure, launchErr)
	}
	if isNilNodeInterface(launched) {
		return result, fmt.Errorf("%w: launcher returned no child ownership", ErrDaemonEnsure)
	}

	for {
		health, observation, probeErr := observeDaemonHealth(ctx, options.Probe,
			options.AssetRevision)
		if probeErr != nil {
			return result, probeErr
		}
		if observation == daemonHealthReady {
			result.Health = health
			if gateErr := verifyDaemonReadyGate(ctx, options.ReadyGate, health); gateErr != nil {
				return result, gateErr
			}
			if releaseErr := launched.Release(); releaseErr != nil {
				return result, fmt.Errorf("%w: release ready mnemond: %w",
					ErrDaemonEnsure, releaseErr)
			}
			launched = nil
			compensationFenced = false
			return result, nil
		}
		// Only the exact child owned by this call may expose a transient,
		// authenticated not_ready state. Existing/concurrent daemons were
		// checked through the strict probe above and still fail closed rather
		// than being shadowed. Unavailable and exact-revision not_ready are
		// both bounded by the one outer ensure deadline.
		if waitErr := waitEnsurePoll(ctx, poll); waitErr != nil {
			return result, fmt.Errorf("%w: daemon did not become ready: %w",
				ErrDaemonEnsure, waitErr)
		}
	}
}

type daemonHealthObservation uint8

const (
	daemonHealthUnavailable daemonHealthObservation = iota + 1
	daemonHealthNotReady
	daemonHealthReady
)

func validateDaemonEnsure(options DaemonEnsureOptions, timing daemonEnsureTiming) error {
	if isNilNodeInterface(options.Probe) {
		return fmt.Errorf("%w: health boundary is unavailable", ErrDaemonEnsure)
	}
	if options.NodeState == "" || !filepath.IsAbs(options.NodeState) ||
		filepath.Clean(options.NodeState) != options.NodeState {
		return fmt.Errorf("%w: Node state path is invalid", ErrDaemonEnsure)
	}
	if _, parseErr := model.ParseDigest(options.AssetRevision); parseErr != nil {
		return fmt.Errorf("%w: asset revision is invalid", ErrDaemonEnsure)
	}
	if timing.deadline <= 0 || timing.poll <= 0 || timing.poll > timing.deadline {
		return fmt.Errorf("%w: bounded timing is invalid", ErrDaemonEnsure)
	}
	return nil
}

func verifyDaemonReadyGate(ctx context.Context, gate DaemonReadyGate,
	health DaemonHealth,
) error {
	if isNilNodeInterface(gate) {
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
) (DaemonHealth, bool, error) {
	health, observation, err := observeDaemonHealth(ctx, probe, expectedRevision)
	if err != nil {
		return DaemonHealth{}, false, err
	}
	switch observation {
	case daemonHealthUnavailable:
		return DaemonHealth{}, true, nil
	case daemonHealthReady:
		return health, false, nil
	case daemonHealthNotReady:
		return DaemonHealth{}, false,
			fmt.Errorf("%w: %w: daemon is not ready", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	default:
		return DaemonHealth{}, false,
			fmt.Errorf("%w: %w: health observation is invalid", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
}

func observeDaemonHealth(ctx context.Context, probe DaemonHealthProbe,
	expectedRevision string,
) (DaemonHealth, daemonHealthObservation, error) {
	if err := ctx.Err(); err != nil {
		return DaemonHealth{}, 0,
			fmt.Errorf("%w: bounded deadline: %w", ErrDaemonEnsure, err)
	}
	health, err := probe.ProbeDaemonHealth(ctx)
	if err != nil {
		if errors.Is(err, ErrDaemonControlUnavailable) {
			return DaemonHealth{}, daemonHealthUnavailable, nil
		}
		return DaemonHealth{}, 0,
			fmt.Errorf("%w: authenticated health failed: %w", ErrDaemonEnsure, err)
	}
	if _, err := model.ParseDigest(health.AssetRevision); err != nil {
		return DaemonHealth{}, 0,
			fmt.Errorf("%w: %w: revision is invalid", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
	if health.AssetRevision != expectedRevision {
		return DaemonHealth{}, 0,
			fmt.Errorf("%w: %w: revision differs", ErrDaemonEnsure,
				ErrDaemonHealthAuthority)
	}
	if !health.Ready {
		return health, daemonHealthNotReady, nil
	}
	return health, daemonHealthReady, nil
}

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
	if lock == nil {
		return nil
	}
	var err error
	if lock.file != nil && lock.state != nil {
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
