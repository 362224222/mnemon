package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	// The controller permits up to five seconds for in-flight HTTP requests to
	// drain. Leave a bounded margin for socket removal and companion writer
	// confirmation before declaring quiescence unproven.
	daemonLifecycleDeadline = 7 * time.Second
	daemonLifecyclePoll     = 20 * time.Millisecond
	controlSocketName       = "control.sock"
)

var ErrDaemonLifecycle = errors.New("mnemond lifecycle lease")

// DaemonLifecycleOptions freezes the one workspace-local Node protected by a
// lifecycle lease. Callers that also own setup.lock must acquire that outer
// transaction lock first, then acquire this lease.
type DaemonLifecycleOptions struct {
	Workspace string
	NodeState string
}

// DaemonLifecycleLease promotes ensure.lock from a launch-only mutex to the
// serialization authority for stop, offline mutation, and restart. It does
// not use a pidfile as authority and never kills a process.
type DaemonLifecycleLease struct {
	mu        sync.Mutex
	workspace string
	nodeState string
	peerID    model.PeerID
	lock      *ensureLock
	closed    bool
}

// DaemonLifecycleClient is the authenticated online control boundary required
// by Quiesce. A localapi.Client satisfies it directly.
type DaemonLifecycleClient interface {
	ShutdownForMutation(context.Context, localapi.AuthorityResponse) (
		localapi.ShutdownResponse, *localapi.APIError,
	)
}

// DaemonOfflineConfirmer is the companion-owned offline writer proof. It must
// open the existing Store writer, compare the exact expected authority, and
// recover an unreachable owner socket while still retaining that writer. A
// successful response is the same closed envelope used by the controller.
type DaemonOfflineConfirmer interface {
	ConfirmOffline(context.Context, localapi.AuthorityResponse) (localapi.AuthorityResponse, error)
}

// DaemonOfflineConfirmerFunc adapts one bounded companion confirmation.
type DaemonOfflineConfirmerFunc func(context.Context,
	localapi.AuthorityResponse,
) (localapi.AuthorityResponse, error)

func (confirm DaemonOfflineConfirmerFunc) ConfirmOffline(ctx context.Context,
	expected localapi.AuthorityResponse,
) (localapi.AuthorityResponse, error) {
	if confirm == nil {
		return localapi.AuthorityResponse{}, errors.New("offline authority inspector is unavailable")
	}
	return confirm(ctx, expected)
}

type daemonLifecycleTiming struct {
	deadline time.Duration
	poll     time.Duration
}

// AcquireDaemonLifecycle acquires the existing owner-only ensure.lock without
// creating or repairing Node authority. The returned lease is independent of
// the acquisition context and remains held until Close.
func AcquireDaemonLifecycle(ctx context.Context,
	options DaemonLifecycleOptions,
) (*DaemonLifecycleLease, error) {
	return acquireDaemonLifecycle(ctx, options, daemonLifecycleTiming{
		deadline: daemonLifecycleDeadline,
		poll:     daemonLifecyclePoll,
	})
}

func acquireDaemonLifecycle(ctx context.Context, options DaemonLifecycleOptions,
	timing daemonLifecycleTiming,
) (*DaemonLifecycleLease, error) {
	if ctx == nil {
		return nil, lifecycleError("acquire", errors.New("context is unavailable"))
	}
	if timing.deadline <= 0 || timing.poll <= 0 || timing.poll > timing.deadline {
		return nil, lifecycleError("acquire", errors.New("bounded timing is invalid"))
	}
	workspace, err := validateDaemonWorkspace(options.Workspace)
	if err != nil {
		return nil, lifecycleError("acquire", err)
	}
	wantedNodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if options.NodeState != wantedNodeState {
		return nil, lifecycleError("acquire", errors.New("Node state is outside the managed workspace"))
	}
	bounded, cancel := context.WithTimeout(ctx, timing.deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, lifecycleError("acquire", err)
	}
	lock, err := acquireEnsureLock(bounded, wantedNodeState, timing.poll)
	if err != nil {
		return nil, lifecycleError("acquire", err)
	}
	if err := bounded.Err(); err != nil {
		_ = lock.close()
		return nil, lifecycleError("acquire", err)
	}
	identity, err := loadExistingDaemonIdentity(bounded, wantedNodeState)
	if err != nil {
		_ = lock.close()
		return nil, lifecycleError("acquire identity", err)
	}
	if err := bounded.Err(); err != nil {
		_ = lock.close()
		return nil, lifecycleError("acquire identity", err)
	}
	return &DaemonLifecycleLease{workspace: workspace, nodeState: wantedNodeState,
		peerID: identity.PeerID(), lock: lock}, nil
}

// Quiesce obtains an authenticated authority generation, requests graceful
// shutdown when the daemon is online, and then proves offline writer access at
// the exact expected authority. A successful shutdown response is only the
// start of the fence: this method continues until the socket is gone and the
// Store writer has actually been released.
func (lease *DaemonLifecycleLease) Quiesce(ctx context.Context, client DaemonLifecycleClient,
	confirmer DaemonOfflineConfirmer, expected localapi.AuthorityResponse,
) (localapi.AuthorityResponse, error) {
	return lease.quiesce(ctx, client, confirmer, expected, daemonLifecycleTiming{
		deadline: daemonLifecycleDeadline,
		poll:     daemonLifecyclePoll,
	})
}

func (lease *DaemonLifecycleLease) quiesce(ctx context.Context, client DaemonLifecycleClient,
	confirmer DaemonOfflineConfirmer, expected localapi.AuthorityResponse,
	timing daemonLifecycleTiming,
) (authority localapi.AuthorityResponse, err error) {
	if lease == nil {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce",
			errors.New("lease is unavailable"))
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if err := lease.validateHeld(); err != nil {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce", err)
	}
	if ctx == nil {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce",
			errors.New("context is unavailable"))
	}
	if client == nil || confirmer == nil {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce",
			errors.New("online control or offline inspector is unavailable"))
	}
	if timing.deadline <= 0 || timing.poll <= 0 || timing.poll > timing.deadline {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce",
			errors.New("bounded timing is invalid"))
	}
	expectedSnapshot, err := authorityResponseSnapshot(expected)
	if err != nil {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce expected authority", err)
	}
	if expectedSnapshot.PeerID != lease.peerID {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce expected authority",
			errors.New("authority belongs to another Node"))
	}

	bounded, cancel := context.WithTimeout(ctx, timing.deadline)
	defer cancel()
	socketIdentity, socketPresent, socketErr := lease.inspectControlSocket()
	if socketErr != nil {
		return localapi.AuthorityResponse{}, lifecycleError("quiesce control socket", socketErr)
	}
	if socketPresent {
		response, shutdownErr := client.ShutdownForMutation(bounded, expected)
		if err := bounded.Err(); err != nil {
			return localapi.AuthorityResponse{}, lifecycleError("request graceful shutdown", err)
		}
		if shutdownErr != nil && shutdownErr.Code != localapi.CodeMnemondUnavailable {
			return localapi.AuthorityResponse{}, lifecycleError("request graceful shutdown", shutdownErr)
		}
		if shutdownErr == nil {
			expectedDigest, digestErr := localapi.AuthorityDigest(expected)
			actualDigest, parseErr := model.ParseDigest(response.AuthorityDigest)
			if digestErr != nil || parseErr != nil || response.SchemaVersion != localapi.SchemaVersion ||
				response.Status != "stopping" || actualDigest != expectedDigest {
				return localapi.AuthorityResponse{}, lifecycleError("request graceful shutdown",
					errors.New("shutdown response is noncanonical"))
			}
			if err := lease.waitForExactSocketRemoval(bounded, socketIdentity, timing.poll); err != nil {
				return localapi.AuthorityResponse{}, lifecycleError("wait for control socket removal", err)
			}
		}
	}

	authority, err = lease.waitForOfflineAuthority(bounded, confirmer, expected, timing.poll)
	if err != nil {
		return localapi.AuthorityResponse{}, lifecycleError("inspect offline authority", err)
	}
	if _, err := authorityResponseSnapshot(authority); err != nil || authority != expected {
		if err == nil {
			err = errors.New("authority or generation differs from expected")
		}
		return localapi.AuthorityResponse{}, lifecycleError("confirm offline authority", err)
	}
	if err := lease.confirmControlSocketAbsent(); err != nil {
		return localapi.AuthorityResponse{}, lifecycleError("confirm quiescent control socket", err)
	}
	if err := lease.validateHeld(); err != nil {
		return localapi.AuthorityResponse{}, lifecycleError("confirm lifecycle lease", err)
	}
	if err := bounded.Err(); err != nil {
		return localapi.AuthorityResponse{}, lifecycleError("confirm quiescence bound", err)
	}
	return authority, nil
}

// Ensure runs the normal exact-ready launch core while retaining this lease.
// It therefore cannot self-deadlock by attempting to acquire ensure.lock a
// second time, and outside Ensure callers remain serialized until Close.
func (lease *DaemonLifecycleLease) Ensure(ctx context.Context,
	options DaemonEnsureOptions,
) (result DaemonEnsureResult, err error) {
	defer func() {
		if err == nil {
			result.FailureOutcome = DaemonEnsureFailureNone
		} else if result.FailureOutcome == DaemonEnsureFailureNone {
			result.FailureOutcome = DaemonEnsureFailureUnproven
		}
	}()
	if lease == nil {
		return DaemonEnsureResult{}, lifecycleError("ensure", errors.New("lease is unavailable"))
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if err := lease.validateHeld(); err != nil {
		return DaemonEnsureResult{}, lifecycleError("ensure", err)
	}
	if options.NodeState != lease.nodeState {
		return DaemonEnsureResult{}, lifecycleError("ensure",
			errors.New("Ensure targets another Node"))
	}
	timing := daemonEnsureTiming{deadline: daemonEnsureDeadline, poll: daemonEnsurePoll}
	if err := validateDaemonEnsure(options, timing); err != nil {
		return DaemonEnsureResult{}, err
	}
	if ctx == nil {
		return DaemonEnsureResult{}, fmt.Errorf("%w: context is unavailable", ErrDaemonEnsure)
	}
	bounded, cancel := context.WithTimeout(ctx, timing.deadline)
	defer cancel()
	result, err = ensureDaemonLocked(bounded, options, lease.lock, timing.poll)
	if leaseErr := lease.validateHeld(); leaseErr != nil {
		result.FailureOutcome = DaemonEnsureFailureUnproven
		err = errors.Join(err, lifecycleError("confirm lease after ensure", leaseErr))
	}
	return result, err
}

// Close releases the lifecycle lease. Replacement or permission drift is
// reported while the descriptor remains locked; Close remains idempotent.
func (lease *DaemonLifecycleLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return nil
	}
	lease.closed = true
	lock := lease.lock
	lease.lock = nil
	if lock == nil {
		return lifecycleError("close", errors.New("lease lock is unavailable"))
	}
	if err := lock.close(); err != nil {
		return lifecycleError("close", err)
	}
	return nil
}

func (lease *DaemonLifecycleLease) validateHeld() error {
	if lease.closed || lease.lock == nil {
		return errors.New("lease is closed")
	}
	if lease.workspace == "" || lease.nodeState == "" ||
		lease.peerID.IsZero() ||
		filepath.Join(lease.workspace, ".mnemon", "harness", "node") != lease.nodeState {
		return errors.New("lease Node binding is invalid")
	}
	return validateEnsureLockFile(lease.lock)
}

func (lease *DaemonLifecycleLease) inspectControlSocket() (os.FileInfo, bool, error) {
	if err := lease.validateHeld(); err != nil {
		return nil, false, err
	}
	info, err := lease.lock.state.root.Lstat(controlSocketName)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if err := validateLifecycleSocket(info, lease.lock.state.ownerUID); err != nil {
		return nil, false, err
	}
	return info, true, nil
}

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

func (lease *DaemonLifecycleLease) waitForOfflineAuthority(ctx context.Context,
	confirmer DaemonOfflineConfirmer, expected localapi.AuthorityResponse, poll time.Duration,
) (localapi.AuthorityResponse, error) {
	for {
		if err := lease.validateHeld(); err != nil {
			return localapi.AuthorityResponse{}, err
		}
		authority, err := confirmer.ConfirmOffline(ctx, expected)
		if contextErr := ctx.Err(); contextErr != nil {
			return localapi.AuthorityResponse{}, contextErr
		}
		if err == nil {
			return authority, nil
		}
		if !errors.Is(err, ErrOfflineAuthorityActive) {
			return localapi.AuthorityResponse{}, err
		}
		if err := waitEnsurePoll(ctx, poll); err != nil {
			return localapi.AuthorityResponse{}, err
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

func authorityResponseSnapshot(response localapi.AuthorityResponse) (localapi.AuthoritySnapshot, error) {
	peerID, err := model.ParsePeerID(response.PeerID)
	if err != nil {
		return localapi.AuthoritySnapshot{}, err
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	if err != nil {
		return localapi.AuthoritySnapshot{}, err
	}
	snapshot := localapi.AuthoritySnapshot{Host: model.HostKind(response.Host),
		Runtime: model.RuntimeKind(response.Runtime), Enabled: response.Enabled,
		AssetRevision: response.AssetRevision, UpdatedAt: updatedAt, PeerID: peerID,
		ActiveAssetRevision: response.ActiveAssetRevision}
	canonical, err := localapi.NewAuthorityResponse(snapshot)
	if err != nil || canonical != response {
		if err == nil {
			err = errors.New("authority response is noncanonical")
		}
		return localapi.AuthoritySnapshot{}, err
	}
	return snapshot, nil
}

func lifecycleError(operation string, err error) error {
	if err == nil {
		err = errors.New("unknown lifecycle failure")
	}
	return fmt.Errorf("%w: %s: %w", ErrDaemonLifecycle, operation, err)
}
