package node

import (
	"context"
	"errors"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEnsureDaemonReturnsExactReadyHealthWithoutLockOrLaunch(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("already-ready")
	health := readyEnsureHealth(revision)
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			return health, nil
		}),
	})
	if err != nil || result.Health != health || result.Started {
		t.Fatalf("EnsureDaemon() = (%#v, %v)", result, err)
	}
	if _, err := os.Lstat(filepath.Join(nodeState, ensureLockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ready path created ensure lock: %v", err)
	}
}

func TestEnsureDaemonRunsCallerReadyGateWithoutCreatingAnotherAuthority(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("ready-gate")
	health := readyEnsureHealth(revision)
	failed := errors.New("projected Hook self-check failed")
	var gates atomic.Int32
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			return health, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(_ context.Context, received HealthResponse) error {
			gates.Add(1)
			if received != health {
				t.Fatalf("ready gate health = %#v", received)
			}
			return failed
		}),
	})
	if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) || gates.Load() != 1 ||
		result.Started || result.FailureOutcome != DaemonEnsureFailureUnproven {
		t.Fatalf("EnsureDaemon() = (%#v, %v), gates=%d", result, err, gates.Load())
	}
	if _, err := os.Lstat(filepath.Join(nodeState, ensureLockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("existing ready gate created launch lock: %v", err)
	}
}

func TestEnsureDaemonFailsClosedForEveryReachableNonreadyState(t *testing.T) {
	t.Parallel()
	wanted := ensureTestRevision("wanted")
	other := ensureTestRevision("other")
	tests := map[string]ensureProbeFunc{
		"not ready": func(context.Context) (HealthResponse, *APIError) {
			return HealthResponse{AssetRevision: wanted, SchemaVersion: SchemaVersion,
				Status: "not_ready"}, nil
		},
		"different revision": func(context.Context) (HealthResponse, *APIError) {
			return readyEnsureHealth(other), nil
		},
		"authentication failure": func(context.Context) (HealthResponse, *APIError) {
			return HealthResponse{}, NewAPIError(
				CodeAuthenticationFailed, "authentication failed")
		},
		"noncanonical response": func(context.Context) (HealthResponse, *APIError) {
			return HealthResponse{AssetRevision: wanted,
				SchemaVersion: SchemaVersion + 1, Status: "ready"}, nil
		},
	}
	for name, probe := range tests {
		t.Run(name, func(t *testing.T) {
			nodeState := newEnsureNodeState(t)
			var preflights atomic.Int32
			var launches atomic.Int32
			_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
				NodeState: nodeState, AssetRevision: wanted, Probe: probe,
				Preflight: DaemonEnsurePreflightFunc(func(context.Context) error {
					preflights.Add(1)
					return nil
				}),
				Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
					launches.Add(1)
					return newRecordingDaemonLaunch(), nil
				}),
			})
			if !errors.Is(err, ErrDaemonEnsure) {
				t.Fatalf("EnsureDaemon() error = %v", err)
			}
			if name != "authentication failure" && !errors.Is(err, ErrDaemonHealthAuthority) {
				t.Fatalf("reachable authority error = %v, want ErrDaemonHealthAuthority", err)
			}
			if preflights.Load() != 0 || launches.Load() != 0 {
				t.Fatalf("reachable failure ran preflight=%d launch=%d",
					preflights.Load(), launches.Load())
			}
			if _, statErr := os.Lstat(filepath.Join(nodeState, ensureLockName)); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("reachable failure created ensure lock: %v", statErr)
			}
		})
	}
}

func TestEnsureDaemonRechecksUnderLockBeforeStrictPreflight(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("recheck")
	var probes atomic.Int32
	var preflights atomic.Int32
	var launches atomic.Int32
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			if probes.Add(1) == 1 {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error {
			preflights.Add(1)
			return nil
		}),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launches.Add(1)
			return newRecordingDaemonLaunch(), nil
		}),
	})
	if err != nil || result.Health != readyEnsureHealth(revision) || result.Started {
		t.Fatalf("EnsureDaemon() = (%#v, %v)", result, err)
	}
	if probes.Load() != 2 || preflights.Load() != 0 || launches.Load() != 0 {
		t.Fatalf("recheck calls probes=%d preflights=%d launches=%d",
			probes.Load(), preflights.Load(), launches.Load())
	}
	assertEnsureLock(t, nodeState)
}

func TestEnsureDaemonRunsStrictPreflightThenOneLaunchAndWaitsForReady(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("launch")
	var probes atomic.Int32
	var preflights atomic.Int32
	var launches atomic.Int32
	var launched atomic.Bool
	var receivedPermit DaemonLaunchPermit
	handle := newRecordingDaemonLaunch()
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			probes.Add(1)
			if !launched.Load() {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error {
			preflights.Add(1)
			return nil
		}),
		Launcher: DaemonLauncherFunc(func(_ context.Context, permit DaemonLaunchPermit) (DaemonLaunch, error) {
			launches.Add(1)
			receivedPermit = permit
			if err := validateHeldEnsureLock(permit.lock, nodeState); err != nil {
				t.Fatalf("launcher received invalid permit: %v", err)
			}
			launched.Store(true)
			return handle, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(context.Context, HealthResponse) error {
			if err := validateHeldEnsureLock(receivedPermit.lock, nodeState); err != nil {
				t.Fatalf("ready gate observed released launch permit: %v", err)
			}
			assertEnsureLockContended(t, nodeState)
			return nil
		}),
	})
	if err != nil || result.Health != readyEnsureHealth(revision) || !result.Started {
		t.Fatalf("EnsureDaemon() = (%#v, %v)", result, err)
	}
	if probes.Load() != 3 || preflights.Load() != 1 || launches.Load() != 1 {
		t.Fatalf("launch calls probes=%d preflights=%d launches=%d",
			probes.Load(), preflights.Load(), launches.Load())
	}
	if handle.releases.Load() != 1 || handle.terminations.Load() != 0 {
		t.Fatalf("ready child releases=%d terminations=%d",
			handle.releases.Load(), handle.terminations.Load())
	}
	assertEnsureLock(t, nodeState)
}

func TestEnsureDaemonWaitsForOwnedChildExactNotReadyThenReleasesReady(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("owned-not-ready")
	var probes atomic.Int32
	var launched atomic.Bool
	var readyGates atomic.Int32
	handle := newRecordingDaemonLaunch()
	options := DaemonEnsureOptions{NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			call := probes.Add(1)
			if !launched.Load() {
				return unavailableEnsureHealth()
			}
			if call == 3 {
				return HealthResponse{AssetRevision: revision,
					SchemaVersion: SchemaVersion, Status: "not_ready"}, nil
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launched.Store(true)
			return handle, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(_ context.Context, health HealthResponse) error {
			readyGates.Add(1)
			if health != readyEnsureHealth(revision) {
				t.Fatalf("ready gate health = %#v", health)
			}
			return nil
		}),
	}
	result, err := ensureDaemon(context.Background(), options,
		daemonEnsureTiming{deadline: 200 * time.Millisecond, poll: time.Millisecond})
	if err != nil || !result.Started || result.Health != readyEnsureHealth(revision) ||
		probes.Load() != 4 || readyGates.Load() != 1 || handle.releases.Load() != 1 ||
		handle.terminations.Load() != 0 {
		t.Fatalf("ensureDaemon() = (%#v, %v), probes=%d gates=%d releases=%d terminations=%d",
			result, err, probes.Load(), readyGates.Load(), handle.releases.Load(),
			handle.terminations.Load())
	}
}

func TestEnsureDaemonTerminatesNewChildWhenReadyGateFails(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("launched-ready-gate")
	failed := errors.New("actual Hook failed")
	var launched atomic.Bool
	handle := newRecordingDaemonLaunch()
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			if !launched.Load() {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launched.Store(true)
			return handle, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(context.Context, HealthResponse) error {
			return failed
		}),
	})
	if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) || !result.Started ||
		result.Health != readyEnsureHealth(revision) ||
		result.FailureOutcome != DaemonEnsureFailureOwnedChildCleaned ||
		handle.releases.Load() != 0 || handle.terminations.Load() != 1 {
		t.Fatalf("EnsureDaemon() = (%#v, %v), releases=%d terminations=%d", result, err,
			handle.releases.Load(), handle.terminations.Load())
	}
}

func TestEnsureDaemonConcurrentCallersLaunchExactlyOnce(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("concurrent")
	const callers = 20
	probe := &barrierEnsureProbe{revision: revision, callers: callers, release: make(chan struct{})}
	var preflights atomic.Int32
	var launches atomic.Int32
	handle := newRecordingDaemonLaunch()
	options := DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision, Probe: probe,
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error {
			preflights.Add(1)
			return nil
		}),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launches.Add(1)
			probe.ready.Store(true)
			return handle, nil
		}),
	}

	var wait sync.WaitGroup
	wait.Add(callers)
	errorsFound := make(chan error, callers)
	started := make(chan bool, callers)
	for range callers {
		go func() {
			defer wait.Done()
			result, err := EnsureDaemon(context.Background(), options)
			if err != nil {
				errorsFound <- err
				return
			}
			if result.Health != readyEnsureHealth(revision) {
				errorsFound <- errors.New("caller received the wrong health receipt")
				return
			}
			started <- result.Started
		}()
	}
	wait.Wait()
	close(errorsFound)
	close(started)
	for err := range errorsFound {
		t.Errorf("EnsureDaemon() error = %v", err)
	}
	startedCount := 0
	for value := range started {
		if value {
			startedCount++
		}
	}
	if launches.Load() != 1 || preflights.Load() != 1 || startedCount != 1 {
		t.Fatalf("concurrent calls launches=%d preflights=%d started=%d",
			launches.Load(), preflights.Load(), startedCount)
	}
	if handle.releases.Load() != 1 || handle.terminations.Load() != 0 {
		t.Fatalf("concurrent child releases=%d terminations=%d",
			handle.releases.Load(), handle.terminations.Load())
	}
	assertEnsureLock(t, nodeState)
}

func TestEnsureDaemonPreflightLaunchAndDeadlineFailuresStayClosed(t *testing.T) {
	t.Parallel()
	t.Run("preflight", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("preflight-failure")
		failed := errors.New("preflight rejected authority")
		var launches atomic.Int32
		result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return failed }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				launches.Add(1)
				return newRecordingDaemonLaunch(), nil
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) || launches.Load() != 0 ||
			result.FailureOutcome != DaemonEnsureFailureCompensationFenced {
			t.Fatalf("EnsureDaemon() = (%#v, %v), launches=%d", result, err, launches.Load())
		}
	})
	t.Run("launcher", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("launcher-failure")
		failed := errors.New("launcher failed")
		handle := newRecordingDaemonLaunch()
		result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				return nil, failed
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) ||
			result.FailureOutcome != DaemonEnsureFailureCompensationFenced {
			t.Fatalf("EnsureDaemon() = (%#v, %v)", result, err)
		}
		if handle.terminations.Load() != 0 {
			t.Fatalf("prelaunch failure terminated an unowned child %d times", handle.terminations.Load())
		}
	})
	t.Run("deadline", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("deadline")
		var launches atomic.Int32
		handle := newRecordingDaemonLaunch()
		options := DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				launches.Add(1)
				return handle, nil
			}),
		}
		_, err := ensureDaemon(context.Background(), options,
			daemonEnsureTiming{deadline: 2 * time.Second, poll: 10 * time.Millisecond})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, context.DeadlineExceeded) ||
			launches.Load() != 1 || handle.releases.Load() != 0 || handle.terminations.Load() != 1 {
			t.Fatalf("ensureDaemon() = %v, launches=%d releases=%d terminations=%d", err,
				launches.Load(), handle.releases.Load(), handle.terminations.Load())
		}
	})
	t.Run("contended launch lock deadline", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("contended-deadline")
		held, err := acquireEnsureLock(context.Background(), nodeState, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		defer held.close()
		var preflights atomic.Int32
		var launches atomic.Int32
		options := unavailableEnsureOptions(nodeState, revision, &launches)
		options.Preflight = DaemonEnsurePreflightFunc(func(context.Context) error {
			preflights.Add(1)
			return nil
		})
		_, err = ensureDaemon(context.Background(), options,
			daemonEnsureTiming{deadline: 50 * time.Millisecond, poll: 5 * time.Millisecond})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, context.DeadlineExceeded) ||
			preflights.Load() != 0 || launches.Load() != 0 {
			t.Fatalf("contended ensureDaemon() = %v, preflights=%d launches=%d",
				err, preflights.Load(), launches.Load())
		}
	})
}

func TestEnsureDaemonTerminatesOnlyItsOwnedChildAfterPostLaunchFailures(t *testing.T) {
	t.Run("failed termination withholds compensation proof", testEnsureFailedTerminationWithholdsProof)

	t.Run("owned child stays not ready until deadline", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("post-launch-not-ready")
		handle := newRecordingDaemonLaunch()
		var launched atomic.Bool
		options := DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				if !launched.Load() {
					return unavailableEnsureHealth()
				}
				return HealthResponse{AssetRevision: revision,
					SchemaVersion: SchemaVersion, Status: "not_ready"}, nil
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				launched.Store(true)
				return handle, nil
			}),
		}
		_, err := ensureDaemon(context.Background(), options,
			daemonEnsureTiming{deadline: 2 * time.Second, poll: 10 * time.Millisecond})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, context.DeadlineExceeded) ||
			handle.releases.Load() != 0 ||
			handle.terminations.Load() != 1 {
			t.Fatalf("EnsureDaemon() = %v, releases=%d terminations=%d", err,
				handle.releases.Load(), handle.terminations.Load())
		}
	})
	t.Run("owned child revision drift fails immediately", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("post-launch-revision")
		other := ensureTestRevision("post-launch-revision-other")
		handle := newRecordingDaemonLaunch()
		var launched atomic.Bool
		var probes atomic.Int32
		_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				probes.Add(1)
				if !launched.Load() {
					return unavailableEnsureHealth()
				}
				return readyEnsureHealth(other), nil
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				launched.Store(true)
				return handle, nil
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, ErrDaemonHealthAuthority) ||
			probes.Load() != 3 || handle.releases.Load() != 0 || handle.terminations.Load() != 1 {
			t.Fatalf("EnsureDaemon() = %v, probes=%d releases=%d terminations=%d", err,
				probes.Load(), handle.releases.Load(), handle.terminations.Load())
		}
	})
	t.Run("ready release failure", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("release-failure")
		failed := errors.New("release failed")
		handle := newRecordingDaemonLaunch()
		handle.releaseErr = failed
		var launched atomic.Bool
		_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				if !launched.Load() {
					return unavailableEnsureHealth()
				}
				return readyEnsureHealth(revision), nil
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				launched.Store(true)
				return handle, nil
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) ||
			handle.releases.Load() != 1 || handle.terminations.Load() != 1 {
			t.Fatalf("EnsureDaemon() = %v, releases=%d terminations=%d", err,
				handle.releases.Load(), handle.terminations.Load())
		}
	})
	t.Run("launcher returns owned child with error", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("launch-owned-failure")
		failed := errors.New("launch publication failed")
		handle := newRecordingDaemonLaunch()
		_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
				return handle, failed
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) ||
			handle.releases.Load() != 0 || handle.terminations.Load() != 1 {
			t.Fatalf("EnsureDaemon() = %v, releases=%d terminations=%d", err,
				handle.releases.Load(), handle.terminations.Load())
		}
	})
}

func testEnsureFailedTerminationWithholdsProof(t *testing.T) {
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("termination-failure")
	gateFailure := errors.New("projected Hook failed")
	terminateFailure := errors.New("child did not stop")
	handle := newRecordingDaemonLaunch()
	handle.terminateErr = terminateFailure
	var launched atomic.Bool
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			if !launched.Load() {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launched.Store(true)
			return handle, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(context.Context, HealthResponse) error {
			return gateFailure
		}),
	})
	if !errors.Is(err, gateFailure) || !errors.Is(err, terminateFailure) ||
		result.FailureOutcome != DaemonEnsureFailureUnproven || !result.Started ||
		handle.terminations.Load() != 1 {
		t.Fatalf("EnsureDaemon() = (%#v, %v), terminations=%d", result, err,
			handle.terminations.Load())
	}
}

func TestEnsureDaemonWithholdsCompensationWhenLockCloseFailsAfterChildRelease(t *testing.T) {
	t.Parallel()
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("release-then-lock-drift")
	handle := newRecordingDaemonLaunch()
	var launched atomic.Bool
	handle.releaseHook = func() {
		path := filepath.Join(nodeState, ensureLockName)
		if err := os.Rename(path, filepath.Join(nodeState, "displaced.ensure.lock")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, ensureLockMode); err != nil {
			t.Fatal(err)
		}
	}
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			if !launched.Load() {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launched.Store(true)
			return handle, nil
		}),
	})
	if !errors.Is(err, ErrDaemonEnsure) || result.FailureOutcome != DaemonEnsureFailureUnproven ||
		!result.Started || handle.releases.Load() != 1 || handle.terminations.Load() != 0 {
		t.Fatalf("EnsureDaemon() = (%#v, %v), releases=%d terminations=%d", result, err,
			handle.releases.Load(), handle.terminations.Load())
	}
}

func TestEnsureDaemonRejectsUnsafeLockAndInvalidInputWithoutLaunch(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*testing.T, string){
		"symlink lock": func(t *testing.T, nodeState string) {
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, nil, ensureLockMode); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(nodeState, ensureLockName)); err != nil {
				t.Fatal(err)
			}
		},
		"public lock": func(t *testing.T, nodeState string) {
			path := filepath.Join(nodeState, ensureLockName)
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			nodeState := newEnsureNodeState(t)
			prepare(t, nodeState)
			var launches atomic.Int32
			_, err := EnsureDaemon(context.Background(), unavailableEnsureOptions(
				nodeState, ensureTestRevision(name), &launches))
			if !errors.Is(err, ErrDaemonEnsure) || launches.Load() != 0 {
				t.Fatalf("EnsureDaemon() = %v, launches=%d", err, launches.Load())
			}
		})
	}

	var probes atomic.Int32
	options := unavailableEnsureOptions("relative", ensureTestRevision("invalid"), new(atomic.Int32))
	options.Probe = ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
		probes.Add(1)
		return unavailableEnsureHealth()
	})
	if _, err := EnsureDaemon(context.Background(), options); !errors.Is(err, ErrDaemonEnsure) || probes.Load() != 0 {
		t.Fatalf("invalid input EnsureDaemon() = %v, probes=%d", err, probes.Load())
	}
}

type ensureProbeFunc func(context.Context) (HealthResponse, *APIError)

func (probe ensureProbeFunc) ProbeHealth(ctx context.Context) (HealthResponse, *APIError) {
	return probe(ctx)
}

type recordingDaemonLaunch struct {
	releases     atomic.Int32
	terminations atomic.Int32
	releaseErr   error
	releaseHook  func()
	terminateErr error
}

func newRecordingDaemonLaunch() *recordingDaemonLaunch {
	return &recordingDaemonLaunch{}
}

func (launch *recordingDaemonLaunch) Release() error {
	launch.releases.Add(1)
	if launch.releaseHook != nil {
		launch.releaseHook()
	}
	return launch.releaseErr
}

func (launch *recordingDaemonLaunch) Terminate(ctx context.Context) error {
	launch.terminations.Add(1)
	if err := ctx.Err(); err != nil {
		return err
	}
	return launch.terminateErr
}

type barrierEnsureProbe struct {
	revision string
	callers  int32
	entered  atomic.Int32
	release  chan struct{}
	once     sync.Once
	ready    atomic.Bool
}

func (probe *barrierEnsureProbe) ProbeHealth(ctx context.Context) (HealthResponse,
	*APIError,
) {
	if probe.ready.Load() {
		return readyEnsureHealth(probe.revision), nil
	}
	entered := probe.entered.Add(1)
	if entered <= probe.callers {
		if entered == probe.callers {
			probe.once.Do(func() { close(probe.release) })
		}
		select {
		case <-ctx.Done():
			return HealthResponse{}, NewAPIError(
				CodeMnemondUnavailable, "mnemond unavailable")
		case <-probe.release:
			return unavailableEnsureHealth()
		}
	}
	if probe.ready.Load() {
		return readyEnsureHealth(probe.revision), nil
	}
	return unavailableEnsureHealth()
}

func newEnsureNodeState(t *testing.T) string {
	t.Helper()
	nodeState := filepath.Join(t.TempDir(), "node")
	if err := os.Mkdir(nodeState, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	return nodeState
}

func ensureTestRevision(label string) string {
	return model.Sum([]byte("daemon-ensure-" + label)).String()
}

func readyEnsureHealth(revision string) HealthResponse {
	return HealthResponse{AssetRevision: revision,
		SchemaVersion: SchemaVersion, Status: "ready"}
}

func unavailableEnsureHealth() (HealthResponse, *APIError) {
	return HealthResponse{}, NewAPIError(
		CodeMnemondUnavailable, "mnemond unavailable")
}

func unavailableEnsureOptions(nodeState, revision string,
	launches *atomic.Int32,
) DaemonEnsureOptions {
	return DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
			return unavailableEnsureHealth()
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launches.Add(1)
			return newRecordingDaemonLaunch(), nil
		}),
	}
}

func assertEnsureLock(t *testing.T, nodeState string) {
	t.Helper()
	info, err := os.Lstat(filepath.Join(nodeState, ensureLockName))
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != ensureLockMode ||
		info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("ensure.lock mode = %v", info.Mode())
	}
}
