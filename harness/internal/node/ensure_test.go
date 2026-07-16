package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestEnsureDaemonReturnsExactReadyHealthWithoutLockOrLaunch(t *testing.T) {
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("already-ready")
	health := readyEnsureHealth(revision)
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
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
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("ready-gate")
	health := readyEnsureHealth(revision)
	failed := errors.New("projected Hook self-check failed")
	var gates atomic.Int32
	_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			return health, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(_ context.Context, received localapi.HealthResponse) error {
			gates.Add(1)
			if received != health {
				t.Fatalf("ready gate health = %#v", received)
			}
			return failed
		}),
	})
	if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) || gates.Load() != 1 {
		t.Fatalf("EnsureDaemon() = %v, gates=%d", err, gates.Load())
	}
	if _, err := os.Lstat(filepath.Join(nodeState, ensureLockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("existing ready gate created launch lock: %v", err)
	}
}

func TestEnsureDaemonFailsClosedForEveryReachableNonreadyState(t *testing.T) {
	wanted := ensureTestRevision("wanted")
	other := ensureTestRevision("other")
	tests := map[string]ensureProbeFunc{
		"not ready": func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			return localapi.HealthResponse{AssetRevision: wanted, SchemaVersion: localapi.SchemaVersion,
				Status: "not_ready"}, nil
		},
		"different revision": func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			return readyEnsureHealth(other), nil
		},
		"authentication failure": func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			return localapi.HealthResponse{}, localapi.NewAPIError(
				localapi.CodeAuthenticationFailed, "authentication failed")
		},
		"noncanonical response": func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			return localapi.HealthResponse{AssetRevision: wanted,
				SchemaVersion: localapi.SchemaVersion + 1, Status: "ready"}, nil
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
				Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
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
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("recheck")
	var probes atomic.Int32
	var preflights atomic.Int32
	var launches atomic.Int32
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			if probes.Add(1) == 1 {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error {
			preflights.Add(1)
			return nil
		}),
		Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
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
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("launch")
	var probes atomic.Int32
	var preflights atomic.Int32
	var launches atomic.Int32
	var launched atomic.Bool
	handle := newRecordingDaemonLaunch()
	result, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
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
		Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
			launches.Add(1)
			launched.Store(true)
			return handle, nil
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

func TestEnsureDaemonTerminatesNewChildWhenReadyGateFails(t *testing.T) {
	nodeState := newEnsureNodeState(t)
	revision := ensureTestRevision("launched-ready-gate")
	failed := errors.New("actual Hook failed")
	var launched atomic.Bool
	handle := newRecordingDaemonLaunch()
	_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			if !launched.Load() {
				return unavailableEnsureHealth()
			}
			return readyEnsureHealth(revision), nil
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
			launched.Store(true)
			return handle, nil
		}),
		ReadyGate: DaemonReadyGateFunc(func(context.Context, localapi.HealthResponse) error {
			return failed
		}),
	})
	if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) ||
		handle.releases.Load() != 0 || handle.terminations.Load() != 1 {
		t.Fatalf("EnsureDaemon() = %v, releases=%d terminations=%d", err,
			handle.releases.Load(), handle.terminations.Load())
	}
}

func TestEnsureDaemonConcurrentCallersLaunchExactlyOnce(t *testing.T) {
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
		Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
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
	t.Run("preflight", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("preflight-failure")
		failed := errors.New("preflight rejected authority")
		var launches atomic.Int32
		_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return failed }),
			Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
				launches.Add(1)
				return newRecordingDaemonLaunch(), nil
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) || launches.Load() != 0 {
			t.Fatalf("EnsureDaemon() = %v, launches=%d", err, launches.Load())
		}
	})
	t.Run("launcher", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("launcher-failure")
		failed := errors.New("launcher failed")
		handle := newRecordingDaemonLaunch()
		_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
				return nil, failed
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || !errors.Is(err, failed) {
			t.Fatalf("EnsureDaemon() error = %v", err)
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
			Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
				launches.Add(1)
				return handle, nil
			}),
		}
		_, err := ensureDaemon(context.Background(), options,
			daemonEnsureTiming{deadline: 50 * time.Millisecond, poll: 5 * time.Millisecond})
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
	t.Run("reachable not ready", func(t *testing.T) {
		nodeState := newEnsureNodeState(t)
		revision := ensureTestRevision("post-launch-not-ready")
		handle := newRecordingDaemonLaunch()
		var launched atomic.Bool
		_, err := EnsureDaemon(context.Background(), DaemonEnsureOptions{
			NodeState: nodeState, AssetRevision: revision,
			Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
				if !launched.Load() {
					return unavailableEnsureHealth()
				}
				return localapi.HealthResponse{AssetRevision: revision,
					SchemaVersion: localapi.SchemaVersion, Status: "not_ready"}, nil
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
				launched.Store(true)
				return handle, nil
			}),
		})
		if !errors.Is(err, ErrDaemonEnsure) || handle.releases.Load() != 0 ||
			handle.terminations.Load() != 1 {
			t.Fatalf("EnsureDaemon() = %v, releases=%d terminations=%d", err,
				handle.releases.Load(), handle.terminations.Load())
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
			Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
				if !launched.Load() {
					return unavailableEnsureHealth()
				}
				return readyEnsureHealth(revision), nil
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
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
			Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
				return unavailableEnsureHealth()
			}),
			Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
			Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
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

func TestEnsureDaemonRejectsUnsafeLockAndInvalidInputWithoutLaunch(t *testing.T) {
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
	options.Probe = ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
		probes.Add(1)
		return unavailableEnsureHealth()
	})
	if _, err := EnsureDaemon(context.Background(), options); !errors.Is(err, ErrDaemonEnsure) || probes.Load() != 0 {
		t.Fatalf("invalid input EnsureDaemon() = %v, probes=%d", err, probes.Load())
	}
}

type ensureProbeFunc func(context.Context) (localapi.HealthResponse, *localapi.APIError)

func (probe ensureProbeFunc) ProbeHealth(ctx context.Context) (localapi.HealthResponse, *localapi.APIError) {
	return probe(ctx)
}

type recordingDaemonLaunch struct {
	releases     atomic.Int32
	terminations atomic.Int32
	releaseErr   error
	terminateErr error
}

func newRecordingDaemonLaunch() *recordingDaemonLaunch {
	return &recordingDaemonLaunch{}
}

func (launch *recordingDaemonLaunch) Release() error {
	launch.releases.Add(1)
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

func (probe *barrierEnsureProbe) ProbeHealth(ctx context.Context) (localapi.HealthResponse,
	*localapi.APIError,
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
			return localapi.HealthResponse{}, localapi.NewAPIError(
				localapi.CodeMnemondUnavailable, "mnemond unavailable")
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

func readyEnsureHealth(revision string) localapi.HealthResponse {
	return localapi.HealthResponse{AssetRevision: revision,
		SchemaVersion: localapi.SchemaVersion, Status: "ready"}
}

func unavailableEnsureHealth() (localapi.HealthResponse, *localapi.APIError) {
	return localapi.HealthResponse{}, localapi.NewAPIError(
		localapi.CodeMnemondUnavailable, "mnemond unavailable")
}

func unavailableEnsureOptions(nodeState, revision string,
	launches *atomic.Int32,
) DaemonEnsureOptions {
	return DaemonEnsureOptions{
		NodeState: nodeState, AssetRevision: revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			return unavailableEnsureHealth()
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context) (DaemonLaunch, error) {
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
