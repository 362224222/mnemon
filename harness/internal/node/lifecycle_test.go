package node

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestDaemonLifecycleQuiesceWaitsPastShutdownResponseForWriterRelease(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	serveCtx, cancelServe := context.WithCancel(context.Background())
	defer cancelServe()
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(serveCtx) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, controlSocketName), served)

	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	expected, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	lease := acquireTestDaemonLifecycle(t, fixture)
	writerActive := make(chan struct{})
	releaseWriterActive := make(chan struct{})
	var writerActiveOnce sync.Once
	var releaseWriterActiveOnce sync.Once
	releaseWriterObservation := func() {
		releaseWriterActiveOnce.Do(func() { close(releaseWriterActive) })
	}
	defer releaseWriterObservation()
	confirmer := DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected localapi.AuthorityResponse,
	) (localapi.AuthorityResponse, error) {
		response, err := daemonFixtureOfflineConfirmer(fixture).ConfirmOffline(ctx, expected)
		if errors.Is(err, ErrOfflineAuthorityActive) {
			writerActiveOnce.Do(func() {
				close(writerActive)
				select {
				case <-releaseWriterActive:
				case <-ctx.Done():
				}
			})
		}
		return response, err
	})
	quiesced := make(chan struct {
		authority localapi.AuthorityResponse
		err       error
	}, 1)
	go func() {
		authority, err := lease.quiesce(context.Background(), client, confirmer, expected,
			daemonLifecycleTiming{deadline: time.Second, poll: 5 * time.Millisecond})
		quiesced <- struct {
			authority localapi.AuthorityResponse
			err       error
		}{authority: authority, err: err}
	}()

	if err := <-served; err != nil {
		t.Fatalf("daemon Serve() after shutdown = %v", err)
	}
	select {
	case <-writerActive:
	case <-time.After(time.Second):
		t.Fatal("Quiesce did not observe the retained Store writer")
	}
	select {
	case result := <-quiesced:
		t.Fatalf("Quiesce returned while its writer-active confirmation was paused: (%#v, %v)",
			result.authority, result.err)
	default:
	}
	if st, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db")); st != nil || !errors.Is(err, store.ErrWriterActive) {
		if st != nil {
			_ = st.Close()
		}
		t.Fatalf("daemon writer before Close = (%v, %v)", st, err)
	}
	releaseWriterObservation()
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	result := <-quiesced
	if result.err != nil || result.authority != expected {
		t.Fatalf("Quiesce() = (%#v, %v)", result.authority, result.err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDaemonLifecycleQuiesceBusyMutationLeavesDaemonOnline(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	insertControllerBusyRun(t, fixture)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, controlSocketName), served)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	expected, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	lease := acquireTestDaemonLifecycle(t, fixture)
	var confirmations atomic.Int32
	confirmer := DaemonOfflineConfirmerFunc(func(context.Context,
		localapi.AuthorityResponse,
	) (localapi.AuthorityResponse, error) {
		confirmations.Add(1)
		return localapi.AuthorityResponse{}, errors.New("offline confirmation must not run")
	})
	_, err = lease.Quiesce(context.Background(), client, confirmer, expected)
	var mutationErr *localapi.APIError
	if !errors.Is(err, ErrDaemonLifecycle) || !errors.As(err, &mutationErr) ||
		mutationErr.Code != localapi.CodeOperationPending || confirmations.Load() != 0 {
		t.Fatalf("busy Quiesce() = %v, API=%#v confirmations=%d", err, mutationErr,
			confirmations.Load())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if health, apiErr := client.ProbeHealth(context.Background()); apiErr != nil ||
		health.Status != "ready" {
		t.Fatalf("busy Quiesce health = (%#v, %#v)", health, apiErr)
	}
	if _, apiErr := client.Shutdown(context.Background(), expected); apiErr != nil {
		t.Fatal(apiErr)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}

func TestDaemonLifecycleLeaseBlocksConcurrentEnsureLaunch(t *testing.T) {
	fixture := newDaemonFixture(t, false)
	lease := acquireTestDaemonLifecycle(t, fixture)
	var probes atomic.Int32
	var launches atomic.Int32
	var ready atomic.Bool
	options := DaemonEnsureOptions{
		NodeState: fixture.nodeState, AssetRevision: fixture.revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			probes.Add(1)
			if ready.Load() {
				return readyEnsureHealth(fixture.revision), nil
			}
			return unavailableEnsureHealth()
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launches.Add(1)
			ready.Store(true)
			return newRecordingDaemonLaunch(), nil
		}),
	}
	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 60*time.Millisecond)
	blocked, blockedErr := ensureDaemon(blockedCtx, options,
		daemonEnsureTiming{deadline: time.Second, poll: 5 * time.Millisecond})
	cancelBlocked()
	if !errors.Is(blockedErr, context.DeadlineExceeded) || blocked.Started ||
		launches.Load() != 0 || probes.Load() == 0 {
		t.Fatalf("Ensure while lifecycle lease held = (%#v, %v), probes=%d launches=%d",
			blocked, blockedErr, probes.Load(), launches.Load())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	ensured, err := ensureDaemon(context.Background(), options,
		daemonEnsureTiming{deadline: time.Second, poll: 5 * time.Millisecond})
	if err != nil || !ensured.Started || launches.Load() != 1 {
		t.Fatalf("Ensure after lifecycle release = (%#v, %v), launches=%d",
			ensured, err, launches.Load())
	}
}

func TestDaemonLifecycleQuiesceRejectsGenerationAndSocketDrift(t *testing.T) {
	t.Run("offline generation drift", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		current := daemonFixtureAuthorityResponse(t, fixture)
		drifted := current
		generation, err := time.Parse(time.RFC3339Nano, current.UpdatedAt)
		if err != nil {
			t.Fatal(err)
		}
		drifted.UpdatedAt = generation.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)
		withoutGeneration := drifted
		withoutGeneration.UpdatedAt = current.UpdatedAt
		if withoutGeneration != current || drifted.UpdatedAt == current.UpdatedAt {
			t.Fatal("generation-drift fixture changed non-generation authority")
		}
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		var shutdowns atomic.Int32
		client := lifecycleClientStub{shutdown: func(context.Context,
			localapi.AuthorityResponse,
		) (localapi.ShutdownResponse, *localapi.APIError) {
			shutdowns.Add(1)
			return localapi.ShutdownResponse{}, localapi.NewAPIError(
				localapi.CodeInternal, "unexpected shutdown")
		}}
		_, err = lease.Quiesce(context.Background(), client,
			daemonFixtureOfflineConfirmer(fixture), drifted)
		if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, ErrOfflineAuthority) ||
			shutdowns.Load() != 0 {
			t.Fatalf("generation-drift Quiesce() = %v", err)
		}
		snapshot, inspectErr := InspectAuthority(context.Background(), fixture.workspace)
		observed, responseErr := localapi.NewAuthorityResponse(snapshot)
		if inspectErr != nil || responseErr != nil || observed != current {
			t.Fatalf("generation drift changed durable authority = (%#v, %v, %v)",
				observed, inspectErr, responseErr)
		}
	})

	t.Run("unsafe offline socket", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		_, err := lease.Quiesce(context.Background(), daemonFixtureLifecycleClient(t, fixture),
			daemonFixtureOfflineConfirmer(fixture), daemonFixtureAuthorityResponse(t, fixture))
		if !errors.Is(err, ErrDaemonLifecycle) {
			t.Fatalf("unsafe-socket Quiesce() = %v", err)
		}
		if info, statErr := os.Lstat(path); statErr != nil || !info.Mode().IsRegular() {
			t.Fatalf("unsafe socket path was changed: (%v, %v)", info, statErr)
		}
	})

	t.Run("online socket replacement", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		expected := daemonFixtureAuthorityResponse(t, fixture)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		original, err := localapi.ListenOwnerUnix(path)
		if err != nil {
			t.Fatal(err)
		}
		defer original.Close()
		var replacement net.Listener
		var confirmations atomic.Int32
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		client := lifecycleClientStub{
			shutdown: func(_ context.Context,
				observed localapi.AuthorityResponse,
			) (localapi.ShutdownResponse, *localapi.APIError) {
				if observed != expected {
					t.Errorf("shutdown expected authority = %#v", observed)
				}
				if err := original.Close(); err != nil {
					t.Errorf("close original socket: %v", err)
				}
				replacement, err = localapi.ListenOwnerUnix(path)
				if err != nil {
					t.Errorf("create replacement socket: %v", err)
				}
				return daemonFixtureShutdownResponse(t, expected), nil
			},
		}
		confirmer := DaemonOfflineConfirmerFunc(func(context.Context,
			localapi.AuthorityResponse,
		) (localapi.AuthorityResponse, error) {
			confirmations.Add(1)
			return expected, nil
		})
		_, err = lease.Quiesce(context.Background(), client, confirmer, expected)
		if replacement != nil {
			_ = replacement.Close()
		}
		if !errors.Is(err, ErrDaemonLifecycle) || errors.Is(err, context.DeadlineExceeded) ||
			confirmations.Load() != 0 {
			t.Fatalf("replacement-socket Quiesce() = %v, confirmations=%d",
				err, confirmations.Load())
		}
	})
}

func TestDaemonLifecycleQuiesceRecoversStaleSocketAndBoundsWriterWait(t *testing.T) {
	t.Run("stale owner socket", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, controlSocketName)
		listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			t.Fatal(err)
		}
		listener.SetUnlinkOnClose(false)
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		expected := daemonFixtureAuthorityResponse(t, fixture)
		authority, err := lease.Quiesce(context.Background(), daemonFixtureLifecycleClient(t, fixture),
			daemonFixtureOfflineConfirmer(fixture), expected)
		if err != nil || authority != expected {
			t.Fatalf("stale-socket Quiesce() = (%#v, %v)", authority, err)
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale socket remains: %v", err)
		}
	})

	t.Run("writer timeout", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		expected := daemonFixtureAuthorityResponse(t, fixture)
		daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install})
		if err != nil {
			t.Fatal(err)
		}
		defer daemon.Close()
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		_, err = lease.quiesce(context.Background(), daemonFixtureLifecycleClient(t, fixture),
			daemonFixtureOfflineConfirmer(fixture), expected,
			daemonLifecycleTiming{deadline: 60 * time.Millisecond, poll: 5 * time.Millisecond})
		if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("writer-timeout Quiesce() = %v", err)
		}
	})

	t.Run("cancelled during writer wait", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		expected := daemonFixtureAuthorityResponse(t, fixture)
		daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install})
		if err != nil {
			t.Fatal(err)
		}
		defer daemon.Close()
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		writerWaitStarted := make(chan struct{})
		var writerWaitOnce sync.Once
		confirmer := DaemonOfflineConfirmerFunc(func(ctx context.Context,
			expected localapi.AuthorityResponse,
		) (localapi.AuthorityResponse, error) {
			response, err := daemonFixtureOfflineConfirmer(fixture).ConfirmOffline(ctx, expected)
			if errors.Is(err, ErrOfflineAuthorityActive) {
				writerWaitOnce.Do(func() { close(writerWaitStarted) })
			}
			return response, err
		})
		client := daemonFixtureLifecycleClient(t, fixture)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := lease.quiesce(ctx, client, confirmer, expected,
				daemonLifecycleTiming{deadline: time.Second, poll: 5 * time.Millisecond})
			result <- err
		}()
		select {
		case <-writerWaitStarted:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("Quiesce never entered its writer-active wait")
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, context.Canceled) {
				t.Fatalf("mid-wait cancelled Quiesce() = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("mid-wait cancellation did not stop Quiesce")
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := lease.Quiesce(ctx, daemonFixtureLifecycleClient(t, fixture),
			daemonFixtureOfflineConfirmer(fixture), daemonFixtureAuthorityResponse(t, fixture))
		if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled Quiesce() = %v", err)
		}
	})
}

func TestDaemonLifecycleRejectsWrongNodeAndClosedLease(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	other := newDaemonFixture(t, true)
	if lease, err := AcquireDaemonLifecycle(context.Background(), DaemonLifecycleOptions{
		Workspace: fixture.workspace, NodeState: other.nodeState,
	}); lease != nil || !errors.Is(err, ErrDaemonLifecycle) {
		t.Fatalf("wrong-node AcquireDaemonLifecycle() = (%v, %v)", lease, err)
	}
	lease := acquireTestDaemonLifecycle(t, fixture)
	var wrongReads atomic.Int32
	wrongClient := lifecycleClientStub{
		shutdown: func(context.Context,
			localapi.AuthorityResponse,
		) (localapi.ShutdownResponse, *localapi.APIError) {
			wrongReads.Add(1)
			t.Fatal("wrong-Node client reached shutdown")
			return localapi.ShutdownResponse{}, nil
		},
	}
	if _, err := lease.Quiesce(context.Background(), wrongClient,
		daemonFixtureOfflineConfirmer(other), daemonFixtureAuthorityResponse(t, other)); !errors.Is(err, ErrDaemonLifecycle) || wrongReads.Load() != 0 {
		t.Fatalf("wrong-node lease.Quiesce() = %v, reads=%d", err, wrongReads.Load())
	}
	var probes atomic.Int32
	options := unavailableEnsureOptions(other.nodeState, other.revision, new(atomic.Int32))
	options.Probe = ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
		probes.Add(1)
		return unavailableEnsureHealth()
	})
	if _, err := lease.Ensure(context.Background(), options); !errors.Is(err, ErrDaemonLifecycle) ||
		probes.Load() != 0 {
		t.Fatalf("wrong-node lease.Ensure() = %v, probes=%d", err, probes.Load())
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if _, err := lease.Quiesce(context.Background(), nil, nil,
		daemonFixtureAuthorityResponse(t, fixture)); !errors.Is(err, ErrDaemonLifecycle) {
		t.Fatalf("closed lease Quiesce() = %v", err)
	}
	options = unavailableEnsureOptions(fixture.nodeState, fixture.revision, new(atomic.Int32))
	if _, err := lease.Ensure(context.Background(), options); !errors.Is(err, ErrDaemonLifecycle) {
		t.Fatalf("closed lease Ensure() = %v", err)
	}
}

func TestDaemonLifecycleEnsureUsesHeldLeaseWithoutRelocking(t *testing.T) {
	fixture := newDaemonFixture(t, false)
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	var ready atomic.Bool
	var launches atomic.Int32
	handle := newRecordingDaemonLaunch()
	result, err := lease.Ensure(context.Background(), DaemonEnsureOptions{
		NodeState: fixture.nodeState, AssetRevision: fixture.revision,
		Probe: ensureProbeFunc(func(context.Context) (localapi.HealthResponse, *localapi.APIError) {
			if ready.Load() {
				return readyEnsureHealth(fixture.revision), nil
			}
			return unavailableEnsureHealth()
		}),
		Preflight: DaemonEnsurePreflightFunc(func(context.Context) error { return nil }),
		Launcher: DaemonLauncherFunc(func(context.Context, DaemonLaunchPermit) (DaemonLaunch, error) {
			launches.Add(1)
			ready.Store(true)
			return handle, nil
		}),
	})
	if err != nil || !result.Started || result.Health.Status != "ready" || launches.Load() != 1 ||
		handle.releases.Load() != 1 || handle.terminations.Load() != 0 {
		t.Fatalf("lease.Ensure() = (%#v, %v), launches=%d releases=%d terminations=%d",
			result, err, launches.Load(), handle.releases.Load(), handle.terminations.Load())
	}
}

type lifecycleClientStub struct {
	shutdown func(context.Context, localapi.AuthorityResponse) (
		localapi.ShutdownResponse, *localapi.APIError,
	)
}

func (client lifecycleClientStub) ShutdownForMutation(ctx context.Context,
	expected localapi.AuthorityResponse,
) (
	localapi.ShutdownResponse, *localapi.APIError,
) {
	return client.shutdown(ctx, expected)
}

func acquireTestDaemonLifecycle(t *testing.T, fixture daemonFixture) *DaemonLifecycleLease {
	t.Helper()
	lease, err := AcquireDaemonLifecycle(context.Background(), DaemonLifecycleOptions{
		Workspace: fixture.workspace, NodeState: fixture.nodeState,
	})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func daemonFixtureAuthorityResponse(t *testing.T, fixture daemonFixture) localapi.AuthorityResponse {
	t.Helper()
	response, err := localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{
		Host: fixture.profile.Host(), Runtime: fixture.profile.Runtime(),
		Enabled: fixture.profile.Enabled(), AssetRevision: fixture.profile.ActiveAssetRevision(),
		UpdatedAt: fixture.profile.UpdatedAt(), PeerID: fixture.identity.PeerID(),
		ActiveAssetRevision: fixture.revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func daemonFixtureLifecycleClient(t *testing.T, fixture daemonFixture) DaemonLifecycleClient {
	t.Helper()
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func daemonFixtureOfflineConfirmer(fixture daemonFixture) DaemonOfflineConfirmer {
	return DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected localapi.AuthorityResponse,
	) (localapi.AuthorityResponse, error) {
		digest, err := localapi.AuthorityDigest(expected)
		if err != nil {
			return localapi.AuthorityResponse{}, err
		}
		return ConfirmOfflineAuthority(ctx, fixture.workspace, digest)
	})
}

func daemonFixtureShutdownResponse(t *testing.T,
	expected localapi.AuthorityResponse,
) localapi.ShutdownResponse {
	t.Helper()
	digest, err := localapi.AuthorityDigest(expected)
	if err != nil {
		t.Fatal(err)
	}
	return localapi.ShutdownResponse{AuthorityDigest: digest.String(),
		SchemaVersion: localapi.SchemaVersion, Status: "stopping"}
}

func waitAtomicAtLeast(t *testing.T, value *atomic.Int32, expected int32) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for value.Load() < expected && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if value.Load() < expected {
		t.Fatalf("atomic value = %d, want at least %d", value.Load(), expected)
	}
}
