package node

import (
	"context"
	"errors"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

	client, err := NewClient(fixture.nodeState)
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
		expected AuthorityResponse,
	) (AuthorityResponse, error) {
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
		authority AuthorityResponse
		err       error
	}, 1)
	go func() {
		authority, err := lease.quiesce(context.Background(), client, confirmer, expected,
			daemonLifecycleTiming{deadline: 10 * time.Second, poll: 5 * time.Millisecond})
		quiesced <- struct {
			authority AuthorityResponse
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
	assertDaemonWriterActive(t, fixture.nodeState)
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

func assertDaemonWriterActive(t *testing.T, nodeState string) {
	t.Helper()
	st, err := store.OpenExisting(context.Background(), filepath.Join(nodeState, "node.db"))
	if st != nil {
		_ = st.Close()
	}
	if st != nil || !errors.Is(err, store.ErrWriterActive) {
		t.Fatalf("daemon writer before Close = (%v, %v)", st, err)
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
	client, err := NewClient(fixture.nodeState)
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
		AuthorityResponse,
	) (AuthorityResponse, error) {
		confirmations.Add(1)
		return AuthorityResponse{}, errors.New("offline confirmation must not run")
	})
	_, err = lease.Quiesce(context.Background(), client, confirmer, expected)
	var mutationErr *APIError
	if !errors.Is(err, ErrDaemonLifecycle) || !errors.As(err, &mutationErr) ||
		mutationErr.Code != CodeOperationPending || confirmations.Load() != 0 {
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
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
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
			AuthorityResponse,
		) (ShutdownResponse, *APIError) {
			wrongReads.Add(1)
			t.Fatal("wrong-Node client reached shutdown")
			return ShutdownResponse{}, nil
		},
	}
	if _, err := lease.Quiesce(context.Background(), wrongClient,
		daemonFixtureOfflineConfirmer(other), daemonFixtureAuthorityResponse(t, other)); !errors.Is(err, ErrDaemonLifecycle) || wrongReads.Load() != 0 {
		t.Fatalf("wrong-node lease.Quiesce() = %v, reads=%d", err, wrongReads.Load())
	}
	var probes atomic.Int32
	options := unavailableEnsureOptions(other.nodeState, other.revision, new(atomic.Int32))
	options.Probe = ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
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
		Probe: ensureProbeFunc(func(context.Context) (HealthResponse, *APIError) {
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
	shutdown func(context.Context, AuthorityResponse) (
		ShutdownResponse, *APIError,
	)
}

func (client lifecycleClientStub) ShutdownForMutation(ctx context.Context,
	expected AuthorityResponse,
) (
	ShutdownResponse, *APIError,
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

func daemonFixtureAuthorityResponse(t *testing.T, fixture daemonFixture) AuthorityResponse {
	t.Helper()
	response, err := NewAuthorityResponse(AuthoritySnapshot{
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
	client, err := NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func daemonFixtureOfflineConfirmer(fixture daemonFixture) DaemonOfflineConfirmer {
	return DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected AuthorityResponse,
	) (AuthorityResponse, error) {
		digest, err := AuthorityDigest(expected)
		if err != nil {
			return AuthorityResponse{}, err
		}
		return ConfirmOfflineAuthority(ctx, fixture.workspace, digest)
	})
}

func daemonFixtureShutdownResponse(t *testing.T,
	expected AuthorityResponse,
) ShutdownResponse {
	t.Helper()
	digest, err := AuthorityDigest(expected)
	if err != nil {
		t.Fatal(err)
	}
	return ShutdownResponse{AuthorityDigest: digest.String(),
		SchemaVersion: SchemaVersion, Status: "stopping"}
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
