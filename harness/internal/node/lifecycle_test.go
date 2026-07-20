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
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestDaemonLifecycleQuiesceWaitsPastShutdownResponseForWriterRelease(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install, Credentials: testProfileCredentials{},
		Control: newTestControlTransportFactory()})
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
	expected := readTestAuthority(t, client)
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
		expected Authority,
	) (Authority, error) {
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
		authority Authority
		err       error
	}, 1)
	go func() {
		authority, err := lease.quiesce(context.Background(), localAPILifecycleClient{client}, confirmer, expected,
			daemonLifecycleTiming{deadline: time.Second, poll: 5 * time.Millisecond})
		quiesced <- struct {
			authority Authority
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
		Install: fixture.install, Credentials: testProfileCredentials{},
		Control: newTestControlTransportFactory()})
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
	wireAuthority, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	expected := testAuthorityFromLocalAPI(t, wireAuthority)
	lease := acquireTestDaemonLifecycle(t, fixture)
	var confirmations atomic.Int32
	confirmer := DaemonOfflineConfirmerFunc(func(context.Context,
		Authority,
	) (Authority, error) {
		confirmations.Add(1)
		return Authority{}, errors.New("offline confirmation must not run")
	})
	_, err = lease.Quiesce(context.Background(), localAPILifecycleClient{client}, confirmer, expected)
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
	if _, apiErr := client.Shutdown(context.Background(), wireAuthority); apiErr != nil {
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
		Probe: ensureProbeFunc(func(context.Context) (DaemonHealth, error) {
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
		drifted.UpdatedAt = current.UpdatedAt.Add(time.Nanosecond)
		withoutGeneration := drifted
		withoutGeneration.UpdatedAt = current.UpdatedAt
		if withoutGeneration != current || drifted.UpdatedAt == current.UpdatedAt {
			t.Fatal("generation-drift fixture changed non-generation authority")
		}
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		var shutdowns atomic.Int32
		client := lifecycleClientStub{shutdown: func(context.Context,
			Authority,
		) error {
			shutdowns.Add(1)
			return errors.New("unexpected shutdown")
		}}
		_, err := lease.Quiesce(context.Background(), client,
			daemonFixtureOfflineConfirmer(fixture), drifted)
		if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, ErrOfflineAuthority) ||
			shutdowns.Load() != 0 {
			t.Fatalf("generation-drift Quiesce() = %v", err)
		}
		observed, inspectErr := InspectAuthority(context.Background(), fixture.workspace,
			testProfileCredentials{})
		if inspectErr != nil || observed != current {
			t.Fatalf("generation drift changed durable authority = (%#v, %v)",
				observed, inspectErr)
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
				observed Authority,
			) error {
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
				return nil
			},
		}
		confirmer := DaemonOfflineConfirmerFunc(func(context.Context,
			Authority,
		) (Authority, error) {
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
			Install: fixture.install, Credentials: testProfileCredentials{}, Control: newTestControlTransportFactory()})
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
			Install: fixture.install, Credentials: testProfileCredentials{}, Control: newTestControlTransportFactory()})
		if err != nil {
			t.Fatal(err)
		}
		defer daemon.Close()
		lease := acquireTestDaemonLifecycle(t, fixture)
		defer lease.Close()
		writerWaitStarted := make(chan struct{})
		var writerWaitOnce sync.Once
		confirmer := DaemonOfflineConfirmerFunc(func(ctx context.Context,
			expected Authority,
		) (Authority, error) {
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
			Authority,
		) error {
			wrongReads.Add(1)
			t.Fatal("wrong-Node client reached shutdown")
			return nil
		},
	}
	if _, err := lease.Quiesce(context.Background(), wrongClient,
		daemonFixtureOfflineConfirmer(other), daemonFixtureAuthorityResponse(t, other)); !errors.Is(err, ErrDaemonLifecycle) || wrongReads.Load() != 0 {
		t.Fatalf("wrong-node lease.Quiesce() = %v, reads=%d", err, wrongReads.Load())
	}
	var probes atomic.Int32
	options := unavailableEnsureOptions(other.nodeState, other.revision, new(atomic.Int32))
	options.Probe = ensureProbeFunc(func(context.Context) (DaemonHealth, error) {
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

func TestDaemonLifecycleRejectsTypedNilControlPortsBeforeInvocation(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	expected := daemonFixtureAuthorityResponse(t, fixture)
	var client *panicLifecycleClient
	if _, err := lease.Quiesce(context.Background(), client,
		daemonFixtureOfflineConfirmer(fixture), expected); !errors.Is(err, ErrDaemonLifecycle) {
		t.Fatalf("Quiesce(typed nil client) = %v", err)
	}
	var confirmer *panicOfflineConfirmer
	if _, err := lease.Quiesce(context.Background(), lifecycleClientStub{
		shutdown: func(context.Context, Authority) error { return nil },
	}, confirmer, expected); !errors.Is(err, ErrDaemonLifecycle) {
		t.Fatalf("Quiesce(typed nil confirmer) = %v", err)
	}
}

type panicLifecycleClient struct{}

func (*panicLifecycleClient) ShutdownDaemonForMutation(context.Context, Authority) error {
	panic("typed-nil lifecycle client must be rejected before invocation")
}

type panicOfflineConfirmer struct{}

func (*panicOfflineConfirmer) ConfirmOffline(context.Context, Authority) (Authority, error) {
	panic("typed-nil offline confirmer must be rejected before invocation")
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
		Probe: ensureProbeFunc(func(context.Context) (DaemonHealth, error) {
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
	if err != nil || !result.Started || !result.Health.Ready || launches.Load() != 1 ||
		handle.releases.Load() != 1 || handle.terminations.Load() != 0 {
		t.Fatalf("lease.Ensure() = (%#v, %v), launches=%d releases=%d terminations=%d",
			result, err, launches.Load(), handle.releases.Load(), handle.terminations.Load())
	}
}

type lifecycleClientStub struct {
	shutdown func(context.Context, Authority) error
}

func (client lifecycleClientStub) ShutdownDaemonForMutation(ctx context.Context,
	expected Authority,
) error {
	return client.shutdown(ctx, expected)
}

type localAPILifecycleClient struct{ client *localapi.Client }

func (client localAPILifecycleClient) ShutdownDaemonForMutation(ctx context.Context,
	expected Authority,
) error {
	wire, err := localAPIAuthorityResponse(expected)
	if err != nil {
		return err
	}
	response, apiErr := client.client.ShutdownForMutation(ctx, wire)
	if apiErr != nil {
		if apiErr.Code == localapi.CodeMnemondUnavailable {
			return errors.Join(ErrDaemonControlUnavailable, apiErr)
		}
		return apiErr
	}
	wanted, err := localapi.AuthorityDigest(wire)
	observed, parseErr := model.ParseDigest(response.AuthorityDigest)
	if err != nil || parseErr != nil || response.SchemaVersion != localapi.SchemaVersion ||
		response.Status != "stopping" || observed != wanted {
		return errors.New("local shutdown response is not canonical")
	}
	return nil
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

func daemonFixtureAuthorityResponse(t *testing.T, fixture daemonFixture) Authority {
	t.Helper()
	authority := Authority{
		Host: fixture.profile.Host(), Runtime: fixture.profile.Runtime(),
		Enabled: fixture.profile.Enabled(), AssetRevision: fixture.profile.ActiveAssetRevision(),
		UpdatedAt: fixture.profile.UpdatedAt(), PeerID: fixture.identity.PeerID(),
		ActiveAssetRevision: fixture.revision,
	}
	if err := authority.Validate(); err != nil {
		t.Fatal(err)
	}
	return authority
}

func daemonFixtureLifecycleClient(t *testing.T, fixture daemonFixture) DaemonLifecycleClient {
	t.Helper()
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	return localAPILifecycleClient{client: client}
}

func daemonFixtureOfflineConfirmer(fixture daemonFixture) DaemonOfflineConfirmer {
	return DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected Authority,
	) (Authority, error) {
		digest, err := expected.Digest()
		if err != nil {
			return Authority{}, err
		}
		return ConfirmOfflineAuthority(ctx, fixture.workspace, digest,
			testProfileCredentials{}, localapi.RemoveStaleOwnerUnix)
	})
}

func localAPIAuthorityResponse(expected Authority) (localapi.AuthorityResponse, error) {
	return localapi.NewAuthorityResponse(localapi.AuthoritySnapshot{
		Host: expected.Host, Runtime: expected.Runtime, Enabled: expected.Enabled,
		AssetRevision: expected.AssetRevision, UpdatedAt: expected.UpdatedAt,
		PeerID: expected.PeerID, ActiveAssetRevision: expected.ActiveAssetRevision,
	})
}

func testAuthorityFromLocalAPI(t *testing.T, response localapi.AuthorityResponse) Authority {
	t.Helper()
	peerID, peerErr := model.ParsePeerID(response.PeerID)
	updatedAt, timeErr := time.Parse(time.RFC3339Nano, response.UpdatedAt)
	authority := Authority{Host: model.HostKind(response.Host), Runtime: model.RuntimeKind(response.Runtime),
		Enabled: response.Enabled, AssetRevision: response.AssetRevision, UpdatedAt: updatedAt,
		PeerID: peerID, ActiveAssetRevision: response.ActiveAssetRevision}
	if peerErr != nil || timeErr != nil {
		t.Fatalf("parse local authority = (%v, %v)", peerErr, timeErr)
	}
	wire, err := localAPIAuthorityResponse(authority)
	if err != nil {
		t.Fatal(err)
	}
	if wire != response {
		t.Fatalf("noncanonical local authority = %#v", response)
	}
	return authority
}

func readTestAuthority(t *testing.T, client *localapi.Client) Authority {
	t.Helper()
	response, apiErr := client.ReadAuthority(context.Background())
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	return testAuthorityFromLocalAPI(t, response)
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
