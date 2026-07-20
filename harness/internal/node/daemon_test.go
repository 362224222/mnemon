package node

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"golang.org/x/sys/unix"
)

func TestOpenDaemonBindsIdentityStoreCredentialAssetsAndSocket(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install,
		Credentials: testProfileCredentials{}, Control: newTestControlTransportFactory()})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if daemon.Workspace() != fixture.workspace || daemon.NodeState() != fixture.nodeState ||
		daemon.PeerID() != fixture.identity.PeerID() {
		t.Fatalf("OpenDaemon() identity = %s %s %s", daemon.Workspace(), daemon.NodeState(), daemon.PeerID())
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(serveCtx) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, "control.sock"), served)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	health, apiErr := client.ProbeHealth(context.Background())
	if apiErr != nil || health.Status != "ready" || health.AssetRevision != fixture.revision {
		t.Fatalf("ProbeHealth() = (%#v, %v)", health, apiErr)
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatalf("Close() retained writer lock: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDaemonRejectsMissingActionAuthorityBeforeStoreOrInstallationVerification(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	writer, err := store.OpenExisting(context.Background(), filepath.Join(fixture.nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	called := false
	install := InstallationVerifierFunc(func(context.Context, model.Profile) error {
		called = true
		return nil
	})
	daemon, openErr := OpenDaemon(context.Background(), DaemonOptions{
		Workspace: fixture.workspace, Install: install, Credentials: testProfileCredentials{},
	})
	if daemon != nil || !errors.Is(openErr, ErrDaemonAuthority) || errors.Is(openErr, store.ErrWriterActive) {
		t.Fatalf("OpenDaemon() = (%v, %v)", daemon, openErr)
	}
	if called {
		t.Fatal("action authority failure reached installation verification")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenDaemonRejectsAuthorityDriftAndNeverCreatesMissingDatabase(t *testing.T) {
	t.Run("identity replacement", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		if err := os.Remove(filepath.Join(fixture.nodeState, identityKeyName)); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(fixture.nodeState); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install, Credentials: testProfileCredentials{}}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("credential replacement", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		writeDaemonToken(t, fixture.nodeState, bytes.Repeat([]byte{0x99}, 32), true)
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install, Credentials: testProfileCredentials{}}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("disabled Profile", func(t *testing.T) {
		fixture := newDaemonFixture(t, false)
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install, Credentials: testProfileCredentials{}}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("Host projection drift", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install, Credentials: testProfileCredentials{}}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
	t.Run("missing database", func(t *testing.T) {
		workspace := newDaemonWorkspace(t)
		nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
		if err := os.MkdirAll(nodeState, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(nodeState, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := EnsureIdentity(nodeState); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: workspace,
			Credentials: testProfileCredentials{}}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
		if _, err := os.Lstat(filepath.Join(nodeState, "node.db")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("strict restart created node.db: %v", err)
		}
	})
	t.Run("empty database", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		path := filepath.Join(fixture.nodeState, "node.db")
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
			Install: fixture.install, Credentials: testProfileCredentials{}}); daemon != nil || !errors.Is(err, store.ErrUnsupportedSchema) {
			t.Fatalf("OpenDaemon(empty node.db) = (%v, %v)", daemon, err)
		}
		info, err := os.Stat(path)
		if err != nil || info.Size() != 0 {
			t.Fatalf("strict restart initialized empty node.db: (%v, %v)", info, err)
		}
	})
	t.Run("relative workspace", func(t *testing.T) {
		if daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: "."}); daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
			t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
		}
	})
}

func TestOpenDaemonComposesWakeAdapterFromExactDurableAuthority(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	var captured WakeAdapterFactoryOptions
	called := 0
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{
		Workspace:   fixture.workspace,
		Clock:       controllerTestClock{fixture.profile.UpdatedAt()},
		Install:     fixture.install,
		Credentials: testProfileCredentials{},
		Control:     newTestControlTransportFactory(),
		Attachments: &testWakeAttachmentFilesystem{},
		WakeAdapterFactory: WakeAdapterFactoryFunc(func(_ context.Context,
			options WakeAdapterFactoryOptions,
		) (agent.WakeWorkerAdapter, error) {
			called++
			captured = options
			return daemonTestWakeAdapter{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if called != 1 {
		t.Fatalf("NewWakeAdapter() calls = %d, want 1", called)
	}
	if captured.Workspace != fixture.workspace || captured.NodeState != fixture.nodeState ||
		!sameControllerProfile(captured.Profile, fixture.profile) {
		t.Fatalf("NewWakeAdapter() options = %#v", captured)
	}
	if daemon.controller.wakeWorker == nil {
		t.Fatal("OpenDaemon() did not compose the managed wake worker")
	}
}

func TestOpenDaemonFactoryFailureAndNilAdapterReleaseStoreAuthority(t *testing.T) {
	tests := []struct {
		name        string
		factory     WakeAdapterFactory
		wantFactory error
	}{
		{
			name: "factory error",
			factory: WakeAdapterFactoryFunc(func(context.Context,
				WakeAdapterFactoryOptions,
			) (agent.WakeWorkerAdapter, error) {
				return nil, errDaemonTestFactory
			}),
			wantFactory: errDaemonTestFactory,
		},
		{
			name: "nil adapter",
			factory: WakeAdapterFactoryFunc(func(context.Context,
				WakeAdapterFactoryOptions,
			) (agent.WakeWorkerAdapter, error) {
				return nil, nil
			}),
		},
		{
			name: "typed nil adapter",
			factory: WakeAdapterFactoryFunc(func(context.Context,
				WakeAdapterFactoryOptions,
			) (agent.WakeWorkerAdapter, error) {
				var adapter *daemonTestWakeAdapter
				return adapter, nil
			}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDaemonFixture(t, true)
			daemon, err := OpenDaemon(context.Background(), DaemonOptions{
				Workspace:          fixture.workspace,
				Install:            fixture.install,
				Credentials:        testProfileCredentials{},
				WakeAdapterFactory: test.factory,
			})
			if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
				test.wantFactory != nil && !errors.Is(err, test.wantFactory) {
				t.Fatalf("OpenDaemon() = (%v, %v)", daemon, err)
			}
			assertDaemonStoreReopenable(t, fixture.nodeState)
		})
	}
}

func TestOpenManagedDaemonRejectsMissingOrTypedNilCompositionAfterConsumingPermit(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*DaemonOptions)
	}{
		{name: "missing wake factory", mutate: func(options *DaemonOptions) {
			options.WakeAdapterFactory = nil
		}},
		{name: "typed nil wake factory", mutate: func(options *DaemonOptions) {
			options.WakeAdapterFactory = (*daemonTypedNilWakeAdapterFactory)(nil)
		}},
		{name: "missing attachments", mutate: func(options *DaemonOptions) {
			options.Attachments = nil
		}},
		{name: "typed nil attachments", mutate: func(options *DaemonOptions) {
			options.Attachments = (*daemonTypedNilWakeAttachmentFilesystem)(nil)
		}},
		{name: "missing control factory", mutate: func(options *DaemonOptions) {
			options.Control = nil
		}},
		{name: "typed nil control factory", mutate: func(options *DaemonOptions) {
			options.Control = (*controllerTypedNilControlFactory)(nil)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDaemonFixture(t, true)
			parent := acquirePermitTestEnsureLock(t, fixture.nodeState)
			defer parent.close()
			childFD, err := unix.Dup(int(parent.file.Fd()))
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(childFD))
			options := DaemonOptions{Workspace: fixture.workspace, Install: fixture.install,
				Credentials: testProfileCredentials{}, WakeAdapterFactory: permitTestWakeFactory(),
				Attachments: &testWakeAttachmentFilesystem{},
				Control:     newTestControlTransportFactory()}
			test.mutate(&options)
			daemon, err := OpenManagedDaemon(context.Background(), options)
			if daemon != nil || !errors.Is(err, ErrDaemonAuthority) {
				t.Fatalf("OpenManagedDaemon() = (%v, %v)", daemon, err)
			}
			assertClosedDescriptor(t, childFD)
			if err := validateHeldEnsureLock(parent, fixture.nodeState); err != nil {
				t.Fatalf("invalid composition disturbed parent permit: %v", err)
			}
			assertDaemonStoreReopenable(t, fixture.nodeState)
		})
	}
}

func TestManagedDaemonCloseBeforeServeReleasesPermitAndStore(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	parent := acquirePermitTestEnsureLock(t, fixture.nodeState)
	defer parent.close()
	childFD, err := unix.Dup(int(parent.file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(childFD))
	daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{
		Workspace:   fixture.workspace,
		Clock:       controllerTestClock{fixture.profile.UpdatedAt()},
		Install:     fixture.install,
		Credentials: testProfileCredentials{},
		Control:     newTestControlTransportFactory(),
		Attachments: &testWakeAttachmentFilesystem{},
		WakeAdapterFactory: WakeAdapterFactoryFunc(func(context.Context,
			WakeAdapterFactoryOptions,
		) (agent.WakeWorkerAdapter, error) {
			return daemonTestWakeAdapter{}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOpenDescriptor(t, childFD)
	if err := daemon.Serve(nil); !errors.Is(err, ErrDaemonAuthority) {
		t.Fatalf("Serve(nil) = %v", err)
	}
	assertOpenDescriptor(t, childFD)
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	assertClosedDescriptor(t, childFD)
	if err := daemon.Close(); err != nil {
		t.Fatalf("repeated Close() = %v", err)
	}
	if err := daemon.Serve(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
		t.Fatalf("Serve() after Close-before-Serve = %v", err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestDaemonCloseWaitsForServeAndWorkerSettlementBeforeStoreClose(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{
		Workspace:   fixture.workspace,
		Clock:       controllerTestClock{fixture.profile.UpdatedAt()},
		Install:     fixture.install,
		Credentials: testProfileCredentials{},
		Control:     newTestControlTransportFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	worker := newDaemonCloseWorker()
	daemon.controller.wakeWorker = worker
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, controlSocketName), served)
	select {
	case <-worker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("managed worker did not start")
	}

	closed := make(chan error, 1)
	go func() { closed <- daemon.Close() }()
	select {
	case <-worker.stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not cancel managed worker")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close() returned before worker settlement: %v", err)
	default:
	}
	writer, writerErr := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db"))
	if writer != nil || !errors.Is(writerErr, store.ErrWriterActive) {
		t.Fatalf("Store authority during settlement = (%v, %v)", writer, writerErr)
	}

	close(worker.release)
	if err := <-served; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestDaemonConcurrentCloseAndServeReuseFailClosed(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{
		Workspace:   fixture.workspace,
		Clock:       controllerTestClock{fixture.profile.UpdatedAt()},
		Install:     fixture.install,
		Credentials: testProfileCredentials{},
		Control:     newTestControlTransportFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, controlSocketName), served)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	if health, apiErr := client.ProbeHealth(context.Background()); apiErr != nil || health.Status != "ready" {
		t.Fatalf("ProbeHealth() = (%#v, %v)", health, apiErr)
	}
	if err := daemon.Serve(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
		t.Fatalf("concurrent second Serve() = %v", err)
	}

	const closers = 16
	start := make(chan struct{})
	results := make(chan error, closers)
	var group sync.WaitGroup
	for range closers {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			results <- daemon.Close()
		}()
	}
	close(start)
	group.Wait()
	close(results)
	for closeErr := range results {
		if closeErr != nil {
			t.Fatalf("concurrent Close() error = %v", closeErr)
		}
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
	if err := daemon.Serve(context.Background()); !errors.Is(err, ErrDaemonAuthority) {
		t.Fatalf("Serve() after Close = %v", err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestDaemonCloseRetainsStoreWhenControlHandlersCannotBeDrained(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	transport := &daemonUndrainedControlTransport{started: make(chan struct{})}
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{
		Workspace: fixture.workspace, Install: fixture.install,
		Credentials: testProfileCredentials{},
		Control: ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
			ControlBindings,
		) (PreparedControlTransport, error) {
			return transport, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("control transport did not start")
	}
	if err := daemon.Close(); !errors.Is(err, ErrControlTransportUndrained) {
		t.Fatalf("Close() error = %v, want retained Store authority", err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, ErrControlTransportUndrained) {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return its undrained authority signal")
	}
	if reopened, err := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db")); reopened != nil ||
		!errors.Is(err, store.ErrWriterActive) {
		t.Fatalf("unsafe drain released Store = (%v, %v)", reopened, err)
	}
	if err := daemon.store.Close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

type daemonUndrainedControlTransport struct {
	started chan struct{}
	once    sync.Once
}

func (transport *daemonUndrainedControlTransport) Run(ctx context.Context) error {
	transport.once.Do(func() { close(transport.started) })
	<-ctx.Done()
	return nil
}

func (transport *daemonUndrainedControlTransport) Readiness(ctx context.Context) error {
	select {
	case <-transport.started:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*daemonUndrainedControlTransport) Shutdown(context.Context) error {
	return ErrControlTransportUndrained
}

func (*daemonUndrainedControlTransport) Close() error { return nil }

var errDaemonTestFactory = errors.New("wake adapter factory failed")

type daemonTestWakeAdapter struct{}

func (daemonTestWakeAdapter) Run(context.Context,
	agent.CodexWakeRequest,
) (agent.CodexWakeResult, error) {
	return agent.CodexWakeResult{}, nil
}

type daemonTypedNilWakeAdapterFactory struct{}

func (*daemonTypedNilWakeAdapterFactory) NewWakeAdapter(context.Context,
	WakeAdapterFactoryOptions,
) (agent.WakeWorkerAdapter, error) {
	panic("typed-nil wake adapter factory must be rejected before invocation")
}

type daemonTypedNilWakeAttachmentFilesystem struct{}

func (*daemonTypedNilWakeAttachmentFilesystem) ListCandidates() ([]agent.WakeAttachmentCandidate, error) {
	panic("typed-nil wake attachment filesystem must be rejected before invocation")
}

func (*daemonTypedNilWakeAttachmentFilesystem) RemoveReapable(model.RunID, model.Digest) (bool, error) {
	panic("typed-nil wake attachment filesystem must be rejected before invocation")
}

func (*daemonTypedNilWakeAttachmentFilesystem) CleanupStages(time.Time) (int, error) {
	panic("typed-nil wake attachment filesystem must be rejected before invocation")
}

func (*daemonTypedNilWakeAttachmentFilesystem) Stage(io.Reader) (agent.StagedRunAttachment, error) {
	panic("typed-nil wake attachment filesystem must be rejected before invocation")
}

type daemonCloseWorker struct {
	started  chan struct{}
	stopping chan struct{}
	release  chan struct{}
	once     sync.Once
}

func newDaemonCloseWorker() *daemonCloseWorker {
	return &daemonCloseWorker{started: make(chan struct{}), stopping: make(chan struct{}),
		release: make(chan struct{})}
}

func (worker *daemonCloseWorker) Run(ctx context.Context) error {
	worker.once.Do(func() { close(worker.started) })
	<-ctx.Done()
	close(worker.stopping)
	<-worker.release
	return nil
}

func (*daemonCloseWorker) Snapshot() agent.WakeWorkerSnapshot {
	return agent.WakeWorkerSnapshot{Running: true, Ready: true, Healthy: true}
}

func assertDaemonStoreReopenable(t *testing.T, nodeState string) {
	t.Helper()
	reopened, err := store.OpenExisting(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatalf("daemon retained Store writer authority: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

type daemonFixture struct {
	workspace string
	nodeState string
	identity  *Identity
	profile   model.Profile
	revision  string
	install   InstallationVerifier
}

func newDaemonFixture(t *testing.T, enabled bool) daemonFixture {
	t.Helper()
	workspace := newDaemonWorkspace(t)
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, 0o700); err != nil {
		t.Fatal(err)
	}
	identity, err := EnsureIdentity(nodeState)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 7, 17, 5, 0, 0, 0, time.UTC)
	epoch, _ := model.ParseOriginEpoch("epoch-daemon-fixture")
	nodeValue, err := model.NewNode(model.NodeSpec{PeerID: identity.PeerID(), OriginEpoch: epoch,
		NextOriginSequence: 1, ActiveAssetRevision: bundle.Manifest().AssetRevision,
		CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	credential := bytes.Repeat([]byte{0x73}, 32)
	profile, err := model.NewProfile(model.ProfileSpec{ID: model.TeamworkProfileID(),
		Principal: "principal-daemon", WorkspaceRoot: workspace, Host: model.HostCodex,
		Runtime: model.RuntimeCodexAppServer, CredentialHash: model.Sum(credential),
		ActiveAssetRevision: bundle.Manifest().AssetRevision,
		HandlingBudget:      model.DefaultHandlingBudget().JSON(), CreatedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(context.Background(), filepath.Join(nodeState, "node.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InitializeNode(context.Background(), nodeValue, profile); err != nil {
		t.Fatal(err)
	}
	if enabled {
		spec := profile.Spec()
		spec.Enabled = true
		spec.UpdatedAt = at.Add(time.Second)
		profile, err = model.NewProfile(spec)
		if err != nil {
			t.Fatal(err)
		}
		activated, err := st.ActivateProfile(context.Background(), profile, at, profile.UpdatedAt())
		if err != nil {
			t.Fatal(err)
		}
		profile = activated.Profile
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	writeDaemonToken(t, nodeState, credential, false)
	return daemonFixture{workspace: workspace, nodeState: nodeState, identity: identity,
		profile: profile, revision: bundle.Manifest().AssetRevision,
		install: testInstallationVerifier(workspace, nodeState, bundle)}
}

func newDaemonWorkspace(t *testing.T) string {
	t.Helper()
	workspace, err := os.MkdirTemp("/tmp", "mnemon-r5-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	return workspace
}

func writeDaemonToken(t *testing.T, nodeState string, credential []byte, replace bool) {
	t.Helper()
	profiles := filepath.Join(nodeState, "profiles")
	if err := os.Mkdir(profiles, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	if err := os.Chmod(profiles, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(profiles, model.TeamworkProfileID().String()+".token")
	if replace {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	raw := append([]byte(base64.RawURLEncoding.EncodeToString(credential)), '\n')
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}
