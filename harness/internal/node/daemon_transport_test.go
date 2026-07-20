package node

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func TestOpenManagedDaemonComposesOneSharedArtifactAndChannelAuthority(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	options := daemonTestManagedOptions(fixture)
	var casCalls int
	var sharedCAS *artifact.CAS
	options.artifactCASFactory = func(root string) (*artifact.CAS, error) {
		casCalls++
		if root != filepath.Join(fixture.nodeState, "objects", "sha256") {
			t.Fatalf("CAS root = %q", root)
		}
		var err error
		sharedCAS, err = artifact.NewCAS(root)
		return sharedCAS, err
	}
	var capturedRuntime *peer.MeshRuntime
	var captured peer.MeshTransportOptions
	options.meshTransportFactory = func(runtime *peer.MeshRuntime,
		transportOptions peer.MeshTransportOptions,
	) (managedMeshTransport, error) {
		capturedRuntime, captured = runtime, transportOptions
		return peer.NewMeshTransport(runtime, transportOptions)
	}
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	if casCalls != 1 || sharedCAS == nil || daemon.artifactCAS != sharedCAS ||
		daemon.controller.artifactCAS != sharedCAS || captured.ArtifactCAS != sharedCAS {
		t.Fatalf("shared CAS = calls %d daemon=%p controller=%p transport=%T",
			casCalls, daemon.artifactCAS, daemon.controller.artifactCAS, captured.ArtifactCAS)
	}
	if daemon.channelAuthority == nil || capturedRuntime != daemon.mesh ||
		captured.Enrollment.Controller != daemon.channelAuthority ||
		captured.Member.Controller != daemon.channelAuthority ||
		captured.EventSource != daemon.store || captured.ArtifactStore != daemon.store {
		t.Fatalf("transport composition = runtime %p authority=%p options=%#v",
			capturedRuntime, daemon.channelAuthority, captured)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	assertClosedDescriptor(t, childFD)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenManagedDaemonTransportFactoryFailureRollsBackAuthority(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	options := daemonTestManagedOptions(fixture)
	var casCalls atomic.Int32
	options.artifactCASFactory = func(root string) (*artifact.CAS, error) {
		casCalls.Add(1)
		return artifact.NewCAS(root)
	}
	factoryErr := errors.New("injected mesh transport construction failure")
	partial := newDaemonTransportStub()
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedMeshTransport, error) {
		return partial, factoryErr
	}
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) || !errors.Is(err, factoryErr) ||
		casCalls.Load() != 1 || partial.closeCalls.Load() != 1 {
		t.Fatalf("OpenManagedDaemon() = (%v,%v), CAS calls=%d partial closes=%d",
			daemon, err, casCalls.Load(), partial.closeCalls.Load())
	}
	assertClosedDescriptor(t, childFD)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	final, ok := state.finalAuthority()
	if inspectErr != nil || !ok {
		t.Fatalf("rollback endpoint = (%#v,%v)", state, inspectErr)
	}
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonMeshReadinessPrecedesPermitAndLocalAccept(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	mesh := newDaemonTransportStub()
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedMeshTransport, error) {
		return mesh, nil
	}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	waitControllerCompositionSignal(t, mesh.runStarted)
	assertOpenDescriptor(t, childFD)
	if channelClosed(control.runStarted) {
		t.Fatal("local control accepted before mesh readiness")
	}
	mesh.ready <- nil
	waitControllerCompositionSignal(t, control.runStarted)
	assertClosedDescriptor(t, childFD)
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	if err := waitControllerCompositionError(t, served); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonDrainRetainsHostAndStoreUntilTransportClose(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, _ := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	mesh := newDaemonTransportStub()
	mesh.blockClose = true
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedMeshTransport, error) {
		return mesh, nil
	}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	state, err := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	final, ok := state.finalAuthority()
	if err != nil || !ok {
		t.Fatalf("managed endpoint = (%#v,%v)", state, err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	mesh.ready <- nil
	waitControllerCompositionSignal(t, control.runStarted)
	closed := make(chan error, 1)
	go func() { closed <- daemon.Close() }()
	waitControllerCompositionSignal(t, mesh.closeStarted)
	assertDaemonTestMeshPort(t, final, false)
	if reopened, openErr := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db")); openErr == nil {
		_ = reopened.Close()
		t.Fatal("Store writer authority was released before transport drain")
	}
	close(mesh.releaseClose)
	if err := waitControllerCompositionError(t, closed); err != nil {
		t.Fatal(err)
	}
	if err := waitControllerCompositionError(t, served); err != nil {
		t.Fatal(err)
	}
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonPropagatesMeshTransportTerminalFailure(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, _ := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	mesh := newDaemonTransportStub()
	control := newControllerCompositionControl(nil)
	options := daemonTestManagedOptions(fixture)
	options.meshTransportFactory = func(*peer.MeshRuntime,
		peer.MeshTransportOptions,
	) (managedMeshTransport, error) {
		return mesh, nil
	}
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return control, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	mesh.ready <- nil
	waitControllerCompositionSignal(t, control.runStarted)
	terminalErr := errors.New("injected managed mesh terminal failure")
	mesh.terminal <- terminalErr
	err = waitControllerCompositionError(t, served)
	if !errors.Is(err, terminalErr) {
		t.Fatalf("Serve() = %v", err)
	}
	if reopened, openErr := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db")); openErr == nil {
		_ = reopened.Close()
		t.Fatal("terminal component exit released Daemon Store ownership")
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

type daemonTransportStub struct {
	ready        chan error
	terminal     chan error
	runStarted   chan struct{}
	closeStarted chan struct{}
	releaseClose chan struct{}
	stop         chan struct{}
	blockClose   bool
	runOnce      sync.Once
	closeOnce    sync.Once
	stopOnce     sync.Once
	closeCalls   atomic.Int32
}

func newDaemonTransportStub() *daemonTransportStub {
	return &daemonTransportStub{ready: make(chan error, 1), terminal: make(chan error, 1),
		runStarted: make(chan struct{}), closeStarted: make(chan struct{}),
		releaseClose: make(chan struct{}), stop: make(chan struct{})}
}

func (transport *daemonTransportStub) Run(ctx context.Context) error {
	transport.runOnce.Do(func() { close(transport.runStarted) })
	select {
	case err := <-transport.terminal:
		return err
	case <-transport.stop:
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (transport *daemonTransportStub) Readiness(ctx context.Context) error {
	select {
	case err := <-transport.ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (transport *daemonTransportStub) Close() error {
	transport.closeCalls.Add(1)
	transport.closeOnce.Do(func() { close(transport.closeStarted) })
	if transport.blockClose {
		select {
		case <-transport.releaseClose:
		case <-time.After(5 * time.Second):
			return errors.New("timed out waiting to release test mesh transport")
		}
	}
	transport.stopOnce.Do(func() { close(transport.stop) })
	return nil
}
