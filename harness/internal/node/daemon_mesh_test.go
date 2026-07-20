package node

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/sys/unix"
)

func TestOpenManagedDaemonRejectsAbsentEndpointWithoutCreatingPending(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	daemon, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
		!errors.Is(err, errManagedDaemonMesh) {
		t.Fatalf("OpenManagedDaemon() = (%v,%v)", daemon, err)
	}
	assertClosedDescriptor(t, childFD)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if inspectErr != nil || state.stateKind() != meshEndpointStateAbsent {
		t.Fatalf("absent endpoint after rejection = (%#v,%v)", state, inspectErr)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonMeshCancellationPrecedesDependencyValidation(t *testing.T) {
	cancelCause := errors.New("managed mesh test cancellation")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cancelCause)
	runtime, err := openManagedDaemonMesh(ctx, "", existingDaemonAuthority{}, nil)
	if runtime != nil || !errors.Is(err, errManagedDaemonMesh) ||
		!errors.Is(err, context.Canceled) || !errors.Is(err, cancelCause) {
		t.Fatalf("openManagedDaemonMesh() = (%v,%v)", runtime, err)
	}
	deadline, deadlineCancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer deadlineCancel()
	runtime, err = openManagedDaemonMesh(deadline, "", existingDaemonAuthority{}, nil)
	if runtime != nil || !errors.Is(err, errManagedDaemonMesh) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("openManagedDaemonMesh(deadline) = (%v,%v)", runtime, err)
	}
}

func TestOpenManagedDaemonRequiresExactPristineStoreForPending(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	advanceDaemonTestOriginSequence(t, fixture.nodeState)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	daemon, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
		!errors.Is(err, store.ErrMeshPristineAuthority) {
		t.Fatalf("OpenManagedDaemon() = (%v,%v)", daemon, err)
	}
	assertClosedDescriptor(t, childFD)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if inspectErr != nil || state.stateKind() != meshEndpointStatePending {
		t.Fatalf("non-pristine pending endpoint = (%#v,%v)", state, inspectErr)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenManagedDaemonPreservesNonPristineCauseForAbsentEndpoint(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	advanceDaemonTestOriginSequence(t, fixture.nodeState)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	daemon, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
		!errors.Is(err, store.ErrMeshPristineAuthority) {
		t.Fatalf("OpenManagedDaemon() = (%v,%v)", daemon, err)
	}
	assertClosedDescriptor(t, childFD)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if inspectErr != nil || state.stateKind() != meshEndpointStateAbsent {
		t.Fatalf("non-pristine absent endpoint = (%#v,%v)", state, inspectErr)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenManagedDaemonConvergesAllRestartableEndpointStates(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, daemonFixture) (meshEndpoint, bool)
	}{
		{name: "pending", prepare: func(t *testing.T, fixture daemonFixture) (meshEndpoint, bool) {
			publishDaemonTestMeshPending(t, fixture)
			return meshEndpoint{}, false
		}},
		{name: "final with pending", prepare: func(t *testing.T, fixture daemonFixture) (meshEndpoint, bool) {
			return publishDaemonTestMeshFinal(t, fixture, false), true
		}},
		{name: "final", prepare: func(t *testing.T, fixture daemonFixture) (meshEndpoint, bool) {
			return publishDaemonTestMeshFinal(t, fixture, true), true
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDaemonFixture(t, true)
			want, hasWant := test.prepare(t, fixture)
			parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
			defer parent.close()
			daemon, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
			if err != nil {
				t.Fatal(err)
			}
			if daemon.mesh == nil {
				t.Fatal("managed daemon did not own a MeshRuntime")
			}
			assertOpenDescriptor(t, childFD)
			state, err := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
			if err != nil || state.stateKind() != meshEndpointStateFinal {
				t.Fatalf("converged endpoint = (%#v,%v)", state, err)
			}
			final, _ := state.finalAuthority()
			if hasWant && string(final.canonicalJSON()) != string(want.canonicalJSON()) {
				t.Fatalf("reopened final changed:\n got %s\nwant %s",
					final.canonicalJSON(), want.canonicalJSON())
			}
			advertised, err := daemon.mesh.LocalEnrollmentMultiaddrs()
			if err != nil || !equalManagedDaemonStrings(advertised, final.advertisedAddresses()) {
				t.Fatalf("runtime advertisement = (%v,%v), want %v",
					advertised, err, final.advertisedAddresses())
			}
			assertDaemonTestMeshPort(t, final, false)
			if err := daemon.Close(); err != nil {
				t.Fatal(err)
			}
			assertClosedDescriptor(t, childFD)
			assertDaemonTestMeshPort(t, final, true)
			assertDaemonStoreReopenable(t, fixture.nodeState)
		})
	}
}

func TestOpenManagedDaemonBindFailurePreservesFinalWithPending(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	final := publishDaemonTestMeshFinal(t, fixture, false)
	pendingPath := filepath.Join(fixture.nodeState, meshEndpointPendingName)
	finalPath := filepath.Join(fixture.nodeState, meshEndpointName)
	pendingBefore, err := os.ReadFile(pendingPath)
	if err != nil {
		t.Fatal(err)
	}
	finalBefore, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	occupied := listenDaemonTestMeshPort(t, final)
	defer occupied.Close()
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	daemon, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
		!errors.Is(err, peer.ErrMeshHost) {
		t.Fatalf("OpenManagedDaemon() = (%v,%v)", daemon, err)
	}
	assertClosedDescriptor(t, childFD)
	pendingAfter, pendingErr := os.ReadFile(pendingPath)
	finalAfter, finalErr := os.ReadFile(finalPath)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if pendingErr != nil || finalErr != nil || inspectErr != nil ||
		state.stateKind() != meshEndpointStateFinalWithPending ||
		string(pendingAfter) != string(pendingBefore) || string(finalAfter) != string(finalBefore) {
		t.Fatalf("bind failure changed endpoint authority: state=%#v errors=(%v,%v,%v)",
			state, pendingErr, finalErr, inspectErr)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenManagedDaemonFinalPreCanceledReleasesEveryResource(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	final := publishDaemonTestMeshFinal(t, fixture, true)
	finalBefore, err := os.ReadFile(filepath.Join(fixture.nodeState, meshEndpointName))
	if err != nil {
		t.Fatal(err)
	}
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	daemon, err := OpenManagedDaemon(ctx, daemonTestManagedOptions(fixture))
	if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
		!errors.Is(err, context.Canceled) {
		t.Fatalf("OpenManagedDaemon() = (%v,%v)", daemon, err)
	}
	assertClosedDescriptor(t, childFD)
	finalAfter, readErr := os.ReadFile(filepath.Join(fixture.nodeState, meshEndpointName))
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if readErr != nil || inspectErr != nil || state.stateKind() != meshEndpointStateFinal ||
		string(finalAfter) != string(finalBefore) {
		t.Fatalf("pre-canceled final changed: state=%#v errors=(%v,%v)", state, readErr, inspectErr)
	}
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenManagedDaemonFreezeFailureLeavesRestartableFinal(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, childFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	freezeFailure := errors.New("injected initial mesh reconcile failure")
	freezeCalls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	freezer := managedDaemonMeshFreezer(func(_ context.Context, _ *peer.BoundMeshHost,
		mesh store.ChannelMeshAuthority,
	) (*peer.MeshRuntime, error) {
		freezeCalls++
		if mesh.LocalPeerID() != fixture.identity.PeerID() {
			return nil, errors.New("freezer received the wrong whole-Store authority")
		}
		cancel()
		return nil, freezeFailure
	})
	daemon, err := openManagedDaemonWithMeshFreezer(ctx,
		daemonTestManagedOptions(fixture), freezer)
	if daemon != nil || freezeCalls != 1 || !errors.Is(err, ErrDaemonAuthority) ||
		!errors.Is(err, freezeFailure) || !errors.Is(err, context.Canceled) {
		t.Fatalf("openManagedDaemonWithMeshFreezer() = (%v,%v), calls=%d",
			daemon, err, freezeCalls)
	}
	assertClosedDescriptor(t, childFD)
	state, inspectErr := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if inspectErr != nil || state.stateKind() != meshEndpointStateFinal {
		t.Fatalf("failed Freeze endpoint = (%#v,%v)", state, inspectErr)
	}
	final, _ := state.finalAuthority()
	finalBefore := final.canonicalJSON()
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
	if err := parent.close(); err != nil {
		t.Fatal(err)
	}

	restartParent, restartChildFD := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer restartParent.close()
	restarted, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
	if err != nil {
		t.Fatal(err)
	}
	assertOpenDescriptor(t, restartChildFD)
	restartedState, err := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	restartedFinal, ok := restartedState.finalAuthority()
	if err != nil || !ok || restartedState.stateKind() != meshEndpointStateFinal ||
		string(restartedFinal.canonicalJSON()) != string(finalBefore) {
		t.Fatalf("final restart endpoint = (%#v,%v)", restartedState, err)
	}
	assertDaemonTestMeshPort(t, restartedFinal, false)
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	assertClosedDescriptor(t, restartChildFD)
	assertDaemonTestMeshPort(t, restartedFinal, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestManagedDaemonUnsafeControlDrainRetainsMeshAndStore(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent, _ := installDaemonTestLaunchPermit(t, fixture.nodeState)
	defer parent.close()
	transport := &daemonUndrainedControlTransport{started: make(chan struct{})}
	options := daemonTestManagedOptions(fixture)
	options.Control = ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
		ControlBindings,
	) (PreparedControlTransport, error) {
		return transport, nil
	})
	daemon, err := OpenManagedDaemon(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	state, err := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if err != nil {
		t.Fatal(err)
	}
	final, _ := state.finalAuthority()
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(context.Background()) }()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("control transport did not start")
	}
	if err := daemon.Close(); !errors.Is(err, ErrControlTransportUndrained) {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-served:
		if !errors.Is(err, ErrControlTransportUndrained) {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not report unsafe drain")
	}
	assertDaemonTestMeshPort(t, final, false)
	if reopened, err := store.OpenExisting(context.Background(),
		filepath.Join(fixture.nodeState, "node.db")); reopened != nil ||
		!errors.Is(err, store.ErrWriterActive) {
		t.Fatalf("unsafe drain released Store = (%v,%v)", reopened, err)
	}
	closeRetainedDaemonMeshTransport(t, daemon.meshTransport)
	if err := daemon.mesh.Close(); err != nil {
		t.Fatal(err)
	}
	if err := daemon.store.Close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonTestMeshPort(t, final, true)
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func closeRetainedDaemonMeshTransport(t *testing.T, transport managedMeshTransport) {
	t.Helper()
	readyCtx, cancelReady := context.WithTimeout(context.Background(), time.Second)
	defer cancelReady()
	if err := transport.Readiness(readyCtx); err != nil {
		t.Fatalf("unsafe drain revoked live mesh transport: %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedDaemonCloseBeforeServeReleasesPermitAfterMeshAndStore(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	childFD := installSoleDaemonTestLaunchPermit(t, fixture.nodeState)
	daemon, err := OpenManagedDaemon(context.Background(), daemonTestManagedOptions(fixture))
	if err != nil {
		t.Fatal(err)
	}
	mesh, err := daemon.store.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	transition, err := daemon.mesh.BeginAuthorityTransition(mesh)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- daemon.Close() }()
	probeCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	probe, probeErr := acquireEnsureLock(probeCtx, fixture.nodeState, 5*time.Millisecond)
	cancel()
	if probe != nil {
		_ = probe.close()
	}
	abortErr := transition.Abort()
	closeErr := <-closed
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		t.Fatalf("ensure permit released while mesh Close was blocked: (%v,%v)", probe, probeErr)
	}
	if abortErr != nil || closeErr != nil {
		t.Fatalf("settle transition and Close = (%v,%v)", abortErr, closeErr)
	}
	assertClosedDescriptor(t, childFD)
	reacquired := acquirePermitTestEnsureLock(t, fixture.nodeState)
	if err := reacquired.close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

func TestOpenDaemonRemainsAnExplicitMeshlessCompositionSurface(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install, Credentials: testProfileCredentials{},
		Control: newTestControlTransportFactory()})
	if err != nil {
		t.Fatal(err)
	}
	if daemon.mesh != nil {
		t.Fatal("low-level OpenDaemon unexpectedly created a mesh runtime")
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := inspectMeshEndpointState(fixture.nodeState, fixture.identity.PeerID())
	if err != nil || state.stateKind() != meshEndpointStateAbsent {
		t.Fatalf("low-level endpoint state = (%#v,%v)", state, err)
	}
}

func publishDaemonTestMeshPending(t *testing.T, fixture daemonFixture) meshEndpointPending {
	t.Helper()
	pending, err := newMeshEndpointPending(meshEndpointPendingSpec{PeerID: fixture.identity.PeerID(),
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	created, err := publishMeshEndpointPending(fixture.nodeState, pending)
	if err != nil || !created {
		t.Fatalf("publish pending = (%t,%v)", created, err)
	}
	return pending
}

func publishDaemonTestMeshFinal(t *testing.T, fixture daemonFixture,
	retire bool,
) meshEndpoint {
	t.Helper()
	pending := publishDaemonTestMeshPending(t, fixture)
	listen, err := ma.NewMultiaddr(pending.listenAddresses()[0])
	if err != nil {
		t.Fatal(err)
	}
	bound, err := peer.BindMeshHost(context.Background(), fixture.identity.PrivateKey(),
		peer.MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{listen}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := bound.Endpoint()
	if err != nil {
		_ = bound.Close()
		t.Fatal(err)
	}
	final, err := newMeshEndpoint(meshEndpointSpec{PeerID: snapshot.PeerID(),
		ListenAddrs: snapshot.ListenAddrs(), AdvertisedAddrs: snapshot.AdvertisedAddrs()})
	if err != nil {
		_ = bound.Close()
		t.Fatal(err)
	}
	created, err := publishMeshEndpointFinal(fixture.nodeState, pending, final)
	if err != nil || !created {
		_ = bound.Close()
		t.Fatalf("publish final = (%t,%v)", created, err)
	}
	if err := bound.Close(); err != nil {
		t.Fatal(err)
	}
	if retire {
		if err := retireMeshEndpointPending(fixture.nodeState, pending, final); err != nil {
			t.Fatal(err)
		}
	}
	return final
}

func installDaemonTestLaunchPermit(t *testing.T, nodeState string) (*ensureLock, int) {
	t.Helper()
	parent := acquirePermitTestEnsureLock(t, nodeState)
	childFD, err := unix.Dup(int(parent.file.Fd()))
	if err != nil {
		_ = parent.close()
		t.Fatal(err)
	}
	t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(childFD))
	return parent, childFD
}

func installSoleDaemonTestLaunchPermit(t *testing.T, nodeState string) int {
	t.Helper()
	fd, err := unix.Open(filepath.Join(nodeState, ensureLockName),
		unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(ensureLockMode))
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Fchmod(fd, uint32(ensureLockMode)); err != nil {
		_ = unix.Close(fd)
		t.Fatal(err)
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = unix.Close(fd)
		t.Fatal(err)
	}
	t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(fd))
	return fd
}

func daemonTestManagedOptions(fixture daemonFixture) DaemonOptions {
	return DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install,
		Credentials: testProfileCredentials{}, Control: newTestControlTransportFactory(),
		Attachments:        &testWakeAttachmentFilesystem{},
		WakeAdapterFactory: permitTestWakeFactory()}
}

func advanceDaemonTestOriginSequence(t *testing.T, nodeState string) {
	t.Helper()
	path := filepath.Join(nodeState, "node.db")
	u := url.URL{Scheme: "file", Path: path}
	query := u.Query()
	query.Set("mode", "rw")
	u.RawQuery = query.Encode()
	database, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("UPDATE node SET next_origin_seq=2 WHERE singleton=1"); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Chmod(path+suffix, 0o600); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

func assertDaemonTestMeshPort(t *testing.T, endpoint meshEndpoint, available bool) {
	t.Helper()
	listener, listenErr := openDaemonTestMeshPort(endpoint)
	if available {
		if listenErr != nil {
			t.Fatalf("mesh port remained held: %v", listenErr)
		}
		_ = listener.Close()
		return
	}
	if listenErr == nil {
		_ = listener.Close()
		t.Fatal("mesh port was not held by the managed daemon")
	}
}

func listenDaemonTestMeshPort(t *testing.T, endpoint meshEndpoint) net.Listener {
	t.Helper()
	listener, err := openDaemonTestMeshPort(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func openDaemonTestMeshPort(endpoint meshEndpoint) (net.Listener, error) {
	address, err := ma.NewMultiaddr(endpoint.listenAddresses()[0])
	if err != nil {
		return nil, err
	}
	ip, err := address.ValueForProtocol(ma.P_IP4)
	if err != nil {
		return nil, err
	}
	port, err := address.ValueForProtocol(ma.P_TCP)
	if err != nil {
		return nil, err
	}
	return net.Listen("tcp4", net.JoinHostPort(ip, port))
}
