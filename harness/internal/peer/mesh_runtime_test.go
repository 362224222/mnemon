package peer

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshRuntimeSnapshotsLocalEnrollmentMultiaddrs(t *testing.T) {
	identity := testkit.NewIdentity(t, "mesh-runtime-local-enrollment-addresses")
	st := openPeerMeshStore(t, identity, peerMeshTime(t, "2026-07-20T04:00:00Z"))
	runtime := newTestMeshRuntime(t, context.Background(), identity,
		readMeshRuntimeAuthority(t, st))

	wantAddrs := runtime.managedRuntimeHost().Addrs()
	want := make([]string, len(wantAddrs))
	for index, address := range wantAddrs {
		want[index] = address.String()
	}
	sort.Strings(want)
	first, err := runtime.LocalEnrollmentMultiaddrs()
	if err != nil || !reflect.DeepEqual(first, want) {
		t.Fatalf("LocalEnrollmentMultiaddrs() = (%v,%v), want %v", first, err, want)
	}
	if _, err := model.AdvertisedAddressDigest(first); err != nil {
		t.Fatalf("local enrollment snapshot is invalid: %v", err)
	}
	first[0] = "/ip4/127.0.0.1/tcp/1"
	second, err := runtime.LocalEnrollmentMultiaddrs()
	if err != nil || !reflect.DeepEqual(second, want) {
		t.Fatalf("LocalEnrollmentMultiaddrs() after caller mutation = (%v,%v), want %v",
			second, err, want)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if addresses, err := runtime.LocalEnrollmentMultiaddrs(); addresses != nil ||
		!errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("LocalEnrollmentMultiaddrs() after close = (%v,%v)", addresses, err)
	}
}

func TestMeshRuntimeStartsOneRouterAndJoinsEveryActiveChannel(t *testing.T) {
	ctx := context.Background()
	owner := testkit.NewIdentity(t, "mesh-runtime-owner")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T04:00:00Z"))
	alpha := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-alpha", owner,
		peerMeshTime(t, "2026-07-18T04:00:00Z"))
	beta := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-beta", owner,
		peerMeshTime(t, "2026-07-18T04:01:00Z"))
	createPeerMeshChannel(t, st, alpha, "runtime-alpha")
	createPeerMeshChannel(t, st, beta, "runtime-beta")
	mesh := readMeshRuntimeAuthority(t, st)
	runtime := newTestMeshRuntime(t, ctx, owner, mesh)
	if runtime.managedRuntimeHost() == nil ||
		runtime.managedRuntimeHost().ID().String() != owner.PeerID().String() ||
		!runtime.HasCurrentSession(alpha.Channel().ID()) ||
		!runtime.HasCurrentSession(beta.Channel().ID()) {
		t.Fatal("runtime did not expose one identity-bound router with both active topics")
	}
	if _, err := runtime.Session(alpha.Channel().ID()); err != nil {
		t.Fatalf("acquire current session: %v", err)
	}
	if protocols := runtime.managedRuntimeHost().Mux().Protocols(); countProtocol(protocols, GossipProtocol) != 1 {
		t.Fatalf("Gossip protocol handler count = %d, want one", countProtocol(protocols, GossipProtocol))
	}
}

func TestMeshRuntimeReconcileRollsBackAddressesAndJoinsNewChannel(t *testing.T) {
	ctx := context.Background()
	owner := testkit.NewIdentity(t, "mesh-runtime-reconcile-owner")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T05:00:00Z"))
	alpha := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-reconcile-alpha", owner,
		peerMeshTime(t, "2026-07-18T05:00:00Z"))
	createPeerMeshChannel(t, st, alpha, "runtime-reconcile-alpha")
	runtime := newTestMeshRuntime(t, ctx, owner, readMeshRuntimeAuthority(t, st))

	remote := alpha.AppendActive(t, "mesh-runtime-reconcile-remote")
	mergePeerMeshRoster(t, st, alpha, remote.Member(), remote.Member().CreatedAt())
	beta := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-reconcile-beta", owner,
		peerMeshTime(t, "2026-07-18T05:01:00Z"))
	createPeerMeshChannel(t, st, beta, "runtime-reconcile-beta")
	candidate := readMeshRuntimeAuthority(t, st)
	remoteID := meshRuntimeLibp2pID(t, remote.Identity().PeerID())
	transition, err := runtime.BeginAuthorityTransition(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("candidate Peer addresses were not staged for the durable window")
	}
	if !runtime.mu.TryLock() {
		t.Fatal("durable window retained MeshRuntime mutex")
	}
	runtime.mu.Unlock()
	active, err := runtime.BeginAuthorityTransition(candidate)
	if active != transition || !errors.Is(err, ErrMeshAuthorityTransitionInProgress) {
		t.Fatalf("transition reentry = (%p, %v), want active %p", active, err, transition)
	}
	if err := transition.Abort(); err != nil || transition.Wait() != nil {
		t.Fatalf("abort authority transition = %v / wait %v", err, transition.Wait())
	}
	if runtime.HasCurrentSession(beta.Channel().ID()) ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) != 0 ||
		runtime.authority.CanConnect(remoteID) {
		t.Fatal("aborted durable transition leaked candidate topic, addresses or authority")
	}
	transition, err = runtime.BeginAuthorityTransition(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Install(); err != nil {
		t.Fatal(err)
	}
	if !runtime.HasCurrentSession(beta.Channel().ID()) ||
		len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 ||
		!runtime.authority.CanConnect(remoteID) {
		t.Fatal("successful reconcile did not atomically expose candidate mesh")
	}
}

func TestMeshRuntimeKeepsOverlappingPeerAddressesUntilLastChannelRevokes(t *testing.T) {
	ctx := context.Background()
	owner := testkit.NewIdentity(t, "mesh-runtime-overlap-owner")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T06:00:00Z"))
	alpha := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-overlap-alpha", owner,
		peerMeshTime(t, "2026-07-18T06:00:00Z"))
	beta := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-overlap-beta", owner,
		peerMeshTime(t, "2026-07-18T06:01:00Z"))
	createPeerMeshChannel(t, st, alpha, "runtime-overlap-alpha")
	createPeerMeshChannel(t, st, beta, "runtime-overlap-beta")
	alphaRemote := alpha.AppendActive(t, "mesh-runtime-shared")
	betaRemote := beta.AppendActive(t, "mesh-runtime-shared")
	if alphaRemote.Identity().PeerID() != betaRemote.Identity().PeerID() {
		t.Fatal("overlap fixture did not reuse one Peer identity")
	}
	mergePeerMeshRoster(t, st, alpha, alphaRemote.Member(), alphaRemote.Member().CreatedAt())
	mergePeerMeshRoster(t, st, beta, betaRemote.Member(), betaRemote.Member().CreatedAt())
	runtime := newTestMeshRuntime(t, ctx, owner, readMeshRuntimeAuthority(t, st))
	remoteID := meshRuntimeLibp2pID(t, alphaRemote.Identity().PeerID())
	remoteHost := newEnrollmentTestHost(t, alphaRemote.Identity())
	if err := remoteHost.Connect(ctx, libp2ppeer.AddrInfo{ID: runtime.managedRuntimeHost().ID(),
		Addrs: runtime.managedRuntimeHost().Addrs()}); err != nil {
		t.Fatal(err)
	}
	if len(runtime.managedRuntimeHost().Network().ConnsToPeer(remoteID)) == 0 {
		t.Fatal("shared Peer did not establish the pre-revoke physical connection")
	}
	unchangedBeta, err := runtime.Session(beta.Channel().ID())
	if err != nil {
		t.Fatal(err)
	}

	alphaRevoked := alpha.AppendTerminal(t, alphaRemote.Identity().PeerID(), model.MemberRevoked)
	mergePeerMeshRoster(t, st, alpha, alphaRevoked.Member(), alphaRevoked.Member().CreatedAt())
	transition, err := runtime.BeginAuthorityTransition(readMeshRuntimeAuthority(t, st))
	if err != nil {
		t.Fatal(err)
	}
	duringTransition, err := runtime.Session(beta.Channel().ID())
	if err != nil || duringTransition != unchangedBeta {
		t.Fatalf("unchanged Channel session during durable window = (%p, %v), want %p",
			duringTransition, err, unchangedBeta)
	}
	if err := transition.Install(); err != nil {
		t.Fatal(err)
	}
	if len(runtime.managedRuntimeHost().Peerstore().Addrs(remoteID)) == 0 ||
		!runtime.authority.CanConnect(remoteID) ||
		len(runtime.managedRuntimeHost().Network().ConnsToPeer(remoteID)) == 0 {
		t.Fatal("one Channel revoke removed a Peer still authorized by overlapping Channel")
	}

	betaRevoked := beta.AppendTerminal(t, betaRemote.Identity().PeerID(), model.MemberRevoked)
	mergePeerMeshRoster(t, st, beta, betaRevoked.Member(), betaRevoked.Member().CreatedAt())
	installMeshAuthority(t, runtime, readMeshRuntimeAuthority(t, st))
	waitPeerDisconnected(t, runtime.managedRuntimeHost(), remoteID)
	if len(runtime.addresses[remoteID]) != 0 || runtime.authority.CanConnect(remoteID) ||
		len(runtime.managedRuntimeHost().Network().ConnsToPeer(remoteID)) != 0 {
		t.Fatal("last Channel revoke retained stale physical Peer authority or connection")
	}
}

func TestMeshRuntimeRejectsAddressIdentityMismatchAndClosesOnce(t *testing.T) {
	owner := testkit.NewIdentity(t, "mesh-runtime-close-owner")
	other := testkit.NewIdentity(t, "mesh-runtime-address-other")
	otherID := meshRuntimeLibp2pID(t, other.PeerID())
	address, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4001/p2p/" + otherID.String())
	if err != nil {
		t.Fatal(err)
	}
	expected := meshRuntimeLibp2pID(t, owner.PeerID())
	if _, err := canonicalPeerAddresses(expected, address.String()); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("mismatched /p2p address error = %v", err)
	}

	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T07:00:00Z"))
	channel := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-close", owner,
		peerMeshTime(t, "2026-07-18T07:00:00Z"))
	createPeerMeshChannel(t, st, channel, "runtime-close")
	runtime := newTestMeshRuntime(t, context.Background(), owner, readMeshRuntimeAuthority(t, st))
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Session(channel.Channel().ID()); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("session after close error = %v", err)
	}
}

func TestMeshRuntimeTerminalSignalClosesOnceWithConcurrentClose(t *testing.T) {
	owner := testkit.NewIdentity(t, "mesh-runtime-terminal-close")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-20T10:00:00Z"))
	channel := testkit.NewSignedChannelForOwnerAt(t, "mesh-runtime-terminal-close", owner,
		peerMeshTime(t, "2026-07-20T10:00:00Z"))
	createPeerMeshChannel(t, st, channel, "runtime-terminal-close")
	runtime := newTestMeshRuntime(t, context.Background(), owner,
		readMeshRuntimeAuthority(t, st))
	signal := runtime.terminalSignal()
	if signal != runtime.terminalSignal() {
		t.Fatal("terminalSignal did not retain one immutable channel")
	}
	select {
	case <-signal:
		t.Fatal("live MeshRuntime exposed a closed terminal signal")
	default:
	}
	if err := runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("terminalError before Close = %v", err)
	}

	const closers = 32
	start := make(chan struct{})
	errs := make(chan error, closers)
	var callers sync.WaitGroup
	callers.Add(closers)
	for index := 0; index < closers; index++ {
		go func() {
			defer callers.Done()
			<-start
			errs <- runtime.Close()
		}()
	}
	close(start)
	callers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Close error = %v", err)
		}
	}
	select {
	case <-signal:
	default:
		t.Fatal("normal Close did not publish authority termination")
	}
	if err := runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("normal terminalError = %v, want ErrMeshRuntime", err)
	}
	if signal != runtime.terminalSignal() {
		t.Fatal("Close replaced the terminal signal channel")
	}
}

func TestMeshRuntimeZeroTerminalSignalFailsClosed(t *testing.T) {
	for name, runtime := range map[string]*MeshRuntime{"nil": nil, "malformed": {}} {
		t.Run(name, func(t *testing.T) {
			select {
			case <-runtime.terminalSignal():
			default:
				t.Fatal("unavailable runtime returned a live terminal signal")
			}
			if err := runtime.terminalError(); !errors.Is(err, ErrMeshRuntime) {
				t.Fatalf("terminalError = %v, want ErrMeshRuntime", err)
			}
		})
	}
}

func newTestMeshRuntime(t *testing.T, ctx context.Context, identity testkit.Identity,
	mesh store.ChannelMeshAuthority,
) *MeshRuntime {
	t.Helper()
	key, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	listen, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewMeshRuntime(ctx, key, []ma.Multiaddr{listen}, mesh)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Errorf("close mesh runtime: %v", err)
		}
	})
	return runtime
}

func installMeshAuthority(t *testing.T, runtime *MeshRuntime, mesh store.ChannelMeshAuthority) {
	t.Helper()
	transition, err := runtime.BeginAuthorityTransition(mesh)
	if err != nil {
		t.Fatal(err)
	}
	if err := transition.Install(); err != nil {
		t.Fatal(err)
	}
}

func readMeshRuntimeAuthority(t *testing.T, st *store.Store) store.ChannelMeshAuthority {
	t.Helper()
	mesh, err := st.ReadChannelMeshAuthority(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return mesh
}

func meshRuntimeLibp2pID(t *testing.T, value model.PeerID) libp2ppeer.ID {
	t.Helper()
	peerID, err := libp2ppeer.Decode(value.String())
	if err != nil {
		t.Fatal(err)
	}
	return peerID
}

func countProtocol(protocols []protocol.ID, expected protocol.ID) int {
	count := 0
	for _, current := range protocols {
		if current == expected {
			count++
		}
	}
	return count
}
