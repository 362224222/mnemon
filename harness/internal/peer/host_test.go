package peer

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

func TestNodeHostWiresLimitsGatesNotificationsAndManagedStreams(t *testing.T) {
	local := testAuthorityPeer(t, "node-host-local")
	remote := testAuthorityPeer(t, "node-host-remote")
	unknown := testAuthorityPeer(t, "node-host-unknown")
	channel := testAuthorityChannel(t, "node-host-channel", model.BindingActive, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if node.managedRuntimeHost() == nil || node.managedRuntimeHost().ID() != local.libp2pID ||
		len(node.managedRuntimeHost().Addrs()) == 0 || node.managedRuntimeHost().Network().ResourceManager() == nil || node.Gater() == nil {
		t.Fatal("Node Host did not expose its identity, listener, resource manager and authority gate")
	}
	if _, managed := node.managedRuntimeHost().(*managedHost); !managed {
		t.Fatal("Node Host exposed the raw libp2p Host instead of its managed stream surface")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remoteHost := newBarePeerHost(t, remote)
	defer remoteHost.Close()
	unknownHost := newBarePeerHost(t, unknown)
	defer unknownHost.Close()

	eventAccepted := make(chan network.Stream, 1)
	releaseEvent := make(chan struct{})
	node.managedRuntimeHost().SetStreamHandler(EventsProtocol, func(stream network.Stream) {
		eventAccepted <- stream
		<-releaseEvent
		_ = stream.Close()
	})
	node.managedRuntimeHost().SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Write([]byte{'c'})
		_ = stream.Close()
	})
	if err := node.managedRuntimeHost().Connect(ctx, libp2ppeer.AddrInfo{ID: remoteHost.ID(),
		Addrs: remoteHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	eventStream, err := remoteHost.NewStream(ctx, node.managedRuntimeHost().ID(), EventsProtocol)
	if err != nil {
		t.Fatal(err)
	}
	// v0.47 lazily sends the multistream header on first I/O when identify has
	// already cached protocol support.
	if _, err := eventStream.Write([]byte{'e'}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-eventAccepted:
	case <-time.After(2 * time.Second):
		t.Fatal("active Peer did not reach the managed Event handler")
	}
	if err := node.managedRuntimeHost().Connect(ctx, libp2ppeer.AddrInfo{ID: unknownHost.ID(),
		Addrs: unknownHost.Addrs()}); err == nil {
		t.Fatal("unknown outbound Peer bypassed the connection gater")
	}
	if err := unknownHost.Connect(ctx, libp2ppeer.AddrInfo{ID: node.managedRuntimeHost().ID(),
		Addrs: node.managedRuntimeHost().Addrs()}); err != nil {
		t.Fatal(err)
	}
	waitUnknownConnections(t, node.Gater(), 1)
	channelStream, err := unknownHost.NewStream(ctx, node.managedRuntimeHost().ID(), ChannelProtocol)
	if err != nil {
		t.Fatal(err)
	}
	_ = channelStream.SetReadDeadline(time.Now().Add(time.Second))
	one := make([]byte, 1)
	if count, err := channelStream.Read(one); err != nil || count != 1 || one[0] != 'c' {
		t.Fatalf("unknown enrollment stream response = (%q, %v)", one[:count], err)
	}
	_ = channelStream.Close()
	unknownEvent, unknownEventErr := unknownHost.NewStream(ctx, node.managedRuntimeHost().ID(), EventsProtocol)
	if unknownEventErr == nil {
		_ = unknownEvent.SetReadDeadline(time.Now().Add(time.Second))
		if _, err := unknownEvent.Read(one); err == nil {
			t.Fatal("unknown Peer reached a data-plane Event stream")
		}
		_ = unknownEvent.Close()
	}

	pending := channel
	pending.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingPending}}
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{pending}}); err != nil {
		t.Fatal(err)
	}
	if err := node.ReconcileConnections(); err != nil {
		t.Fatal(err)
	}
	if node.managedRuntimeHost().Network().Connectedness(remoteHost.ID()) != network.Connected {
		t.Fatal("pending Peer lost its physical enrollment connection")
	}
	_ = eventStream.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := eventStream.Read(one); err == nil {
		t.Fatal("pending-only Peer retained an existing Event data-plane stream")
	}
	close(releaseEvent)

	if err := unknownHost.Network().ClosePeer(node.managedRuntimeHost().ID()); err != nil {
		t.Fatal(err)
	}
	waitUnknownConnections(t, node.Gater(), 0)
	revoked := channel
	revoked.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingRevoked}}
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{revoked}}); err != nil {
		t.Fatal(err)
	}
	if err := node.ReconcileConnections(); err != nil {
		t.Fatal(err)
	}
	waitPeerDisconnected(t, node.managedRuntimeHost(), remoteHost.ID())
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if err := node.Close(); err != nil {
		t.Fatal("Node Host Close was not idempotent")
	}
}

func TestManagedHostPostChecksAuthorityAfterBlockedOutboundOpen(t *testing.T) {
	local := testAuthorityPeer(t, "managed-stream-local")
	remote := testAuthorityPeer(t, "managed-stream-remote")
	channel := testAuthorityChannel(t, "managed-stream-channel", model.BindingActive, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	localHost := newBarePeerHost(t, local)
	defer localHost.Close()
	remoteHost := newBarePeerHost(t, remote)
	defer remoteHost.Close()
	remoteHost.SetStreamHandler(EventsProtocol, func(stream network.Stream) {
		buffer := make([]byte, 1)
		_, _ = stream.Read(buffer)
		_ = stream.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := localHost.Connect(ctx, libp2ppeer.AddrInfo{ID: remoteHost.ID(),
		Addrs: remoteHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	blocking := &blockingNewStreamHost{Host: localHost, entered: make(chan struct{}, 1),
		release: make(chan struct{})}
	gater := NewConnectionGater(authority)
	managed := &managedHost{Host: blocking, gater: gater}
	result := make(chan error, 1)
	go func() {
		stream, err := managed.NewStream(ctx, remoteHost.ID(), EventsProtocol)
		if stream != nil {
			_ = stream.Close()
		}
		result <- err
	}()
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("outbound stream did not reach the controlled creation boundary")
	}
	revoked := channel
	revoked.Bindings = []BindingAuthoritySnapshot{{PeerID: remote.modelID, State: model.BindingRevoked}}
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{revoked}}); err != nil {
		t.Fatal(err)
	}
	if err := gater.ReconcileConnections(localHost.Network()); err != nil {
		t.Fatal(err)
	}
	close(blocking.release)
	select {
	case err := <-result:
		if !errors.Is(err, ErrNodeHost) {
			t.Fatalf("post-revoke NewStream() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-revoke stream creation did not complete")
	}
}

func TestNodeHostCloseQuiescesUnknownEnrollmentState(t *testing.T) {
	local := testAuthorityPeer(t, "node-host-close-local")
	unknown := testAuthorityPeer(t, "node-host-close-unknown")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	unknownHost := newBarePeerHost(t, unknown)
	defer unknownHost.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := unknownHost.Connect(ctx, libp2ppeer.AddrInfo{ID: node.managedRuntimeHost().ID(),
		Addrs: node.managedRuntimeHost().Addrs()}); err != nil {
		t.Fatal(err)
	}
	waitUnknownConnections(t, node.Gater(), 1)
	gater := node.Gater()
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	if gater.UnknownEnrollmentSlots() != 0 || gater.UnknownConnections() != 0 {
		t.Fatal("Node Host close returned with live unknown enrollment state")
	}
}

func TestNewNodeHostRejectsInvalidIdentityAndListenConfiguration(t *testing.T) {
	local := testAuthorityPeer(t, "node-host-invalid-local")
	remote := testAuthorityPeer(t, "node-host-invalid-remote")
	authority, _ := NewAuthority(local.modelID)
	listen := []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")}
	if node, err := NewNodeHost(remote.libp2pPrivate, authority, listen); node != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("mismatched identity NewNodeHost() = (%v, %v)", node, err)
	}
	nonEd25519, _, err := libp2pcrypto.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if node, err := NewNodeHost(nonEd25519, authority, listen); node != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("non-Ed25519 NewNodeHost() = (%v, %v)", node, err)
	}
	if node, err := NewNodeHost(local.libp2pPrivate, authority, nil); node != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("empty-listen NewNodeHost() = (%v, %v)", node, err)
	}
	if node, err := NewNodeHost(local.libp2pPrivate, authority, []ma.Multiaddr{nil}); node != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("nil-listen NewNodeHost() = (%v, %v)", node, err)
	}
}

type blockingNewStreamHost struct {
	host.Host
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (blocking *blockingNewStreamHost) NewStream(ctx context.Context, peerID libp2ppeer.ID,
	protocolIDs ...protocol.ID,
) (network.Stream, error) {
	blocking.once.Do(func() { blocking.entered <- struct{}{} })
	select {
	case <-blocking.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return blocking.Host.NewStream(ctx, peerID, protocolIDs...)
}

func newBarePeerHost(t *testing.T, identity authorityTestPeer) host.Host {
	t.Helper()
	nodeHost, err := libp2p.New(libp2p.Identity(identity.libp2pPrivate),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	return nodeHost
}

func waitUnknownConnections(t *testing.T, gater *ConnectionGater, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if gater.UnknownConnections() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("unknown connections = %d, want %d", gater.UnknownConnections(), want)
}
