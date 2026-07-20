package peer

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

func TestEnrollmentTransportDoesNotExposeFullHostOrCustomProtocols(t *testing.T) {
	t.Parallel()
	if _, exposed := reflect.TypeOf((*NodeHost)(nil)).MethodByName("Host"); exposed {
		t.Fatal("NodeHost exported the full libp2p Host graph")
	}
	if _, exposed := reflect.TypeOf((*MeshRuntime)(nil)).MethodByName("Host"); exposed {
		t.Fatal("MeshRuntime exported the full libp2p Host graph")
	}
	local := testAuthorityPeer(t, "host-surface-local")
	owner := testAuthorityPeer(t, "host-surface-owner")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	address := ma.StringCast("/ip4/127.0.0.1/tcp/44301")
	permit, err := node.gater.acquireOutboundEnrollmentPermit(context.Background(),
		testEnrollmentTransportPermitSpec(t, owner, []string{address.String()}, "host-surface"), nil)
	if err != nil || !node.gater.claimOutboundEnrollmentStream(permit) {
		t.Fatalf("claim test permit = %v", err)
	}
	defer node.gater.releaseOutboundEnrollmentPermit(permit)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if stream, err := node.managedRuntimeHost().NewStream(ctx, owner.libp2pID); stream != nil ||
		!errors.Is(err, ErrNodeHost) {
		t.Fatalf("zero-protocol stream through claimed permit = (%v, %v)", stream, err)
	}
	custom := protocol.ID("/mnemon/unreviewed/1")
	if stream, err := node.managedRuntimeHost().NewStream(ctx, owner.libp2pID, custom); stream != nil ||
		!errors.Is(err, ErrNodeHost) {
		t.Fatalf("custom protocol through claimed permit = (%v, %v)", stream, err)
	}
}

func TestNodeHostExactEnrollmentOpenerIsSingleUseAndNotGenericAuthority(t *testing.T) {
	local := testAuthorityPeer(t, "host-permit-local")
	owner := testAuthorityPeer(t, "host-permit-owner")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ownerHost := newBarePeerHost(t, owner)
	defer ownerHost.Close()
	ownerHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Write([]byte{'e'})
		_ = stream.Close()
	})
	addresses := ownerHost.Addrs()
	sort.Slice(addresses, func(left, right int) bool {
		return addresses[left].String() < addresses[right].String()
	})
	rawAddresses := make([]string, len(addresses))
	for index, address := range addresses {
		rawAddresses[index] = address.String()
	}
	spec := testEnrollmentTransportPermitSpec(t, owner, rawAddresses, "host")
	permit, err := node.gater.acquireOutboundEnrollmentPermit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer node.gater.releaseOutboundEnrollmentPermit(permit)
	node.managedRuntimeHost().Peerstore().AddAddrs(owner.libp2pID, addresses, peerstore.TempAddrTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if stream, err := node.managedRuntimeHost().NewStream(ctx, owner.libp2pID, ChannelProtocol); stream != nil ||
		!errors.Is(err, ErrNodeHost) {
		t.Fatalf("generic managed NewStream() = (%v, %v), want authority rejection", stream, err)
	}
	stream, err := node.openEnrollmentStream(ctx, permit)
	if err != nil {
		t.Fatal(err)
	}
	one := make([]byte, 1)
	if count, err := stream.Read(one); err != nil || count != 1 || one[0] != 'e' {
		t.Fatalf("exact enrollment response = (%q, %v)", one[:count], err)
	}
	_ = stream.Close()
	if replay, err := node.openEnrollmentStream(ctx, permit); replay != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("replayed exact opener = (%v, %v), want consumed rejection", replay, err)
	}
}

func TestNodeHostExactEnrollmentOpenerRejectsAlternateExistingConnection(t *testing.T) {
	local := testAuthorityPeer(t, "host-alternate-local")
	owner := testAuthorityPeer(t, "host-alternate-owner")
	localHost := newBarePeerHost(t, local)
	defer localHost.Close()
	ownerHost := newBarePeerHost(t, owner)
	defer ownerHost.Close()
	ownerHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		one := make([]byte, 1)
		_, _ = stream.Read(one)
		_ = stream.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := localHost.Connect(ctx, libp2ppeer.AddrInfo{ID: ownerHost.ID(),
		Addrs: ownerHost.Addrs()}); err != nil {
		t.Fatal(err)
	}
	authority, _ := NewAuthority(local.modelID)
	gater := NewConnectionGater(authority)
	defer gater.shutdown()
	// The signed address is canonical and bound to this owner identity, but it
	// is deliberately not the address of the already-open connection.
	signed := "/ip4/127.0.0.1/tcp/49999"
	spec := testEnrollmentTransportPermitSpec(t, owner, []string{signed}, "alternate")
	permit, err := gater.acquireOutboundEnrollmentPermit(ctx, spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer gater.releaseOutboundEnrollmentPermit(permit)
	node := &NodeHost{host: localHost, managed: &managedHost{Host: localHost, gater: gater}, gater: gater}
	stream, err := node.openEnrollmentStream(ctx, permit)
	if stream != nil || !errors.Is(err, ErrNodeHost) {
		t.Fatalf("alternate-connection exact open = (%v, %v), want address-bound rejection", stream, err)
	}
}

func TestNodeHostExactEnrollmentOpenerReusesInboundConnectionOnlyForLocalOutboundStream(t *testing.T) {
	local := testAuthorityPeer(t, "host-inbound-reuse-local")
	owner := testAuthorityPeer(t, "host-inbound-reuse-owner")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ownerHost := newBarePeerHost(t, owner)
	defer ownerHost.Close()
	ownerHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) { _ = stream.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ownerHost.Connect(ctx, libp2ppeer.AddrInfo{ID: node.host.ID(), Addrs: node.host.Addrs()}); err != nil {
		t.Fatal(err)
	}
	connections := node.host.Network().ConnsToPeer(owner.libp2pID)
	if len(connections) != 1 || connections[0].Stat().Direction != network.DirInbound {
		t.Fatalf("fixture connection direction = %v, want one inbound connection", connections)
	}
	addresses := ownerHost.Addrs()
	sort.Slice(addresses, func(left, right int) bool { return addresses[left].String() < addresses[right].String() })
	raw := make([]string, len(addresses))
	for index := range addresses {
		raw[index] = addresses[index].String()
	}
	permit, err := node.gater.acquireOutboundEnrollmentPermit(ctx,
		testEnrollmentTransportPermitSpec(t, owner, raw, "inbound-reuse"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer node.gater.releaseOutboundEnrollmentPermit(permit)
	node.host.Peerstore().AddAddrs(owner.libp2pID, addresses, peerstore.TempAddrTTL)
	stream, err := node.openEnrollmentStream(ctx, permit)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if stream.Conn().Stat().Direction != network.DirInbound ||
		stream.Stat().Direction != network.DirOutbound {
		t.Fatalf("reused conn/stream direction = %s/%s, want inbound/outbound",
			stream.Conn().Stat().Direction, stream.Stat().Direction)
	}
}

func TestNodeHostPermitReleaseResetsPromotedStreamButKeepsDurableConnection(t *testing.T) {
	local := testAuthorityPeer(t, "host-promoted-stream-local")
	owner := testAuthorityPeer(t, "host-promoted-stream-owner")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	ownerHost := newBarePeerHost(t, owner)
	defer ownerHost.Close()
	remoteStream := make(chan network.Stream, 1)
	ownerHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		_, _ = stream.Read(make([]byte, 1))
		remoteStream <- stream
	})
	addresses := ownerHost.Addrs()
	sort.Slice(addresses, func(left, right int) bool { return addresses[left].String() < addresses[right].String() })
	raw := make([]string, len(addresses))
	for index := range addresses {
		raw[index] = addresses[index].String()
	}
	permit, err := node.gater.acquireOutboundEnrollmentPermit(context.Background(),
		testEnrollmentTransportPermitSpec(t, owner, raw, "promoted-stream"), nil)
	if err != nil {
		t.Fatal(err)
	}
	node.host.Peerstore().AddAddrs(owner.libp2pID, addresses, peerstore.TempAddrTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := node.openEnrollmentStream(ctx, permit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{'p'}); err != nil {
		t.Fatal(err)
	}
	var remote network.Stream
	select {
	case remote = <-remoteStream:
	case <-ctx.Done():
		t.Fatal("remote handler did not observe promoted enrollment stream")
	}
	channel := testAuthorityChannel(t, "host-promoted-stream-channel", model.BindingPending, local, owner)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	if !node.gater.releaseOutboundEnrollmentPermit(permit) {
		t.Fatal("release promoted permit")
	}
	_ = remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := remote.Read(make([]byte, 1)); err == nil {
		t.Fatal("durable promotion allowed the exact enrollment stream to survive release")
	}
	if node.host.Network().Connectedness(owner.libp2pID) != network.Connected ||
		len(node.host.Network().ConnsToPeer(owner.libp2pID)) == 0 {
		t.Fatal("exact stream release closed the independently durable physical connection")
	}
	_ = stream.Close()
}
