package peer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshRuntimeAgencyRouteUsesSharedHostWithoutR5Authority(t *testing.T) {
	localIdentity := testkit.NewIdentity(t, "agency-overlay-local")
	remoteIdentity := testkit.NewIdentity(t, "agency-overlay-remote")
	localStore := openPeerMeshStore(t, localIdentity, peerMeshTime(t, "2026-08-03T08:00:00Z"))
	remoteStore := openPeerMeshStore(t, remoteIdentity, peerMeshTime(t, "2026-08-03T08:00:00Z"))
	local := newTestMeshRuntime(t, context.Background(), localIdentity,
		readMeshRuntimeAuthority(t, localStore))
	remote := newTestMeshRuntime(t, context.Background(), remoteIdentity,
		readMeshRuntimeAuthority(t, remoteStore))

	if err := remote.ReconcileAgencyPeers([]AgencyPeerRoute{{PeerID: localIdentity.PeerID(),
		Multiaddrs: hostAddressStrings(local.Host())}}); err != nil {
		t.Fatal(err)
	}
	route := []AgencyPeerRoute{{PeerID: remoteIdentity.PeerID(),
		Multiaddrs: hostAddressStrings(remote.Host())}}
	if err := local.ReconcileAgencyPeers(route); err != nil {
		t.Fatal(err)
	}
	remoteID := meshRuntimeLibp2pID(t, remoteIdentity.PeerID())
	if !local.authority.CanUseAgency(remoteID) || !local.authority.CanConnect(remoteID) ||
		local.authority.CanUseChannelControl(remoteID) || local.authority.CanOpenDataPlane(remoteID) {
		t.Fatal("Agency route did not remain isolated from R5 authority")
	}

	received := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	remote.Host().SetStreamHandler(AgencyDeliveryProtocol, func(stream network.Stream) {
		one := make([]byte, 1)
		_, _ = stream.Read(one)
		received <- struct{}{}
		<-release
		_ = stream.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := local.Host().NewStream(ctx, remoteID, AgencyDeliveryProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte{'a'}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("Agency delivery did not reach the enrolled Peer")
	}
	for _, r5Protocol := range []protocol.ID{ChannelProtocol, EventsProtocol,
		ArtifactsProtocol, GossipProtocol} {
		if denied, deniedErr := local.Host().NewStream(ctx, remoteID, r5Protocol); deniedErr == nil ||
			denied != nil || !errors.Is(deniedErr, ErrNodeHost) {
			t.Fatalf("Agency-only Peer opened R5 protocol %s: stream=%v error=%v",
				r5Protocol, denied, deniedErr)
		}
	}

	if err := local.ReconcileAgencyPeers(nil); err != nil {
		t.Fatal(err)
	}
	if local.authority.CanConnect(remoteID) || local.authority.CanUseAgency(remoteID) ||
		len(local.Host().Peerstore().Addrs(remoteID)) != 0 {
		t.Fatal("route revoke retained physical authority or managed addresses")
	}
	waitPeerDisconnected(t, local.Host(), remoteID)
	_ = stream.Close()
}

func TestMeshRuntimeAgencyRouteRejectsInvalidSnapshotWithoutChangingCurrent(t *testing.T) {
	localIdentity := testkit.NewIdentity(t, "agency-overlay-rollback-local")
	remoteIdentity := testAuthorityPeer(t, "agency-overlay-rollback-remote")
	localStore := openPeerMeshStore(t, localIdentity, peerMeshTime(t, "2026-08-03T09:00:00Z"))
	local := newTestMeshRuntime(t, context.Background(), localIdentity,
		readMeshRuntimeAuthority(t, localStore))
	remote := newBarePeerHost(t, remoteIdentity)
	defer remote.Close()
	routes := []AgencyPeerRoute{{PeerID: remoteIdentity.modelID,
		Multiaddrs: hostAddressStrings(remote)}}
	if err := local.ReconcileAgencyPeers(routes); err != nil {
		t.Fatal(err)
	}
	remoteID := remoteIdentity.libp2pID
	routes[0].Multiaddrs[0] = "/invalid"
	if err := local.Reconcile(readMeshRuntimeAuthority(t, localStore)); err != nil {
		t.Fatal(err)
	}
	if !local.authority.CanUseAgency(remoteID) || len(local.Host().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("caller mutation changed the installed route snapshot")
	}
	duplicate := []AgencyPeerRoute{
		{PeerID: remoteIdentity.modelID, Multiaddrs: hostAddressStrings(remote)},
		{PeerID: remoteIdentity.modelID, Multiaddrs: hostAddressStrings(remote)},
	}
	if err := local.ReconcileAgencyPeers(duplicate); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("duplicate route error = %v", err)
	}
	if !local.authority.CanUseAgency(remoteID) || len(local.Host().Peerstore().Addrs(remoteID)) == 0 {
		t.Fatal("rejected snapshot changed current Agency authority")
	}
}

func TestMeshRuntimeConcurrentAgencySnapshotsRepairToNewestRevision(t *testing.T) {
	localIdentity := testkit.NewIdentity(t, "agency-overlay-generation-local")
	oldPeer := testAuthorityPeer(t, "agency-overlay-generation-old")
	newPeer := testAuthorityPeer(t, "agency-overlay-generation-new")
	oldHost := newBarePeerHost(t, oldPeer)
	defer oldHost.Close()
	newHost := newBarePeerHost(t, newPeer)
	defer newHost.Close()
	st := openPeerMeshStore(t, localIdentity, peerMeshTime(t, "2026-08-03T10:00:00Z"))
	runtime := newTestMeshRuntime(t, context.Background(), localIdentity,
		readMeshRuntimeAuthority(t, st))

	runtime.gossip.mu.Lock()
	locked := true
	defer func() {
		if locked {
			runtime.gossip.mu.Unlock()
		}
	}()
	oldResult := make(chan error, 1)
	go func() {
		oldResult <- runtime.ReconcileAgencyPeers([]AgencyPeerRoute{{PeerID: oldPeer.modelID,
			Multiaddrs: hostAddressStrings(oldHost)}})
	}()
	waitMeshRuntimeApplied(t, runtime, oldPeer.libp2pID, true)
	newResult := make(chan error, 1)
	go func() {
		newResult <- runtime.ReconcileAgencyPeers([]AgencyPeerRoute{{PeerID: newPeer.modelID,
			Multiaddrs: hostAddressStrings(newHost)}})
	}()
	waitMeshRuntimeApplied(t, runtime, newPeer.libp2pID, true)
	runtime.gossip.mu.Unlock()
	locked = false
	if err := <-newResult; err != nil {
		t.Fatalf("newest Agency snapshot: %v", err)
	}
	if err := <-oldResult; err != nil {
		t.Fatalf("late Agency snapshot repair: %v", err)
	}
	if runtime.authority.CanUseAgency(oldPeer.libp2pID) ||
		len(runtime.Host().Peerstore().Addrs(oldPeer.libp2pID)) != 0 {
		t.Fatal("late Agency projection retained the superseded route")
	}
	if !runtime.authority.CanUseAgency(newPeer.libp2pID) ||
		len(runtime.Host().Peerstore().Addrs(newPeer.libp2pID)) == 0 {
		t.Fatal("late Agency projection regressed the newest route")
	}
}

func TestNodeHostAgencyRevokeResetsOnlyAgencyStreamsWhenChannelRemains(t *testing.T) {
	local := testAuthorityPeer(t, "agency-revoke-local")
	remote := testAuthorityPeer(t, "agency-revoke-remote")
	channel := testAuthorityChannel(t, "agency-revoke-channel", model.BindingActive, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels:    []ChannelAuthoritySnapshot{channel},
		AgencyPeers: []model.PeerID{remote.modelID}}); err != nil {
		t.Fatal(err)
	}
	node, remoteHost, agencyStream := prepareAgencyRevokeConnection(t, authority, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	if err := node.ReconcileConnections(); err != nil {
		t.Fatal(err)
	}
	if node.Host().Network().Connectedness(remoteHost.ID()) != network.Connected {
		t.Fatal("Agency revoke closed a connection still authorized by R5")
	}
	_ = agencyStream.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := agencyStream.Read(make([]byte, 1)); err == nil {
		t.Fatal("Agency revoke retained an existing Agency stream")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	eventStream, err := remoteHost.NewStream(ctx, node.Host().ID(), EventsProtocol)
	if err != nil {
		t.Fatal(err)
	}
	_ = eventStream.SetReadDeadline(time.Now().Add(time.Second))
	one := make([]byte, 1)
	if count, err := eventStream.Read(one); err != nil || count != 1 || one[0] != 'r' {
		t.Fatalf("R5 Event stream after Agency revoke = (%q, %v)", one[:count], err)
	}
	_ = eventStream.Close()
}

func prepareAgencyRevokeConnection(t *testing.T, authority *Authority,
	local, remote authorityTestPeer,
) (*NodeHost, host.Host, network.Stream) {
	t.Helper()
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	remoteHost := newBarePeerHost(t, remote)
	t.Cleanup(func() { _ = remoteHost.Close() })
	agencyAccepted := make(chan struct{}, 1)
	releaseAgency := make(chan struct{})
	t.Cleanup(func() { close(releaseAgency) })
	node.Host().SetStreamHandler(AgencyDeliveryProtocol, func(stream network.Stream) {
		one := make([]byte, 1)
		_, _ = stream.Read(one)
		agencyAccepted <- struct{}{}
		<-releaseAgency
		_ = stream.Close()
	})
	node.Host().SetStreamHandler(EventsProtocol, func(stream network.Stream) {
		_, _ = stream.Write([]byte{'r'})
		_ = stream.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := remoteHost.Connect(ctx, libp2ppeer.AddrInfo{ID: node.Host().ID(),
		Addrs: node.Host().Addrs()}); err != nil {
		t.Fatal(err)
	}
	agencyStream, err := remoteHost.NewStream(ctx, node.Host().ID(), AgencyDeliveryProtocol)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agencyStream.Close() })
	if _, err := agencyStream.Write([]byte{'a'}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-agencyAccepted:
	case <-ctx.Done():
		t.Fatal("Agency handler did not accept the pre-revoke stream")
	}
	return node, remoteHost, agencyStream
}

func hostAddressStrings(value host.Host) []string {
	addresses := value.Addrs()
	result := make([]string, len(addresses))
	for index, address := range addresses {
		result[index] = address.String()
	}
	return result
}
