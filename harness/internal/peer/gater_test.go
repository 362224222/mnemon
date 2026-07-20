package peer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

func TestConnectionGaterUsesAnyChannelForPhysicalAuthority(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-local")
	remote := testAuthorityPeer(t, "gater-remote")
	authority, _ := NewAuthority(local.modelID)
	alpha := testAuthorityChannel(t, "gater-alpha", model.BindingRevoked, local, remote)
	beta := testAuthorityChannel(t, "gater-beta", model.BindingPending, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alpha, beta}}); err != nil {
		t.Fatal(err)
	}
	gater := NewConnectionGater(authority)
	if !gater.InterceptPeerDial(remote.libp2pID) ||
		!gater.InterceptSecured(network.DirOutbound, remote.libp2pID, nil) ||
		!gater.admitUpgraded(network.DirOutbound, remote.libp2pID, "known-1", nil) {
		t.Fatal("pending authority in one Channel did not permit the shared physical connection")
	}
	beta.Bindings[0].State = model.BindingRevoked
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{alpha, beta}}); err != nil {
		t.Fatal(err)
	}
	if gater.InterceptPeerDial(remote.libp2pID) ||
		gater.InterceptSecured(network.DirOutbound, remote.libp2pID, nil) ||
		gater.admitUpgraded(network.DirOutbound, remote.libp2pID, "revoked-1", nil) {
		t.Fatal("outbound connection survived loss of every Channel authority")
	}
}

func TestConnectionGaterReconcilePromotesUnknownWithoutLeakingSlot(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-promote-local")
	remote := testAuthorityPeer(t, "gater-promote-remote")
	authority, _ := NewAuthority(local.modelID)
	gater := NewConnectionGater(authority)
	addresses := testConnectionAddresses()
	if !gater.InterceptSecured(network.DirInbound, remote.libp2pID, addresses) ||
		!gater.admitUpgraded(network.DirInbound, remote.libp2pID, "enrollment", addresses) {
		t.Fatal("initial inbound enrollment was rejected")
	}
	channel := testAuthorityChannel(t, "gater-promote", model.BindingPending, local, remote)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{channel}}); err != nil {
		t.Fatal(err)
	}
	gater.Reconcile()
	if gater.UnknownConnections() != 0 || !gater.InterceptPeerDial(remote.libp2pID) {
		t.Fatal("promoted Peer retained an unknown slot or lacked physical authority")
	}
}

func TestConnectionGaterEnrollmentOwnerPermitIsOutboundOnly(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-enroll-local")
	owner := testAuthorityPeer(t, "gater-enroll-owner")
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		OutboundEnrollmentPeers: []model.PeerID{owner.modelID}}); err != nil {
		t.Fatal(err)
	}
	gater := NewConnectionGater(authority)
	if !gater.InterceptPeerDial(owner.libp2pID) ||
		!gater.InterceptSecured(network.DirOutbound, owner.libp2pID, nil) ||
		!gater.admitUpgraded(network.DirOutbound, owner.libp2pID, "owner-outbound", nil) {
		t.Fatal("fixed enrollment owner could not be dialed")
	}
	addresses := testConnectionAddresses()
	if !gater.InterceptSecured(network.DirInbound, owner.libp2pID, addresses) ||
		!gater.admitUpgraded(network.DirInbound, owner.libp2pID, "owner-inbound", addresses) ||
		gater.UnknownConnections() != 1 {
		t.Fatal("outbound permit incorrectly bypassed the unknown inbound budget")
	}
}

func TestConnectionGaterShutdownRetiresReservationsAndLeaseTimers(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-shutdown-local")
	unknown := testAuthorityPeer(t, "gater-shutdown-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := NewConnectionGater(authority)
	gater.pendingTTL = 25 * time.Millisecond
	addresses := testConnectionAddresses()
	var closed atomic.Int32
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		!gater.admitUpgradedConnection(network.DirInbound, unknown.libp2pID,
			"shutdown-upgraded", addresses, func() error {
				closed.Add(1)
				return nil
			}) ||
		!gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) {
		t.Fatal("failed to construct upgraded and pending enrollment reservations")
	}
	gater.shutdown()
	if gater.UnknownEnrollmentSlots() != 0 || gater.UnknownConnections() != 0 {
		t.Fatal("shutdown retained enrollment reservations")
	}
	time.Sleep(3 * gater.pendingTTL)
	if closed.Load() != 0 {
		t.Fatal("a retired unknown lease callback survived gater shutdown")
	}
}

func TestConnectionGaterShutdownIsAdmissionBarrier(t *testing.T) {
	local := testAuthorityPeer(t, "gater-shutdown-barrier-local")
	unknown := testAuthorityPeer(t, "gater-shutdown-barrier-unknown")
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		OutboundEnrollmentPeers: []model.PeerID{unknown.modelID}}); err != nil {
		t.Fatal(err)
	}
	gater := NewConnectionGater(authority)
	entered := make(chan struct{})
	release := make(chan struct{})
	gater.now = func() time.Time {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return time.Now()
	}
	addresses := testConnectionAddresses()
	secured := make(chan bool, 1)
	go func() {
		secured <- gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses)
	}()
	<-entered
	shutdownDone := make(chan struct{})
	go func() {
		gater.shutdown()
		close(shutdownDone)
	}()
	close(release)
	if !<-secured {
		t.Fatal("admission that linearized before shutdown was unexpectedly rejected")
	}
	<-shutdownDone
	if gater.UnknownEnrollmentSlots() != 0 || gater.UnknownConnections() != 0 {
		t.Fatal("shutdown retained enrollment reservations")
	}
	if gater.InterceptAccept(addresses) || gater.InterceptPeerDial(unknown.libp2pID) ||
		gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		gater.admitUpgradedConnection(network.DirInbound, unknown.libp2pID,
			"shutdown-upgraded", addresses, nil) {
		t.Fatal("shutdown did not become a fail-closed admission barrier")
	}
}

type connectionAddresses struct {
	local  ma.Multiaddr
	remote ma.Multiaddr
}

func (addresses connectionAddresses) LocalMultiaddr() ma.Multiaddr  { return addresses.local }
func (addresses connectionAddresses) RemoteMultiaddr() ma.Multiaddr { return addresses.remote }

func testConnectionAddresses() connectionAddresses {
	return connectionAddresses{
		local:  ma.StringCast("/ip4/127.0.0.1/tcp/41001"),
		remote: ma.StringCast("/ip4/127.0.0.1/tcp/41002"),
	}
}
