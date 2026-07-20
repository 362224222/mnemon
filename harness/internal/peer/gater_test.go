package peer

import (
	"fmt"
	"runtime"
	"sync"
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
	gater := newTestConnectionGater(t, authority)
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

func TestConnectionGaterBoundsAndReleasesUnknownEnrollment(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-budget-local")
	unknown := testAuthorityPeer(t, "gater-budget-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	addresses := testConnectionAddresses()
	const attempts = 32
	start := make(chan struct{})
	var secured atomic.Int32
	var wait sync.WaitGroup
	wait.Add(attempts)
	for index := 0; index < attempts; index++ {
		go func() {
			defer wait.Done()
			<-start
			if gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) {
				secured.Add(1)
			}
		}()
	}
	close(start)
	wait.Wait()
	limit := int32(HermeticLimits().UnknownEnrollmentConnections)
	if secured.Load() != limit || gater.UnknownEnrollmentSlots() != int(limit) {
		t.Fatalf("unknown reservations = %d/%d, want %d", secured.Load(), gater.UnknownEnrollmentSlots(), limit)
	}
	for index := int32(0); index < limit; index++ {
		if !gater.admitUpgraded(network.DirInbound, unknown.libp2pID,
			fmt.Sprintf("unknown-%d", index), addresses) {
			t.Fatalf("reserved connection %d was rejected at upgrade", index)
		}
	}
	if gater.UnknownConnections() != int(limit) ||
		gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		gater.admitUpgraded(network.DirOutbound, unknown.libp2pID, "unknown-outbound", nil) {
		t.Fatal("full inbound budget or unknown outbound connection was admitted")
	}
	gater.mu.Lock()
	var released string
	for connectionID := range gater.unknown {
		released = connectionID
		break
	}
	gater.mu.Unlock()
	gater.releaseUnknown(released)
	if gater.UnknownConnections() != int(limit)-1 ||
		!gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		!gater.admitUpgraded(network.DirInbound, unknown.libp2pID, "replacement", addresses) {
		t.Fatal("disconnect did not release one unknown enrollment slot")
	}
}

func TestConnectionGaterExpiresFailedUpgradeReservation(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-expiry-local")
	unknown := testAuthorityPeer(t, "gater-expiry-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	current := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	gater.now = func() time.Time { return current }
	addresses := testConnectionAddresses()
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		gater.UnknownEnrollmentSlots() != 1 {
		t.Fatal("secured connection did not reserve an enrollment slot")
	}
	current = current.Add(gater.pendingTTL + time.Nanosecond)
	if gater.UnknownEnrollmentSlots() != 0 ||
		gater.admitUpgraded(network.DirInbound, unknown.libp2pID, "expired", addresses) {
		t.Fatal("expired reservation survived or authorized a late upgrade")
	}
}

func TestConnectionGaterExpiresAndClosesUpgradedUnknownConnections(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-lease-local")
	unknown := testAuthorityPeer(t, "gater-lease-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	gater.pendingTTL = 25 * time.Millisecond
	addresses := testConnectionAddresses()
	limit := HermeticLimits().UnknownEnrollmentConnections
	var closed atomic.Int32
	for index := 0; index < limit; index++ {
		if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) {
			t.Fatalf("unknown reservation %d was rejected", index)
		}
		connectionID := fmt.Sprintf("held-open-%d", index)
		if !gater.admitUpgradedConnection(network.DirInbound, unknown.libp2pID,
			connectionID, addresses, func() error {
				closed.Add(1)
				return nil
			}) {
			t.Fatalf("unknown connection %d was rejected", index)
		}
	}
	if gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) {
		t.Fatal("hold-open unknown connections exceeded the independent enrollment budget")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) &&
		(closed.Load() != int32(limit) || gater.UnknownEnrollmentSlots() != 0) {
		time.Sleep(time.Millisecond)
	}
	if closed.Load() != int32(limit) || gater.UnknownEnrollmentSlots() != 0 {
		t.Fatalf("expired leases closed/released = %d/%d, slots %d",
			closed.Load(), limit, gater.UnknownEnrollmentSlots())
	}
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) {
		t.Fatal("expired hold-open connections permanently exhausted enrollment capacity")
	}
}

func TestConnectionGaterExpiryDoesNotCrossReusedConnectionID(t *testing.T) {
	local := testAuthorityPeer(t, "gater-reuse-local")
	unknown := testAuthorityPeer(t, "gater-reuse-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	addresses := testConnectionAddresses()
	const connectionID = "reused-connection"
	const firstTTL = 20 * time.Millisecond
	gater.pendingTTL = firstTTL
	var firstClosed atomic.Int32
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		!gater.admitUpgradedConnection(network.DirInbound, unknown.libp2pID,
			connectionID, addresses, func() error {
				firstClosed.Add(1)
				return nil
			}) {
		t.Fatal("failed to admit first exact connection")
	}
	gater.releaseUnknown(connectionID)

	gater.pendingTTL = 10 * time.Second
	var replacementClosed atomic.Int32
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		!gater.admitUpgradedConnection(network.DirInbound, unknown.libp2pID,
			connectionID, addresses, func() error {
				replacementClosed.Add(1)
				return nil
			}) {
		t.Fatal("failed to admit replacement exact connection")
	}
	time.Sleep(3 * firstTTL)
	if firstClosed.Load() != 0 || replacementClosed.Load() != 0 || gater.UnknownConnections() != 1 {
		t.Fatalf("stale expiry crossed reused connection ID: first=%d replacement=%d unknown=%d",
			firstClosed.Load(), replacementClosed.Load(), gater.UnknownConnections())
	}
}

func TestConnectionGaterReconcilePromotesUnknownWithoutLeakingSlot(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-promote-local")
	remote := testAuthorityPeer(t, "gater-promote-remote")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
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
	gater := newTestConnectionGater(t, authority)
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

func TestConnectionGaterShutdownRetiresReservationsAndExpiryOwner(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-shutdown-local")
	unknown := testAuthorityPeer(t, "gater-shutdown-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
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
	gater.mu.Lock()
	running := gater.expiryRunning
	gater.mu.Unlock()
	if running {
		t.Fatal("shutdown returned before the expiry owner stopped")
	}
	time.Sleep(3 * gater.pendingTTL)
	if closed.Load() != 0 {
		t.Fatal("shutdown-first lease invoked a late exact-connection close")
	}
}

func TestConnectionGaterShutdownDrainsInFlightExpiryClose(t *testing.T) {
	local := testAuthorityPeer(t, "gater-drain-local")
	unknown := testAuthorityPeer(t, "gater-drain-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	gater.pendingTTL = time.Millisecond
	addresses := testConnectionAddresses()
	callbackEntered := make(chan int, 1)
	callbackRelease := make(chan struct{})
	callbackReturned := make(chan struct{})
	var closeCount atomic.Int32
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		!gater.admitUpgradedConnection(network.DirInbound, unknown.libp2pID,
			"draining-expiry", addresses, func() error {
				// This re-entry would deadlock if the expiry owner invoked the
				// unknown network callback while holding the state mutex.
				callbackEntered <- gater.UnknownConnections()
				closeCount.Add(1)
				<-callbackRelease
				close(callbackReturned)
				return nil
			}) {
		t.Fatal("failed to admit the expiring unknown connection")
	}
	select {
	case remaining := <-callbackEntered:
		if remaining != 0 {
			t.Fatalf("expired exact connection remained visible during close: %d", remaining)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expiry owner did not enter the exact-connection close")
	}

	shutdownStarted := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		close(shutdownStarted)
		gater.shutdown()
		close(shutdownDone)
	}()
	<-shutdownStarted
	for !gater.closed.Load() {
		runtime.Gosched()
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while an expiry close callback was in flight")
	default:
	}
	close(callbackRelease)
	select {
	case <-callbackReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("expiry close callback did not drain")
	}
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not join the drained expiry owner")
	}
	if closeCount.Load() != 1 {
		t.Fatalf("exact-connection close count = %d, want 1", closeCount.Load())
	}
	assertConnectionGaterShutdownDrained(t, gater)
}

func assertConnectionGaterShutdownDrained(t *testing.T, gater *ConnectionGater) {
	t.Helper()
	gater.mu.Lock()
	running := gater.expiryRunning
	gater.mu.Unlock()
	if running || gater.UnknownEnrollmentSlots() != 0 {
		t.Fatal("drained shutdown retained expiry ownership or enrollment state")
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
	gater := newTestConnectionGater(t, authority)
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

func newTestConnectionGater(t *testing.T, authority *Authority) *ConnectionGater {
	t.Helper()
	gater := NewConnectionGater(authority)
	t.Cleanup(gater.shutdown)
	return gater
}
