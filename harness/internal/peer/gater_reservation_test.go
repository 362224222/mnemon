package peer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
)

func TestConnectionGaterBoundsAndReleasesUnknownEnrollment(t *testing.T) {
	t.Parallel()

	local := testAuthorityPeer(t, "gater-budget-local")
	unknown := testAuthorityPeer(t, "gater-budget-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := NewConnectionGater(authority)
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
	gater := NewConnectionGater(authority)
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
	gater := NewConnectionGater(authority)
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
