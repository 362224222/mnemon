package peer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

func TestConnectionGaterOutboundEnrollmentPermitBindsExactAttemptAndAddress(t *testing.T) {
	t.Parallel()
	local := testAuthorityPeer(t, "exact-permit-local")
	owner := testAuthorityPeer(t, "exact-permit-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/ip4/127.0.0.1/tcp/42001"}, "exact")
	token, err := gater.acquireOutboundEnrollmentPermit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	address := ma.StringCast(spec.OwnerMultiaddrs[0])
	wrongAddress := ma.StringCast("/ip4/127.0.0.1/tcp/42002")
	if gater.InterceptPeerDial(owner.libp2pID) ||
		gater.InterceptAddrDial(owner.libp2pID, address) ||
		gater.InterceptSecured(network.DirOutbound, owner.libp2pID,
			testOutboundConnectionAddresses(address)) {
		t.Fatal("unclaimed permit leaked dial authority")
	}
	if !gater.claimOutboundEnrollmentStream(token) || gater.claimOutboundEnrollmentStream(token) ||
		!gater.outboundEnrollmentPermitCurrent(token) {
		t.Fatal("permit did not authorize exactly one Channel stream claim")
	}
	if !gater.InterceptPeerDial(owner.libp2pID) ||
		!gater.InterceptAddrDial(owner.libp2pID, address) ||
		gater.InterceptAddrDial(owner.libp2pID, wrongAddress) ||
		!gater.InterceptSecured(network.DirOutbound, owner.libp2pID,
			testOutboundConnectionAddresses(address)) ||
		gater.InterceptSecured(network.DirOutbound, owner.libp2pID,
			testOutboundConnectionAddresses(wrongAddress)) {
		t.Fatal("permit did not enforce its exact owner address")
	}
	if !gater.admitUpgraded(network.DirOutbound, owner.libp2pID, "exact-owner",
		testOutboundConnectionAddresses(address)) ||
		gater.admitUpgraded(network.DirOutbound, owner.libp2pID, "wrong-owner-address",
			testOutboundConnectionAddresses(wrongAddress)) {
		t.Fatal("upgraded outbound connection did not retain the exact address fence")
	}
	if !gater.releaseOutboundEnrollmentPermit(token) ||
		gater.releaseOutboundEnrollmentPermit(token) ||
		gater.InterceptPeerDial(owner.libp2pID) || gater.outboundEnrollmentPermitCurrent(token) {
		t.Fatal("exact permit survived release or accepted a stale handle")
	}
}

func TestConnectionGaterEnrollmentPermitRejectsStaleGeneration(t *testing.T) {
	t.Parallel()
	local := testAuthorityPeer(t, "stale-permit-local")
	owner := testAuthorityPeer(t, "stale-permit-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/ip4/127.0.0.1/tcp/42003"}, "stale")
	token, err := gater.acquireOutboundEnrollmentPermit(context.Background(), spec, nil)
	if err != nil || !gater.releaseOutboundEnrollmentPermit(token) {
		t.Fatalf("acquire/release initial generation = %v", err)
	}
	replacement, err := gater.acquireOutboundEnrollmentPermit(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gater.releaseOutboundEnrollmentPermit(token) ||
		!gater.outboundEnrollmentPermitCurrent(replacement) {
		t.Fatal("stale generation released a replacement with the same stable identity")
	}
	gater.releaseOutboundEnrollmentPermit(replacement)
}

func TestConnectionGaterEnrollmentPermitUsesSharedBudget(t *testing.T) {
	t.Parallel()
	local := testAuthorityPeer(t, "permit-budget-local")
	owner := testAuthorityPeer(t, "permit-budget-owner")
	unknown := testAuthorityPeer(t, "permit-budget-unknown")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	for index := 0; index < HermeticLimits().UnknownEnrollmentConnections-1; index++ {
		spec := testEnrollmentTransportPermitSpec(t, owner,
			[]string{"/ip4/127.0.0.1/tcp/" + testPort(index)}, "budget-"+testPort(index))
		if _, err := gater.acquireOutboundEnrollmentPermit(context.Background(), spec, nil); err != nil {
			t.Fatalf("acquire permit %d: %v", index, err)
		}
	}
	addresses := testConnectionAddresses()
	if !gater.InterceptSecured(network.DirInbound, unknown.libp2pID, addresses) ||
		gater.UnknownEnrollmentSlots() != HermeticLimits().UnknownEnrollmentConnections {
		t.Fatal("inbound reservation did not consume the final shared enrollment slot")
	}
	overflow := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/ip4/127.0.0.1/tcp/42999"}, "budget-overflow")
	if _, err := gater.acquireOutboundEnrollmentPermit(context.Background(), overflow, nil); !errors.Is(err, ErrEnrollmentTransportPermit) {
		t.Fatalf("shared-budget overflow error = %v", err)
	}
}

func TestConnectionGaterEnrollmentPermitExpiresAndRunsReleaseOutsideLock(t *testing.T) {
	local := testAuthorityPeer(t, "permit-expiry-local")
	owner := testAuthorityPeer(t, "permit-expiry-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	gater.pendingTTL = time.Minute
	start := time.Date(2026, 7, 20, 2, 0, 0, 0, time.UTC)
	var clock atomic.Value
	clock.Store(start)
	gater.now = func() time.Time { return clock.Load().(time.Time) }
	released := make(chan int, 1)
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/ip4/127.0.0.1/tcp/43001"}, "expiry")
	if _, err := gater.acquireOutboundEnrollmentPermit(context.Background(), spec,
		func(outboundEnrollmentPermitRef, error) {
			released <- gater.outboundEnrollmentPermits()
		}); err != nil {
		t.Fatal(err)
	}
	clock.Store(start.Add(gater.pendingTTL + time.Nanosecond))
	gater.mu.Lock()
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	select {
	case remaining := <-released:
		if remaining != 0 {
			t.Fatalf("release callback observed %d permits, want 0", remaining)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("single expiry owner did not retire the permit")
	}
	if gater.UnknownEnrollmentSlots() != 0 || gater.InterceptPeerDial(owner.libp2pID) {
		t.Fatal("expired permit retained budget or dial authority")
	}
}

func TestConnectionGaterRejectsNoncanonicalOrMismatchedPermit(t *testing.T) {
	t.Parallel()
	local := testAuthorityPeer(t, "permit-invalid-local")
	owner := testAuthorityPeer(t, "permit-invalid-owner")
	other := testAuthorityPeer(t, "permit-invalid-other")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	otherAddress := "/ip4/127.0.0.1/tcp/44001/p2p/" + other.libp2pID.String()
	base := testEnrollmentTransportPermitSpec(t, owner, []string{otherAddress}, "invalid")
	if _, err := gater.acquireOutboundEnrollmentPermit(context.Background(), base, nil); !errors.Is(err, ErrEnrollmentTransportPermit) {
		t.Fatalf("mismatched identity error = %v", err)
	}
	base.OwnerMultiaddrs = []string{"/ip4/127.0.0.1/tcp/44002", "/ip4/127.0.0.1/tcp/44001"}
	if _, err := gater.acquireOutboundEnrollmentPermit(context.Background(), base, nil); !errors.Is(err, ErrEnrollmentTransportPermit) {
		t.Fatalf("noncanonical address order error = %v", err)
	}
}

func testEnrollmentTransportPermitSpec(t *testing.T, owner authorityTestPeer,
	addresses []string, suffix string,
) enrollmentTransportPermitSpec {
	t.Helper()
	channelID, _ := model.ParseChannelID("channel-permit-" + suffix)
	grantID, _ := model.ParseGrantID("grant-permit-" + suffix)
	requestID, _ := model.ParseEnrollmentRequestID("request-permit-" + suffix)
	return enrollmentTransportPermitSpec{OwnerPeerID: owner.modelID, OwnerMultiaddrs: addresses,
		ChannelID: channelID, GrantID: grantID, EnrollmentRequestID: requestID}
}

func testOutboundConnectionAddresses(remote ma.Multiaddr) connectionAddresses {
	return connectionAddresses{local: ma.StringCast("/ip4/127.0.0.1/tcp/40000"), remote: remote}
}

func testPort(index int) string {
	return []string{"42010", "42011", "42012", "42013", "42014", "42015", "42016", "42017"}[index]
}
