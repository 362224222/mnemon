package peer

import (
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	ma "github.com/multiformats/go-multiaddr"
)

func TestConnectionGaterOnePermitBindsAtMostOneOutboundConnection(t *testing.T) {
	t.Parallel()
	local := testAuthorityPeer(t, "permit-one-connection-local")
	owner := testAuthorityPeer(t, "permit-one-connection-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	address := ma.StringCast("/ip4/127.0.0.1/tcp/44101")
	token, err := gater.acquireOutboundEnrollmentPermit(context.Background(),
		testEnrollmentTransportPermitSpec(t, owner, []string{address.String()}, "one-connection"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if gater.admitUpgraded(network.DirOutbound, owner.libp2pID, "before-claim",
		testOutboundConnectionAddresses(address)) {
		t.Fatal("unclaimed permit admitted an upgraded connection")
	}
	if !gater.claimOutboundEnrollmentStream(token) {
		t.Fatal("claim exact enrollment stream")
	}
	accepted := 0
	for index := 0; index < HermeticLimits().UnknownEnrollmentConnections+1; index++ {
		if gater.admitUpgraded(network.DirOutbound, owner.libp2pID,
			"permit-connection-"+testPortWithOverflow(index), testOutboundConnectionAddresses(address)) {
			accepted++
		}
	}
	if accepted != 1 || gater.UnknownEnrollmentSlots() != 1 {
		t.Fatalf("one permit admitted %d connections with %d slots, want 1/1",
			accepted, gater.UnknownEnrollmentSlots())
	}
}

func testPortWithOverflow(index int) string {
	if index < HermeticLimits().UnknownEnrollmentConnections {
		return testPort(index)
	}
	return "42018"
}
