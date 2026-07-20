package node

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshEndpointValuesAreCanonicalImmutableAndStateful(t *testing.T) {
	peerID := testkit.NewIdentity(t, "mesh-endpoint-value").PeerID()
	listen := []string{"/ip4/0.0.0.0/tcp/0"}
	pending, err := newMeshEndpointPending(meshEndpointPendingSpec{PeerID: peerID, ListenAddrs: listen})
	if err != nil {
		t.Fatalf("newMeshEndpointPending() error = %v", err)
	}
	wantPending := fmt.Sprintf(`{"advertised_addrs":[],"listen_addrs":["/ip4/0.0.0.0/tcp/0"],"peer_id":%q,"schema_version":1}`,
		peerID.String())
	if string(pending.canonicalJSON()) != wantPending || pending.peerIDValue() != peerID {
		t.Fatalf("pending = %s, peer %s", pending.canonicalJSON(), pending.peerIDValue().String())
	}
	listen[0] = "/ip4/127.0.0.1/tcp/1"
	gotListen := pending.listenAddresses()
	gotListen[0] = "changed"
	gotJSON := pending.canonicalJSON()
	gotJSON[0] = 'x'
	if pending.listenAddresses()[0] != "/ip4/0.0.0.0/tcp/0" || string(pending.canonicalJSON()) != wantPending {
		t.Fatal("pending value aliases caller-owned memory")
	}

	advertised := []string{"/ip4/127.0.0.1/tcp/4101", "/dns4/node-a/tcp/4101"}
	endpoint, err := newMeshEndpoint(meshEndpointSpec{PeerID: peerID,
		ListenAddrs: []string{"/ip4/0.0.0.0/tcp/4101"}, AdvertisedAddrs: advertised})
	if err != nil {
		t.Fatalf("newMeshEndpoint() error = %v", err)
	}
	wantAddresses := []string{"/dns4/node-a/tcp/4101", "/ip4/127.0.0.1/tcp/4101"}
	if fmt.Sprint(endpoint.advertisedAddresses()) != fmt.Sprint(wantAddresses) {
		t.Fatalf("advertised = %v, want %v", endpoint.advertisedAddresses(), wantAddresses)
	}
	advertised[0] = "changed"
	clone := endpoint.advertisedAddresses()
	clone[0] = "changed"
	if fmt.Sprint(endpoint.advertisedAddresses()) != fmt.Sprint(wantAddresses) {
		t.Fatal("final endpoint aliases advertised addresses")
	}

	state := meshEndpointState{kind: meshEndpointStateFinalWithPending,
		pending: pending, final: endpoint}
	gotPending, pendingOK := state.pendingAuthority()
	gotFinal, finalOK := state.finalAuthority()
	if state.stateKind() != meshEndpointStateFinalWithPending || !pendingOK || !finalOK ||
		gotPending.peerIDValue() != peerID || gotFinal.peerIDValue() != peerID {
		t.Fatalf("state projection = (%d,%t,%t)", state.stateKind(), pendingOK, finalOK)
	}
	if _, ok := (meshEndpointState{kind: meshEndpointStateAbsent}).pendingAuthority(); ok {
		t.Fatal("absent state exposed pending authority")
	}
}

func TestMeshEndpointValuesRejectInvalidAddressAuthority(t *testing.T) {
	peerID := testkit.NewIdentity(t, "mesh-endpoint-invalid").PeerID()
	invalidPeerID, err := model.ParsePeerID("not-a-libp2p-peer")
	if err != nil {
		t.Fatal(err)
	}
	nine := make([]string, 9)
	for index := range nine {
		nine[index] = fmt.Sprintf("/ip4/127.0.0.%d/tcp/4101", index+1)
	}
	tests := []struct {
		name    string
		pending *meshEndpointPendingSpec
		final   *meshEndpointSpec
	}{
		{name: "zero peer", pending: &meshEndpointPendingSpec{ListenAddrs: []string{"/ip4/0.0.0.0/tcp/0"}}},
		{name: "syntactic peer", pending: &meshEndpointPendingSpec{PeerID: invalidPeerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/0"}}},
		{name: "no listener", pending: &meshEndpointPendingSpec{PeerID: peerID}},
		{name: "two listeners", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"}}},
		{name: "DNS listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/dns4/node-a/tcp/4101"}}},
		{name: "UDP listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/udp/4101"}}},
		{name: "QUIC listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/udp/4101/quic-v1"}}},
		{name: "WebSocket listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/4101/ws"}}},
		{name: "pending multicast listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/224.0.0.1/tcp/0"}}},
		{name: "pending broadcast listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/255.255.255.255/tcp/0"}}},
		{name: "pending link-local listener", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip6/fe80::1/tcp/0"}}},
		{name: "automatic listener with seed", pending: &meshEndpointPendingSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/0"},
			AdvertisedAddrs: []string{"/dns4/node-a/tcp/4101"}}},
		{name: "final zero port", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/0"},
			AdvertisedAddrs: []string{"/ip4/127.0.0.1/tcp/4101"}}},
		{name: "final no address", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/4101"}}},
		{name: "final multicast listener", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip6/ff02::1/tcp/4101"},
			AdvertisedAddrs: []string{"/ip6/::1/tcp/4101"}}},
		{name: "final broadcast listener", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/255.255.255.255/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/127.0.0.1/tcp/4101"}}},
		{name: "final link-local listener", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/169.254.1.1/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/127.0.0.1/tcp/4101"}}},
		{name: "unspecified advertised IP", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/0.0.0.0/tcp/4101"}}},
		{name: "IPv4 multicast", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/224.0.0.1/tcp/4101"}}},
		{name: "IPv6 multicast", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip6/::/tcp/4101"},
			AdvertisedAddrs: []string{"/ip6/ff02::1/tcp/4101"}}},
		{name: "limited broadcast", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/255.255.255.255/tcp/4101"}}},
		{name: "IPv4 link local", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/169.254.1.1/tcp/4101"}}},
		{name: "IPv6 link local without zone", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip6/::/tcp/4101"},
			AdvertisedAddrs: []string{"/ip6/fe80::1/tcp/4101"}}},
		{name: "different ports", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/4101"},
			AdvertisedAddrs: []string{"/ip4/127.0.0.1/tcp/4102"}}},
		{name: "duplicate address", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/4101"}, AdvertisedAddrs: []string{
				"/ip4/127.0.0.1/tcp/4101", "/ip4/127.0.0.1/tcp/4101"}}},
		{name: "too many addresses", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/4101"}, AdvertisedAddrs: nine}},
		{name: "embedded PeerID", final: &meshEndpointSpec{PeerID: peerID,
			ListenAddrs: []string{"/ip4/0.0.0.0/tcp/4101"}, AdvertisedAddrs: []string{
				"/ip4/127.0.0.1/tcp/4101/p2p/" + peerID.String()}}},
	}
	assertInvalidMeshEndpointCases(t, tests)
}

func assertInvalidMeshEndpointCases(t *testing.T, tests []struct {
	name    string
	pending *meshEndpointPendingSpec
	final   *meshEndpointSpec
}) {
	t.Helper()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			if test.pending != nil {
				_, err = newMeshEndpointPending(*test.pending)
			} else {
				_, err = newMeshEndpoint(*test.final)
			}
			if !errors.Is(err, errMeshEndpointAuthority) {
				t.Fatalf("constructor error = %v", err)
			}
		})
	}
}

func TestMeshEndpointTransitionFreezesListenerAndOptionalSeed(t *testing.T) {
	peerID := testkit.NewIdentity(t, "mesh-endpoint-transition").PeerID()
	auto := mustMeshEndpointPending(t, peerID, "/ip4/0.0.0.0/tcp/0", nil)
	final := mustMeshEndpoint(t, peerID, "/ip4/0.0.0.0/tcp/4201",
		[]string{"/ip4/127.0.0.1/tcp/4201"})
	if !meshEndpointAdvances(auto.value, final.value) {
		t.Fatal("automatic listener did not accept its held final port")
	}
	wrongHost := mustMeshEndpoint(t, peerID, "/ip4/127.0.0.1/tcp/4201",
		[]string{"/ip4/127.0.0.1/tcp/4201"})
	if meshEndpointAdvances(auto.value, wrongHost.value) {
		t.Fatal("listener host drift was accepted")
	}
	seed := mustMeshEndpointPending(t, peerID, "/ip4/0.0.0.0/tcp/4301",
		[]string{"/dns4/node-a/tcp/4301"})
	seededFinal := mustMeshEndpoint(t, peerID, "/ip4/0.0.0.0/tcp/4301",
		[]string{"/dns4/node-a/tcp/4301"})
	if !meshEndpointAdvances(seed.value, seededFinal.value) {
		t.Fatal("exact fixed Docker seed was rejected")
	}
	drift := mustMeshEndpoint(t, peerID, "/ip4/0.0.0.0/tcp/4301",
		[]string{"/dns4/node-b/tcp/4301"})
	if meshEndpointAdvances(seed.value, drift.value) {
		t.Fatal("advertised seed drift was accepted")
	}
	if _, err := newMeshEndpoint(meshEndpointSpec{PeerID: peerID,
		ListenAddrs:     []string{"/ip4/0.0.0.0/tcp/4301"},
		AdvertisedAddrs: []string{"/ip4/192.168.1.255/tcp/4301"}}); err != nil {
		t.Fatalf("prefixless address was guessed to be a subnet broadcast: %v", err)
	}
}

func mustMeshEndpointPending(t *testing.T, peerID model.PeerID, listen string,
	advertised []string,
) meshEndpointPending {
	t.Helper()
	value, err := newMeshEndpointPending(meshEndpointPendingSpec{PeerID: peerID,
		ListenAddrs: []string{listen}, AdvertisedAddrs: advertised})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustMeshEndpoint(t *testing.T, peerID model.PeerID, listen string,
	advertised []string,
) meshEndpoint {
	t.Helper()
	value, err := newMeshEndpoint(meshEndpointSpec{PeerID: peerID,
		ListenAddrs: []string{listen}, AdvertisedAddrs: advertised})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
