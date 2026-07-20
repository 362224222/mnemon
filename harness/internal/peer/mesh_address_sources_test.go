package peer

import (
	"context"
	"sync"
	"testing"
	"time"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshAddressSourcesSerializeAbortWithPermitRelease(t *testing.T) {
	local := testAuthorityPeer(t, "address-sources-local")
	remote := testAuthorityPeer(t, "address-sources-remote")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	sources, err := newMeshAddressSources(node.managedRuntimeHost().Peerstore())
	if err != nil {
		t.Fatal(err)
	}
	address := ma.StringCast("/ip4/127.0.0.1/tcp/45001")
	initial := mapPeerAddresses(remote.libp2pID, address)
	if err := sources.installInitial(initial); err != nil {
		t.Fatal(err)
	}
	spec := testEnrollmentTransportPermitSpec(t, remote, []string{address.String()}, "sources")
	token, err := node.gater.acquireOutboundEnrollmentPermit(context.Background(), spec,
		func(ref outboundEnrollmentPermitRef, _ error) { sources.removePermit(ref) })
	if err != nil {
		t.Fatal(err)
	}
	if err := sources.addPermit(token); err != nil {
		t.Fatal(err)
	}
	transition := &MeshAuthorityTransition{}
	if err := sources.stageDurable(transition, nil); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		<-start
		node.gater.releaseOutboundEnrollmentPermit(token)
	}()
	go func() {
		defer wait.Done()
		<-start
		if err := sources.abortDurable(transition); err != nil {
			t.Errorf("abort durable addresses: %v", err)
		}
	}()
	close(start)
	wait.Wait()
	if got := node.managedRuntimeHost().Peerstore().Addrs(remote.libp2pID); !containsAddress(got, address) {
		t.Fatal("permit release raced abort and deleted the retained durable address")
	}
}

func TestMeshAddressSourcesPreservePermitAcrossDurableInstallAndClose(t *testing.T) {
	local := testAuthorityPeer(t, "address-install-local")
	remote := testAuthorityPeer(t, "address-install-remote")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	sources, _ := newMeshAddressSources(node.managedRuntimeHost().Peerstore())
	if err := sources.installInitial(nil); err != nil {
		t.Fatal(err)
	}
	address := ma.StringCast("/ip4/127.0.0.1/tcp/45002")
	spec := testEnrollmentTransportPermitSpec(t, remote, []string{address.String()}, "install")
	token, err := node.gater.acquireOutboundEnrollmentPermit(context.Background(), spec,
		func(ref outboundEnrollmentPermitRef, _ error) { sources.removePermit(ref) })
	if err != nil {
		t.Fatal(err)
	}
	if err := sources.addPermit(token); err != nil {
		t.Fatal(err)
	}
	transition := &MeshAuthorityTransition{}
	if err := sources.stageDurable(transition, mapPeerAddresses(remote.libp2pID, address)); err != nil {
		t.Fatal(err)
	}
	if err := sources.installDurable(transition); err != nil {
		t.Fatal(err)
	}
	if !node.gater.releaseOutboundEnrollmentPermit(token) ||
		!containsAddress(node.managedRuntimeHost().Peerstore().Addrs(remote.libp2pID), address) {
		t.Fatal("permit release erased the independently installed durable address")
	}
	sources.close()
	if containsAddress(node.managedRuntimeHost().Peerstore().Addrs(remote.libp2pID), address) {
		t.Fatal("address source owner close retained managed addresses")
	}
}

func TestMeshAddressSourcesExpiryCallbackCannotEraseAbortedDurableSource(t *testing.T) {
	local := testAuthorityPeer(t, "address-expiry-local")
	remote := testAuthorityPeer(t, "address-expiry-remote")
	authority, _ := NewAuthority(local.modelID)
	node, err := NewNodeHost(local.libp2pPrivate, authority,
		[]ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	sources, _ := newMeshAddressSources(node.managedRuntimeHost().Peerstore())
	address := ma.StringCast("/ip4/127.0.0.1/tcp/45003")
	if err := sources.installInitial(mapPeerAddresses(remote.libp2pID, address)); err != nil {
		t.Fatal(err)
	}
	node.gater.pendingTTL = 20 * time.Millisecond
	callbackEntered := make(chan struct{})
	callbackContinue := make(chan struct{})
	callbackDone := make(chan struct{})
	spec := testEnrollmentTransportPermitSpec(t, remote, []string{address.String()}, "expiry-source")
	token, err := node.gater.acquireOutboundEnrollmentPermit(context.Background(), spec,
		func(ref outboundEnrollmentPermitRef, _ error) {
			close(callbackEntered)
			<-callbackContinue
			sources.removePermit(ref)
			close(callbackDone)
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := sources.addPermit(token); err != nil {
		t.Fatal(err)
	}
	transition := &MeshAuthorityTransition{}
	if err := sources.stageDurable(transition, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callbackEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("permit expiry callback did not start")
	}
	if err := sources.abortDurable(transition); err != nil {
		t.Fatal(err)
	}
	close(callbackContinue)
	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("permit expiry callback did not finish")
	}
	if !containsAddress(node.managedRuntimeHost().Peerstore().Addrs(remote.libp2pID), address) {
		t.Fatal("stale expiry callback erased the durable address restored by abort")
	}
}

func mapPeerAddresses(peerID libp2ppeer.ID, address ma.Multiaddr) map[libp2ppeer.ID][]ma.Multiaddr {
	return map[libp2ppeer.ID][]ma.Multiaddr{peerID: []ma.Multiaddr{address}}
}

func containsAddress(addresses []ma.Multiaddr, expected ma.Multiaddr) bool {
	for _, address := range addresses {
		if address != nil && address.Equal(expected) {
			return true
		}
	}
	return false
}
