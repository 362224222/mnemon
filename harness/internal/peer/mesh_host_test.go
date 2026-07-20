package peer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestBoundMeshHostFreezesEphemeralEndpointAndTransfersSameHost(t *testing.T) {
	identity := testkit.NewIdentity(t, "bound-mesh-host-transfer")
	bound := bindTestMeshHost(t, identity, MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
		ma.StringCast("/ip4/127.0.0.1/tcp/0"),
	}})
	endpoint, err := bound.Endpoint()
	if err != nil {
		t.Fatal(err)
	}
	listener, listenerPort := inspectEndpointListener(t, endpoint)
	if listenerPort == 0 || len(endpoint.AdvertisedAddrs()) == 0 ||
		len(endpoint.AdvertisedAddrs()) > model.MaxMemberMultiaddrs {
		t.Fatalf("bound endpoint = %#v", endpoint)
	}
	for _, raw := range endpoint.AdvertisedAddrs() {
		_, port, err := inspectDirectTCPAddr(ma.StringCast(raw), false, false)
		if err != nil || port != listenerPort {
			t.Fatalf("advertised address %q does not match listener port %d: %v",
				raw, listenerPort, err)
		}
	}
	assertMeshHostListenerOccupied(t, listener)

	ownedNodeHost, ownedRawHost := bound.nodeHost, bound.nodeHost.host
	st := openPeerMeshStore(t, identity, peerMeshTime(t, "2026-07-20T08:00:00Z"))
	runtime, err := bound.Freeze(context.Background(), readMeshRuntimeAuthority(t, st))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.nodeHost != ownedNodeHost || runtime.nodeHost.host != ownedRawHost {
		t.Fatal("Freeze constructed a second Host instead of transferring the bound Host")
	}
	if err := bound.Close(); err != nil {
		t.Fatalf("close transferred owner: %v", err)
	}
	assertMeshHostListenerOccupied(t, listener)
	if got, err := runtime.LocalEnrollmentMultiaddrs(); err != nil ||
		!reflect.DeepEqual(got, endpoint.AdvertisedAddrs()) {
		t.Fatalf("LocalEnrollmentMultiaddrs() = (%v, %v), want frozen %v",
			got, err, endpoint.AdvertisedAddrs())
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	waitMeshHostListenerReusable(t, listener)
}

func TestBindMeshHostKeepsExactAdvertisedSeed(t *testing.T) {
	identity := testkit.NewIdentity(t, "bound-mesh-host-exact-seed")
	port := availableMeshHostPort(t)
	listen := ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port))
	want := []string{fmt.Sprintf("/dns4/node.test/tcp/%d", port),
		fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)}
	bound := bindTestMeshHost(t, identity, MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{listen},
		AdvertisedAddrs: []ma.Multiaddr{ma.StringCast(want[1]), ma.StringCast(want[0])}})
	endpoint, err := bound.Endpoint()
	if err != nil || !reflect.DeepEqual(endpoint.AdvertisedAddrs(), want) {
		t.Fatalf("Endpoint() = (%#v, %v), want %v", endpoint, err, want)
	}
	endpoint.ListenAddrs()[0] = "/ip4/127.0.0.1/tcp/1"
	endpoint.AdvertisedAddrs()[0] = "/ip4/127.0.0.1/tcp/1"
	again, err := bound.Endpoint()
	if err != nil || !reflect.DeepEqual(again.AdvertisedAddrs(), want) {
		t.Fatalf("Endpoint() after caller mutation = (%#v, %v), want %v", again, err, want)
	}
	differentInput := []ma.Multiaddr{ma.StringCast(fmt.Sprintf("/ip4/127.0.0.2/tcp/%d", port))}
	if got := meshHostAddrStrings(bound.addressFactory.apply(differentInput)); !reflect.DeepEqual(got, want) {
		t.Fatalf("frozen AddrsFactory output = %v, want %v", got, want)
	}
	if got := meshHostAddrStrings(bound.nodeHost.managedRuntimeHost().Addrs()); !reflect.DeepEqual(got, want) {
		t.Fatalf("managed Host Addrs() = %v, want %v", got, want)
	}
}

func TestBoundMeshHostFreezeFailuresReleaseListener(t *testing.T) {
	local := testkit.NewIdentity(t, "bound-mesh-host-failure-local")
	other := testkit.NewIdentity(t, "bound-mesh-host-failure-other")
	otherStore := openPeerMeshStore(t, other, peerMeshTime(t, "2026-07-20T08:10:00Z"))
	otherMesh := readMeshRuntimeAuthority(t, otherStore)
	tests := []struct {
		name string
		ctx  func() context.Context
	}{
		{name: "nil", ctx: func() context.Context { return nil }},
		{name: "canceled", ctx: func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}},
		{name: "identity mismatch", ctx: context.Background},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bound := bindTestMeshHost(t, local, MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
				ma.StringCast("/ip4/127.0.0.1/tcp/0"),
			}})
			endpoint, _ := bound.Endpoint()
			listener, _ := inspectEndpointListener(t, endpoint)
			mesh := otherMesh
			if test.name == "canceled" {
				localStore := openPeerMeshStore(t, local,
					peerMeshTime(t, "2026-07-20T08:11:00Z"))
				mesh = readMeshRuntimeAuthority(t, localStore)
			}
			runtime, err := bound.Freeze(test.ctx(), mesh)
			if runtime != nil || !errors.Is(err, ErrMeshHost) {
				t.Fatalf("Freeze() = (%v, %v)", runtime, err)
			}
			waitMeshHostListenerReusable(t, listener)
			if err := bound.Close(); err != nil {
				t.Fatalf("Close() after failed Freeze = %v", err)
			}
		})
	}
}

func TestBoundMeshHostConcurrentFreezeCloseHasOneOwner(t *testing.T) {
	for attempt := 0; attempt < 12; attempt++ {
		identity := testkit.NewIdentity(t, fmt.Sprintf("bound-mesh-host-race-%d", attempt))
		st := openPeerMeshStore(t, identity,
			peerMeshTime(t, fmt.Sprintf("2026-07-20T09:%02d:00Z", attempt)))
		mesh := readMeshRuntimeAuthority(t, st)
		bound := bindTestMeshHost(t, identity, MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
			ma.StringCast("/ip4/127.0.0.1/tcp/0"),
		}})
		endpoint, _ := bound.Endpoint()
		listener, _ := inspectEndpointListener(t, endpoint)
		owned := bound.nodeHost
		start := make(chan struct{})
		var runtime *MeshRuntime
		var freezeErr, closeErr error
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			runtime, freezeErr = bound.Freeze(context.Background(), mesh)
		}()
		go func() {
			defer wait.Done()
			<-start
			closeErr = bound.Close()
		}()
		close(start)
		wait.Wait()
		if closeErr != nil {
			t.Fatalf("attempt %d Close() = %v", attempt, closeErr)
		}
		if runtime != nil {
			if freezeErr != nil || runtime.nodeHost != owned {
				t.Fatalf("attempt %d successful Freeze = (%p, %v), owned %p",
					attempt, runtime.nodeHost, freezeErr, owned)
			}
			assertMeshHostListenerOccupied(t, listener)
			if err := runtime.Close(); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(freezeErr, ErrMeshHost) {
			t.Fatalf("attempt %d failed Freeze error = %v", attempt, freezeErr)
		}
		waitMeshHostListenerReusable(t, listener)
	}
}

func bindTestMeshHost(t *testing.T, identity testkit.Identity,
	spec MeshHostBindSpec,
) *BoundMeshHost {
	t.Helper()
	key, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindMeshHost(context.Background(), key, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bound.Close(); err != nil {
			t.Errorf("close bound mesh Host: %v", err)
		}
	})
	return bound
}

func inspectEndpointListener(t *testing.T, endpoint MeshEndpointSnapshot) (string, uint16) {
	t.Helper()
	listeners := endpoint.ListenAddrs()
	if len(listeners) != 1 {
		t.Fatalf("endpoint listeners = %v", listeners)
	}
	address := ma.StringCast(listeners[0])
	host, err := address.ValueForProtocol(ma.P_IP4)
	if err != nil {
		t.Fatal(err)
	}
	portText, err := address.ValueForProtocol(ma.P_TCP)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort(host, portText), uint16(port)
}

func availableMeshHostPort(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func assertMeshHostListenerOccupied(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp4", address)
	if err == nil {
		_ = listener.Close()
		t.Fatalf("mesh listener %s was not held", address)
	}
}

func waitMeshHostListenerReusable(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		listener, err := net.Listen("tcp4", address)
		if err == nil {
			_ = listener.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("mesh listener %s was not released", address)
}
