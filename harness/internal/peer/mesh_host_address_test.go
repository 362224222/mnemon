package peer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestControlledAddrsFactoryDerivesBoundedImmutableSnapshot(t *testing.T) {
	const port = uint16(4201)
	candidates := make([]ma.Multiaddr, 0, 14)
	for index := 1; index <= 10; index++ {
		candidates = append(candidates,
			ma.StringCast(fmt.Sprintf("/ip4/127.0.0.%d/tcp/%d", index, port)))
	}
	candidates = append(candidates,
		ma.StringCast("/ip4/0.0.0.0/tcp/4201"),
		ma.StringCast("/ip4/127.0.0.1/tcp/4202"),
		ma.StringCast("/ip4/127.0.0.1/udp/4201"),
		ma.StringCast("/ip4/127.0.0.1/tcp/4201"))
	factory := newControlledAddrsFactory(nil)
	if got := factory.apply(candidates); len(got) != 0 {
		t.Fatalf("unfrozen derived factory output = %v", got)
	}
	frozen, err := factory.freeze(candidates, port)
	if err != nil || len(frozen) != model.MaxMemberMultiaddrs {
		t.Fatalf("freeze() = (%v, %v)", frozen, err)
	}
	want := meshHostAddrStrings(frozen)
	for _, raw := range want {
		_, current, err := inspectDirectTCPAddr(ma.StringCast(raw), false, false)
		if err != nil || current != port {
			t.Fatalf("selected address %q = port %d, error %v", raw, current, err)
		}
	}
	frozen[0] = ma.StringCast("/ip4/127.0.0.99/tcp/4201")
	input := []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.100/tcp/4201")}
	if got := meshHostAddrStrings(factory.apply(input)); !reflect.DeepEqual(got, want) {
		t.Fatalf("frozen factory output = %v, want %v", got, want)
	}
	if _, err := factory.freeze(candidates, port); !errors.Is(err, ErrMeshHost) {
		t.Fatalf("second freeze error = %v", err)
	}
}

func TestBindMeshHostRejectsInvalidMechanicalAddressInputs(t *testing.T) {
	identity := testkit.NewIdentity(t, "bound-mesh-host-invalid-addresses")
	key, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	many := make([]ma.Multiaddr, 0, model.MaxMemberMultiaddrs+1)
	for index := 1; index <= model.MaxMemberMultiaddrs+1; index++ {
		many = append(many, ma.StringCast(fmt.Sprintf("/ip4/127.0.0.%d/tcp/4301", index)))
	}
	tests := []struct {
		name string
		spec MeshHostBindSpec
	}{
		{name: "no listener"},
		{name: "two listeners", spec: MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
			ma.StringCast("/ip4/127.0.0.1/tcp/0"), ma.StringCast("/ip4/127.0.0.1/tcp/0")}}},
		{name: "DNS listener", spec: MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
			ma.StringCast("/dns4/node.test/tcp/4301")}}},
		{name: "non TCP listener", spec: MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
			ma.StringCast("/ip4/127.0.0.1/udp/4301")}}},
		{name: "automatic listener with seed", spec: MeshHostBindSpec{
			ListenAddrs:     []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")},
			AdvertisedAddrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4301")}}},
		{name: "different ports", spec: MeshHostBindSpec{
			ListenAddrs:     []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4301")},
			AdvertisedAddrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4302")}}},
		{name: "duplicate seed", spec: MeshHostBindSpec{
			ListenAddrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4301")},
			AdvertisedAddrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4301"),
				ma.StringCast("/ip4/127.0.0.1/tcp/4301")}}},
		{name: "too many seed addresses", spec: MeshHostBindSpec{
			ListenAddrs:     []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4301")},
			AdvertisedAddrs: many}},
		{name: "unspecified advertisement", spec: MeshHostBindSpec{
			ListenAddrs:     []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/4301")},
			AdvertisedAddrs: []ma.Multiaddr{ma.StringCast("/ip4/0.0.0.0/tcp/4301")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bound, err := BindMeshHost(context.Background(), key, test.spec)
			if bound != nil || !errors.Is(err, ErrMeshHost) {
				t.Fatalf("BindMeshHost() = (%v, %v)", bound, err)
			}
		})
	}
	if bound, err := BindMeshHost(nil, key, MeshHostBindSpec{ListenAddrs: []ma.Multiaddr{
		ma.StringCast("/ip4/127.0.0.1/tcp/0")}}); bound != nil || !errors.Is(err, ErrMeshHost) {
		t.Fatalf("nil-context BindMeshHost() = (%v, %v)", bound, err)
	}
}
