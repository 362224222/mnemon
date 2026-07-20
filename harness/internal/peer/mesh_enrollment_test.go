package peer

import (
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshRuntimeAdvertisesConcreteBoundedAddresses(t *testing.T) {
	t.Parallel()
	owner := testkit.NewIdentity(t, "mesh-enrollment-addresses")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, "2026-07-18T08:00:00Z"))
	runtime := newTestMeshRuntime(t, context.Background(), owner, readMeshRuntimeAuthority(t, st))
	addresses := runtime.AdvertisedMultiaddrs()
	if len(addresses) == 0 {
		t.Fatal("Mesh runtime advertised no concrete listener address")
	}
	for _, address := range addresses {
		if address == "/ip4/0.0.0.0/tcp/0" || address == "/ip6/::/tcp/0" {
			t.Fatalf("Mesh runtime advertised wildcard address %q", address)
		}
	}
}
