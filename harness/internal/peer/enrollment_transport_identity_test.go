package peer

import (
	"errors"
	"testing"

	ma "github.com/multiformats/go-multiaddr"
)

func TestCanonicalEnrollmentTransportIdentityRejectsAddressAliasAndFixesWireScope(t *testing.T) {
	t.Parallel()
	owner := testAuthorityPeer(t, "permit-identity-owner")
	spec := testEnrollmentTransportPermitSpec(t, owner,
		[]string{"/ip4/127.0.0.1/tcp/44001"}, "identity")
	key, addresses, err := canonicalOutboundEnrollmentPermit(spec)
	if err != nil {
		t.Fatal(err)
	}
	if key.protocol != string(ChannelProtocol) || key.frameVersion != ChannelFrameVersion ||
		len(addresses) != 1 || !addresses[0].Equal(ma.StringCast(spec.OwnerMultiaddrs[0])) {
		t.Fatal("canonical permit identity changed its fixed protocol, frame, or exact transport")
	}
	spec.OwnerMultiaddrs = []string{"/ip4/127.0.0.1/tcp/044001"}
	if _, _, err := canonicalOutboundEnrollmentPermit(spec); !errors.Is(err, ErrEnrollmentTransportPermit) {
		t.Fatalf("noncanonical address alias error = %v", err)
	}
}
