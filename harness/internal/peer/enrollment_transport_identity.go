package peer

import (
	"fmt"
	"sort"
	"strings"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	ma "github.com/multiformats/go-multiaddr"
)

// enrollmentTransportPermitSpec binds one short-lived outbound transport
// exception to the immutable, secret-free enrollment attempt identity. The
// owner addresses must be the exact canonical list carried by the verified
// enrollment token. ChannelProtocol and ChannelFrameVersion are fixed by R5.
type enrollmentTransportPermitSpec struct {
	OwnerPeerID         model.PeerID
	OwnerMultiaddrs     []string
	ChannelID           model.ChannelID
	GrantID             model.GrantID
	EnrollmentRequestID model.EnrollmentRequestID
}

type outboundEnrollmentPermitKey struct {
	ownerPeerID         libp2ppeer.ID
	canonicalAddresses  string
	channelID           string
	grantID             string
	enrollmentRequestID string
	protocol            string
	frameVersion        uint8
}

func canonicalOutboundEnrollmentPermit(spec enrollmentTransportPermitSpec) (
	outboundEnrollmentPermitKey, []ma.Multiaddr, error,
) {
	owner, err := canonicalLibp2pID(spec.OwnerPeerID)
	if err != nil || spec.ChannelID.IsZero() || spec.GrantID.IsZero() ||
		spec.EnrollmentRequestID.IsZero() || len(spec.OwnerMultiaddrs) == 0 ||
		len(spec.OwnerMultiaddrs) > model.MaxMemberMultiaddrs {
		return outboundEnrollmentPermitKey{}, nil,
			fmt.Errorf("%w: complete owner, Channel, grant, request and addresses are required",
				ErrEnrollmentTransportPermit)
	}
	signed := append([]string(nil), spec.OwnerMultiaddrs...)
	if !sort.StringsAreSorted(signed) {
		return outboundEnrollmentPermitKey{}, nil,
			fmt.Errorf("%w: owner addresses are not in canonical order", ErrEnrollmentTransportPermit)
	}
	canonical := make([]ma.Multiaddr, 0, len(signed))
	for index, raw := range signed {
		address, parseErr := ma.NewMultiaddr(raw)
		if parseErr != nil || address.String() != raw || (index != 0 && raw == signed[index-1]) {
			return outboundEnrollmentPermitKey{}, nil,
				fmt.Errorf("%w: owner addresses are not canonical and unique", ErrEnrollmentTransportPermit)
		}
		if resolved, resolveErr := canonicalPeerAddresses(owner, raw); resolveErr != nil ||
			len(resolved) != 1 {
			return outboundEnrollmentPermitKey{}, nil,
				fmt.Errorf("%w: owner address identity mismatch", ErrEnrollmentTransportPermit)
		}
		canonical = append(canonical, address)
	}
	key := outboundEnrollmentPermitKey{ownerPeerID: owner,
		canonicalAddresses: strings.Join(signed, "\x00"), channelID: spec.ChannelID.String(),
		grantID: spec.GrantID.String(), enrollmentRequestID: spec.EnrollmentRequestID.String(),
		protocol: string(ChannelProtocol), frameVersion: ChannelFrameVersion}
	return key, canonical, nil
}
