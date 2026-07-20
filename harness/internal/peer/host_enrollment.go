package peer

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/network"
)

// openEnrollmentStream is the only managed-host bypass for a Peer that lacks
// durable Channel authority. It consumes one exact gater capability and opens
// only ChannelProtocol to the bound owner. General Host.NewStream calls never
// consult this permit path.
func (node *NodeHost) openEnrollmentStream(ctx context.Context,
	permit outboundEnrollmentPermitToken,
) (network.Stream, error) {
	if node == nil || node.host == nil || node.gater == nil || ctx == nil || ctx.Err() != nil ||
		permit.generation == 0 || permit.key.protocol != string(ChannelProtocol) ||
		permit.key.frameVersion != ChannelFrameVersion {
		return nil, fmt.Errorf("%w: exact enrollment transport is unavailable", ErrNodeHost)
	}
	if !node.gater.claimOutboundEnrollmentStream(permit) {
		return nil, fmt.Errorf("%w: enrollment transport permit is absent, expired, or consumed", ErrNodeHost)
	}
	stream, err := node.host.NewStream(ctx, permit.key.ownerPeerID, ChannelProtocol)
	if err != nil {
		return nil, fmt.Errorf("%w: open exact enrollment stream: %w", ErrNodeHost, err)
	}
	if stream.Protocol() != ChannelProtocol || stream.Conn() == nil ||
		stream.Stat().Direction != network.DirOutbound ||
		stream.Conn().RemotePeer() != permit.key.ownerPeerID ||
		!permit.allowsAddress(stream.Conn().RemoteMultiaddr()) ||
		!node.gater.registerOutboundEnrollmentStream(permit, stream) {
		_ = stream.Reset()
		return nil, fmt.Errorf("%w: enrollment transport authority changed during stream open", ErrNodeHost)
	}
	return stream, nil
}
