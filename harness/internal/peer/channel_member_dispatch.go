package peer

import (
	"context"

	"github.com/libp2p/go-libp2p/core/network"
)

func (service *ChannelMemberService) HandleChannelRequest(ctx context.Context,
	stream network.Stream, first ChannelFrame,
) error {
	if !validChannelMemberDispatch(service, ctx, stream, first) {
		return ErrChannelMemberProtocol
	}
	localPeerID, _, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil {
		return err
	}
	remotePeerID, remotePublicKey, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || remotePeerID == localPeerID {
		return ErrChannelMemberProtocol
	}
	at := service.clock.Now()
	if at.IsZero() {
		return ErrChannelMemberProtocol
	}
	switch first.Type() {
	case ChannelFrameMemberHello:
		payload, ok := first.Payload().(MemberHello)
		if !ok {
			return ErrChannelMemberProtocol
		}
		return service.handleHello(ctx, stream, first.RequestID(), remotePeerID,
			remotePublicKey, payload, at)
	case ChannelFrameSyncRequest:
		payload, ok := first.Payload().(SyncRequest)
		if !ok {
			return ErrChannelMemberProtocol
		}
		return service.handleSync(ctx, stream, first.RequestID(), remotePeerID,
			remotePublicKey, payload, at)
	case ChannelFrameDataBaseline:
		payload, ok := first.Payload().(DataBaseline)
		if !ok {
			return ErrChannelMemberProtocol
		}
		return service.handleBaseline(ctx, stream, first.RequestID(), remotePeerID,
			remotePublicKey, payload, at)
	case ChannelFrameLeaveRequest:
		payload, ok := first.Payload().(LeaveRequest)
		if !ok {
			return ErrChannelMemberProtocol
		}
		return service.handleLeave(ctx, stream, first.RequestID(), remotePeerID,
			remotePublicKey, payload, at)
	default:
		return ErrChannelMemberProtocol
	}
}

func validChannelMemberDispatch(service *ChannelMemberService, ctx context.Context,
	stream network.Stream, first ChannelFrame,
) bool {
	return service != nil && service.controller != nil && service.clock != nil && ctx != nil &&
		stream != nil && stream.Protocol() == ChannelProtocol && stream.Conn() != nil && !first.IsZero()
}
