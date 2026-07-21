package peer

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type ChannelMemberLeaveControl struct {
	AuthenticatedPeerID model.PeerID
	Request             model.SignedChannelLeaveRequest
	At                  time.Time
}

type ChannelMemberLeaveAuthority struct {
	Descriptor   model.SignedChannelDescriptor
	ActiveMember model.Member
	Receipt      model.SignedChannelLeaveReceipt
}

// ChannelMemberLeaveController is a separate capability so member hello/sync
// implementations cannot accidentally accept a membership mutation. The
// concrete Channel manager implements both interfaces.
type ChannelMemberLeaveController interface {
	AcceptMemberLeaveGate(context.Context,
		ChannelMemberLeaveControl,
	) (ChannelMemberLeaveAuthority, error)
}

func (service *ChannelMemberService) handleLeave(ctx context.Context, stream network.Stream,
	requestID ChannelRequestID, remotePeerID model.PeerID, remotePublicKey []byte,
	payload LeaveRequest, at time.Time,
) error {
	request := payload.SignedRequest()
	if request.Record().MemberPeerID() != remotePeerID {
		return ErrChannelMemberProtocol
	}
	controller, ok := service.controller.(ChannelMemberLeaveController)
	if !ok || controller == nil {
		return ErrChannelMemberProtocol
	}
	result, err := controller.AcceptMemberLeaveGate(ctx, ChannelMemberLeaveControl{
		AuthenticatedPeerID: remotePeerID, Request: request, At: at})
	if err != nil {
		return service.respondControllerFailure(stream, requestID, err)
	}
	if result.ActiveMember.PeerID() != remotePeerID ||
		!bytes.Equal(result.ActiveMember.PublicKey(), remotePublicKey) ||
		model.VerifyChannelLeaveReceipt(result.Descriptor, result.ActiveMember,
			request, result.Receipt) != nil {
		return fmt.Errorf("%w: controller returned invalid leave authority", ErrChannelMemberProtocol)
	}
	receipt, err := NewLeaveReceipt(result.Receipt)
	if err != nil {
		return fmt.Errorf("%w: construct leave receipt: %v", ErrChannelMemberProtocol, err)
	}
	return writeChannelMemberFrame(stream, requestID, receipt)
}
