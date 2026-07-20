package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readChannelBaselineAuthority(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	remotePeer model.PeerID) (model.Node, verifiedChannelAuthority, model.PeerBinding, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{},
			fmt.Errorf("%w: Node: %v", ErrChannelBaselineAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{},
			fmt.Errorf("%w: %v", ErrChannelBaselineAuthority, err)
	}
	if authority.channel.Status() != model.ChannelActive {
		return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelBaselineAuthority
	}
	localMember, ok := authority.roster.CurrentMember(node.PeerID())
	if !ok || localMember.Status() != model.MemberActive ||
		localMember.OriginEpoch() != node.OriginEpoch() {
		return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelBaselineAuthority
	}
	for _, binding := range authority.bindings {
		if binding.PeerID() == remotePeer {
			if binding.State() == model.BindingRevoked {
				return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelBaselineConflict
			}
			remoteMember, exists := authority.roster.CurrentMember(remotePeer)
			if !exists || remoteMember.Status() != model.MemberActive ||
				remoteMember.OriginEpoch() != binding.OriginEpoch() {
				return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelBaselineAuthority
			}
			return node, authority, binding, nil
		}
	}
	return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelBaselineAuthority
}

func readExactOutboundChannelBaselineAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID, remotePeer model.PeerID, expectedRosterHead model.RecordHead,
) (model.Node, verifiedChannelAuthority, model.PeerBinding, error) {
	node, authority, binding, err := readChannelBaselineAuthority(ctx, tx, channelID, remotePeer)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, err
	}
	if authority.roster.Head() != expectedRosterHead {
		return model.Node{}, verifiedChannelAuthority{}, model.PeerBinding{}, ErrChannelBaselineConflict
	}
	return node, authority, binding, nil
}

func validReserveOutboundChannelBaseline(s *Store, ctx context.Context,
	spec ReserveOutboundChannelBaselineSpec,
) bool {
	return s != nil && s.db != nil && ctx != nil && !spec.ChannelID.IsZero() &&
		!spec.TargetPeerID.IsZero() && !spec.ExpectedRosterHead.IsZero()
}

func validConfirmOutboundChannelBaseline(s *Store, ctx context.Context,
	spec ConfirmOutboundChannelBaselineSpec,
) bool {
	return s != nil && s.db != nil && ctx != nil && !spec.AuthenticatedPeerID.IsZero() &&
		!spec.ExpectedRosterHead.IsZero() && validChannelDataBaseline(ChannelDataBaseline(spec.Ack))
}
