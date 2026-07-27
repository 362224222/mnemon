package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func readGossipPublicationAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (model.Node, verifiedChannelAuthority, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, fmt.Errorf("%w: Node: %v",
			ErrGossipPublicationAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, fmt.Errorf("%w: %v",
			ErrGossipPublicationAuthority, err)
	}
	return node, authority, nil
}

func validateGossipPublicationAuthority(node model.Node, authority verifiedChannelAuthority,
	publication model.SignedPublication,
) error {
	if authority.channel.Status() != model.ChannelActive ||
		authority.channel.TopicState() != model.TopicJoined {
		return ErrGossipPublicationAuthority
	}
	event := publication.Event()
	scope := event.Scope()
	if event.Source() != model.EventSourceLocal || scope.ChannelID() != authority.channel.ID() ||
		scope.OriginPeerID() != node.PeerID() || scope.OriginEpoch() != node.OriginEpoch() {
		return ErrGossipPublicationAuthority
	}
	members := authority.roster.Members()
	if scope.PublicationRoster().Revision() == 0 ||
		scope.PublicationRoster().Revision() > uint64(len(members)) ||
		members[scope.PublicationRoster().Revision()-1].Head() != scope.PublicationRoster() ||
		scope.OriginMember().Revision() == 0 ||
		scope.OriginMember().Revision() > scope.PublicationRoster().Revision() {
		return ErrGossipPublicationAuthority
	}
	origin := members[scope.OriginMember().Revision()-1]
	if origin.Head() != scope.OriginMember() || origin.PeerID() != node.PeerID() ||
		origin.OriginEpoch() != node.OriginEpoch() || origin.Status() != model.MemberActive {
		return ErrGossipPublicationAuthority
	}
	if _, err := validateLocalPublication(publication, origin.PublicKey()); err != nil {
		return fmt.Errorf("%w: %v", ErrGossipPublicationInvariant, err)
	}
	return nil
}
