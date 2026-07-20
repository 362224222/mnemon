package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelBaselineInput         = errors.New("invalid Channel baseline input")
	ErrChannelBaselineAuthority     = errors.New("Channel baseline authority is unavailable")
	ErrChannelBaselineConflict      = errors.New("Channel baseline conflicts with durable state")
	ErrChannelBaselineEpochMismatch = errors.New("Channel baseline origin epoch mismatch")
)

// ChannelDataBaseline freezes one origin's publication head for one new
// directional binding. It is protocol-neutral durable state: the peer layer
// is responsible for encoding it into /mnemon/channel/1 frames.
type ChannelDataBaseline struct {
	ChannelID               model.ChannelID
	OriginPeerID            model.PeerID
	OriginEpoch             model.OriginEpoch
	BaselineChannelSequence uint64
}

// ChannelDataBaselineAck is the exact durable acknowledgement of a baseline.
// An ACK confirms only the frozen baseline; later Pull acknowledgements are a
// separate monotonic cursor operation.
type ChannelDataBaselineAck struct {
	ChannelID               model.ChannelID
	OriginPeerID            model.PeerID
	OriginEpoch             model.OriginEpoch
	BaselineChannelSequence uint64
}

// canonicalChannelBaselineTime canonicalizes a caller-supplied baseline
// transaction time, rejecting unusable instants as baseline input errors.
func canonicalChannelBaselineTime(at time.Time) (time.Time, error) {
	canonical, err := canonicalStoreTime(at)
	if err != nil || canonical.IsZero() {
		return time.Time{}, ErrChannelBaselineInput
	}
	return canonical, nil
}

// readLocalPublicationHead loads the local origin's current publication head
// for one Channel. A missing or overflowing epoch row is an authority failure.
func readLocalPublicationHead(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	node model.Node,
) (uint64, error) {
	var sourceHead uint64
	err := tx.QueryRowContext(ctx, `SELECT source_head_channel_seq FROM publication_epochs
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, channelID.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&sourceHead)
	if err != nil || sourceHead > model.MaxSQLiteInteger {
		return 0, fmt.Errorf("%w: local publication epoch: %v", ErrChannelBaselineAuthority, err)
	}
	return sourceHead, nil
}

func readChannelBaselineAuthority(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	remotePeer model.PeerID,
) (model.Node, verifiedChannelAuthority, model.PeerBinding, error) {
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

func validChannelDataBaseline(baseline ChannelDataBaseline) bool {
	return !baseline.ChannelID.IsZero() && !baseline.OriginPeerID.IsZero() &&
		!baseline.OriginEpoch.IsZero() && baseline.BaselineChannelSequence <= model.MaxSQLiteInteger
}

func mapChannelBaselineMutationError(operation string, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %w", operation, ErrChannelBaselineConflict)
	}
	return fmt.Errorf("%s: %w: %v", operation, ErrChannelBaselineConflict, err)
}
