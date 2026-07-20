package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// prepareWorkDeadlineScopeTx freezes local publication authority without
// requiring a joined transport topic. A home deadline remains authoritative
// while transport is recovering, but never after Channel or membership exit.
func prepareWorkDeadlineScopeTx(ctx context.Context, tx *sql.Tx, channel model.ChannelID,
	count uint8,
) (LocalAdmissionScope, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare Work deadline: Node: %w", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare Work deadline: Profile: %w", err)
	}
	if !profile.Enabled() || profile.ActiveAssetRevision() != node.ActiveAssetRevision() {
		return LocalAdmissionScope{}, fmt.Errorf("%w: Profile is disabled or asset revision drifted",
			ErrChannelUnavailable)
	}
	roster, err := readWorkDeadlineChannel(ctx, tx, channel)
	if err != nil {
		return LocalAdmissionScope{}, err
	}
	member, err := readLocalAdmissionMember(ctx, tx, channel, node)
	if err != nil {
		return LocalAdmissionScope{}, err
	}
	sourceHead, err := readLocalAdmissionSourceHead(ctx, tx, channel, node)
	if err != nil {
		return LocalAdmissionScope{}, err
	}
	if node.NextOriginSequence() > model.MaxSQLiteInteger-uint64(count) ||
		sourceHead > model.MaxSQLiteInteger-uint64(count) {
		return LocalAdmissionScope{}, errors.New("prepare Work deadline: sequence range exhausted")
	}
	return LocalAdmissionScope{node: node, profile: profile, channelID: channel,
		originMember: member, publicationRoster: roster,
		firstOriginSequence: node.NextOriginSequence(), firstChannelSequence: sourceHead + 1,
		count: count}, nil
}

func readWorkDeadlineChannel(ctx context.Context, tx *sql.Tx,
	channel model.ChannelID,
) (model.RecordHead, error) {
	var status string
	var revision uint64
	var digestBytes []byte
	if err := tx.QueryRowContext(ctx, `SELECT status,roster_head_revision,roster_head_hash
		FROM channels WHERE channel_id=?`, channel.String()).
		Scan(&status, &revision, &digestBytes); err != nil {
		return model.RecordHead{}, fmt.Errorf("prepare Work deadline: Channel: %w", err)
	}
	if status != string(model.ChannelActive) {
		return model.RecordHead{}, fmt.Errorf("%w: status=%s", ErrChannelUnavailable, status)
	}
	digest, err := model.DigestFromBytes(digestBytes)
	if err != nil {
		return model.RecordHead{}, fmt.Errorf("prepare Work deadline: roster digest: %w", err)
	}
	head, err := model.NewRecordHead(revision, digest)
	if err != nil {
		return model.RecordHead{}, fmt.Errorf("prepare Work deadline: roster head: %w", err)
	}
	return head, nil
}

func readWorkDeadlineAudienceBinding(ctx context.Context, tx *sql.Tx, scope LocalAdmissionScope,
	target model.PeerID,
) (model.BindingState, sql.NullString, error) {
	var binding string
	var confirmed sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT b.state,a.baseline_confirmed_at
		FROM peer_bindings b LEFT JOIN peer_pull_acks a
		ON a.channel_id=b.channel_id AND a.target_peer_id=b.peer_id
		AND a.origin_peer_id=? AND a.origin_epoch=?
		WHERE b.channel_id=? AND b.peer_id=?`, scope.Node().PeerID().String(),
		scope.Node().OriginEpoch().String(), scope.ChannelID().String(), target.String()).
		Scan(&binding, &confirmed)
	return model.BindingState(binding), confirmed, err
}
