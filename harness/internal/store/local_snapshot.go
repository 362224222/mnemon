package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelUnavailable  = errors.New("Channel is not active and ready")
	ErrAudienceUnavailable = errors.New("audience binding baseline is not ready")
	ErrAdmissionConflict   = errors.New("local admission snapshot is stale")
)

// LocalAdmissionScope is a read-only optimistic snapshot. Event assembly may
// happen outside SQLite; CommitLocalAcceptance must compare every frozen head
// and sequence again before writing.
type LocalAdmissionScope struct {
	node                 model.Node
	profile              model.Profile
	channelID            model.ChannelID
	originMember         model.RecordHead
	publicationRoster    model.RecordHead
	firstOriginSequence  uint64
	firstChannelSequence uint64
	count                uint8
}

func (s LocalAdmissionScope) Node() model.Node                    { return s.node }
func (s LocalAdmissionScope) Profile() model.Profile              { return s.profile }
func (s LocalAdmissionScope) ChannelID() model.ChannelID          { return s.channelID }
func (s LocalAdmissionScope) OriginMember() model.RecordHead      { return s.originMember }
func (s LocalAdmissionScope) PublicationRoster() model.RecordHead { return s.publicationRoster }
func (s LocalAdmissionScope) FirstOriginSequence() uint64         { return s.firstOriginSequence }
func (s LocalAdmissionScope) FirstChannelSequence() uint64        { return s.firstChannelSequence }
func (s LocalAdmissionScope) Count() uint8                        { return s.count }

func (s LocalAdmissionScope) EventScope(index uint8, work model.WorkRef) (model.EventScope, error) {
	if index >= s.count {
		return model.EventScope{}, fmt.Errorf("admission scope index %d outside batch of %d", index, s.count)
	}
	return model.NewEventScope(s.channelID, s.node.PeerID(), s.node.OriginEpoch(),
		s.firstOriginSequence+uint64(index), s.firstChannelSequence+uint64(index),
		s.originMember, s.publicationRoster, work)
}

// PrepareLocalAdmission freezes trusted Node/Profile/Channel authority and a
// consecutive sequence range without writing. Every audience target must
// already have an active binding and confirmed outbound baseline.
func (s *Store) PrepareLocalAdmission(ctx context.Context, channel model.ChannelID, audience model.Audience,
	count uint8,
) (LocalAdmissionScope, error) {
	if s == nil || s.db == nil || ctx == nil || channel.IsZero() || audience.Len() == 0 ||
		count == 0 || count > model.MaxChildWorks {
		return LocalAdmissionScope{}, errors.New("prepare local admission: incomplete or out-of-range input")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: Node: %w", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: Profile: %w", err)
	}
	if !profile.Enabled() || profile.ActiveAssetRevision() != node.ActiveAssetRevision() {
		return LocalAdmissionScope{}, fmt.Errorf("%w: Profile is disabled or asset revision drifted", ErrChannelUnavailable)
	}

	var status, topic string
	var rosterRevision uint64
	var rosterDigestBytes []byte
	if err := tx.QueryRowContext(ctx,
		"SELECT status, topic_state, roster_head_revision, roster_head_hash FROM channels WHERE channel_id = ?",
		channel.String()).Scan(&status, &topic, &rosterRevision, &rosterDigestBytes); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: Channel: %w", err)
	}
	if status != string(model.ChannelActive) || topic != string(model.TopicJoined) {
		return LocalAdmissionScope{}, fmt.Errorf("%w: status=%s topic=%s", ErrChannelUnavailable, status, topic)
	}
	rosterDigest, err := model.DigestFromBytes(rosterDigestBytes)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: roster digest: %w", err)
	}
	rosterHead, err := model.NewRecordHead(rosterRevision, rosterDigest)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: roster head: %w", err)
	}

	var memberRevision uint64
	var memberDigestBytes []byte
	var memberEpoch, memberStatus string
	if err := tx.QueryRowContext(ctx,
		"SELECT revision, record_hash, origin_epoch, status FROM channel_members WHERE channel_id = ? AND member_peer_id = ? ORDER BY revision DESC LIMIT 1",
		channel.String(), node.PeerID().String()).Scan(&memberRevision, &memberDigestBytes, &memberEpoch, &memberStatus); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: origin member: %w", err)
	}
	if memberEpoch != node.OriginEpoch().String() || memberStatus != string(model.MemberActive) {
		return LocalAdmissionScope{}, fmt.Errorf("%w: local origin membership is not active", ErrChannelUnavailable)
	}
	memberDigest, err := model.DigestFromBytes(memberDigestBytes)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: member digest: %w", err)
	}
	memberHead, err := model.NewRecordHead(memberRevision, memberDigest)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: member head: %w", err)
	}

	var sourceHead uint64
	if err := tx.QueryRowContext(ctx,
		"SELECT source_head_channel_seq FROM publication_epochs WHERE channel_id = ? AND origin_peer_id = ? AND origin_epoch = ?",
		channel.String(), node.PeerID().String(), node.OriginEpoch().String()).Scan(&sourceHead); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: publication epoch: %w", err)
	}
	// next_origin_seq records the next value after commit, so unlike the
	// publication head it must retain one representable successor.
	if node.NextOriginSequence() > model.MaxSQLiteInteger-uint64(count) ||
		sourceHead > model.MaxSQLiteInteger-uint64(count) {
		return LocalAdmissionScope{}, errors.New("prepare local admission: sequence range exhausted")
	}

	for _, target := range audience.Peers() {
		var bindingState string
		var confirmed sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT b.state, a.baseline_confirmed_at
			FROM peer_bindings b LEFT JOIN peer_pull_acks a
			ON a.channel_id=b.channel_id AND a.target_peer_id=b.peer_id
			AND a.origin_peer_id=? AND a.origin_epoch=?
			WHERE b.channel_id=? AND b.peer_id=?`, node.PeerID().String(), node.OriginEpoch().String(),
			channel.String(), target.String()).Scan(&bindingState, &confirmed)
		if err != nil || bindingState != string(model.BindingActive) || !confirmed.Valid {
			return LocalAdmissionScope{}, fmt.Errorf("%w: target %s", ErrAudienceUnavailable, target.String())
		}
	}

	if err := tx.Commit(); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: commit read: %w", err)
	}
	return LocalAdmissionScope{node: node, profile: profile, channelID: channel, originMember: memberHead,
		publicationRoster: rosterHead, firstOriginSequence: node.NextOriginSequence(),
		firstChannelSequence: sourceHead + 1, count: count}, nil
}
