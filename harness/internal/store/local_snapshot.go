package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelUnavailable  = errors.New("Channel is not active and ready")
	ErrAudienceUnavailable = errors.New("audience binding baseline is not ready")
	ErrAdmissionConflict   = errors.New("local admission snapshot is stale")
)

type localAdmissionRequirements struct {
	requireJoinedTopic bool
	requireAudience    bool
}

var ordinaryLocalAdmission = localAdmissionRequirements{requireJoinedTopic: true, requireAudience: true}

func validateAdmissionAuthority(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, acceptedAt time.Time,
) ([]byte, error) {
	return validateAdmissionAuthorityWithRequirements(ctx, tx, spec, operation, acceptedAt,
		ordinaryLocalAdmission)
}

func validateAdmissionAuthorityWithRequirements(ctx context.Context, tx *sql.Tx,
	spec LocalAcceptanceSpec, operation model.Operation, acceptedAt time.Time,
	requirements localAdmissionRequirements,
) ([]byte, error) {
	node, _, err := validateAdmissionNodeProfile(ctx, tx, spec, operation, acceptedAt)
	if err != nil {
		return nil, err
	}
	publicKey, err := validateAdmissionChannelMember(ctx, tx, spec, node, requirements)
	if err != nil {
		return nil, err
	}
	if requirements.requireAudience {
		if err := validateAdmissionAudience(ctx, tx, spec); err != nil {
			return nil, err
		}
	}
	return publicKey, nil
}

func validateAdmissionNodeProfile(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, acceptedAt time.Time,
) (model.Node, model.Profile, error) {
	snapshot := spec.Scope
	node, err := readNode(ctx, tx)
	if err != nil || node.PeerID() != snapshot.Node().PeerID() ||
		node.OriginEpoch() != snapshot.Node().OriginEpoch() ||
		node.NextOriginSequence() != snapshot.FirstOriginSequence() ||
		node.ActiveAssetRevision() != snapshot.Node().ActiveAssetRevision() {
		return model.Node{}, model.Profile{}, fmt.Errorf("%w: Node authority changed", ErrAdmissionConflict)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil || !profile.Enabled() || !sameProfileIdentity(profile, snapshot.Profile()) ||
		!sameProfileAuthority(profile, snapshot.Profile()) ||
		profile.ActiveAssetRevision() != node.ActiveAssetRevision() {
		return model.Node{}, model.Profile{}, fmt.Errorf("%w: Profile authority changed", ErrAdmissionConflict)
	}
	if err := validateAdmissionAgentRun(ctx, tx, spec, operation, profile); err != nil {
		return model.Node{}, model.Profile{}, err
	}
	count := uint64(len(spec.Items))
	if node.NextOriginSequence() > model.MaxSQLiteInteger-count || acceptedAt.Before(node.UpdatedAt()) {
		return model.Node{}, model.Profile{}, fmt.Errorf("%w: origin sequence successor exhausted or clock regressed",
			ErrAdmissionConflict)
	}
	return node, profile, nil
}

func validateAdmissionAgentRun(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, profile model.Profile,
) error {
	if spec.Operation == nil {
		return nil
	}
	var runProfile, runtime, runStatus string
	err := tx.QueryRowContext(ctx, `SELECT profile_id,runtime_kind,status FROM agent_runs WHERE run_id=?`,
		operation.AgentRunID().String()).Scan(&runProfile, &runtime, &runStatus)
	if err != nil || runProfile != profile.ID().String() || runtime != string(profile.Runtime()) ||
		(runStatus != "starting" && runStatus != "running" && runStatus != "runtime_finished") {
		return fmt.Errorf("%w: acting AgentRun authority changed", ErrAdmissionConflict)
	}
	return nil
}

func validateAdmissionChannelMember(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	node model.Node, requirements localAdmissionRequirements,
) ([]byte, error) {
	snapshot := spec.Scope
	count := uint64(len(spec.Items))
	var status, topic string
	var rosterRevision, sourceHead uint64
	var rosterHash []byte
	err := tx.QueryRowContext(ctx, `SELECT c.status,c.topic_state,c.roster_head_revision,c.roster_head_hash,
		p.source_head_channel_seq FROM channels c JOIN publication_epochs p ON p.channel_id=c.channel_id
		AND p.origin_peer_id=? AND p.origin_epoch=? WHERE c.channel_id=?`, node.PeerID().String(),
		node.OriginEpoch().String(), snapshot.ChannelID().String()).
		Scan(&status, &topic, &rosterRevision, &rosterHash, &sourceHead)
	if err != nil || status != string(model.ChannelActive) ||
		(requirements.requireJoinedTopic && topic != string(model.TopicJoined)) ||
		rosterRevision != snapshot.PublicationRoster().Revision() ||
		!bytes.Equal(rosterHash, snapshot.PublicationRoster().Digest().Bytes()) ||
		sourceHead+1 != snapshot.FirstChannelSequence() || sourceHead > model.MaxSQLiteInteger-count {
		return nil, fmt.Errorf("%w: Channel or publication head changed", ErrAdmissionConflict)
	}
	var memberRevision uint64
	var memberHash, publicKey []byte
	var epoch, memberStatus string
	err = tx.QueryRowContext(ctx, `SELECT revision,record_hash,origin_epoch,status,public_key FROM channel_members
		WHERE channel_id=? AND member_peer_id=? ORDER BY revision DESC LIMIT 1`, snapshot.ChannelID().String(),
		node.PeerID().String()).Scan(&memberRevision, &memberHash, &epoch, &memberStatus, &publicKey)
	if err != nil || memberRevision != snapshot.OriginMember().Revision() ||
		!bytes.Equal(memberHash, snapshot.OriginMember().Digest().Bytes()) ||
		epoch != node.OriginEpoch().String() || memberStatus != string(model.MemberActive) {
		return nil, fmt.Errorf("%w: origin member head changed", ErrAdmissionConflict)
	}
	return append([]byte(nil), publicKey...), nil
}

func validateAdmissionAudience(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec) error {
	seen := make(map[model.PeerID]struct{})
	for _, item := range spec.Items {
		for _, target := range item.Publication.Event().Audience().Peers() {
			if _, ok := seen[target]; ok {
				continue
			}
			seen[target] = struct{}{}
			binding, confirmed, err := readAudienceBindingTx(ctx, tx, spec.Scope, target)
			if err != nil || binding != model.BindingActive || !confirmed.Valid {
				return fmt.Errorf("%w: target %s", ErrAudienceUnavailable, target.String())
			}
		}
	}
	return nil
}

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
	scope, err := prepareLocalAdmissionScopeTx(ctx, tx, channel, count, true)
	if err != nil {
		return LocalAdmissionScope{}, err
	}
	for _, target := range audience.Peers() {
		binding, confirmed, err := readAudienceBindingTx(ctx, tx, scope, target)
		if err != nil || binding != model.BindingActive || !confirmed.Valid {
			return LocalAdmissionScope{}, fmt.Errorf("%w: target %s", ErrAudienceUnavailable, target.String())
		}
	}

	if err := tx.Commit(); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: commit read: %w", err)
	}
	return scope, nil
}

// prepareLocalAdmissionScopeTx owns the common immutable authority slice used
// by ordinary admission and home-deadline expiry. Home expiry may persist while
// the transport topic is recovering, but no caller may write on a non-active
// Channel or without a current active local membership.
func prepareLocalAdmissionScopeTx(ctx context.Context, tx *sql.Tx, channel model.ChannelID,
	count uint8, requireJoinedTopic bool,
) (LocalAdmissionScope, error) {
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
	var rosterBytes []byte
	if err := tx.QueryRowContext(ctx, `SELECT status,topic_state,roster_head_revision,
		roster_head_hash FROM channels WHERE channel_id=?`, channel.String()).
		Scan(&status, &topic, &rosterRevision, &rosterBytes); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: Channel: %w", err)
	}
	if status != string(model.ChannelActive) ||
		(requireJoinedTopic && topic != string(model.TopicJoined)) {
		return LocalAdmissionScope{}, fmt.Errorf("%w: status=%s topic=%s", ErrChannelUnavailable, status, topic)
	}
	rosterDigest, err := model.DigestFromBytes(rosterBytes)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: roster digest: %w", err)
	}
	rosterHead, err := model.NewRecordHead(rosterRevision, rosterDigest)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: roster head: %w", err)
	}

	var memberRevision uint64
	var memberBytes []byte
	var memberEpoch, memberStatus string
	if err := tx.QueryRowContext(ctx, `SELECT revision,record_hash,origin_epoch,status
		FROM channel_members WHERE channel_id=? AND member_peer_id=?
		ORDER BY revision DESC LIMIT 1`, channel.String(), node.PeerID().String()).
		Scan(&memberRevision, &memberBytes, &memberEpoch, &memberStatus); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: origin member: %w", err)
	}
	if memberEpoch != node.OriginEpoch().String() || memberStatus != string(model.MemberActive) {
		return LocalAdmissionScope{}, fmt.Errorf("%w: local origin membership is not active", ErrChannelUnavailable)
	}
	memberDigest, err := model.DigestFromBytes(memberBytes)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: member digest: %w", err)
	}
	memberHead, err := model.NewRecordHead(memberRevision, memberDigest)
	if err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: member head: %w", err)
	}

	var sourceHead uint64
	if err := tx.QueryRowContext(ctx, `SELECT source_head_channel_seq FROM publication_epochs
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, channel.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&sourceHead); err != nil {
		return LocalAdmissionScope{}, fmt.Errorf("prepare local admission: publication epoch: %w", err)
	}
	if node.NextOriginSequence() > model.MaxSQLiteInteger-uint64(count) ||
		sourceHead > model.MaxSQLiteInteger-uint64(count) {
		return LocalAdmissionScope{}, errors.New("prepare local admission: sequence range exhausted")
	}
	return LocalAdmissionScope{node: node, profile: profile, channelID: channel,
		originMember: memberHead, publicationRoster: rosterHead,
		firstOriginSequence: node.NextOriginSequence(), firstChannelSequence: sourceHead + 1,
		count: count}, nil
}

func readAudienceBindingTx(ctx context.Context, tx *sql.Tx, scope LocalAdmissionScope,
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
