package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ChannelPeerReadiness is a read-only projection of the two directional
// baseline gates. Ready is deliberately derived rather than persisted.
type ChannelPeerReadiness struct {
	ChannelID     model.ChannelID
	PeerID        model.PeerID
	OriginEpoch   model.OriginEpoch
	BindingState  model.BindingState
	TopicState    model.TopicState
	RosterHead    model.RecordHead
	InboundReady  bool
	OutboundReady bool
}

func (readiness ChannelPeerReadiness) Ready() bool {
	return readiness.BindingState == model.BindingActive &&
		readiness.TopicState == model.TopicJoined && readiness.InboundReady && readiness.OutboundReady
}

// ReadChannelBaselineReadiness derives readiness exclusively from durable
// signed authority and the two directional evidence tables. It never repairs
// or persists an aggregate readiness bit.
func (s *Store) ReadChannelBaselineReadiness(ctx context.Context,
	channelID model.ChannelID,
) ([]ChannelPeerReadiness, error) {
	if s == nil || s.db == nil || ctx == nil || channelID.IsZero() {
		return nil, ErrChannelBaselineInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("read Channel baseline readiness: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: Node: %v", ErrChannelBaselineAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrChannelBaselineAuthority, err)
	}

	localSourceHead, err := readLocalPublicationHead(ctx, tx, channelID, node)
	if err != nil {
		return nil, err
	}

	result := make([]ChannelPeerReadiness, 0, len(authority.bindings))
	for _, binding := range authority.bindings {
		inboundReady, err := readInboundBaselineReadiness(ctx, tx, authority, binding)
		if err != nil {
			return nil, err
		}
		outboundReady, err := readOutboundBaselineReadiness(ctx, tx, authority, binding, node,
			localSourceHead)
		if err != nil {
			return nil, err
		}
		result = append(result, ChannelPeerReadiness{ChannelID: channelID, PeerID: binding.PeerID(),
			OriginEpoch: binding.OriginEpoch(), BindingState: binding.State(),
			TopicState: authority.channel.TopicState(), RosterHead: authority.roster.Head(),
			InboundReady: inboundReady, OutboundReady: outboundReady})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("read Channel baseline readiness: commit read: %w", err)
	}
	return result, nil
}

func readInboundBaselineReadiness(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, binding model.PeerBinding,
) (bool, error) {
	var baseline, contiguous, observed uint64
	var updatedText string
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,contiguous_channel_seq,
		observed_channel_seq,updated_at FROM peer_cursors WHERE channel_id=?
		AND origin_peer_id=? AND origin_epoch=?`, authority.channel.ID().String(),
		binding.PeerID().String(), binding.OriginEpoch().String()).
		Scan(&baseline, &contiguous, &observed, &updatedText)
	present := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read Channel baseline readiness: inbound gate: %w", err)
	}
	if present {
		updatedAt, parseErr := parseCanonicalStoreTime(updatedText)
		if parseErr != nil || baseline > model.MaxSQLiteInteger || contiguous < baseline ||
			observed < contiguous || observed > model.MaxSQLiteInteger ||
			updatedAt.Before(authority.channel.CreatedAt()) || binding.State() == model.BindingPending {
			return false, ErrChannelBaselineAuthority
		}
	}
	if binding.State() == model.BindingActive && !present {
		return false, ErrChannelBaselineAuthority
	}
	return binding.State() == model.BindingActive && present, nil
}

func readOutboundBaselineReadiness(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, binding model.PeerBinding, node model.Node, localSourceHead uint64,
) (bool, error) {
	var baseline, acknowledged uint64
	var confirmedText sql.NullString
	var updatedText string
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
		AND origin_peer_id=? AND origin_epoch=?`, authority.channel.ID().String(),
		binding.PeerID().String(), node.PeerID().String(), node.OriginEpoch().String()).
		Scan(&baseline, &acknowledged, &confirmedText, &updatedText)
	present := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read Channel baseline readiness: outbound gate: %w", err)
	}
	if !present {
		return false, nil
	}
	updatedAt, parseErr := parseCanonicalStoreTime(updatedText)
	if parseErr != nil || baseline > model.MaxSQLiteInteger || acknowledged < baseline ||
		acknowledged > localSourceHead || baseline > localSourceHead ||
		updatedAt.Before(authority.channel.CreatedAt()) {
		return false, ErrChannelBaselineAuthority
	}
	if !confirmedText.Valid {
		return false, nil
	}
	confirmedAt, parseErr := parseCanonicalStoreTime(confirmedText.String)
	if parseErr != nil || confirmedAt.Before(authority.channel.CreatedAt()) ||
		confirmedAt.After(updatedAt) {
		return false, ErrChannelBaselineAuthority
	}
	return binding.State() != model.BindingRevoked, nil
}
