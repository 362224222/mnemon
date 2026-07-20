package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type ReserveOutboundChannelBaselineSpec struct {
	ChannelID    model.ChannelID
	TargetPeerID model.PeerID
	At           time.Time
}

type ReserveOutboundChannelBaselineResult struct {
	Baseline ChannelDataBaseline
	Reserved bool
}

type ConfirmOutboundChannelBaselineSpec struct {
	AuthenticatedPeerID model.PeerID
	Ack                 ChannelDataBaselineAck
	At                  time.Time
}

type ConfirmOutboundChannelBaselineResult struct {
	Ack         ChannelDataBaselineAck
	Confirmed   bool
	ConfirmedAt time.Time
}

// ReserveOutboundChannelBaseline freezes the local current publication head
// for a target. A replay returns the original reservation even when the local
// head has advanced, so a lost frame cannot silently skip new history.
func (s *Store) ReserveOutboundChannelBaseline(ctx context.Context,
	spec ReserveOutboundChannelBaselineSpec,
) (ReserveOutboundChannelBaselineResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() ||
		spec.TargetPeerID.IsZero() {
		return ReserveOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	at, err := canonicalChannelBaselineTime(spec.At)
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("reserve outbound Channel baseline: begin: %w", err)
	}
	defer tx.Rollback()
	node, authority, binding, err := readChannelBaselineAuthority(ctx, tx, spec.ChannelID,
		spec.TargetPeerID)
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, err
	}
	if at.Before(authority.channel.UpdatedAt()) || binding.State() == model.BindingRevoked {
		return ReserveOutboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}
	sourceHead, err := readLocalPublicationHead(ctx, tx, spec.ChannelID, node)
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, err
	}
	baseline := ChannelDataBaseline{ChannelID: spec.ChannelID, OriginPeerID: node.PeerID(),
		OriginEpoch: node.OriginEpoch(), BaselineChannelSequence: sourceHead}

	replayed, found, err := replayOutboundChannelReservation(ctx, tx, spec, node, authority, at,
		sourceHead, baseline)
	if err != nil || found {
		return replayed, err
	}
	if err := appendOutboundChannelReservation(ctx, tx, spec, node, sourceHead, at); err != nil {
		return ReserveOutboundChannelBaselineResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReserveOutboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"reserve outbound Channel baseline", err)
	}
	return ReserveOutboundChannelBaselineResult{Baseline: baseline, Reserved: true}, nil
}

// replayOutboundChannelReservation returns the originally frozen reservation
// when one already exists for the target. A replay is read-only: it validates
// the durable row and commits without mutating it, even when the local head
// has since advanced past the frozen sequence.
func replayOutboundChannelReservation(ctx context.Context, tx *sql.Tx,
	spec ReserveOutboundChannelBaselineSpec, node model.Node,
	authority verifiedChannelAuthority, at time.Time, sourceHead uint64,
	baseline ChannelDataBaseline,
) (ReserveOutboundChannelBaselineResult, bool, error) {
	var reservedSequence, acknowledgedSequence uint64
	var updatedAtText string
	var confirmedAt sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
		AND origin_peer_id=? AND origin_epoch=?`, spec.ChannelID.String(), spec.TargetPeerID.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&reservedSequence,
		&acknowledgedSequence, &confirmedAt, &updatedAtText)
	if errors.Is(err, sql.ErrNoRows) {
		return ReserveOutboundChannelBaselineResult{}, false, nil
	}
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, false,
			fmt.Errorf("reserve outbound Channel baseline: read replay: %w", err)
	}
	updatedAt, parseErr := parseCanonicalStoreTime(updatedAtText)
	if parseErr != nil || reservedSequence > model.MaxSQLiteInteger ||
		acknowledgedSequence < reservedSequence || acknowledgedSequence > model.MaxSQLiteInteger ||
		reservedSequence > sourceHead || acknowledgedSequence > sourceHead ||
		updatedAt.Before(authority.channel.CreatedAt()) {
		return ReserveOutboundChannelBaselineResult{}, false, fmt.Errorf("%w: invalid outbound reservation",
			ErrChannelBaselineAuthority)
	}
	if at.Before(updatedAt) {
		return ReserveOutboundChannelBaselineResult{}, false, ErrChannelBaselineInput
	}
	if confirmedAt.Valid {
		confirmed, parseErr := parseCanonicalStoreTime(confirmedAt.String)
		if parseErr != nil || confirmed.After(updatedAt) || confirmed.Before(authority.channel.CreatedAt()) {
			return ReserveOutboundChannelBaselineResult{}, false, fmt.Errorf("%w: invalid outbound confirmation",
				ErrChannelBaselineAuthority)
		}
	}
	baseline.BaselineChannelSequence = reservedSequence
	if commitErr := tx.Commit(); commitErr != nil {
		return ReserveOutboundChannelBaselineResult{}, false,
			fmt.Errorf("reserve outbound Channel baseline: commit replay: %w", commitErr)
	}
	return ReserveOutboundChannelBaselineResult{Baseline: baseline}, true, nil
}

// appendOutboundChannelReservation freezes the current local head as the
// durable reservation row for one target peer.
func appendOutboundChannelReservation(ctx context.Context, tx *sql.Tx,
	spec ReserveOutboundChannelBaselineSpec, node model.Node, sourceHead uint64, at time.Time,
) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
		origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
		VALUES(?,?,?,?,?,?,NULL,?)`, spec.ChannelID.String(), spec.TargetPeerID.String(),
		node.PeerID().String(), node.OriginEpoch().String(), sourceHead, sourceHead, storeTime(at))
	if err != nil {
		return mapChannelBaselineMutationError("reserve outbound Channel baseline", err)
	}
	return nil
}

// ConfirmOutboundChannelBaseline opens the local-origin delivery gate only
// after the authenticated target echoes the exact durable reservation.
func (s *Store) ConfirmOutboundChannelBaseline(ctx context.Context,
	spec ConfirmOutboundChannelBaselineSpec,
) (ConfirmOutboundChannelBaselineResult, error) {
	ack := spec.Ack
	if s == nil || s.db == nil || ctx == nil || spec.AuthenticatedPeerID.IsZero() ||
		!validChannelDataBaseline(ChannelDataBaseline(ack)) {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	at, err := canonicalChannelBaselineTime(spec.At)
	if err != nil {
		return ConfirmOutboundChannelBaselineResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ConfirmOutboundChannelBaselineResult{}, fmt.Errorf("confirm outbound Channel baseline: begin: %w", err)
	}
	defer tx.Rollback()
	node, authority, binding, err := readChannelBaselineAuthority(ctx, tx, ack.ChannelID,
		spec.AuthenticatedPeerID)
	if err != nil {
		return ConfirmOutboundChannelBaselineResult{}, err
	}
	if ack.OriginPeerID != node.PeerID() || ack.OriginEpoch != node.OriginEpoch() {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineEpochMismatch
	}
	if at.Before(authority.channel.UpdatedAt()) || binding.State() == model.BindingRevoked {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}

	row, err := readOutboundReservationRow(ctx, tx, ack, spec.AuthenticatedPeerID, node, at)
	if err != nil {
		return ConfirmOutboundChannelBaselineResult{}, err
	}
	if row.reservedSequence != ack.BaselineChannelSequence {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineConflict
	}
	if row.confirmedAt.Valid {
		return replayConfirmedOutboundBaseline(tx, ack, row, authority)
	}
	if err := persistOutboundConfirmation(ctx, tx, ack, spec.AuthenticatedPeerID, node, at); err != nil {
		return ConfirmOutboundChannelBaselineResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ConfirmOutboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"confirm outbound Channel baseline", err)
	}
	return ConfirmOutboundChannelBaselineResult{Ack: ack, Confirmed: true, ConfirmedAt: at}, nil
}

type outboundReservationRow struct {
	reservedSequence uint64
	confirmedAt      sql.NullString
	updatedAt        time.Time
}

// readOutboundReservationRow loads and validates the durable reservation that
// an authenticated ACK must echo exactly.
func readOutboundReservationRow(ctx context.Context, tx *sql.Tx, ack ChannelDataBaselineAck,
	targetPeerID model.PeerID, node model.Node, at time.Time,
) (outboundReservationRow, error) {
	var reservedSequence, acknowledgedSequence uint64
	var confirmedAtText sql.NullString
	var updatedAtText string
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
		AND origin_peer_id=? AND origin_epoch=?`, ack.ChannelID.String(),
		targetPeerID.String(), node.PeerID().String(), node.OriginEpoch().String()).
		Scan(&reservedSequence, &acknowledgedSequence, &confirmedAtText, &updatedAtText)
	if errors.Is(err, sql.ErrNoRows) {
		return outboundReservationRow{}, ErrChannelBaselineConflict
	}
	if err != nil || reservedSequence > model.MaxSQLiteInteger ||
		acknowledgedSequence < reservedSequence || acknowledgedSequence > model.MaxSQLiteInteger ||
		(!confirmedAtText.Valid && acknowledgedSequence != reservedSequence) {
		return outboundReservationRow{}, fmt.Errorf("%w: invalid outbound reservation: %v",
			ErrChannelBaselineAuthority, err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedAtText)
	if err != nil || at.Before(updatedAt) {
		return outboundReservationRow{}, ErrChannelBaselineInput
	}
	return outboundReservationRow{reservedSequence: reservedSequence,
		confirmedAt: confirmedAtText, updatedAt: updatedAt}, nil
}

// replayConfirmedOutboundBaseline validates and returns an already confirmed
// reservation without touching the durable confirmation timestamps.
func replayConfirmedOutboundBaseline(tx *sql.Tx, ack ChannelDataBaselineAck,
	row outboundReservationRow, authority verifiedChannelAuthority,
) (ConfirmOutboundChannelBaselineResult, error) {
	confirmedAt, parseErr := parseCanonicalStoreTime(row.confirmedAt.String)
	if parseErr != nil || confirmedAt.After(row.updatedAt) ||
		confirmedAt.Before(authority.channel.CreatedAt()) {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return ConfirmOutboundChannelBaselineResult{}, fmt.Errorf("confirm outbound Channel baseline: commit replay: %w", commitErr)
	}
	return ConfirmOutboundChannelBaselineResult{Ack: ack, ConfirmedAt: confirmedAt}, nil
}

// persistOutboundConfirmation stamps the confirmation exactly once; a lost
// reservation row or a concurrent confirmation surfaces as a conflict.
func persistOutboundConfirmation(ctx context.Context, tx *sql.Tx, ack ChannelDataBaselineAck,
	targetPeerID model.PeerID, node model.Node, at time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE peer_pull_acks SET baseline_confirmed_at=?,updated_at=?
		WHERE channel_id=? AND target_peer_id=? AND origin_peer_id=? AND origin_epoch=?
		AND baseline_channel_seq=? AND baseline_confirmed_at IS NULL`, storeTime(at), storeTime(at),
		ack.ChannelID.String(), targetPeerID.String(), node.PeerID().String(),
		node.OriginEpoch().String(), ack.BaselineChannelSequence)
	if err != nil || exactlyOne(result) != nil {
		if err == nil {
			err = errors.New("outbound baseline confirmation lost its reservation")
		}
		return mapChannelBaselineMutationError("confirm outbound Channel baseline", err)
	}
	return nil
}
