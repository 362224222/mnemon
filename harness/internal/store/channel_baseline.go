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

// ChannelDataBaseline freezes one origin publication head for a directional binding.
type ChannelDataBaseline struct {
	ChannelID               model.ChannelID
	OriginPeerID            model.PeerID
	OriginEpoch             model.OriginEpoch
	BaselineChannelSequence uint64
}

// ChannelDataBaselineAck acknowledges only the baseline, not later Pull cursors.
type ChannelDataBaselineAck struct {
	ChannelID               model.ChannelID
	OriginPeerID            model.PeerID
	OriginEpoch             model.OriginEpoch
	BaselineChannelSequence uint64
}
type ReserveOutboundChannelBaselineSpec struct {
	ChannelID          model.ChannelID
	TargetPeerID       model.PeerID
	ExpectedRosterHead model.RecordHead
	At                 time.Time
}
type ReserveOutboundChannelBaselineResult struct {
	Baseline ChannelDataBaseline
	Reserved bool
}
type InstallInboundChannelBaselineSpec struct {
	AuthenticatedPeerID model.PeerID
	Baseline            ChannelDataBaseline
	At                  time.Time
}
type InstallInboundChannelBaselineResult struct {
	Baseline    ChannelDataBaseline
	Installed   bool
	InstalledAt time.Time
}

type ConfirmOutboundChannelBaselineSpec struct {
	AuthenticatedPeerID model.PeerID
	ExpectedRosterHead  model.RecordHead
	Ack                 ChannelDataBaselineAck
	At                  time.Time
}
type ConfirmOutboundChannelBaselineResult struct {
	Ack         ChannelDataBaselineAck
	Confirmed   bool
	ConfirmedAt time.Time
}

// ChannelPeerReadiness derives the two directional gates without persisting an aggregate bit.
type ChannelPeerReadiness struct {
	ChannelID                   model.ChannelID
	PeerID                      model.PeerID
	OriginEpoch                 model.OriginEpoch
	BindingState                model.BindingState
	TopicState                  model.TopicState
	RosterHead                  model.RecordHead
	InboundReady, OutboundReady bool
}

func (readiness ChannelPeerReadiness) Ready() bool {
	return readiness.BindingState == model.BindingActive &&
		readiness.TopicState == model.TopicJoined && readiness.InboundReady && readiness.OutboundReady
}

// ReserveOutboundChannelBaseline freezes the current publication head only
// while the caller's authenticated roster generation remains current; replay
// returns the original reservation under that same authority fence.
func (s *Store) ReserveOutboundChannelBaseline(ctx context.Context, spec ReserveOutboundChannelBaselineSpec) (ReserveOutboundChannelBaselineResult, error) {
	if !validReserveOutboundChannelBaseline(s, ctx, spec) {
		return ReserveOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return ReserveOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("reserve outbound Channel baseline: begin: %w", err)
	}
	defer tx.Rollback()
	node, authority, binding, err := readExactOutboundChannelBaselineAuthority(ctx, tx,
		spec.ChannelID, spec.TargetPeerID, spec.ExpectedRosterHead)
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, err
	}
	if at.Before(authority.channel.UpdatedAt()) || binding.State() == model.BindingRevoked {
		return ReserveOutboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}
	var sourceHead uint64
	err = tx.QueryRowContext(ctx, `SELECT source_head_channel_seq FROM publication_epochs
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, spec.ChannelID.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&sourceHead)
	if err != nil || sourceHead > model.MaxSQLiteInteger {
		return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("%w: local publication epoch: %v",
			ErrChannelBaselineAuthority, err)
	}
	baseline := ChannelDataBaseline{ChannelID: spec.ChannelID, OriginPeerID: node.PeerID(),
		OriginEpoch: node.OriginEpoch(), BaselineChannelSequence: sourceHead}
	var reservedSequence, acknowledgedSequence uint64
	var updatedAtText string
	var confirmedAt sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
		AND origin_peer_id=? AND origin_epoch=?`, spec.ChannelID.String(), spec.TargetPeerID.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&reservedSequence,
		&acknowledgedSequence, &confirmedAt, &updatedAtText)
	if err == nil {
		updatedAt, parseErr := parseCanonicalStoreTime(updatedAtText)
		if parseErr != nil || reservedSequence > model.MaxSQLiteInteger ||
			acknowledgedSequence < reservedSequence || acknowledgedSequence > model.MaxSQLiteInteger ||
			reservedSequence > sourceHead || acknowledgedSequence > sourceHead ||
			updatedAt.Before(authority.channel.CreatedAt()) {
			return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("%w: invalid outbound reservation",
				ErrChannelBaselineAuthority)
		}
		if at.Before(updatedAt) {
			return ReserveOutboundChannelBaselineResult{}, ErrChannelBaselineInput
		}
		if confirmedAt.Valid {
			confirmed, parseErr := parseCanonicalStoreTime(confirmedAt.String)
			if parseErr != nil || confirmed.After(updatedAt) || confirmed.Before(authority.channel.CreatedAt()) {
				return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("%w: invalid outbound confirmation",
					ErrChannelBaselineAuthority)
			}
		}
		baseline.BaselineChannelSequence = reservedSequence
		if commitErr := tx.Commit(); commitErr != nil {
			return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("reserve outbound Channel baseline: commit replay: %w", commitErr)
		}
		return ReserveOutboundChannelBaselineResult{Baseline: baseline}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ReserveOutboundChannelBaselineResult{}, fmt.Errorf("reserve outbound Channel baseline: read replay: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
		origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
		VALUES(?,?,?,?,?,?,NULL,?)`, spec.ChannelID.String(), spec.TargetPeerID.String(),
		node.PeerID().String(), node.OriginEpoch().String(), sourceHead, sourceHead, storeTime(at))
	if err != nil {
		return ReserveOutboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"reserve outbound Channel baseline", err)
	}
	if err := tx.Commit(); err != nil {
		return ReserveOutboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"reserve outbound Channel baseline", err)
	}
	return ReserveOutboundChannelBaselineResult{Baseline: baseline, Reserved: true}, nil
}

func (s *Store) CommitInboundChannelBaseline(ctx context.Context, plan InboundChannelBaselinePlan) (InstallInboundChannelBaselineResult, error) {
	if !validChannelDataBaseline(plan.spec.Baseline) {
		return InstallInboundChannelBaselineResult{}, ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, false)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}
	defer tx.Rollback()
	if resolution == ChannelAuthorityPlanCandidate {
		if err := verifyInboundChannelBaselineEvidence(ctx, tx, plan); err != nil {
			return InstallInboundChannelBaselineResult{}, err
		}
		return plan.result, nil
	}
	if resolution != ChannelAuthorityPlanUnchanged {
		return InstallInboundChannelBaselineResult{}, ErrChannelAuthorityPlanDiverged
	}
	_, authority, binding, err := readChannelBaselineAuthority(ctx, tx,
		plan.spec.Baseline.ChannelID, plan.spec.Baseline.OriginPeerID)
	if err != nil || authority.channel.RosterHead() != plan.expectedRosterHead ||
		binding.State() != plan.expectedBinding.State() ||
		binding.OriginEpoch() != plan.expectedBinding.OriginEpoch() ||
		binding.MemberHead() != plan.expectedBinding.MemberHead() {
		return InstallInboundChannelBaselineResult{}, ErrChannelAuthorityPlanDiverged
	}
	result, err := applyInboundChannelBaseline(ctx, tx, authority, binding, plan.spec)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}
	expected := plan.result
	expected.Installed = plan.ChangesAuthority()
	if result.Installed != expected.Installed || result.Baseline != expected.Baseline ||
		!result.InstalledAt.Equal(expected.InstalledAt) {
		return InstallInboundChannelBaselineResult{}, ErrChannelAuthorityPlanDiverged
	}
	after, err := inspectChannelAuthorityPlan(ctx, tx, plan.channelAuthorityPlan)
	if err != nil {
		return InstallInboundChannelBaselineResult{}, err
	}
	if after != ChannelAuthorityPlanCandidate &&
		!(after == ChannelAuthorityPlanUnchanged && !plan.ChangesAuthority()) {
		return InstallInboundChannelBaselineResult{}, ErrChannelAuthorityPlanDiverged
	}
	if err := tx.Commit(); err != nil {
		return InstallInboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"commit inbound Channel baseline", err)
	}
	return result, nil
}

func applyInboundChannelBaseline(ctx context.Context, tx *sql.Tx, authority verifiedChannelAuthority,
	binding model.PeerBinding, spec InstallInboundChannelBaselineSpec) (InstallInboundChannelBaselineResult, error) {
	baseline := spec.Baseline
	if binding.OriginEpoch() != baseline.OriginEpoch {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineEpochMismatch
	}
	if spec.At.Before(authority.channel.UpdatedAt()) || binding.State() == model.BindingRevoked {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}
	var installed, contiguous, observed uint64
	var installedAtText string
	err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,contiguous_channel_seq,
		observed_channel_seq,updated_at FROM peer_cursors WHERE channel_id=? AND origin_peer_id=?
		AND origin_epoch=?`, baseline.ChannelID.String(), baseline.OriginPeerID.String(),
		baseline.OriginEpoch.String()).Scan(&installed, &contiguous, &observed, &installedAtText)
	if err == nil {
		installedAt, parseErr := parseCanonicalStoreTime(installedAtText)
		if parseErr != nil || binding.State() != model.BindingActive || installed > model.MaxSQLiteInteger ||
			contiguous < installed || observed < contiguous || observed > model.MaxSQLiteInteger {
			return InstallInboundChannelBaselineResult{}, ErrChannelBaselineAuthority
		}
		if installed != baseline.BaselineChannelSequence {
			return InstallInboundChannelBaselineResult{}, ErrChannelBaselineConflict
		}
		if spec.At.Before(installedAt) {
			return InstallInboundChannelBaselineResult{}, ErrChannelBaselineInput
		}
		return InstallInboundChannelBaselineResult{Baseline: baseline, InstalledAt: installedAt}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return InstallInboundChannelBaselineResult{}, fmt.Errorf("install inbound Channel baseline: read replay: %w", err)
	}
	if binding.State() != model.BindingPending {
		return InstallInboundChannelBaselineResult{}, ErrChannelBaselineConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES(?,?,?,?,?,?,?)`, baseline.ChannelID.String(), baseline.OriginPeerID.String(),
		baseline.OriginEpoch.String(), baseline.BaselineChannelSequence,
		baseline.BaselineChannelSequence, baseline.BaselineChannelSequence, storeTime(spec.At)); err != nil {
		return InstallInboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"install inbound Channel baseline cursor", err)
	}
	updated, err := tx.ExecContext(ctx, `UPDATE peer_bindings SET state='active'
		WHERE channel_id=? AND peer_id=? AND origin_epoch=? AND state='pending'`,
		baseline.ChannelID.String(), baseline.OriginPeerID.String(), baseline.OriginEpoch.String())
	if err != nil || exactlyOne(updated) != nil {
		if err == nil {
			err = errors.New("binding activation lost its pending authority")
		}
		return InstallInboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"install inbound Channel baseline activation", err)
	}
	return InstallInboundChannelBaselineResult{Baseline: baseline, Installed: true, InstalledAt: spec.At}, nil
}

func verifyInboundChannelBaselineEvidence(ctx context.Context, tx *sql.Tx, plan InboundChannelBaselinePlan) error {
	baseline := plan.spec.Baseline
	var installed, contiguous, observed uint64
	var updatedAtText string
	if err := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,contiguous_channel_seq,
		observed_channel_seq,updated_at FROM peer_cursors
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, baseline.ChannelID.String(),
		baseline.OriginPeerID.String(), baseline.OriginEpoch.String()).Scan(
		&installed, &contiguous, &observed, &updatedAtText); err != nil {
		return ErrChannelAuthorityPlanDiverged
	}
	updatedAt, err := parseCanonicalStoreTime(updatedAtText)
	if err != nil || installed != baseline.BaselineChannelSequence || contiguous != installed ||
		observed != installed || !updatedAt.Equal(plan.result.InstalledAt) {
		return ErrChannelAuthorityPlanDiverged
	}
	return nil
}

// ConfirmOutboundChannelBaseline opens egress after the target echoes the
// exact reservation and the Hello roster generation is still current.
func (s *Store) ConfirmOutboundChannelBaseline(ctx context.Context, spec ConfirmOutboundChannelBaselineSpec) (ConfirmOutboundChannelBaselineResult, error) {
	ack := spec.Ack
	if !validConfirmOutboundChannelBaseline(s, ctx, spec) {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ConfirmOutboundChannelBaselineResult{}, fmt.Errorf("confirm outbound Channel baseline: begin: %w", err)
	}
	defer tx.Rollback()
	node, authority, binding, err := readExactOutboundChannelBaselineAuthority(ctx, tx,
		ack.ChannelID, spec.AuthenticatedPeerID, spec.ExpectedRosterHead)
	if err != nil {
		return ConfirmOutboundChannelBaselineResult{}, err
	}
	if ack.OriginPeerID != node.PeerID() || ack.OriginEpoch != node.OriginEpoch() {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineEpochMismatch
	}
	if at.Before(authority.channel.UpdatedAt()) || binding.State() == model.BindingRevoked {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineAuthority
	}
	var reservedSequence, acknowledgedSequence uint64
	var confirmedAtText sql.NullString
	var updatedAtText string
	err = tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
		baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
		AND origin_peer_id=? AND origin_epoch=?`, ack.ChannelID.String(),
		spec.AuthenticatedPeerID.String(), node.PeerID().String(), node.OriginEpoch().String()).
		Scan(&reservedSequence, &acknowledgedSequence, &confirmedAtText, &updatedAtText)
	if errors.Is(err, sql.ErrNoRows) {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineConflict
	}
	if err != nil || reservedSequence > model.MaxSQLiteInteger ||
		acknowledgedSequence < reservedSequence || acknowledgedSequence > model.MaxSQLiteInteger ||
		(!confirmedAtText.Valid && acknowledgedSequence != reservedSequence) {
		return ConfirmOutboundChannelBaselineResult{}, fmt.Errorf("%w: invalid outbound reservation: %v",
			ErrChannelBaselineAuthority, err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedAtText)
	if err != nil || at.Before(updatedAt) {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineInput
	}
	if reservedSequence != ack.BaselineChannelSequence {
		return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineConflict
	}
	if confirmedAtText.Valid {
		confirmedAt, parseErr := parseCanonicalStoreTime(confirmedAtText.String)
		if parseErr != nil || confirmedAt.After(updatedAt) || confirmedAt.Before(authority.channel.CreatedAt()) {
			return ConfirmOutboundChannelBaselineResult{}, ErrChannelBaselineAuthority
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return ConfirmOutboundChannelBaselineResult{}, fmt.Errorf("confirm outbound Channel baseline: commit replay: %w", commitErr)
		}
		return ConfirmOutboundChannelBaselineResult{Ack: ack, ConfirmedAt: confirmedAt}, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE peer_pull_acks SET baseline_confirmed_at=?,updated_at=?
		WHERE channel_id=? AND target_peer_id=? AND origin_peer_id=? AND origin_epoch=?
		AND baseline_channel_seq=? AND baseline_confirmed_at IS NULL`, storeTime(at), storeTime(at),
		ack.ChannelID.String(), spec.AuthenticatedPeerID.String(), node.PeerID().String(),
		node.OriginEpoch().String(), ack.BaselineChannelSequence)
	if err != nil || exactlyOne(result) != nil {
		if err == nil {
			err = errors.New("outbound baseline confirmation lost its reservation")
		}
		return ConfirmOutboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"confirm outbound Channel baseline", err)
	}
	if err := tx.Commit(); err != nil {
		return ConfirmOutboundChannelBaselineResult{}, mapChannelBaselineMutationError(
			"confirm outbound Channel baseline", err)
	}
	return ConfirmOutboundChannelBaselineResult{Ack: ack, Confirmed: true, ConfirmedAt: at}, nil
}

// ReadChannelBaselineReadiness derives readiness from signed authority and directional evidence.
func (s *Store) ReadChannelBaselineReadiness(ctx context.Context, channelID model.ChannelID) ([]ChannelPeerReadiness, error) {
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
	var localSourceHead uint64
	if err := tx.QueryRowContext(ctx, `SELECT source_head_channel_seq FROM publication_epochs
		WHERE channel_id=? AND origin_peer_id=? AND origin_epoch=?`, channelID.String(),
		node.PeerID().String(), node.OriginEpoch().String()).Scan(&localSourceHead); err != nil ||
		localSourceHead > model.MaxSQLiteInteger {
		return nil, fmt.Errorf("%w: local publication epoch: %v", ErrChannelBaselineAuthority, err)
	}
	result := make([]ChannelPeerReadiness, 0, len(authority.bindings))
	for _, binding := range authority.bindings {
		var cursorBaseline, contiguous, observed uint64
		var cursorUpdated string
		cursorErr := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,contiguous_channel_seq,
			observed_channel_seq,updated_at FROM peer_cursors WHERE channel_id=?
			AND origin_peer_id=? AND origin_epoch=?`, channelID.String(), binding.PeerID().String(),
			binding.OriginEpoch().String()).Scan(&cursorBaseline, &contiguous, &observed, &cursorUpdated)
		cursorPresent := cursorErr == nil
		if cursorErr != nil && !errors.Is(cursorErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("read Channel baseline readiness: inbound gate: %w", cursorErr)
		}
		if cursorPresent {
			cursorAt, parseErr := parseCanonicalStoreTime(cursorUpdated)
			if parseErr != nil || cursorBaseline > model.MaxSQLiteInteger || contiguous < cursorBaseline ||
				observed < contiguous || observed > model.MaxSQLiteInteger ||
				cursorAt.Before(authority.channel.CreatedAt()) || binding.State() == model.BindingPending {
				return nil, ErrChannelBaselineAuthority
			}
		}
		if binding.State() == model.BindingActive && !cursorPresent {
			return nil, ErrChannelBaselineAuthority
		}
		var outboundBaseline, acknowledged uint64
		var confirmedAt sql.NullString
		var outboundUpdated string
		outboundErr := tx.QueryRowContext(ctx, `SELECT baseline_channel_seq,acknowledged_channel_seq,
			baseline_confirmed_at,updated_at FROM peer_pull_acks WHERE channel_id=? AND target_peer_id=?
			AND origin_peer_id=? AND origin_epoch=?`, channelID.String(), binding.PeerID().String(),
			node.PeerID().String(), node.OriginEpoch().String()).Scan(&outboundBaseline, &acknowledged,
			&confirmedAt, &outboundUpdated)
		outboundPresent := outboundErr == nil
		if outboundErr != nil && !errors.Is(outboundErr, sql.ErrNoRows) {
			return nil, fmt.Errorf("read Channel baseline readiness: outbound gate: %w", outboundErr)
		}
		if outboundPresent {
			updatedAt, parseErr := parseCanonicalStoreTime(outboundUpdated)
			if parseErr != nil || outboundBaseline > model.MaxSQLiteInteger ||
				acknowledged < outboundBaseline || acknowledged > localSourceHead ||
				outboundBaseline > localSourceHead ||
				updatedAt.Before(authority.channel.CreatedAt()) {
				return nil, ErrChannelBaselineAuthority
			}
			if confirmedAt.Valid {
				confirmed, parseErr := parseCanonicalStoreTime(confirmedAt.String)
				if parseErr != nil || confirmed.Before(authority.channel.CreatedAt()) || confirmed.After(updatedAt) {
					return nil, ErrChannelBaselineAuthority
				}
			}
		}
		result = append(result, ChannelPeerReadiness{ChannelID: channelID, PeerID: binding.PeerID(),
			OriginEpoch: binding.OriginEpoch(), BindingState: binding.State(),
			TopicState: authority.channel.TopicState(), RosterHead: authority.roster.Head(),
			InboundReady:  binding.State() == model.BindingActive && cursorPresent,
			OutboundReady: binding.State() != model.BindingRevoked && outboundPresent && confirmedAt.Valid})
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("read Channel baseline readiness: commit read: %w", err)
	}
	return result, nil
}

func mapChannelBaselineMutationError(operation string, err error) error {
	return fmt.Errorf("%s: %w: %v", operation, ErrChannelBaselineConflict, err)
}
