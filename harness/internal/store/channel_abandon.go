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
	ErrChannelAbandonInput    = errors.New("invalid Channel abandon input")
	ErrChannelAbandonMissing  = errors.New("Channel abandon target is unavailable")
	ErrChannelAbandonTerminal = errors.New("Channel is already canonically terminal")
	ErrChannelAbandonStale    = errors.New("Channel abandon fence is stale")
)

// ChannelForensicCounts is a bounded, Channel-scoped inventory of the durable
// evidence retained by an abandon transition. Counts are read in the same
// transaction that installs the terminal gate, so a replay returns the exact
// current forensic inventory without inventing a second state authority.
type ChannelForensicCounts struct {
	Bindings      uint64
	Conflicts     uint64
	Cursors       uint64
	Deliveries    uint64
	Events        uint64
	Inboxes       uint64
	MemberRecords uint64
	Publications  uint64
	PullACKs      uint64
	Works         uint64
}

type AbandonChannelSpec struct {
	ChannelAlias   string
	ConfirmedAlias string
	Force          bool
	At             time.Time
}

type AbandonChannelResult struct {
	Alias          string
	ChannelID      model.ChannelID
	TransitionedAt time.Time
	Evidence       ChannelForensicCounts
	Changed        bool
	Replayed       bool
}

// AbandonChannel installs the local forensic terminal gate and retires every
// unfinished source publication and semantic delivery for exactly one Channel.
// Signed roster, binding, cursor, Event, Work, Inbox and Artifact-linked rows
// remain untouched. In particular this never fabricates an owner record or a
// canonical Work outcome.
func (s *Store) AbandonChannel(ctx context.Context,
	spec AbandonChannelSpec,
) (AbandonChannelResult, error) {
	at, err := validateAbandonChannelCall(s, ctx, spec)
	if err != nil {
		return AbandonChannelResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AbandonChannelResult{}, fmt.Errorf("abandon Channel: begin: %w", err)
	}
	defer tx.Rollback()
	result, status, topic, err := readChannelAbandonTarget(ctx, tx, spec.ChannelAlias)
	if err != nil {
		return AbandonChannelResult{}, err
	}
	result, err = settleChannelAbandon(ctx, tx, result, status, topic, at)
	if err != nil {
		return AbandonChannelResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AbandonChannelResult{}, fmt.Errorf("abandon Channel: commit: %w", err)
	}
	return result, nil
}

func validateAbandonChannelCall(s *Store, ctx context.Context,
	spec AbandonChannelSpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || !spec.Force ||
		spec.ChannelAlias != spec.ConfirmedAlias ||
		validateDurableAgentAlias(spec.ChannelAlias) != nil {
		return time.Time{}, ErrChannelAbandonInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: transition time: %v", ErrChannelAbandonInput, err)
	}
	return at, nil
}

func settleChannelAbandon(ctx context.Context, tx *sql.Tx, result AbandonChannelResult,
	status model.ChannelStatus, topic model.TopicState, at time.Time,
) (AbandonChannelResult, error) {
	var err error
	if status == model.ChannelAbandoned {
		result.Evidence, err = readChannelForensicCounts(ctx, tx, result.ChannelID)
		if err != nil {
			return AbandonChannelResult{}, err
		}
		result.Replayed = true
		return result, nil
	}
	if status == model.ChannelLeft || status == model.ChannelClosed {
		return AbandonChannelResult{}, ErrChannelAbandonTerminal
	}
	if status != model.ChannelActive && status != model.ChannelLeaving && status != model.ChannelConflicted {
		return AbandonChannelResult{}, ErrChannelAbandonStale
	}

	if err := applyChannelAbandon(ctx, tx, result, status, topic, at); err != nil {
		return AbandonChannelResult{}, err
	}
	result.TransitionedAt = at
	result.Evidence, err = readChannelForensicCounts(ctx, tx, result.ChannelID)
	if err != nil {
		return AbandonChannelResult{}, err
	}
	result.Changed = true
	return result, nil
}

func readChannelAbandonTarget(ctx context.Context, tx *sql.Tx,
	alias string,
) (AbandonChannelResult, model.ChannelStatus, model.TopicState, error) {
	var channelText, statusText, topicText, updatedText string
	err := tx.QueryRowContext(ctx, `SELECT channel_id,status,topic_state,updated_at
		FROM channels WHERE local_alias=?`, alias).Scan(&channelText, &statusText, &topicText, &updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return AbandonChannelResult{}, "", "", ErrChannelAbandonMissing
	}
	if err != nil {
		return AbandonChannelResult{}, "", "", fmt.Errorf("abandon Channel: read target: %w", err)
	}
	channelID, idErr := model.ParseChannelID(channelText)
	updatedAt, timeErr := parseCanonicalStoreTime(updatedText)
	status := model.ChannelStatus(statusText)
	topic := model.TopicState(topicText)
	if idErr != nil || timeErr != nil || !status.Valid() || !topic.Valid() {
		return AbandonChannelResult{}, "", "", fmt.Errorf("%w: malformed durable target", ErrChannelAbandonStale)
	}
	return AbandonChannelResult{Alias: alias, ChannelID: channelID,
		TransitionedAt: updatedAt}, status, topic, nil
}

func applyChannelAbandon(ctx context.Context, tx *sql.Tx, target AbandonChannelResult,
	status model.ChannelStatus, topic model.TopicState, at time.Time,
) error {
	mutation, err := tx.ExecContext(ctx, `UPDATE channels
		SET status='abandoned',topic_state='left',updated_at=?
		WHERE channel_id=? AND local_alias=? AND status=? AND topic_state=? AND updated_at=?`,
		storeTime(at), target.ChannelID.String(), target.Alias, string(status), string(topic),
		storeTime(target.TransitionedAt))
	if err != nil {
		return fmt.Errorf("abandon Channel: install terminal gate: %w", err)
	}
	if exactlyOne(mutation) != nil {
		return ErrChannelAbandonStale
	}
	if _, err := tx.ExecContext(ctx, `UPDATE gossip_publications
		SET status='abandoned',lease_owner=NULL,lease_until=NULL,published_at=NULL,
			last_error='Channel was forensically abandoned',updated_at=?
		WHERE channel_id=? AND status IN ('queued','leased')`, storeTime(at),
		target.ChannelID.String()); err != nil {
		return fmt.Errorf("abandon Channel: retire publications: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE peer_deliveries
		SET status='abandoned',scanned_at=NULL,
			last_error='Channel was forensically abandoned',updated_at=?
		WHERE channel_id=? AND status IN ('pending','blocked')`, storeTime(at),
		target.ChannelID.String()); err != nil {
		return fmt.Errorf("abandon Channel: retire deliveries: %w", err)
	}
	return nil
}

func readChannelForensicCounts(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (ChannelForensicCounts, error) {
	var counts ChannelForensicCounts
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM channel_members WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_bindings WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_cursors WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_pull_acks WHERE channel_id=?),
		(SELECT COUNT(*) FROM events WHERE channel_id=?),
		(SELECT COUNT(*) FROM works WHERE channel_id=?),
		(SELECT COUNT(*) FROM gossip_publications WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_deliveries WHERE channel_id=?),
		(SELECT COUNT(*) FROM peer_inbox WHERE channel_id=?),
		(SELECT COUNT(*) FROM channel_conflicts WHERE channel_id=?)`,
		channelID.String(), channelID.String(), channelID.String(), channelID.String(),
		channelID.String(), channelID.String(), channelID.String(), channelID.String(),
		channelID.String(), channelID.String()).Scan(&counts.MemberRecords, &counts.Bindings,
		&counts.Cursors, &counts.PullACKs, &counts.Events, &counts.Works, &counts.Publications,
		&counts.Deliveries, &counts.Inboxes, &counts.Conflicts)
	if err != nil {
		return ChannelForensicCounts{}, fmt.Errorf("abandon Channel: count forensic evidence: %w", err)
	}
	return counts, nil
}
