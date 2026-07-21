package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type ChannelLeaveTarget struct {
	channel       model.Channel
	roster        model.VerifiedRoster
	request       model.SignedChannelLeaveRequest
	owner         model.Member
	attempts      uint64
	nextAttemptAt time.Time
}

func (target ChannelLeaveTarget) Channel() model.Channel                   { return target.channel }
func (target ChannelLeaveTarget) Roster() model.VerifiedRoster             { return target.roster }
func (target ChannelLeaveTarget) Request() model.SignedChannelLeaveRequest { return target.request }
func (target ChannelLeaveTarget) Owner() model.Member                      { return target.owner }
func (target ChannelLeaveTarget) Attempts() uint64                         { return target.attempts }
func (target ChannelLeaveTarget) NextAttemptAt() time.Time                 { return target.nextAttemptAt }

// ReadDueChannelLeaveTargets returns at most one open request per local
// Channel and at most MaxChannelsPerNode overall. Every row is re-bound to a
// verified roster before network code can observe it.
func (s *Store) ReadDueChannelLeaveTargets(ctx context.Context,
	at time.Time,
) ([]ChannelLeaveTarget, error) {
	if s == nil || s.db == nil || ctx == nil {
		return nil, ErrChannelLeaveInput
	}
	at, err := canonicalStoreTime(at)
	if err != nil {
		return nil, ErrChannelLeaveInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("read Channel leave targets: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("%w: Node: %v", ErrChannelLeaveAuthority, err)
	}
	ids, err := readDueChannelLeaveIDs(ctx, tx, at)
	if err != nil {
		return nil, err
	}
	targets := make([]ChannelLeaveTarget, 0, len(ids))
	for _, id := range ids {
		target, err := readDueChannelLeaveTarget(ctx, tx, node.PeerID(), id)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	if err := tx.Commit(); err != nil {
		return nil, mapChannelLeaveError(err)
	}
	return targets, nil
}

func readDueChannelLeaveIDs(ctx context.Context, tx *sql.Tx,
	at time.Time,
) ([]model.ChannelID, error) {
	rows, err := tx.QueryContext(ctx, `SELECT channel_id FROM channel_leave_requests
		WHERE status IN ('queued','sent') AND next_attempt_at<=? ORDER BY next_attempt_at,request_id
		LIMIT ?`, storeTime(at), model.MaxChannelsPerNode+1)
	if err != nil {
		return nil, fmt.Errorf("%w: requests: %v", ErrChannelLeaveAuthority, err)
	}
	ids := make([]model.ChannelID, 0, model.MaxChannelsPerNode)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: scan request: %v", ErrChannelLeaveAuthority, err)
		}
		id, parseErr := model.ParseChannelID(value)
		if parseErr != nil {
			rows.Close()
			return nil, fmt.Errorf("%w: request Channel: %v", ErrChannelLeaveAuthority, parseErr)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil || rows.Close() != nil || len(ids) > model.MaxChannelsPerNode {
		return nil, fmt.Errorf("%w: request bound exceeded: %v", ErrChannelLeaveAuthority, err)
	}
	return ids, nil
}

func readDueChannelLeaveTarget(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	id model.ChannelID,
) (ChannelLeaveTarget, error) {
	authority, err := readVerifiedChannelAuthority(ctx, tx, localPeerID, id)
	if err != nil || authority.channel.Status() != model.ChannelLeaving {
		return ChannelLeaveTarget{}, fmt.Errorf("%w: Channel %q", ErrChannelLeaveAuthority, id.String())
	}
	request, err := readOpenChannelLeaveRequest(ctx, tx, authority, localPeerID)
	if err != nil {
		return ChannelLeaveTarget{}, err
	}
	owner, ok := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
	if !ok || owner.Status() != model.MemberActive {
		return ChannelLeaveTarget{}, fmt.Errorf("%w: Channel %q owner is terminal",
			ErrChannelLeaveAuthority, id.String())
	}
	return ChannelLeaveTarget{channel: authority.channel, roster: authority.roster,
		request: request.request, owner: owner, attempts: request.attempts,
		nextAttemptAt: request.nextAttemptAt}, nil
}

type StartChannelLeaveAttemptSpec struct {
	RequestID             model.ChannelLeaveRequestID
	ExpectedAttempts      uint64
	ExpectedNextAttemptAt time.Time
	AttemptedAt           time.Time
	RetryAt               time.Time
}

// StartChannelLeaveAttempt advances the durable retry fence before network
// I/O. The existing serial member reconciler is the sole caller; the CAS still
// rejects stale in-process work and survives process loss.
func (s *Store) StartChannelLeaveAttempt(ctx context.Context,
	spec StartChannelLeaveAttemptSpec,
) error {
	if s == nil || s.db == nil || ctx == nil || spec.RequestID.IsZero() ||
		spec.ExpectedAttempts >= model.MaxSQLiteInteger {
		return ErrChannelLeaveInput
	}
	expectedNext, err := canonicalStoreTime(spec.ExpectedNextAttemptAt)
	if err != nil {
		return ErrChannelLeaveInput
	}
	attemptedAt, err := canonicalStoreTime(spec.AttemptedAt)
	if err != nil || attemptedAt.Before(expectedNext) {
		return ErrChannelLeaveInput
	}
	retryAt, err := canonicalStoreTime(spec.RetryAt)
	if err != nil || !retryAt.After(attemptedAt) {
		return ErrChannelLeaveInput
	}
	result, err := s.db.ExecContext(ctx, `UPDATE channel_leave_requests SET status='sent',
		attempts=attempts+1,next_attempt_at=?,updated_at=? WHERE request_id=?
		AND status IN ('queued','sent') AND attempts=? AND next_attempt_at=? AND updated_at<=?`,
		storeTime(retryAt), storeTime(attemptedAt), spec.RequestID.String(),
		spec.ExpectedAttempts, storeTime(expectedNext), storeTime(attemptedAt))
	if err != nil {
		return mapChannelLeaveError(err)
	}
	if changed, changedErr := result.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrChannelLeaveConflict
	}
	return nil
}
