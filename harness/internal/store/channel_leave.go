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
	ErrChannelLeaveInput     = errors.New("invalid Channel leave input")
	ErrChannelLeaveAuthority = errors.New("Channel leave authority is unavailable")
	ErrChannelLeaveConflict  = errors.New("Channel leave conflicts with durable authority")
)

type BeginChannelLeaveSpec struct {
	ChannelID model.ChannelID
	Request   model.SignedChannelLeaveRequest
}

type BeginChannelLeaveResult struct {
	Channel model.Channel
	Roster  model.VerifiedRoster
	Request model.SignedChannelLeaveRequest
	Replay  bool
}

// BeginChannelLeave is the local voluntary-exit boundary for a non-owner. The
// topic and all new egress are closed in the same transaction that persists
// the member-signed retry request. A leaving replay returns the original bytes
// without advancing timestamps or attempt state.
func (s *Store) BeginChannelLeave(ctx context.Context,
	spec BeginChannelLeaveSpec,
) (BeginChannelLeaveResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() {
		return BeginChannelLeaveResult{}, ErrChannelLeaveInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BeginChannelLeaveResult{}, fmt.Errorf("begin Channel leave: %w", err)
	}
	defer tx.Rollback()
	node, authority, err := readChannelLeaveAuthority(ctx, tx, spec.ChannelID)
	if err != nil {
		return BeginChannelLeaveResult{}, err
	}
	if authority.channel.OwnerPeerID() == node.PeerID() {
		return BeginChannelLeaveResult{}, ErrChannelLeaveConflict
	}
	switch authority.channel.Status() {
	case model.ChannelLeaving:
		return replayChannelLeave(ctx, tx, node.PeerID(), authority)
	case model.ChannelActive:
		return s.beginFreshChannelLeave(ctx, tx, node.PeerID(), authority, spec.Request)
	default:
		return BeginChannelLeaveResult{}, ErrChannelLeaveConflict
	}
}

func replayChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	authority verifiedChannelAuthority,
) (BeginChannelLeaveResult, error) {
	request, err := readOpenChannelLeaveRequest(ctx, tx, authority, localPeerID)
	if err != nil {
		return BeginChannelLeaveResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return BeginChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return BeginChannelLeaveResult{Channel: authority.channel, Roster: authority.roster,
		Request: request.request, Replay: true}, nil
}

func (s *Store) beginFreshChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	authority verifiedChannelAuthority, request model.SignedChannelLeaveRequest,
) (BeginChannelLeaveResult, error) {
	localMember, ok := authority.roster.CurrentMember(localPeerID)
	if !ok || localMember.Status() != model.MemberActive ||
		request.Record().KnownRosterHead() != authority.roster.Head() ||
		request.Record().MemberPeerID() != localPeerID ||
		request.Record().ChannelID() != authority.channel.ID() ||
		model.VerifyChannelLeaveRequest(authority.channel.Descriptor(), localMember, request) != nil {
		return BeginChannelLeaveResult{}, ErrChannelLeaveInput
	}
	at := request.Record().RequestedAt()
	if at.Before(authority.channel.UpdatedAt()) {
		return BeginChannelLeaveResult{}, ErrChannelLeaveConflict
	}
	if err := applyFreshChannelLeave(ctx, tx, localPeerID, authority, request, at); err != nil {
		return BeginChannelLeaveResult{}, err
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, localPeerID, authority.channel.ID())
	if err != nil || committed.channel.Status() != model.ChannelLeaving ||
		committed.channel.TopicState() != model.TopicLeft {
		return BeginChannelLeaveResult{}, ErrChannelLeaveConflict
	}
	if err := tx.Commit(); err != nil {
		return BeginChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return BeginChannelLeaveResult{Channel: committed.channel, Roster: committed.roster,
		Request: request}, nil
}

func applyFreshChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	authority verifiedChannelAuthority, request model.SignedChannelLeaveRequest, at time.Time,
) error {
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET status='leaving',topic_state='left',
		updated_at=? WHERE channel_id=? AND status='active' AND roster_head_revision=?
		AND roster_head_hash=?`, storeTime(at), authority.channel.ID().String(),
		authority.channel.RosterHead().Revision(), authority.channel.RosterHead().Digest().Bytes())
	if err != nil {
		return mapChannelLeaveError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrChannelLeaveConflict
	}
	if err := blockChannelEgress(ctx, tx, authority.channel.ID(), nil,
		"local Channel leave is pending owner acknowledgement", at); err != nil {
		return mapChannelLeaveError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_leave_requests(request_id,channel_id,
		member_peer_id,request_digest,request_json,member_signature,status,attempts,
		next_attempt_at,receipt_json,created_at,updated_at) VALUES(?,?,?,?,?,?,'queued',0,?,NULL,?,?)`,
		request.RequestID().String(), authority.channel.ID().String(), localPeerID.String(),
		request.Digest().Bytes(), request.Record().CanonicalJSON().Bytes(), request.MemberSignature(),
		storeTime(at), storeTime(at), storeTime(at))
	if err != nil {
		return mapChannelLeaveError(err)
	}
	return nil
}

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
	targets := make([]ChannelLeaveTarget, 0, len(ids))
	for _, id := range ids {
		authority, readErr := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), id)
		if readErr != nil || authority.channel.Status() != model.ChannelLeaving {
			return nil, fmt.Errorf("%w: Channel %q", ErrChannelLeaveAuthority, id.String())
		}
		request, readErr := readOpenChannelLeaveRequest(ctx, tx, authority, node.PeerID())
		if readErr != nil {
			return nil, readErr
		}
		owner, ok := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
		if !ok || owner.Status() != model.MemberActive {
			return nil, fmt.Errorf("%w: Channel %q owner is terminal", ErrChannelLeaveAuthority, id.String())
		}
		targets = append(targets, ChannelLeaveTarget{channel: authority.channel,
			roster: authority.roster, request: request.request, owner: owner,
			attempts: request.attempts, nextAttemptAt: request.nextAttemptAt})
	}
	if err := tx.Commit(); err != nil {
		return nil, mapChannelLeaveError(err)
	}
	return targets, nil
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

type SettleChannelLeaveSpec struct {
	RequestID model.ChannelLeaveRequestID
	Receipt   model.SignedChannelLeaveReceipt
	At        time.Time
}

type SettleChannelLeaveResult struct {
	Channel model.Channel
	Roster  model.VerifiedRoster
	Replay  bool
}

// SettleChannelLeave verifies the owner-signed receipt against the historical
// active member named by the request, merges its complete continuous suffix,
// and marks the request accepted in one transaction.
func (s *Store) SettleChannelLeave(ctx context.Context,
	spec SettleChannelLeaveSpec,
) (SettleChannelLeaveResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.RequestID.IsZero() || spec.Receipt.IsZero() {
		return SettleChannelLeaveResult{}, ErrChannelLeaveInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.Before(spec.Receipt.Record().AcceptedAt()) {
		return SettleChannelLeaveResult{}, ErrChannelLeaveInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SettleChannelLeaveResult{}, fmt.Errorf("settle Channel leave: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil {
		return SettleChannelLeaveResult{}, fmt.Errorf("%w: Node: %v", ErrChannelLeaveAuthority, err)
	}
	request, authority, err := readChannelLeaveRequestByID(ctx, tx, node.PeerID(), spec.RequestID)
	if err != nil {
		return SettleChannelLeaveResult{}, err
	}
	if request.status == "accepted" {
		return replaySettledChannelLeave(tx, request, authority, spec.Receipt)
	}
	return s.settleOpenChannelLeave(ctx, tx, node.PeerID(), request, authority, spec, at)
}

func replaySettledChannelLeave(tx *sql.Tx, request durableChannelLeaveRequest,
	authority verifiedChannelAuthority, receipt model.SignedChannelLeaveReceipt,
) (SettleChannelLeaveResult, error) {
	if !bytes.Equal(request.receiptJSON, receipt.WireJSON().Bytes()) {
		return SettleChannelLeaveResult{}, ErrChannelLeaveConflict
	}
	if err := tx.Commit(); err != nil {
		return SettleChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return SettleChannelLeaveResult{Channel: authority.channel, Roster: authority.roster,
		Replay: true}, nil
}

func (s *Store) settleOpenChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	request durableChannelLeaveRequest, authority verifiedChannelAuthority,
	spec SettleChannelLeaveSpec, at time.Time,
) (SettleChannelLeaveResult, error) {
	if request.status != "queued" && request.status != "sent" ||
		authority.channel.Status() != model.ChannelLeaving || at.Before(authority.channel.UpdatedAt()) {
		return SettleChannelLeaveResult{}, ErrChannelLeaveConflict
	}
	candidate, err := prepareChannelLeaveCandidate(authority, request.request, spec.Receipt,
		localPeerID, at)
	if err != nil {
		return SettleChannelLeaveResult{}, err
	}
	result := MergeChannelRosterResult{Status: ChannelRosterDuplicate, Channel: authority.channel,
		Roster: authority.roster, ExpectedNextRevision: authority.roster.Head().Revision() + 1}
	if len(candidate.Members()) > len(authority.roster.Members()) {
		result, err = s.applyChannelRosterCandidateTx(ctx, tx, localPeerID, authority, candidate, at)
		if err != nil {
			return SettleChannelLeaveResult{}, err
		}
	}
	if !settledChannelLeaveAuthority(result, localPeerID) {
		return SettleChannelLeaveResult{}, ErrChannelLeaveConflict
	}
	if err := acceptChannelLeaveRequest(ctx, tx, spec, at); err != nil {
		return SettleChannelLeaveResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SettleChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return SettleChannelLeaveResult{Channel: result.Channel, Roster: result.Roster}, nil
}

func prepareChannelLeaveCandidate(authority verifiedChannelAuthority,
	request model.SignedChannelLeaveRequest, receipt model.SignedChannelLeaveReceipt,
	localPeerID model.PeerID, at time.Time,
) (model.VerifiedRoster, error) {
	active, err := channelLeaveHistoricalMember(authority.roster,
		request.Record().ActiveMemberHead(), localPeerID)
	if err != nil || model.VerifyChannelLeaveReceipt(authority.channel.Descriptor(), active,
		request, receipt) != nil {
		return model.VerifiedRoster{}, ErrChannelLeaveInput
	}
	records := receipt.Record().RosterRecords()
	if err := validateChannelRosterPage(authority.channel.Descriptor(), records); err != nil {
		return model.VerifiedRoster{}, ErrChannelLeaveInput
	}
	candidate, prefixLength, err := prepareChannelRosterCandidate(authority, MergeChannelRosterSpec{
		ChannelID: authority.channel.ID(), AuthenticatedTransportPeerID: authority.channel.OwnerPeerID(),
		Records: records, At: at}, records, records[0].Head().Revision())
	if err != nil {
		return model.VerifiedRoster{}, ErrChannelLeaveInput
	}
	if _, _, conflict := channelRosterConflict(authority.roster.Members(), records, prefixLength); conflict {
		return model.VerifiedRoster{}, ErrChannelLeaveConflict
	}
	return candidate, nil
}

func settledChannelLeaveAuthority(result MergeChannelRosterResult, localPeerID model.PeerID) bool {
	local, ok := result.Roster.CurrentMember(localPeerID)
	return ok && local.Status().Terminal() &&
		(result.Channel.Status() == model.ChannelLeft || result.Channel.Status() == model.ChannelClosed)
}

func acceptChannelLeaveRequest(ctx context.Context, tx *sql.Tx, spec SettleChannelLeaveSpec,
	at time.Time,
) error {
	updated, err := tx.ExecContext(ctx, `UPDATE channel_leave_requests SET status='accepted',
		receipt_json=?,updated_at=? WHERE request_id=? AND status IN ('queued','sent')`,
		spec.Receipt.WireJSON().Bytes(), storeTime(at), spec.RequestID.String())
	if err != nil {
		return mapChannelLeaveError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrChannelLeaveConflict
	}
	return nil
}

type durableChannelLeaveRequest struct {
	request       model.SignedChannelLeaveRequest
	status        string
	attempts      uint64
	nextAttemptAt time.Time
	receiptJSON   []byte
}

func readChannelLeaveAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (model.Node, verifiedChannelAuthority, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, fmt.Errorf("%w: Node: %v",
			ErrChannelLeaveAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return model.Node{}, verifiedChannelAuthority{}, fmt.Errorf("%w: Channel: %v",
			ErrChannelLeaveAuthority, err)
	}
	return node, authority, nil
}

func readOpenChannelLeaveRequest(ctx context.Context, tx *sql.Tx, authority verifiedChannelAuthority,
	localPeerID model.PeerID,
) (durableChannelLeaveRequest, error) {
	row := tx.QueryRowContext(ctx, `SELECT request_id,channel_id,member_peer_id,request_digest,
		request_json,member_signature,status,attempts,next_attempt_at,receipt_json,created_at,updated_at
		FROM channel_leave_requests WHERE channel_id=? AND member_peer_id=?
		AND status IN ('queued','sent')`, authority.channel.ID().String(), localPeerID.String())
	return scanChannelLeaveRequest(row, authority, localPeerID)
}

func readChannelLeaveRequestByID(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	requestID model.ChannelLeaveRequestID,
) (durableChannelLeaveRequest, verifiedChannelAuthority, error) {
	var channelText string
	if err := tx.QueryRowContext(ctx, `SELECT channel_id FROM channel_leave_requests WHERE request_id=?`,
		requestID.String()).Scan(&channelText); err != nil {
		return durableChannelLeaveRequest{}, verifiedChannelAuthority{}, mapChannelLeaveError(err)
	}
	channelID, err := model.ParseChannelID(channelText)
	if err != nil {
		return durableChannelLeaveRequest{}, verifiedChannelAuthority{}, ErrChannelLeaveAuthority
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, localPeerID, channelID)
	if err != nil {
		return durableChannelLeaveRequest{}, verifiedChannelAuthority{}, fmt.Errorf("%w: %v",
			ErrChannelLeaveAuthority, err)
	}
	row := tx.QueryRowContext(ctx, `SELECT request_id,channel_id,member_peer_id,request_digest,
		request_json,member_signature,status,attempts,next_attempt_at,receipt_json,created_at,updated_at
		FROM channel_leave_requests WHERE request_id=?`, requestID.String())
	request, err := scanChannelLeaveRequest(row, authority, localPeerID)
	return request, authority, err
}

type channelLeaveRow interface{ Scan(...any) error }

func scanChannelLeaveRequest(row channelLeaveRow, authority verifiedChannelAuthority,
	localPeerID model.PeerID,
) (durableChannelLeaveRequest, error) {
	var requestIDText, channelText, memberText, status, nextText, createdText, updatedText string
	var requestDigest, requestJSON, memberSignature []byte
	var attempts uint64
	var receiptJSON []byte
	if err := row.Scan(&requestIDText, &channelText, &memberText, &requestDigest, &requestJSON,
		&memberSignature, &status, &attempts, &nextText, &receiptJSON, &createdText, &updatedText); err != nil {
		return durableChannelLeaveRequest{}, mapChannelLeaveError(err)
	}
	record, err := model.ParseChannelLeaveRequestRecord(requestJSON)
	if err != nil {
		return durableChannelLeaveRequest{}, ErrChannelLeaveAuthority
	}
	request, err := model.AttachChannelLeaveRequestSignature(record, memberSignature)
	if err != nil {
		return durableChannelLeaveRequest{}, ErrChannelLeaveAuthority
	}
	digest, digestErr := model.DigestFromBytes(requestDigest)
	requestID, idErr := model.ParseChannelLeaveRequestID(requestIDText)
	nextAttempt, nextErr := parseCanonicalStoreTime(nextText)
	createdAt, createdErr := parseCanonicalStoreTime(createdText)
	updatedAt, updatedErr := parseCanonicalStoreTime(updatedText)
	active, activeErr := channelLeaveHistoricalMember(authority.roster, record.ActiveMemberHead(), localPeerID)
	validStatus := status == "queued" || status == "sent" || status == "accepted" || status == "rejected"
	if digestErr != nil || idErr != nil || nextErr != nil || createdErr != nil || updatedErr != nil ||
		activeErr != nil || attempts > model.MaxSQLiteInteger || !validStatus ||
		requestID != request.RequestID() || digest != request.Digest() ||
		channelText != authority.channel.ID().String() || memberText != localPeerID.String() ||
		record.ChannelID() != authority.channel.ID() || record.MemberPeerID() != localPeerID ||
		!createdAt.Equal(record.RequestedAt()) || updatedAt.Before(createdAt) || nextAttempt.Before(createdAt) ||
		model.VerifyChannelLeaveRequest(authority.channel.Descriptor(), active, request) != nil ||
		(status == "accepted") != (len(receiptJSON) > 0) {
		return durableChannelLeaveRequest{}, ErrChannelLeaveAuthority
	}
	if len(receiptJSON) > 0 {
		receipt, parseErr := model.ParseSignedChannelLeaveReceipt(receiptJSON)
		if parseErr != nil || model.VerifyChannelLeaveReceipt(authority.channel.Descriptor(), active,
			request, receipt) != nil {
			return durableChannelLeaveRequest{}, ErrChannelLeaveAuthority
		}
	}
	return durableChannelLeaveRequest{request: request, status: status, attempts: attempts,
		nextAttemptAt: nextAttempt, receiptJSON: append([]byte(nil), receiptJSON...)}, nil
}

func channelLeaveHistoricalMember(roster model.VerifiedRoster, head model.RecordHead,
	peerID model.PeerID,
) (model.Member, error) {
	members := roster.Members()
	if head.IsZero() || head.Revision() > uint64(len(members)) {
		return model.Member{}, ErrChannelLeaveAuthority
	}
	member := members[head.Revision()-1]
	if member.Head() != head || member.PeerID() != peerID || member.Status() != model.MemberActive {
		return model.Member{}, ErrChannelLeaveAuthority
	}
	return member, nil
}

func mapChannelLeaveError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrChannelAuthorityInvariant) ||
		errors.Is(err, ErrChannelRosterConflict) {
		return fmt.Errorf("%w: %v", ErrChannelLeaveConflict, err)
	}
	return fmt.Errorf("Channel leave: %w", err)
}
