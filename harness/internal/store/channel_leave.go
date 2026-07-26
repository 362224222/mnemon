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
	Operation ChannelLeaveOperation
	At        time.Time
}

type BeginChannelLeaveResult struct {
	Channel model.Channel
	Roster  model.VerifiedRoster
	Request model.SignedChannelLeaveRequest
	Replay  bool
}

// BeginChannelLeave is the local voluntary-exit boundary for a non-owner. The
// topic and all new egress are closed in the same transaction that persists
// the member-signed retry request. An open replay returns the original bytes;
// explicitly repeating a failed leave starts a new fenced retry generation.
func (s *Store) BeginChannelLeave(ctx context.Context,
	spec BeginChannelLeaveSpec,
) (BeginChannelLeaveResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() {
		return BeginChannelLeaveResult{}, ErrChannelLeaveInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || !spec.Operation.valid() {
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
	operation, found, err := readChannelLeaveOperation(ctx, tx, spec.Operation)
	if err != nil {
		return BeginChannelLeaveResult{}, err
	}
	if found {
		return replayChannelLeaveOperation(ctx, tx, node.PeerID(), authority, operation)
	}
	switch authority.channel.Status() {
	case model.ChannelLeaving:
		return resumeChannelLeave(ctx, tx, node.PeerID(), authority, spec.Operation, at)
	case model.ChannelActive:
		return s.beginFreshChannelLeave(ctx, tx, node.PeerID(), authority, spec.Request,
			spec.Operation, at)
	default:
		return BeginChannelLeaveResult{}, ErrChannelLeaveConflict
	}
}

func replayChannelLeaveOperation(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	authority verifiedChannelAuthority, operation ChannelLeaveOperationAuthority,
) (BeginChannelLeaveResult, error) {
	if operation.channelID != authority.channel.ID() || !operation.hasRequest() {
		return BeginChannelLeaveResult{}, ErrChannelLeaveOperationMismatch
	}
	request, _, err := readChannelLeaveRequestByID(ctx, tx, localPeerID, operation.requestID)
	if err != nil || request.request.Record().ChannelID() != operation.channelID ||
		request.retryGeneration < operation.retryGeneration {
		return BeginChannelLeaveResult{}, ErrChannelLeaveAuthority
	}
	if err := tx.Commit(); err != nil {
		return BeginChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return BeginChannelLeaveResult{Channel: authority.channel, Roster: authority.roster,
		Request: request.request, Replay: true}, nil
}

func resumeChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	authority verifiedChannelAuthority, operation ChannelLeaveOperation, at time.Time,
) (BeginChannelLeaveResult, error) {
	request, err := readRecoverableChannelLeaveRequest(ctx, tx, authority, localPeerID)
	if err != nil {
		return BeginChannelLeaveResult{}, err
	}
	if request.status == "failed" {
		if err := recoverFailedChannelLeave(ctx, tx, request, at); err != nil {
			return BeginChannelLeaveResult{}, err
		}
		request, err = readOpenChannelLeaveRequest(ctx, tx, authority, localPeerID)
		if err != nil {
			return BeginChannelLeaveResult{}, err
		}
	}
	if err := insertChannelLeaveOperation(ctx, tx, operation, authority.channel.ID(),
		request.request.RequestID(), request.retryGeneration, at); err != nil {
		return BeginChannelLeaveResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return BeginChannelLeaveResult{}, mapChannelLeaveError(err)
	}
	return BeginChannelLeaveResult{Channel: authority.channel, Roster: authority.roster,
		Request: request.request, Replay: true}, nil
}

func recoverFailedChannelLeave(ctx context.Context, tx *sql.Tx,
	request durableChannelLeaveRequest, at time.Time,
) error {
	if request.retryGeneration >= model.MaxSQLiteInteger {
		return ErrChannelLeaveConflict
	}
	at, err := canonicalStoreTime(at)
	if err != nil {
		return ErrChannelLeaveInput
	}
	result, err := tx.ExecContext(ctx, `UPDATE channel_leave_requests
		SET status='queued',attempts=0,retry_generation=retry_generation+1,
		next_attempt_at=?,failure_code=NULL,updated_at=? WHERE request_id=?
		AND status='failed' AND retry_generation=? AND attempts=? AND next_attempt_at=?
		AND failure_code=? AND updated_at<=?`, storeTime(at), storeTime(at),
		request.request.RequestID().String(), request.retryGeneration, request.attempts,
		storeTime(request.nextAttemptAt), string(request.failureCode), storeTime(at))
	if err != nil {
		return mapChannelLeaveError(err)
	}
	if changed, changedErr := result.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrChannelLeaveConflict
	}
	return nil
}

func (s *Store) beginFreshChannelLeave(ctx context.Context, tx *sql.Tx, localPeerID model.PeerID,
	authority verifiedChannelAuthority, request model.SignedChannelLeaveRequest,
	operation ChannelLeaveOperation, operationAt time.Time,
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
	if err := insertChannelLeaveOperation(ctx, tx, operation, authority.channel.ID(),
		request.RequestID(), 0, operationAt); err != nil {
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
	return result.Channel.Status() == model.ChannelClosed ||
		ok && local.Status().Terminal() && result.Channel.Status() == model.ChannelLeft
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
