package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

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

type channelLeaveRowValues struct {
	requestIDText, channelText, memberText      string
	status, nextText, createdText, updatedText  string
	requestDigest, requestJSON, memberSignature []byte
	receiptJSON                                 []byte
	attempts                                    uint64
}

func scanChannelLeaveRequest(row channelLeaveRow, authority verifiedChannelAuthority,
	localPeerID model.PeerID,
) (durableChannelLeaveRequest, error) {
	var values channelLeaveRowValues
	if err := row.Scan(&values.requestIDText, &values.channelText, &values.memberText,
		&values.requestDigest, &values.requestJSON, &values.memberSignature, &values.status,
		&values.attempts, &values.nextText, &values.receiptJSON, &values.createdText,
		&values.updatedText); err != nil {
		return durableChannelLeaveRequest{}, mapChannelLeaveError(err)
	}
	return validateChannelLeaveRow(values, authority, localPeerID)
}

type parsedChannelLeaveRow struct {
	request                           model.SignedChannelLeaveRequest
	requestID                         model.ChannelLeaveRequestID
	digest                            model.Digest
	nextAttempt, createdAt, updatedAt time.Time
	active                            model.Member
}

func validateChannelLeaveRow(values channelLeaveRowValues, authority verifiedChannelAuthority,
	localPeerID model.PeerID,
) (durableChannelLeaveRequest, error) {
	parsed, err := parseChannelLeaveRow(values, authority, localPeerID)
	if err != nil || !validChannelLeaveRowMetadata(values, parsed, authority, localPeerID) {
		return durableChannelLeaveRequest{}, ErrChannelLeaveAuthority
	}
	if err := validateChannelLeaveRowReceipt(values.receiptJSON, authority,
		parsed.active, parsed.request); err != nil {
		return durableChannelLeaveRequest{}, ErrChannelLeaveAuthority
	}
	return durableChannelLeaveRequest{request: parsed.request, status: values.status,
		attempts: values.attempts, nextAttemptAt: parsed.nextAttempt,
		receiptJSON: append([]byte(nil), values.receiptJSON...)}, nil
}

func parseChannelLeaveRow(values channelLeaveRowValues, authority verifiedChannelAuthority,
	localPeerID model.PeerID,
) (parsedChannelLeaveRow, error) {
	record, recordErr := model.ParseChannelLeaveRequestRecord(values.requestJSON)
	request, requestErr := model.AttachChannelLeaveRequestSignature(record, values.memberSignature)
	digest, digestErr := model.DigestFromBytes(values.requestDigest)
	requestID, idErr := model.ParseChannelLeaveRequestID(values.requestIDText)
	nextAttempt, nextErr := parseCanonicalStoreTime(values.nextText)
	createdAt, createdErr := parseCanonicalStoreTime(values.createdText)
	updatedAt, updatedErr := parseCanonicalStoreTime(values.updatedText)
	active, activeErr := channelLeaveHistoricalMember(authority.roster, record.ActiveMemberHead(), localPeerID)
	if errors.Join(recordErr, requestErr, digestErr, idErr, nextErr, createdErr, updatedErr, activeErr) != nil {
		return parsedChannelLeaveRow{}, ErrChannelLeaveAuthority
	}
	return parsedChannelLeaveRow{request: request, requestID: requestID, digest: digest,
		nextAttempt: nextAttempt, createdAt: createdAt, updatedAt: updatedAt, active: active}, nil
}

func validChannelLeaveRowMetadata(values channelLeaveRowValues, parsed parsedChannelLeaveRow,
	authority verifiedChannelAuthority, localPeerID model.PeerID,
) bool {
	record := parsed.request.Record()
	validStatus := values.status == "queued" || values.status == "sent" ||
		values.status == "accepted" || values.status == "rejected"
	return values.attempts <= model.MaxSQLiteInteger && validStatus &&
		parsed.requestID == parsed.request.RequestID() && parsed.digest == parsed.request.Digest() &&
		values.channelText == authority.channel.ID().String() && values.memberText == localPeerID.String() &&
		record.ChannelID() == authority.channel.ID() && record.MemberPeerID() == localPeerID &&
		parsed.createdAt.Equal(record.RequestedAt()) && !parsed.updatedAt.Before(parsed.createdAt) &&
		!parsed.nextAttempt.Before(parsed.createdAt) &&
		model.VerifyChannelLeaveRequest(authority.channel.Descriptor(), parsed.active, parsed.request) == nil &&
		(values.status == "accepted") == (len(values.receiptJSON) > 0)
}

func validateChannelLeaveRowReceipt(raw []byte, authority verifiedChannelAuthority,
	active model.Member, request model.SignedChannelLeaveRequest,
) error {
	if len(raw) == 0 {
		return nil
	}
	receipt, err := model.ParseSignedChannelLeaveReceipt(raw)
	if err != nil || model.VerifyChannelLeaveReceipt(authority.channel.Descriptor(), active,
		request, receipt) != nil {
		return ErrChannelLeaveAuthority
	}
	return nil
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
