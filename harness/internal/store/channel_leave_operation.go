package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrChannelLeaveOperationMismatch = errors.New(
	"Channel leave operation key conflicts with request",
)

// ChannelLeaveOperation is the non-secret caller identity for one leave or
// explicit leave-recovery invocation.
type ChannelLeaveOperation struct {
	OperationKeyHash model.Digest
	RequestDigest    model.Digest
}

func (operation ChannelLeaveOperation) valid() bool {
	return !operation.OperationKeyHash.IsZero() && !operation.RequestDigest.IsZero()
}

type ChannelLeaveOperationAuthority struct {
	channelID       model.ChannelID
	requestID       model.ChannelLeaveRequestID
	retryGeneration uint64
}

func (authority ChannelLeaveOperationAuthority) ChannelID() model.ChannelID {
	return authority.channelID
}

func (authority ChannelLeaveOperationAuthority) hasRequest() bool {
	return !authority.requestID.IsZero()
}

// ReadChannelLeaveOperation classifies the stable caller key before selector
// resolution. This lets an exact retry recover its Channel even if the default
// selector or current membership changed after the response was lost.
func (s *Store) ReadChannelLeaveOperation(ctx context.Context,
	operation ChannelLeaveOperation,
) (ChannelLeaveOperationAuthority, bool, error) {
	if s == nil || s.db == nil || ctx == nil || !operation.valid() {
		return ChannelLeaveOperationAuthority{}, false, ErrChannelLeaveInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelLeaveOperationAuthority{}, false,
			fmt.Errorf("read Channel leave operation: begin: %w", err)
	}
	defer tx.Rollback()
	authority, found, err := readChannelLeaveOperation(ctx, tx, operation)
	if err != nil {
		return ChannelLeaveOperationAuthority{}, false, err
	}
	if found {
		if err := validateChannelLeaveOperation(ctx, tx, authority); err != nil {
			return ChannelLeaveOperationAuthority{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ChannelLeaveOperationAuthority{}, false, mapChannelLeaveError(err)
	}
	return authority, found, nil
}

func readChannelLeaveOperation(ctx context.Context, tx *sql.Tx,
	operation ChannelLeaveOperation,
) (ChannelLeaveOperationAuthority, bool, error) {
	var conflict int
	conflictErr := tx.QueryRowContext(ctx, `SELECT 1 FROM channel_mutation_operations
		WHERE operation_key_hash=?`, operation.OperationKeyHash.Bytes()).Scan(&conflict)
	if conflictErr == nil {
		return ChannelLeaveOperationAuthority{}, false, ErrChannelLeaveOperationMismatch
	}
	if !errors.Is(conflictErr, sql.ErrNoRows) {
		return ChannelLeaveOperationAuthority{}, false,
			fmt.Errorf("%w: inspect operation key scope: %v",
				ErrChannelLeaveAuthority, conflictErr)
	}
	var requestDigestBytes []byte
	var channelText, committedText string
	var requestText sql.NullString
	var retryGeneration sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT request_digest,channel_id,request_id,
		retry_generation,committed_at FROM channel_leave_operations
		WHERE operation_key_hash=?`, operation.OperationKeyHash.Bytes()).Scan(
		&requestDigestBytes, &channelText, &requestText, &retryGeneration, &committedText)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelLeaveOperationAuthority{}, false, nil
	}
	if err != nil {
		return ChannelLeaveOperationAuthority{}, false,
			fmt.Errorf("%w: read operation: %v", ErrChannelLeaveAuthority, err)
	}
	requestDigest, digestErr := model.DigestFromBytes(requestDigestBytes)
	if digestErr != nil {
		return ChannelLeaveOperationAuthority{}, false, ErrChannelLeaveAuthority
	}
	if requestDigest != operation.RequestDigest {
		return ChannelLeaveOperationAuthority{}, false, ErrChannelLeaveOperationMismatch
	}
	channelID, channelErr := model.ParseChannelID(channelText)
	_, timeErr := parseCanonicalStoreTime(committedText)
	if channelErr != nil || timeErr != nil || requestText.Valid != retryGeneration.Valid ||
		retryGeneration.Valid && (retryGeneration.Int64 < 0 ||
			uint64(retryGeneration.Int64) > model.MaxSQLiteInteger) {
		return ChannelLeaveOperationAuthority{}, false, ErrChannelLeaveAuthority
	}
	authority := ChannelLeaveOperationAuthority{channelID: channelID}
	if requestText.Valid {
		requestID, err := model.ParseChannelLeaveRequestID(requestText.String)
		if err != nil {
			return ChannelLeaveOperationAuthority{}, false, ErrChannelLeaveAuthority
		}
		authority.requestID = requestID
		authority.retryGeneration = uint64(retryGeneration.Int64)
	}
	return authority, true, nil
}

func validateChannelLeaveOperation(ctx context.Context, tx *sql.Tx,
	operation ChannelLeaveOperationAuthority,
) error {
	node, err := readNode(ctx, tx)
	if err != nil {
		return fmt.Errorf("%w: operation Node: %v", ErrChannelLeaveAuthority, err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), operation.channelID)
	if err != nil {
		return fmt.Errorf("%w: operation Channel: %v", ErrChannelLeaveAuthority, err)
	}
	if !operation.hasRequest() {
		if authority.channel.OwnerPeerID() != node.PeerID() ||
			authority.channel.Status() != model.ChannelClosed {
			return ErrChannelLeaveAuthority
		}
		return nil
	}
	request, _, err := readChannelLeaveRequestByID(ctx, tx, node.PeerID(), operation.requestID)
	if err != nil || request.request.Record().ChannelID() != operation.channelID ||
		request.retryGeneration < operation.retryGeneration ||
		authority.channel.OwnerPeerID() == node.PeerID() {
		return ErrChannelLeaveAuthority
	}
	return nil
}

func insertChannelLeaveOperation(ctx context.Context, tx *sql.Tx,
	operation ChannelLeaveOperation, channelID model.ChannelID,
	requestID model.ChannelLeaveRequestID, retryGeneration uint64, at time.Time,
) error {
	if !operation.valid() || channelID.IsZero() || retryGeneration > model.MaxSQLiteInteger {
		return ErrChannelLeaveInput
	}
	at, err := canonicalStoreTime(at)
	if err != nil {
		return ErrChannelLeaveInput
	}
	var requestValue any
	var generationValue any
	if !requestID.IsZero() {
		requestValue = requestID.String()
		generationValue = retryGeneration
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_leave_operations(
		operation_key_hash,request_digest,channel_id,request_id,retry_generation,committed_at)
		VALUES(?,?,?,?,?,?)`, operation.OperationKeyHash.Bytes(), operation.RequestDigest.Bytes(),
		channelID.String(), requestValue, generationValue, storeTime(at))
	if err != nil {
		return fmt.Errorf("%w: insert operation: %v", ErrChannelLeaveConflict, err)
	}
	return nil
}
