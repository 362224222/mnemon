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

type PrepareJoinedChannelSpec struct {
	AuthenticatedLocalPeerID model.PeerID
	LocalPublicKey           []byte
	Descriptor               model.SignedChannelDescriptor
	GrantID                  model.GrantID
	LocalAlias               string
	At                       time.Time
}

type PrepareJoinedChannelResult struct {
	RequestID     model.EnrollmentRequestID
	OriginEpoch   model.OriginEpoch
	Attempt       uint64
	Reserved      bool
	CommitUnknown bool
}

// PrepareJoinedChannel reserves the one local Channel slot and alias before a
// remote owner can consume its grant. The durable row contains only signed or
// public identity metadata; bearer token material never crosses this boundary.
func (s *Store) PrepareJoinedChannel(ctx context.Context,
	spec PrepareJoinedChannelSpec,
) (PrepareJoinedChannelResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.AuthenticatedLocalPeerID.IsZero() ||
		spec.Descriptor.IsZero() || spec.GrantID.IsZero() || spec.LocalAlias == "" ||
		model.VerifyChannelDescriptor(spec.Descriptor) != nil {
		return PrepareJoinedChannelResult{}, ErrChannelJoinInput
	}
	// Channel aliases use the same canonical identifier grammar. Parsing as a
	// ChannelID validates that grammar without manufacturing durable authority.
	if _, err := model.ParseChannelID(spec.LocalAlias); err != nil {
		return PrepareJoinedChannelResult{}, ErrChannelJoinInput
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.Before(spec.Descriptor.Descriptor().CreatedAt()) ||
		spec.Descriptor.Descriptor().OwnerPeerID() == spec.AuthenticatedLocalPeerID {
		return PrepareJoinedChannelResult{}, ErrChannelJoinInput
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PrepareJoinedChannelResult{}, fmt.Errorf("prepare joined Channel: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil || node.PeerID() != spec.AuthenticatedLocalPeerID {
		return PrepareJoinedChannelResult{}, ErrChannelJoinInput
	}
	joinIdentity, err := model.EnrollmentJoinIdentityDigest(spec.Descriptor.Descriptor().ID(),
		spec.GrantID, node.PeerID(), spec.LocalPublicKey, node.OriginEpoch())
	if err != nil {
		return PrepareJoinedChannelResult{}, ErrChannelJoinInput
	}
	requestID, err := model.EnrollmentRequestIDForJoinIdentity(joinIdentity)
	if err != nil {
		return PrepareJoinedChannelResult{}, ErrChannelJoinInput
	}

	var installed int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE channel_id=?`,
		spec.Descriptor.Descriptor().ID().String()).Scan(&installed); err != nil {
		return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
	}
	if installed != 0 {
		authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(),
			spec.Descriptor.Descriptor().ID())
		if err != nil || authority.channel.LocalAlias() != spec.LocalAlias ||
			!bytes.Equal(authority.channel.Descriptor().WireJSON().Bytes(), spec.Descriptor.WireJSON().Bytes()) {
			return PrepareJoinedChannelResult{}, ErrChannelJoinConflict
		}
		storedRequest, err := readReplicaEnrollmentRequest(ctx, tx, authority.channel.ID(), node.PeerID())
		if err != nil || storedRequest != requestID {
			return PrepareJoinedChannelResult{}, ErrChannelJoinConflict
		}
		if err := tx.Commit(); err != nil {
			return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
		}
		return PrepareJoinedChannelResult{RequestID: requestID,
			OriginEpoch: node.OriginEpoch()}, nil
	}

	var requestText, channelText, grantText, peerText, epochText, alias, state string
	var attempt uint64
	var storedJoinIdentity, descriptorDigest []byte
	err = tx.QueryRowContext(ctx, `SELECT request_id,channel_id,grant_id,join_identity_digest,
		descriptor_digest,local_peer_id,origin_epoch,local_alias,attempt,state
		FROM channel_join_reservations WHERE request_id=?`, requestID.String()).Scan(&requestText,
		&channelText, &grantText, &storedJoinIdentity, &descriptorDigest, &peerText, &epochText,
		&alias, &attempt, &state)
	if err == nil {
		if requestText != requestID.String() || channelText != spec.Descriptor.Descriptor().ID().String() ||
			grantText != spec.GrantID.String() || !bytes.Equal(storedJoinIdentity, joinIdentity.Bytes()) ||
			!bytes.Equal(descriptorDigest, spec.Descriptor.Descriptor().Digest().Bytes()) ||
			peerText != node.PeerID().String() || epochText != node.OriginEpoch().String() ||
			alias != spec.LocalAlias || attempt == 0 || attempt >= model.MaxSQLiteInteger ||
			(state != "reserved" && state != "commit_unknown") {
			return PrepareJoinedChannelResult{}, ErrChannelJoinConflict
		}
		result, err := tx.ExecContext(ctx, `UPDATE channel_join_reservations
			SET attempt=?,updated_at=CASE WHEN updated_at > ? THEN updated_at ELSE ? END
			WHERE request_id=? AND local_peer_id=? AND attempt=?`, attempt+1, storeTime(at), storeTime(at),
			requestID.String(), node.PeerID().String(), attempt)
		if err != nil {
			return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
		}
		if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
			return PrepareJoinedChannelResult{}, ErrChannelJoinConflict
		}
		if err := tx.Commit(); err != nil {
			return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
		}
		return PrepareJoinedChannelResult{RequestID: requestID,
			OriginEpoch: node.OriginEpoch(), Attempt: attempt + 1, Reserved: true,
			CommitUnknown: state == "commit_unknown"}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_join_reservations(request_id,channel_id,
		grant_id,join_identity_digest,descriptor_digest,local_peer_id,origin_epoch,local_alias,
		attempt,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,1,'reserved',?,?)`, requestID.String(),
		spec.Descriptor.Descriptor().ID().String(), spec.GrantID.String(), joinIdentity.Bytes(),
		spec.Descriptor.Descriptor().Digest().Bytes(), node.PeerID().String(), node.OriginEpoch().String(),
		spec.LocalAlias, storeTime(at), storeTime(at))
	if err != nil {
		return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
	}
	if err := tx.Commit(); err != nil {
		return PrepareJoinedChannelResult{}, mapChannelJoinError(err)
	}
	return PrepareJoinedChannelResult{RequestID: requestID, OriginEpoch: node.OriginEpoch(),
		Attempt: 1, Reserved: true}, nil
}

// MarkJoinedChannelCommitUnknown durably records the boundary immediately
// before EnrollProof is written. A crash after this point retains the slot for
// same-request recovery instead of allowing a competing join to consume it.
func (s *Store) MarkJoinedChannelCommitUnknown(ctx context.Context,
	requestID model.EnrollmentRequestID, localPeer model.PeerID, attempt uint64, at time.Time,
) error {
	if s == nil || s.db == nil || ctx == nil || requestID.IsZero() || localPeer.IsZero() ||
		attempt == 0 || attempt > model.MaxSQLiteInteger {
		return ErrChannelJoinInput
	}
	canonical, err := canonicalStoreTime(at)
	if err != nil {
		return ErrChannelJoinInput
	}
	result, err := s.db.ExecContext(ctx, `UPDATE channel_join_reservations
		SET state='commit_unknown',updated_at=CASE WHEN updated_at > ? THEN updated_at ELSE ? END
		WHERE request_id=? AND local_peer_id=? AND attempt=?
		AND state IN ('reserved','commit_unknown')`, storeTime(canonical), storeTime(canonical),
		requestID.String(), localPeer.String(), attempt)
	if err != nil {
		return mapChannelJoinError(err)
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
		return ErrChannelJoinConflict
	}
	return nil
}

// ReleaseJoinedChannelReservation is called only after a verified remote
// ProtocolError proves that no owner commit occurred, or before EnrollProof was
// attempted. Ambiguous transport loss deliberately retains commit_unknown.
func (s *Store) ReleaseJoinedChannelReservation(ctx context.Context,
	requestID model.EnrollmentRequestID, localPeer model.PeerID, attempt uint64,
) error {
	if s == nil || s.db == nil || ctx == nil || requestID.IsZero() || localPeer.IsZero() ||
		attempt == 0 || attempt > model.MaxSQLiteInteger {
		return ErrChannelJoinInput
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM channel_join_reservations
		WHERE request_id=? AND local_peer_id=? AND attempt=?`, requestID.String(), localPeer.String(),
		attempt)
	if err != nil {
		return mapChannelJoinError(err)
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed > 1 {
		return ErrChannelJoinConflict
	}
	return nil
}

func readReplicaEnrollmentRequest(ctx context.Context, tx *sql.Tx, channelID model.ChannelID,
	localPeer model.PeerID,
) (model.EnrollmentRequestID, error) {
	var receiptRaw []byte
	err := tx.QueryRowContext(ctx, `SELECT receipt_json FROM enrollment_receipts
		WHERE channel_id=? AND member_peer_id=? AND owner_use_id IS NULL`, channelID.String(),
		localPeer.String()).Scan(&receiptRaw)
	if err != nil {
		return model.EnrollmentRequestID{}, err
	}
	record, err := model.ParseEnrollmentReceiptRecord(receiptRaw)
	if err != nil {
		return model.EnrollmentRequestID{}, err
	}
	return record.RequestID(), nil
}

func verifyJoinedChannelReservation(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec,
) error {
	joinIdentity, err := spec.Transcript.JoinIdentityDigest()
	if err != nil {
		return ErrChannelJoinInput
	}
	var channelText, grantText, peerText, epochText, alias string
	var storedJoinIdentity, descriptorDigest []byte
	err = tx.QueryRowContext(ctx, `SELECT channel_id,grant_id,join_identity_digest,descriptor_digest,
		local_peer_id,origin_epoch,local_alias FROM channel_join_reservations WHERE request_id=?`,
		spec.Transcript.RequestID().String()).Scan(&channelText, &grantText, &storedJoinIdentity,
		&descriptorDigest, &peerText, &epochText, &alias)
	if err != nil || channelText != spec.Transcript.ChannelID().String() ||
		grantText != spec.Transcript.GrantID().String() || !bytes.Equal(storedJoinIdentity, joinIdentity.Bytes()) ||
		!bytes.Equal(descriptorDigest, spec.Descriptor.Descriptor().Digest().Bytes()) ||
		peerText != node.PeerID().String() || epochText != node.OriginEpoch().String() || alias != spec.LocalAlias {
		return ErrChannelJoinInput
	}
	return nil
}

func consumeJoinedChannelReservation(ctx context.Context, tx *sql.Tx,
	requestID model.EnrollmentRequestID,
) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM channel_join_reservations WHERE request_id=?`,
		requestID.String())
	if err != nil {
		return mapChannelJoinError(err)
	}
	if changed, changeErr := result.RowsAffected(); changeErr != nil || changed != 1 {
		return ErrChannelJoinConflict
	}
	return nil
}
