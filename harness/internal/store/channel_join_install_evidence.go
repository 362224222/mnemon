package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func joinedChannelInstallEvidence(ctx context.Context, tx *sql.Tx,
	plan JoinedChannelInstallPlan,
) (bool, error) {
	if plan.result.Channel.ID().IsZero() {
		// A consumed terminal-only reservation is indistinguishable from an
		// authenticated remote rejection release. It is never commit evidence.
		return false, nil
	}
	return joinedChannelReplicaEvidence(ctx, tx, plan.candidate,
		joinedChannelReplicaSnapshot{channel: plan.result.Channel, roster: plan.result.Roster})
}

func joinedChannelInstallPreimage(ctx context.Context, tx *sql.Tx,
	plan JoinedChannelInstallPlan,
) (bool, error) {
	if !plan.reservation.isZero() {
		absent, err := joinedChannelReplicaRowsAbsent(ctx, tx, plan.candidate)
		if err != nil || !absent {
			return false, err
		}
		return verifyJoinedChannelReservationFence(ctx, tx, plan.reservation) == nil, nil
	}
	if plan.before.channel.ID().IsZero() || plan.before.roster.IsZero() {
		return false, nil
	}
	return joinedChannelReplicaEvidence(ctx, tx, plan.candidate, plan.before)
}

func joinedChannelReplicaEvidence(ctx context.Context, tx *sql.Tx,
	candidate joinedChannelInstallCandidate, expected joinedChannelReplicaSnapshot,
) (bool, error) {
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE channel_id=?`,
		candidate.descriptor.Descriptor().ID().String()).Scan(&exists); err != nil {
		return false, fmt.Errorf("inspect joined Channel evidence: %w", err)
	}
	if exists != 1 {
		return false, nil
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, candidate.transcript.JoinerPeerID(),
		candidate.descriptor.Descriptor().ID())
	if err != nil {
		return false, fmt.Errorf("inspect joined Channel authority evidence: %w", err)
	}
	if !sameJoinedChannel(expected.channel, authority.channel) ||
		!sameJoinedRoster(expected.roster, authority.roster) {
		return false, nil
	}
	receiptOK, err := joinedChannelReceiptEvidence(ctx, tx, candidate, authority.roster)
	if err != nil || !receiptOK {
		return false, err
	}
	epochOK, err := joinedChannelPublicationEvidence(ctx, tx, candidate, authority.channel)
	if err != nil || !epochOK {
		return false, err
	}
	clean, err := joinedChannelReplicaHasNoLocalAuthority(ctx, tx, candidate)
	if err != nil || !clean {
		return false, err
	}
	return true, nil
}

func sameJoinedChannel(expected, actual model.Channel) bool {
	return !expected.ID().IsZero() && expected.ID() == actual.ID() &&
		bytes.Equal(expected.Descriptor().WireJSON().Bytes(), actual.Descriptor().WireJSON().Bytes()) &&
		expected.LocalAlias() == actual.LocalAlias() && expected.RosterHead() == actual.RosterHead() &&
		expected.Status() == actual.Status() && expected.TopicState() == actual.TopicState() &&
		expected.UpdatedAt() == actual.UpdatedAt()
}

func sameJoinedRoster(expected, actual model.VerifiedRoster) bool {
	expectedMembers, actualMembers := expected.Members(), actual.Members()
	if expected.IsZero() || expected.Head() != actual.Head() || len(expectedMembers) != len(actualMembers) {
		return false
	}
	for index := range expectedMembers {
		if !bytes.Equal(expectedMembers[index].WireJSON().Bytes(), actualMembers[index].WireJSON().Bytes()) {
			return false
		}
	}
	return true
}

func joinedChannelReceiptEvidence(ctx context.Context, tx *sql.Tx,
	candidate joinedChannelInstallCandidate, roster model.VerifiedRoster,
) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrollment_receipts WHERE channel_id=?`,
		candidate.descriptor.Descriptor().ID().String()).Scan(&count); err != nil {
		return false, fmt.Errorf("inspect joined Channel receipt count: %w", err)
	}
	if count != 1 {
		return false, nil
	}
	receipt, ownerUse, err := readEnrollmentReceipt(ctx, tx, candidate.receipt.ReceiptID())
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	member, ok := memberAtHead(roster, candidate.receipt.MemberHead())
	if err != nil || !ok || ownerUse.Valid ||
		!bytes.Equal(receipt.WireJSON().Bytes(), candidate.receipt.WireJSON().Bytes()) ||
		model.VerifyEnrollmentReceiptEvidence(candidate.descriptor, member, receipt) != nil {
		return false, err
	}
	return true, nil
}

func joinedChannelPublicationEvidence(ctx context.Context, tx *sql.Tx,
	candidate joinedChannelInstallCandidate, channel model.Channel,
) (bool, error) {
	var peerText, epochText, updatedText string
	var floor, head uint64
	err := tx.QueryRowContext(ctx, `SELECT origin_peer_id,origin_epoch,source_floor_channel_seq,
		source_head_channel_seq,updated_at FROM publication_epochs WHERE channel_id=?`,
		channel.ID().String()).Scan(&peerText, &epochText, &floor, &head, &updatedText)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect joined Channel publication epoch: %w", err)
	}
	updatedAt, err := parseCanonicalStoreTime(updatedText)
	if err != nil || peerText != candidate.transcript.JoinerPeerID().String() ||
		epochText != candidate.transcript.JoinerOriginEpoch().String() || floor == 0 ||
		floor > model.MaxSQLiteInteger || head > model.MaxSQLiteInteger || floor > head+1 ||
		updatedAt.Before(channel.CreatedAt()) {
		return false, nil
	}
	return true, nil
}

func joinedChannelReplicaHasNoLocalAuthority(ctx context.Context, tx *sql.Tx,
	candidate joinedChannelInstallCandidate,
) (bool, error) {
	var reservations, grants, uses int
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM channel_join_reservations WHERE channel_id=?),
		(SELECT COUNT(*) FROM enrollment_grants WHERE channel_id=?),
		(SELECT COUNT(*) FROM enrollment_grant_uses WHERE channel_id=?)`,
		candidate.descriptor.Descriptor().ID().String(),
		candidate.descriptor.Descriptor().ID().String(),
		candidate.descriptor.Descriptor().ID().String()).Scan(&reservations, &grants, &uses)
	if err != nil {
		return false, fmt.Errorf("inspect joined Channel local authority: %w", err)
	}
	return reservations == 0 && grants == 0 && uses == 0, nil
}

func joinedChannelTerminalOutcomeUnproven(ctx context.Context, tx *sql.Tx,
	plan JoinedChannelInstallPlan,
) (bool, error) {
	absent, err := joinedChannelReplicaRowsAbsent(ctx, tx, plan.candidate)
	if err != nil || !absent {
		return false, err
	}
	var reservations int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_join_reservations WHERE request_id=?`,
		plan.candidate.transcript.RequestID().String()).Scan(&reservations)
	if err != nil {
		return false, fmt.Errorf("inspect terminal joined Channel reservation: %w", err)
	}
	return reservations == 0, nil
}

func joinedChannelReplicaRowsAbsent(ctx context.Context, tx *sql.Tx,
	candidate joinedChannelInstallCandidate,
) (bool, error) {
	channelID := candidate.descriptor.Descriptor().ID().String()
	var rows int
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM channels WHERE channel_id=?) +
		(SELECT COUNT(*) FROM channel_members WHERE channel_id=?) +
		(SELECT COUNT(*) FROM enrollment_receipts WHERE channel_id=?) +
		(SELECT COUNT(*) FROM publication_epochs WHERE channel_id=?) +
		(SELECT COUNT(*) FROM peer_bindings WHERE channel_id=?) +
		(SELECT COUNT(*) FROM enrollment_grants WHERE channel_id=?) +
		(SELECT COUNT(*) FROM enrollment_grant_uses WHERE channel_id=?)`,
		channelID, channelID, channelID, channelID, channelID, channelID, channelID).Scan(&rows)
	if err != nil {
		return false, fmt.Errorf("inspect absent joined Channel evidence: %w", err)
	}
	return rows == 0, nil
}
