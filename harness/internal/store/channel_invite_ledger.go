package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type enrollmentUseRow struct {
	useID, grantText, peerText, usedText                string
	identityRaw, memberHash                             []byte
	memberRevision                                      uint64
	receiptID, receiptChannel, receiptPeer, receiptTime sql.NullString
	receiptRevision                                     sql.NullInt64
	receiptHash, receiptRaw, receiptSignature           []byte
}

func readOwnedInviteAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (verifiedChannelAuthority, model.Node, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return verifiedChannelAuthority{}, model.Node{},
			fmt.Errorf("rotate Channel invite: read Node: %w", err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return verifiedChannelAuthority{}, model.Node{},
			fmt.Errorf("rotate Channel invite: authority: %w", err)
	}
	return authority, node, nil
}

// verifyOwnedChannelEnrollmentLedger reconstructs grants, exact first-member
// uses and owner receipts before invite authority can mutate.
func verifyOwnedChannelEnrollmentLedger(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority,
) error {
	if authority.channel.ID().IsZero() || authority.roster.IsZero() {
		return fmt.Errorf("%w: incomplete Channel enrollment authority", ErrChannelInviteConflict)
	}
	grants, err := readOwnedChannelEnrollmentGrants(ctx, tx, authority.channel.ID())
	if err != nil {
		return err
	}
	members := authority.roster.Members()
	firstMembers := make(map[model.PeerID]model.Member, authority.channel.MemberLimit())
	for _, member := range members {
		if _, exists := firstMembers[member.PeerID()]; !exists {
			firstMembers[member.PeerID()] = member
		}
	}
	usesByPeer, err := readOwnedChannelEnrollmentUses(
		ctx, tx, authority, grants, members, firstMembers)
	if err != nil {
		return err
	}
	for peerID, member := range firstMembers {
		if peerID == authority.channel.OwnerPeerID() {
			if member.Head().Revision() != 1 {
				return fmt.Errorf("%w: owner is not roster genesis", ErrChannelInviteConflict)
			}
			continue
		}
		if head, exists := usesByPeer[peerID]; !exists || head != member.Head() {
			return fmt.Errorf("%w: non-owner membership lacks exact grant evidence",
				ErrChannelInviteConflict)
		}
	}
	var replicas int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrollment_receipts
		WHERE channel_id=? AND owner_use_id IS NULL`, authority.channel.ID().String()).Scan(&replicas)
	if err != nil || replicas != 0 {
		return fmt.Errorf("%w: owner Channel contains joiner-only receipt rows: %v",
			ErrChannelInviteConflict, err)
	}
	return nil
}

func readOwnedChannelEnrollmentGrants(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (map[model.GrantID]durableEnrollmentGrant, error) {
	rows, err := tx.QueryContext(ctx, `SELECT grant_id FROM enrollment_grants
		WHERE channel_id=? ORDER BY created_at,grant_id`, channelID.String())
	if err != nil {
		return nil, fmt.Errorf("%w: read Channel grants: %v", ErrChannelInviteConflict, err)
	}
	defer rows.Close()
	var grantIDs []model.GrantID
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return nil, fmt.Errorf("%w: scan Channel grant: %v", ErrChannelInviteConflict, err)
		}
		id, err := model.ParseGrantID(text)
		if err != nil {
			return nil, fmt.Errorf("%w: parse Channel grant: %v", ErrChannelInviteConflict, err)
		}
		grantIDs = append(grantIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate Channel grants: %v", ErrChannelInviteConflict, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("%w: close Channel grants: %v", ErrChannelInviteConflict, err)
	}
	grants := make(map[model.GrantID]durableEnrollmentGrant, len(grantIDs))
	var lifecycle time.Time
	for _, id := range grantIDs {
		grant, err := readDurableEnrollmentGrant(ctx, tx, id)
		if err != nil || grant.channelID != channelID ||
			(!lifecycle.IsZero() && grant.createdAt.Before(lifecycle)) {
			return nil, fmt.Errorf("%w: invalid Channel grant %s: %v",
				ErrChannelInviteConflict, id.String(), err)
		}
		grants[id] = grant
		if grant.createdAt.After(lifecycle) {
			lifecycle = grant.createdAt
		}
		if grant.closedAt.Valid {
			closedAt, err := parseCanonicalStoreTime(grant.closedAt.String)
			if err != nil {
				return nil, fmt.Errorf("%w: invalid grant terminal time: %v",
					ErrChannelInviteConflict, err)
			}
			if closedAt.After(lifecycle) {
				lifecycle = closedAt
			}
		}
	}
	return grants, nil
}

func readOwnedChannelEnrollmentUses(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, grants map[model.GrantID]durableEnrollmentGrant,
	members []model.Member, firstMembers map[model.PeerID]model.Member,
) (map[model.PeerID]model.RecordHead, error) {
	rows, err := tx.QueryContext(ctx, `SELECT uses.use_id,uses.grant_id,uses.member_peer_id,
		uses.join_identity_digest,uses.member_revision,uses.member_record_hash,uses.used_at,
		receipts.receipt_id,receipts.channel_id,receipts.member_peer_id,
		receipts.roster_head_revision,receipts.roster_head_hash,receipts.receipt_json,
		receipts.owner_signature,receipts.created_at
		FROM enrollment_grant_uses uses
		LEFT JOIN enrollment_receipts receipts ON receipts.owner_use_id=uses.use_id
		WHERE uses.channel_id=? ORDER BY uses.used_at,uses.use_id`, authority.channel.ID().String())
	if err != nil {
		return nil, fmt.Errorf("%w: read enrollment uses: %v", ErrChannelInviteConflict, err)
	}
	defer rows.Close()
	usesByPeer := make(map[model.PeerID]model.RecordHead, len(firstMembers))
	for rows.Next() {
		peerID, head, err := verifyOwnedChannelEnrollmentUse(
			rows, authority, grants, members, firstMembers, usesByPeer)
		if err != nil {
			return nil, err
		}
		usesByPeer[peerID] = head
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate enrollment uses: %v", ErrChannelInviteConflict, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("%w: close enrollment uses: %v", ErrChannelInviteConflict, err)
	}
	return usesByPeer, nil
}

func verifyOwnedChannelEnrollmentUse(rows *sql.Rows, authority verifiedChannelAuthority,
	grants map[model.GrantID]durableEnrollmentGrant, members []model.Member,
	firstMembers map[model.PeerID]model.Member, usesByPeer map[model.PeerID]model.RecordHead,
) (model.PeerID, model.RecordHead, error) {
	var row enrollmentUseRow
	err := rows.Scan(&row.useID, &row.grantText, &row.peerText, &row.identityRaw,
		&row.memberRevision, &row.memberHash, &row.usedText, &row.receiptID,
		&row.receiptChannel, &row.receiptPeer, &row.receiptRevision, &row.receiptHash,
		&row.receiptRaw, &row.receiptSignature, &row.receiptTime)
	if err != nil {
		return model.PeerID{}, model.RecordHead{},
			fmt.Errorf("%w: scan enrollment use: %v", ErrChannelInviteConflict, err)
	}
	grantID, grantErr := model.ParseGrantID(row.grantText)
	grant, grantExists := grants[grantID]
	peerID, peerErr := model.ParsePeerID(row.peerText)
	identity, identityErr := model.DigestFromBytes(row.identityRaw)
	memberDigest, digestErr := model.DigestFromBytes(row.memberHash)
	memberHead, headErr := model.NewRecordHead(row.memberRevision, memberDigest)
	usedAt, usedErr := parseCanonicalStoreTime(row.usedText)
	if !row.validProjection(grantErr, grantExists, peerErr, identityErr,
		digestErr, headErr, usedErr, len(members)) {
		return model.PeerID{}, model.RecordHead{},
			fmt.Errorf("%w: invalid enrollment use projection", ErrChannelInviteConflict)
	}
	member := members[row.memberRevision-1]
	first, firstExists := firstMembers[peerID]
	if !validEnrollmentUseMember(
		first, firstExists, member, memberHead, peerID, grant, identity, usedAt) {
		return model.PeerID{}, model.RecordHead{}, fmt.Errorf(
			"%w: grant use does not identify the first active MemberRecord",
			ErrChannelInviteConflict)
	}
	if _, duplicate := usesByPeer[peerID]; duplicate {
		return model.PeerID{}, model.RecordHead{},
			fmt.Errorf("%w: duplicate Channel enrollment use", ErrChannelInviteConflict)
	}
	if err := row.verifyReceipt(authority, member, peerID, grantID, identity, usedAt); err != nil {
		return model.PeerID{}, model.RecordHead{}, err
	}
	return peerID, memberHead, nil
}

func (row enrollmentUseRow) validProjection(grantErr error, grantExists bool,
	peerErr, identityErr, digestErr, headErr, usedErr error, memberCount int,
) bool {
	return grantErr == nil && grantExists && peerErr == nil && identityErr == nil &&
		digestErr == nil && headErr == nil && usedErr == nil && row.memberRevision > 0 &&
		row.memberRevision <= uint64(memberCount) && row.useID != ""
}

func validEnrollmentUseMember(first model.Member, firstExists bool, member model.Member,
	head model.RecordHead, peerID model.PeerID, grant durableEnrollmentGrant,
	identity model.Digest, usedAt time.Time,
) bool {
	return firstExists && first.Head() == head && member.Head() == head &&
		member.PeerID() == peerID && member.Status() == model.MemberActive &&
		!member.CreatedAt().Before(grant.createdAt) && !usedAt.Before(member.CreatedAt()) &&
		!usedAt.Before(grant.createdAt) && usedAt.Before(grant.expiresAt) && !identity.IsZero()
}

func (row enrollmentUseRow) verifyReceipt(authority verifiedChannelAuthority,
	member model.Member, peerID model.PeerID, grantID model.GrantID,
	identity model.Digest, usedAt time.Time,
) error {
	if !row.hasReceipt() {
		return fmt.Errorf("%w: enrollment use lacks its owner receipt", ErrChannelInviteConflict)
	}
	record, err := model.ParseEnrollmentReceiptRecord(row.receiptRaw)
	if err != nil {
		return fmt.Errorf("%w: parse enrollment receipt: %v", ErrChannelInviteConflict, err)
	}
	receipt, err := model.AttachEnrollmentReceiptSignature(record, row.receiptSignature)
	createdAt, createdErr := parseCanonicalStoreTime(row.receiptTime.String)
	if err != nil || createdErr != nil ||
		!row.matchesReceipt(authority, member, peerID, grantID, identity, usedAt, receipt, createdAt) {
		return fmt.Errorf("%w: enrollment receipt projection or signature mismatch",
			ErrChannelInviteConflict)
	}
	return nil
}

func (row enrollmentUseRow) hasReceipt() bool {
	return row.receiptID.Valid && row.receiptChannel.Valid && row.receiptPeer.Valid &&
		row.receiptRevision.Valid && row.receiptTime.Valid && len(row.receiptRaw) != 0 &&
		len(row.receiptSignature) != 0
}

func (row enrollmentUseRow) matchesReceipt(authority verifiedChannelAuthority,
	member model.Member, peerID model.PeerID, grantID model.GrantID,
	identity model.Digest, usedAt time.Time, receipt model.EnrollmentReceipt,
	createdAt time.Time,
) bool {
	return model.VerifyEnrollmentReceiptEvidence(authority.channel.Descriptor(), member, receipt) == nil &&
		row.receiptID.String == receipt.ReceiptID().String() &&
		row.receiptChannel.String == authority.channel.ID().String() &&
		row.receiptPeer.String == peerID.String() &&
		row.receiptRevision.Int64 == int64(row.memberRevision) &&
		bytes.Equal(row.receiptHash, row.memberHash) && createdAt == receipt.AcceptedAt() &&
		!createdAt.Before(usedAt) && receipt.GrantID() == grantID &&
		receipt.JoinIdentityDigest() == identity
}
