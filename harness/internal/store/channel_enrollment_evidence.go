package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"sort"
	"strings"
	"time"
	"unicode"
)

type durableChannelEnrollmentUse struct {
	useID        model.EnrollmentUseID
	grantID      model.GrantID
	joinIdentity model.Digest
	member       model.Member
	receipt      model.EnrollmentReceipt
	usedAt       time.Time
}

func readChannelEnrollmentUse(ctx context.Context, tx *sql.Tx, authority verifiedChannelAuthority,
	peerID model.PeerID,
) (durableChannelEnrollmentUse, error) {
	var useText, grantText, usedText string
	var identityRaw, memberHash []byte
	var memberRevision uint64
	err := tx.QueryRowContext(ctx, `SELECT use_id,grant_id,join_identity_digest,member_revision,
		member_record_hash,used_at FROM enrollment_grant_uses WHERE channel_id=? AND member_peer_id=?`,
		authority.channel.ID().String(), peerID.String()).Scan(&useText, &grantText, &identityRaw,
		&memberRevision, &memberHash, &usedText)
	if err != nil {
		return durableChannelEnrollmentUse{}, err
	}
	useID, err := model.ParseEnrollmentUseID(useText)
	grantID, grantErr := model.ParseGrantID(grantText)
	identity, identityErr := model.DigestFromBytes(identityRaw)
	memberDigest, digestErr := model.DigestFromBytes(memberHash)
	memberHead, headErr := model.NewRecordHead(memberRevision, memberDigest)
	usedAt, usedErr := parseCanonicalStoreTime(usedText)
	member, memberExists := memberAtHead(authority.roster, memberHead)
	if err != nil || grantErr != nil || identityErr != nil || digestErr != nil || headErr != nil ||
		usedErr != nil || !memberExists || member.PeerID() != peerID {
		return durableChannelEnrollmentUse{}, ErrChannelEnrollmentConflict
	}
	var receiptText string
	if err := tx.QueryRowContext(ctx, `SELECT receipt_id FROM enrollment_receipts WHERE owner_use_id=?`,
		useID.String()).Scan(&receiptText); err != nil {
		return durableChannelEnrollmentUse{}, ErrChannelEnrollmentConflict
	}
	receiptID, err := model.ParseEnrollmentReceiptID(receiptText)
	if err != nil {
		return durableChannelEnrollmentUse{}, ErrChannelEnrollmentConflict
	}
	receipt, ownerUse, err := readEnrollmentReceipt(ctx, tx, receiptID)
	if err != nil || !ownerUse.Valid || ownerUse.String != useID.String() ||
		model.VerifyEnrollmentReceiptEvidence(authority.channel.Descriptor(), member, receipt) != nil ||
		receipt.JoinIdentityDigest() != identity || receipt.GrantID() != grantID {
		return durableChannelEnrollmentUse{}, ErrChannelEnrollmentConflict
	}
	return durableChannelEnrollmentUse{useID: useID, grantID: grantID, joinIdentity: identity,
		member: member, receipt: receipt, usedAt: usedAt}, nil
}

func enrollmentPredecessor(member model.Member) (model.RecordHead, error) {
	previous, ok := member.PreviousDigest()
	if !ok || member.Head().Revision() <= 1 {
		return model.RecordHead{}, ErrChannelEnrollmentConflict
	}
	return model.NewRecordHead(member.Head().Revision()-1, previous)
}

func memberAtHead(roster model.VerifiedRoster, head model.RecordHead) (model.Member, bool) {
	members := roster.Members()
	if head.IsZero() || head.Revision() > uint64(len(members)) {
		return model.Member{}, false
	}
	member := members[head.Revision()-1]
	return member, member.Head() == head
}

func firstRosterAuthorityForPeer(roster model.VerifiedRoster, candidate model.Member) bool {
	for _, member := range roster.Members() {
		if member.PeerID() != candidate.PeerID() {
			continue
		}
		return member.Head() == candidate.Head() && member.Status() == model.MemberActive
	}
	return false
}

func insertEnrollmentReceipt(ctx context.Context, tx *sql.Tx, receipt model.EnrollmentReceipt,
	ownerUseID string,
) error {
	var ownerUse any
	if ownerUseID != "" {
		ownerUse = ownerUseID
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES(?,?,?,?,?,?,?,?,?)`, receipt.ReceiptID().String(), ownerUse, receipt.ChannelID().String(),
		receipt.MemberPeerID().String(), receipt.MemberHead().Revision(), receipt.MemberHead().Digest().Bytes(),
		receipt.ReceiptJSON().Bytes(), receipt.OwnerSignature(), storeTime(receipt.AcceptedAt()))
	if err != nil {
		return fmt.Errorf("insert enrollment receipt: %w", err)
	}
	return nil
}

func readEnrollmentReceipt(ctx context.Context, tx *sql.Tx,
	receiptID model.EnrollmentReceiptID,
) (model.EnrollmentReceipt, sql.NullString, error) {
	var idText, channelText, peerText, createdText string
	var ownerUse sql.NullString
	var revision uint64
	var headHash, receiptRaw, signature []byte
	err := tx.QueryRowContext(ctx, `SELECT receipt_id,owner_use_id,channel_id,member_peer_id,
		roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at
		FROM enrollment_receipts WHERE receipt_id=?`, receiptID.String()).Scan(&idText, &ownerUse,
		&channelText, &peerText, &revision, &headHash, &receiptRaw, &signature, &createdText)
	if err != nil {
		return model.EnrollmentReceipt{}, sql.NullString{}, err
	}
	record, err := model.ParseEnrollmentReceiptRecord(receiptRaw)
	receipt, attachErr := model.AttachEnrollmentReceiptSignature(record, signature)
	createdAt, timeErr := parseCanonicalStoreTime(createdText)
	if err != nil || attachErr != nil || timeErr != nil || idText != receiptID.String() ||
		receipt.ReceiptID() != receiptID ||
		channelText != receipt.ChannelID().String() || peerText != receipt.MemberPeerID().String() ||
		revision != receipt.MemberHead().Revision() || !bytes.Equal(headHash, receipt.MemberHead().Digest().Bytes()) ||
		createdAt != receipt.AcceptedAt() {
		return model.EnrollmentReceipt{}, sql.NullString{}, ErrChannelEnrollmentConflict
	}
	return receipt, ownerUse, nil
}

func bindingLastSeen(binding model.PeerBinding) *time.Time {
	value, ok := binding.LastSeenAt()
	if !ok {
		return nil
	}
	return &value
}

func uniqueEffectiveAlias(label string, peerID model.PeerID,
	occupied map[string]model.PeerID,
) (string, error) {
	peerText := peerID.String()
	for width := 8; width <= len(peerText); width += 4 {
		if width > len(peerText) {
			width = len(peerText)
		}
		candidate := label + "~" + peerText[len(peerText)-width:]
		if _, exists := occupied[candidate]; !exists {
			return candidate, nil
		}
		if width == len(peerText) {
			break
		}
	}
	base := label + "~" + strings.TrimPrefix(model.Sum([]byte(peerText)).String(), "sha256:")
	for index := 0; index <= len(occupied); index++ {
		candidate := base
		if index != 0 {
			candidate = fmt.Sprintf("%s~%d", base, index)
		}
		if _, exists := occupied[candidate]; !exists {
			return candidate, nil
		}
	}
	return "", ErrChannelJoinConflict
}

func temporaryBindingAlias(channelID model.ChannelID, peerID model.PeerID,
	unavailable map[string]struct{},
) string {
	base := "mnemon-sync~" + strings.TrimPrefix(model.Sum([]byte(
		channelID.String()+"\x00"+peerID.String())).String(), "sha256:")
	for index := 0; ; index++ {
		candidate := base
		if index != 0 {
			candidate = fmt.Sprintf("%s~%d", base, index)
		}
		if _, exists := unavailable[candidate]; !exists {
			return candidate
		}
	}
}

func insertPendingRosterBindings(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, joinedAt time.Time,
) error {
	aliases, members, err := deriveEffectiveAliases(localPeer, roster)
	if err != nil {
		return err
	}
	peers := make([]model.PeerID, 0, len(members))
	for peerID := range members {
		peers = append(peers, peerID)
	}
	sort.Slice(peers, func(left, right int) bool { return peers[left].String() < peers[right].String() })
	for _, peerID := range peers {
		if err := insertPendingPeerBinding(ctx, tx, localPeer, channel, roster, peerID,
			aliases[peerID], joinedAt); err != nil {
			return err
		}
	}
	return nil
}

func insertPendingPeerBinding(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, peerID model.PeerID, alias string,
	joinedAt time.Time,
) error {
	binding, err := model.NewPeerBinding(localPeer, model.PeerBindingSpec{Channel: channel,
		Roster: roster, PeerID: peerID, EffectiveAlias: alias, State: model.BindingPending,
		Reachability: model.ReachabilityUnknown, JoinedAt: joinedAt})
	if err != nil {
		return err
	}
	multiaddrs, err := model.JSONFrom(binding.Multiaddrs())
	if err != nil {
		return err
	}
	protocols, err := model.JSONFrom(binding.Protocols())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,
		effective_alias,public_key,multiaddrs_json,protocols_json,limits_json,member_revision,
		member_record_hash,state,reachability,joined_at,last_seen_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)`, channel.ID().String(), binding.PeerID().String(),
		binding.OriginEpoch().String(), binding.EffectiveAlias(), binding.PublicKey(), multiaddrs.Bytes(),
		protocols.Bytes(), binding.Limits().Bytes(), binding.MemberHead().Revision(),
		binding.MemberHead().Digest().Bytes(), string(binding.State()), string(binding.Reachability()),
		storeTime(binding.JoinedAt()))
	if err != nil {
		return fmt.Errorf("insert pending PeerBinding: %w", err)
	}
	return nil
}

func sanitizeEffectiveAliasLabel(label string) string {
	var result strings.Builder
	result.Grow(len(label))
	separator := false
	for _, character := range label {
		if unicode.IsSpace(character) {
			separator = result.Len() != 0
			continue
		}
		if separator {
			result.WriteByte('-')
			separator = false
		}
		result.WriteRune(character)
	}
	return result.String()
}
