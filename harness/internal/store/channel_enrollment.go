package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelEnrollmentInput          = errors.New("invalid Channel enrollment input")
	ErrChannelEnrollmentOwner          = errors.New("local Node is not the Channel owner")
	ErrChannelEnrollmentProof          = errors.New("Channel enrollment proof is invalid")
	ErrChannelEnrollmentUnavailable    = errors.New("Channel enrollment is unavailable")
	ErrChannelEnrollmentTokenExpired   = errors.New("Channel enrollment token expired")
	ErrChannelEnrollmentTokenClosed    = errors.New("Channel enrollment token closed")
	ErrChannelEnrollmentTokenExhausted = errors.New("Channel enrollment token exhausted")
	ErrChannelEnrollmentChannelClosed  = errors.New("Channel is closed")
	ErrChannelEnrollmentMemberRevoked  = errors.New("Channel member is revoked")
	ErrChannelEnrollmentConflict       = errors.New("Channel enrollment conflicts with durable state")
	ErrChannelEnrollmentStale          = errors.New("Channel enrollment challenge is stale")
	ErrChannelJoinInput                = errors.New("invalid joined Channel input")
	ErrChannelJoinConflict             = errors.New("joined Channel conflicts with durable state")
)

type ChannelEnrollmentStatus string

const (
	ChannelEnrollmentAccepted      ChannelEnrollmentStatus = "accepted"
	ChannelEnrollmentReplayed      ChannelEnrollmentStatus = "replayed"
	ChannelEnrollmentMemberRevoked ChannelEnrollmentStatus = "member_revoked"
	ChannelEnrollmentChannelClosed ChannelEnrollmentStatus = "channel_closed"
)

// ChannelAuthoritySigner is the smallest private-key capability accepted by
// the owner transaction. Store constructs both canonical messages and verifies
// the returned signatures against committed owner authority before writing.
type ChannelAuthoritySigner interface {
	Sign(context.Context, []byte) ([]byte, error)
}

type PrepareChannelEnrollmentSpec struct {
	ChannelID           model.ChannelID
	GrantID             model.GrantID
	RequestID           model.EnrollmentRequestID
	AuthenticatedPeerID model.PeerID
	JoinerOriginEpoch   model.OriginEpoch
	JoinerPublicKey     []byte
	At                  time.Time
}

type PrepareChannelEnrollmentResult struct {
	Status     ChannelEnrollmentStatus
	RosterHead model.RecordHead
}

// PrepareChannelEnrollment returns the exact roster head that a proof must
// bind. A response-loss replay uses the original joining member's predecessor,
// not today's head, so its original signed receipt remains verifiable.
func (s *Store) PrepareChannelEnrollment(ctx context.Context,
	spec PrepareChannelEnrollmentSpec,
) (PrepareChannelEnrollmentResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() || spec.GrantID.IsZero() ||
		spec.RequestID.IsZero() || spec.AuthenticatedPeerID.IsZero() || spec.JoinerOriginEpoch.IsZero() {
		return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentInput
	}
	at, err := canonicalStoreTime(spec.At)
	identity, identityErr := model.EnrollmentJoinIdentityDigest(spec.ChannelID, spec.GrantID,
		spec.AuthenticatedPeerID, spec.JoinerPublicKey, spec.JoinerOriginEpoch)
	if err != nil || identityErr != nil {
		return PrepareChannelEnrollmentResult{}, fmt.Errorf("%w: identity or challenge time",
			ErrChannelEnrollmentInput)
	}
	expectedRequest, requestErr := model.EnrollmentRequestIDForJoinIdentity(identity)
	if requestErr != nil || expectedRequest != spec.RequestID {
		return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentProof
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return PrepareChannelEnrollmentResult{}, fmt.Errorf("prepare Channel enrollment: begin: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readOwnedInviteAuthority(ctx, tx, spec.ChannelID)
	if err != nil {
		return PrepareChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if node.PeerID() != authority.channel.OwnerPeerID() {
		return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentOwner
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return PrepareChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	existing, existingErr := readChannelEnrollmentUse(ctx, tx, authority,
		spec.AuthenticatedPeerID)
	if existingErr == nil {
		if existing.grantID != spec.GrantID || existing.joinIdentity != identity ||
			existing.receipt.RequestID() != spec.RequestID {
			return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentProof
		}
		predecessor, err := enrollmentPredecessor(existing.member)
		if err != nil {
			return PrepareChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
		}
		status, ok := replayedEnrollmentStatus(authority.channel, authority.roster,
			spec.AuthenticatedPeerID)
		if !ok {
			return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
		}
		if err := tx.Commit(); err != nil {
			return PrepareChannelEnrollmentResult{}, fmt.Errorf("prepare Channel enrollment: commit replay: %w", err)
		}
		return PrepareChannelEnrollmentResult{Status: status, RosterHead: predecessor}, nil
	}
	if !errors.Is(existingErr, sql.ErrNoRows) {
		return PrepareChannelEnrollmentResult{}, mapChannelEnrollmentError(existingErr)
	}
	if current, ok := authority.roster.CurrentMember(spec.AuthenticatedPeerID); ok {
		if current.Status().Terminal() {
			return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentMemberRevoked
		}
		return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
	}
	grant, err := readDurableEnrollmentGrant(ctx, tx, spec.GrantID)
	if err != nil || grant.channelID != spec.ChannelID {
		return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentUnavailable
	}
	if err := freshEnrollmentAvailability(authority, grant, at); err != nil {
		return PrepareChannelEnrollmentResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return PrepareChannelEnrollmentResult{}, fmt.Errorf("prepare Channel enrollment: commit: %w", err)
	}
	return PrepareChannelEnrollmentResult{Status: ChannelEnrollmentAccepted,
		RosterHead: authority.channel.RosterHead()}, nil
}

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

// replayedEnrollmentStatus classifies a replayed enrollment against the
// committed roster: a terminal joining member reports its revocation, and a
// terminal owner or closed Channel reports closure. The boolean is false when
// either current member record is missing from the verified roster.
func replayedEnrollmentStatus(channel model.Channel, roster model.VerifiedRoster,
	peerID model.PeerID,
) (ChannelEnrollmentStatus, bool) {
	owner, ownerOK := roster.CurrentMember(channel.OwnerPeerID())
	current, ok := roster.CurrentMember(peerID)
	if !ownerOK || !ok {
		return "", false
	}
	if current.Status().Terminal() {
		return ChannelEnrollmentMemberRevoked, true
	}
	if owner.Status().Terminal() || channel.Status() == model.ChannelClosed {
		return ChannelEnrollmentChannelClosed, true
	}
	return ChannelEnrollmentReplayed, true
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

func mapChannelEnrollmentError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrChannelFull) || errors.Is(err, ErrChannelEnrollmentStale) ||
		errors.Is(err, ErrChannelEnrollmentProof) ||
		errors.Is(err, ErrChannelEnrollmentTokenExpired) ||
		errors.Is(err, ErrChannelEnrollmentTokenClosed) ||
		errors.Is(err, ErrChannelEnrollmentTokenExhausted) ||
		errors.Is(err, ErrChannelEnrollmentChannelClosed) ||
		errors.Is(err, ErrChannelEnrollmentMemberRevoked) {
		return err
	}
	message := err.Error()
	if strings.Contains(message, "channel_full") {
		return fmt.Errorf("%w: %v", ErrChannelFull, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		errors.Is(err, ErrChannelInviteConflict) || errors.Is(err, ErrChannelAuthorityInvariant) {
		return fmt.Errorf("%w: %v", ErrChannelEnrollmentConflict, err)
	}
	return fmt.Errorf("accept Channel enrollment: %w", err)
}

func mapChannelJoinError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "node_channel_limit") {
		return fmt.Errorf("%w: %v", ErrNodeChannelLimit, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		strings.Contains(message, "join reservation conflicts") ||
		strings.Contains(message, "conflicts with reserved join") ||
		strings.Contains(message, "join reservation attempt") ||
		strings.Contains(message, "join reservation state or time") ||
		errors.Is(err, ErrChannelAuthorityInvariant) || errors.Is(err, ErrChannelEnrollmentConflict) {
		return fmt.Errorf("%w: %v", ErrChannelJoinConflict, err)
	}
	return fmt.Errorf("install joined Channel: %w", err)
}

func freshEnrollmentAvailability(authority verifiedChannelAuthority, grant durableEnrollmentGrant,
	at time.Time,
) error {
	switch authority.channel.Status() {
	case model.ChannelClosed, model.ChannelLeaving, model.ChannelLeft, model.ChannelAbandoned:
		return ErrChannelEnrollmentChannelClosed
	case model.ChannelConflicted:
		return ErrChannelEnrollmentConflict
	case model.ChannelActive:
	default:
		return ErrChannelEnrollmentConflict
	}
	if at.Before(grant.createdAt) {
		return fmt.Errorf("%w: acceptance predates grant", ErrChannelEnrollmentInput)
	}
	switch grant.status {
	case "closed":
		return ErrChannelEnrollmentTokenClosed
	case "expired":
		return ErrChannelEnrollmentTokenExpired
	case "exhausted":
		return ErrChannelEnrollmentTokenExhausted
	case "open":
		if !at.Before(grant.expiresAt) {
			return ErrChannelEnrollmentTokenExpired
		}
	default:
		return ErrChannelEnrollmentConflict
	}
	if remainingChannelSeats(authority.roster, authority.channel.MemberLimit()) == 0 {
		return ErrChannelFull
	}
	return nil
}
