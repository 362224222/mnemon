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
	ErrChannelInviteInput       = errors.New("invalid Channel invite input")
	ErrChannelInviteOwner       = errors.New("local Node is not the Channel owner")
	ErrChannelInviteUnavailable = errors.New("Channel invite is unavailable")
	ErrChannelInviteConflict    = errors.New("Channel invite conflicts with durable state")
	ErrChannelFull              = errors.New("Channel member capacity reached")
)

type RotateChannelInviteSpec struct {
	ChannelID model.ChannelID
	Token     model.EnrollmentToken
	At        time.Time
}

type RotateChannelInviteResult struct {
	Created        bool
	GrantID        model.GrantID
	ReplacedGrant  model.GrantID
	RemainingSeats uint8
	Status         string
}

type CloseChannelInviteResult struct {
	Changed bool
	GrantID model.GrantID
	Status  string
}

// RotateChannelInvite atomically retires the current open grant and installs
// one bearer-secret-free replacement derived from an owner-signed token. A
// replay of the replacement's immutable grant identity is a read: it never
// closes a newer invite that may have appeared after the original response was
// lost.
func (s *Store) RotateChannelInvite(ctx context.Context,
	spec RotateChannelInviteSpec,
) (RotateChannelInviteResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.ChannelID.IsZero() || spec.Token.IsZero() {
		return RotateChannelInviteResult{}, ErrChannelInviteInput
	}
	at, err := canonicalStoreTime(spec.At)
	grant, grantErr := model.NewOpenEnrollmentGrantForToken(spec.Token, spec.At)
	if err != nil || grantErr != nil || grant.CreatedAt() != at || grant.ChannelID() != spec.ChannelID {
		return RotateChannelInviteResult{}, fmt.Errorf("%w: invalid creation time or signed token",
			ErrChannelInviteInput)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RotateChannelInviteResult{}, fmt.Errorf("rotate Channel invite: begin: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readOwnedInviteAuthority(ctx, tx, spec.ChannelID)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	if node.PeerID() != authority.channel.OwnerPeerID() {
		return RotateChannelInviteResult{}, ErrChannelInviteOwner
	}
	if !bytes.Equal(spec.Token.Payload().Descriptor().WireJSON().Bytes(),
		authority.channel.Descriptor().WireJSON().Bytes()) {
		return RotateChannelInviteResult{}, ErrChannelInviteInput
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return RotateChannelInviteResult{}, err
	}
	// Replay precedes mutable Channel/grant policy. Once this exact grant was
	// committed, a later membership fill, close, expiry or rotation must not
	// reinterpret the original transaction as a failure or retire its successor.
	existing, existingErr := readDurableEnrollmentGrant(ctx, tx, grant.ID())
	if existingErr == nil {
		if !existing.matches(grant) {
			return RotateChannelInviteResult{}, ErrChannelInviteConflict
		}
		remaining := remainingChannelSeats(authority.roster, authority.channel.MemberLimit())
		if err := tx.Commit(); err != nil {
			return RotateChannelInviteResult{}, fmt.Errorf("rotate Channel invite: commit replay read: %w", err)
		}
		return RotateChannelInviteResult{GrantID: grant.ID(), RemainingSeats: remaining,
			Status: existing.status}, nil
	}
	if !errors.Is(existingErr, sql.ErrNoRows) {
		return RotateChannelInviteResult{}, existingErr
	}
	if authority.channel.Status() != model.ChannelActive {
		return RotateChannelInviteResult{}, ErrChannelInviteUnavailable
	}
	owner, ownerExists := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
	ownerAddresses, ownerAddressErr := model.AdvertisedAddressDigest(owner.Multiaddrs())
	tokenAddresses, tokenAddressErr := model.AdvertisedAddressDigest(spec.Token.Payload().OwnerMultiaddrs())
	if !ownerExists || owner.Status() != model.MemberActive || ownerAddressErr != nil ||
		tokenAddressErr != nil || ownerAddresses != tokenAddresses {
		return RotateChannelInviteResult{}, fmt.Errorf("%w: token owner addresses differ from current roster",
			ErrChannelInviteInput)
	}
	if at.Before(authority.channel.UpdatedAt()) {
		return RotateChannelInviteResult{}, fmt.Errorf("%w: invite predates Channel authority", ErrChannelInviteInput)
	}
	open, openErr := readOpenEnrollmentGrant(ctx, tx, spec.ChannelID)
	if openErr != nil && !errors.Is(openErr, sql.ErrNoRows) {
		return RotateChannelInviteResult{}, openErr
	}
	lifecycleAt, err := latestEnrollmentGrantLifecycle(ctx, tx, spec.ChannelID)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	if !lifecycleAt.IsZero() && (at.Before(lifecycleAt) ||
		(errors.Is(openErr, sql.ErrNoRows) && at.Equal(lifecycleAt))) {
		return RotateChannelInviteResult{}, fmt.Errorf("%w: invite predates durable grant lifecycle",
			ErrChannelInviteInput)
	}
	remaining := remainingChannelSeats(authority.roster, authority.channel.MemberLimit())
	if remaining == 0 {
		return RotateChannelInviteResult{}, ErrChannelFull
	}
	if grant.MaxUses() > remaining {
		return RotateChannelInviteResult{}, fmt.Errorf("%w: grant uses %d exceed %d remaining seats",
			ErrChannelInviteInput, grant.MaxUses(), remaining)
	}

	var replaced model.GrantID
	if openErr == nil {
		if !at.After(open.createdAt) {
			return RotateChannelInviteResult{}, fmt.Errorf("%w: rotation does not advance current grant",
				ErrChannelInviteInput)
		}
		replaced = open.id
		status, closedAt := "closed", at
		if !at.Before(open.expiresAt) {
			status = "expired"
		}
		if _, err := tx.ExecContext(ctx, `UPDATE enrollment_grants SET status=?,closed_at=?
			WHERE grant_id=? AND status='open'`, status, storeTime(closedAt), open.id.String()); err != nil {
			return RotateChannelInviteResult{}, fmt.Errorf("rotate Channel invite: retire current grant: %w", err)
		}
	} else if !errors.Is(openErr, sql.ErrNoRows) {
		return RotateChannelInviteResult{}, openErr
	}

	_, err = tx.ExecContext(ctx, `INSERT INTO enrollment_grants(grant_id,channel_id,verifier,expires_at,
		max_uses,used_uses,status,created_at,closed_at) VALUES(?,?,?,?,?,0,'open',?,NULL)`,
		grant.ID().String(), spec.ChannelID.String(), grant.Verifier().Bytes(),
		storeTime(grant.ExpiresAt()), grant.MaxUses(), storeTime(grant.CreatedAt()))
	if err != nil {
		return RotateChannelInviteResult{}, mapChannelInviteError(err)
	}
	if err := tx.Commit(); err != nil {
		return RotateChannelInviteResult{}, mapChannelInviteError(err)
	}
	return RotateChannelInviteResult{Created: true, GrantID: grant.ID(), ReplacedGrant: replaced,
		RemainingSeats: remaining, Status: "open"}, nil
}

// CloseChannelInvite retires one exact grant. Replaying a close after a later
// rotation observes the historical terminal grant and cannot close the new
// current invite.
func (s *Store) CloseChannelInvite(ctx context.Context, channelID model.ChannelID,
	grantID model.GrantID, at time.Time,
) (CloseChannelInviteResult, error) {
	if s == nil || s.db == nil || ctx == nil || channelID.IsZero() || grantID.IsZero() {
		return CloseChannelInviteResult{}, ErrChannelInviteInput
	}
	closedAt, err := canonicalStoreTime(at)
	if err != nil {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close time: %v", ErrChannelInviteInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CloseChannelInviteResult{}, fmt.Errorf("close Channel invite: begin: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readOwnedInviteAuthority(ctx, tx, channelID)
	if err != nil {
		return CloseChannelInviteResult{}, err
	}
	if node.PeerID() != authority.channel.OwnerPeerID() {
		return CloseChannelInviteResult{}, ErrChannelInviteOwner
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return CloseChannelInviteResult{}, err
	}
	grant, err := readDurableEnrollmentGrant(ctx, tx, grantID)
	if err != nil || grant.channelID != channelID {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: grant is not owned by Channel: %v",
			ErrChannelInviteConflict, err)
	}
	if grant.status != "open" {
		if err := tx.Commit(); err != nil {
			return CloseChannelInviteResult{}, fmt.Errorf("close Channel invite: commit replay read: %w", err)
		}
		return CloseChannelInviteResult{GrantID: grantID, Status: grant.status}, nil
	}
	if authority.channel.Status() != model.ChannelActive {
		return CloseChannelInviteResult{}, ErrChannelInviteUnavailable
	}
	if closedAt.Before(grant.createdAt) {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close predates grant", ErrChannelInviteInput)
	}
	if closedAt.Before(authority.channel.UpdatedAt()) {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close predates Channel authority",
			ErrChannelInviteInput)
	}
	lastUseAt, err := latestEnrollmentGrantUse(ctx, tx, grantID)
	if err != nil {
		return CloseChannelInviteResult{}, err
	}
	if !lastUseAt.IsZero() && closedAt.Before(lastUseAt) {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close predates durable grant use",
			ErrChannelInviteInput)
	}
	status, terminalAt := "closed", closedAt
	if !closedAt.Before(grant.expiresAt) {
		status = "expired"
	}
	result, err := tx.ExecContext(ctx, `UPDATE enrollment_grants SET status=?,closed_at=?
		WHERE grant_id=? AND status='open'`, status, storeTime(terminalAt), grantID.String())
	if err != nil {
		return CloseChannelInviteResult{}, mapChannelInviteError(err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: exact open grant was not retired: %v",
			ErrChannelInviteConflict, err)
	}
	if err := tx.Commit(); err != nil {
		return CloseChannelInviteResult{}, mapChannelInviteError(err)
	}
	return CloseChannelInviteResult{Changed: true, GrantID: grantID, Status: status}, nil
}

type durableEnrollmentGrant struct {
	id        model.GrantID
	channelID model.ChannelID
	verifier  model.EnrollmentVerifier
	expiresAt time.Time
	maxUses   uint8
	usedUses  uint8
	status    string
	createdAt time.Time
	closedAt  sql.NullString
	useCount  uint8
}

func readOwnedInviteAuthority(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (verifiedChannelAuthority, model.Node, error) {
	node, err := readNode(ctx, tx)
	if err != nil {
		return verifiedChannelAuthority{}, model.Node{}, fmt.Errorf("rotate Channel invite: read Node: %w", err)
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return verifiedChannelAuthority{}, model.Node{}, fmt.Errorf("rotate Channel invite: authority: %w", err)
	}
	return authority, node, nil
}

func readDurableEnrollmentGrant(ctx context.Context, tx *sql.Tx,
	grantID model.GrantID,
) (durableEnrollmentGrant, error) {
	var grant durableEnrollmentGrant
	var idText, channelText, expiresText, status, createdText string
	var verifier []byte
	err := tx.QueryRowContext(ctx, `SELECT grant_id,channel_id,verifier,expires_at,max_uses,used_uses,
		status,created_at,closed_at FROM enrollment_grants WHERE grant_id=?`, grantID.String()).Scan(
		&idText, &channelText, &verifier, &expiresText, &grant.maxUses, &grant.usedUses, &status,
		&createdText, &grant.closedAt)
	if err != nil {
		return durableEnrollmentGrant{}, err
	}
	grant.id, err = model.ParseGrantID(idText)
	if err != nil || grant.id != grantID {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable grant ID: %v",
			ErrChannelInviteConflict, err)
	}
	grant.channelID, err = model.ParseChannelID(channelText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable grant Channel: %v",
			ErrChannelInviteConflict, err)
	}
	grant.verifier, err = model.EnrollmentVerifierFromStoredBytes(verifier)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable verifier: %v",
			ErrChannelInviteConflict, err)
	}
	grant.expiresAt, err = parseCanonicalStoreTime(expiresText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable expiry: %v",
			ErrChannelInviteConflict, err)
	}
	grant.createdAt, err = parseCanonicalStoreTime(createdText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable creation time: %v",
			ErrChannelInviteConflict, err)
	}
	grant.status = status
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrollment_grant_uses WHERE grant_id=?`,
		grantID.String()).Scan(&grant.useCount); err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: inspect grant use ledger: %v",
			ErrChannelInviteConflict, err)
	}
	var lastUseText sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT MAX(used_at) FROM enrollment_grant_uses WHERE grant_id=?`,
		grantID.String()).Scan(&lastUseText); err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: inspect latest grant use: %v",
			ErrChannelInviteConflict, err)
	}
	if grant.useCount != grant.usedUses || !validEnrollmentGrantState(grant.status, grant.usedUses,
		grant.maxUses, grant.closedAt, grant.createdAt, grant.expiresAt, lastUseText) {
		return durableEnrollmentGrant{}, ErrChannelInviteConflict
	}
	return grant, nil
}

func readOpenEnrollmentGrant(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (durableEnrollmentGrant, error) {
	var idText string
	if err := tx.QueryRowContext(ctx, `SELECT grant_id FROM enrollment_grants
		WHERE channel_id=? AND status='open'`, channelID.String()).Scan(&idText); err != nil {
		return durableEnrollmentGrant{}, err
	}
	id, err := model.ParseGrantID(idText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: open grant ID: %v", ErrChannelInviteConflict, err)
	}
	return readDurableEnrollmentGrant(ctx, tx, id)
}

func latestEnrollmentGrantLifecycle(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (time.Time, error) {
	var latest sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT MAX(lifecycle_at) FROM (
		SELECT created_at AS lifecycle_at FROM enrollment_grants WHERE channel_id=?
		UNION ALL
		SELECT closed_at AS lifecycle_at FROM enrollment_grants WHERE channel_id=? AND closed_at IS NOT NULL
		UNION ALL
		SELECT uses.used_at AS lifecycle_at FROM enrollment_grant_uses uses
		JOIN enrollment_grants grants ON grants.grant_id=uses.grant_id WHERE grants.channel_id=?
	)`, channelID.String(), channelID.String(), channelID.String()).Scan(&latest)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: inspect grant lifecycle: %v", ErrChannelInviteConflict, err)
	}
	if !latest.Valid {
		return time.Time{}, nil
	}
	parsed, err := parseCanonicalStoreTime(latest.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid grant lifecycle time: %v", ErrChannelInviteConflict, err)
	}
	return parsed, nil
}

func latestEnrollmentGrantUse(ctx context.Context, tx *sql.Tx,
	grantID model.GrantID,
) (time.Time, error) {
	var latest sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT MAX(used_at) FROM enrollment_grant_uses WHERE grant_id=?`,
		grantID.String()).Scan(&latest); err != nil {
		return time.Time{}, fmt.Errorf("%w: inspect grant uses: %v", ErrChannelInviteConflict, err)
	}
	if !latest.Valid {
		return time.Time{}, nil
	}
	parsed, err := parseCanonicalStoreTime(latest.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid grant use time: %v", ErrChannelInviteConflict, err)
	}
	return parsed, nil
}

// verifyOwnedChannelEnrollmentLedger closes the schema's intentionally
// deferred use->receipt direction. Every first non-owner membership must be
// explained by one immutable grant use and one valid owner-signed receipt;
// projections, stable identity, time, counters and terminal causes are all
// reconstructed before invite authority can mutate.
func verifyOwnedChannelEnrollmentLedger(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority,
) error {
	if authority.channel.ID().IsZero() || authority.roster.IsZero() {
		return fmt.Errorf("%w: incomplete Channel enrollment authority", ErrChannelInviteConflict)
	}
	rows, err := tx.QueryContext(ctx, `SELECT grant_id FROM enrollment_grants
		WHERE channel_id=? ORDER BY created_at,grant_id`, authority.channel.ID().String())
	if err != nil {
		return fmt.Errorf("%w: read Channel grants: %v", ErrChannelInviteConflict, err)
	}
	var grantIDs []model.GrantID
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			_ = rows.Close()
			return fmt.Errorf("%w: scan Channel grant: %v", ErrChannelInviteConflict, err)
		}
		id, err := model.ParseGrantID(text)
		if err != nil {
			_ = rows.Close()
			return fmt.Errorf("%w: parse Channel grant: %v", ErrChannelInviteConflict, err)
		}
		grantIDs = append(grantIDs, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("%w: iterate Channel grants: %v", ErrChannelInviteConflict, err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("%w: close Channel grants: %v", ErrChannelInviteConflict, err)
	}
	grants := make(map[model.GrantID]durableEnrollmentGrant, len(grantIDs))
	var lifecycle time.Time
	for _, id := range grantIDs {
		grant, err := readDurableEnrollmentGrant(ctx, tx, id)
		if err != nil || grant.channelID != authority.channel.ID() ||
			(!lifecycle.IsZero() && grant.createdAt.Before(lifecycle)) {
			return fmt.Errorf("%w: invalid Channel grant %s: %v",
				ErrChannelInviteConflict, id.String(), err)
		}
		grants[id] = grant
		if grant.createdAt.After(lifecycle) {
			lifecycle = grant.createdAt
		}
		if grant.closedAt.Valid {
			closedAt, err := parseCanonicalStoreTime(grant.closedAt.String)
			if err != nil {
				return fmt.Errorf("%w: invalid grant terminal time: %v", ErrChannelInviteConflict, err)
			}
			if closedAt.After(lifecycle) {
				lifecycle = closedAt
			}
		}
	}

	members := authority.roster.Members()
	firstMembers := make(map[model.PeerID]model.Member, authority.channel.MemberLimit())
	for _, member := range members {
		if _, exists := firstMembers[member.PeerID()]; !exists {
			firstMembers[member.PeerID()] = member
		}
	}
	usesByPeer := make(map[model.PeerID]model.RecordHead, len(firstMembers))
	useRows, err := tx.QueryContext(ctx, `SELECT uses.use_id,uses.grant_id,uses.member_peer_id,
		uses.join_identity_digest,uses.member_revision,uses.member_record_hash,uses.used_at,
		receipts.receipt_id,receipts.channel_id,receipts.member_peer_id,
		receipts.roster_head_revision,receipts.roster_head_hash,receipts.receipt_json,
		receipts.owner_signature,receipts.created_at
		FROM enrollment_grant_uses uses
		LEFT JOIN enrollment_receipts receipts ON receipts.owner_use_id=uses.use_id
		WHERE uses.channel_id=? ORDER BY uses.used_at,uses.use_id`, authority.channel.ID().String())
	if err != nil {
		return fmt.Errorf("%w: read enrollment uses: %v", ErrChannelInviteConflict, err)
	}
	for useRows.Next() {
		var useID, grantText, peerText, usedText string
		var identityRaw, memberHash []byte
		var memberRevision uint64
		var receiptID, receiptChannel, receiptPeer, receiptCreated sql.NullString
		var receiptRevision sql.NullInt64
		var receiptHash, receiptRaw, receiptSignature []byte
		if err := useRows.Scan(&useID, &grantText, &peerText, &identityRaw, &memberRevision,
			&memberHash, &usedText, &receiptID, &receiptChannel, &receiptPeer, &receiptRevision,
			&receiptHash, &receiptRaw, &receiptSignature, &receiptCreated); err != nil {
			_ = useRows.Close()
			return fmt.Errorf("%w: scan enrollment use: %v", ErrChannelInviteConflict, err)
		}
		grantID, err := model.ParseGrantID(grantText)
		grant, grantExists := grants[grantID]
		peerID, peerErr := model.ParsePeerID(peerText)
		identity, identityErr := model.DigestFromBytes(identityRaw)
		memberDigest, digestErr := model.DigestFromBytes(memberHash)
		memberHead, headErr := model.NewRecordHead(memberRevision, memberDigest)
		usedAt, usedErr := parseCanonicalStoreTime(usedText)
		if err != nil || !grantExists || peerErr != nil || identityErr != nil || digestErr != nil ||
			headErr != nil || usedErr != nil || memberRevision > uint64(len(members)) || useID == "" {
			_ = useRows.Close()
			return fmt.Errorf("%w: invalid enrollment use projection", ErrChannelInviteConflict)
		}
		member := members[memberRevision-1]
		first, firstExists := firstMembers[peerID]
		if !firstExists || first.Head() != memberHead || member.Head() != memberHead || member.PeerID() != peerID ||
			member.Status() != model.MemberActive || member.CreatedAt().Before(grant.createdAt) ||
			usedAt.Before(member.CreatedAt()) ||
			usedAt.Before(grant.createdAt) || !usedAt.Before(grant.expiresAt) || identity.IsZero() {
			_ = useRows.Close()
			return fmt.Errorf("%w: grant use does not identify the first active MemberRecord",
				ErrChannelInviteConflict)
		}
		if _, duplicate := usesByPeer[peerID]; duplicate {
			_ = useRows.Close()
			return fmt.Errorf("%w: duplicate Channel enrollment use", ErrChannelInviteConflict)
		}
		if !receiptID.Valid || !receiptChannel.Valid || !receiptPeer.Valid || !receiptRevision.Valid ||
			!receiptCreated.Valid || len(receiptRaw) == 0 || len(receiptSignature) == 0 {
			_ = useRows.Close()
			return fmt.Errorf("%w: enrollment use lacks its owner receipt", ErrChannelInviteConflict)
		}
		record, err := model.ParseEnrollmentReceiptRecord(receiptRaw)
		if err != nil {
			_ = useRows.Close()
			return fmt.Errorf("%w: parse enrollment receipt: %v", ErrChannelInviteConflict, err)
		}
		receipt, err := model.AttachEnrollmentReceiptSignature(record, receiptSignature)
		createdAt, createdErr := parseCanonicalStoreTime(receiptCreated.String)
		if err != nil || createdErr != nil ||
			model.VerifyEnrollmentReceiptEvidence(authority.channel.Descriptor(), member, receipt) != nil ||
			receiptID.String != receipt.ReceiptID().String() || receiptChannel.String != authority.channel.ID().String() ||
			receiptPeer.String != peerID.String() || receiptRevision.Int64 != int64(memberRevision) ||
			!bytes.Equal(receiptHash, memberHash) || createdAt != receipt.AcceptedAt() || createdAt.Before(usedAt) ||
			receipt.GrantID() != grantID || receipt.JoinIdentityDigest() != identity {
			_ = useRows.Close()
			return fmt.Errorf("%w: enrollment receipt projection or signature mismatch",
				ErrChannelInviteConflict)
		}
		usesByPeer[peerID] = memberHead
	}
	if err := useRows.Err(); err != nil {
		_ = useRows.Close()
		return fmt.Errorf("%w: iterate enrollment uses: %v", ErrChannelInviteConflict, err)
	}
	if err := useRows.Close(); err != nil {
		return fmt.Errorf("%w: close enrollment uses: %v", ErrChannelInviteConflict, err)
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
	var replicaReceipts int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrollment_receipts
		WHERE channel_id=? AND owner_use_id IS NULL`, authority.channel.ID().String()).Scan(&replicaReceipts); err != nil ||
		replicaReceipts != 0 {
		return fmt.Errorf("%w: owner Channel contains joiner-only receipt rows: %v",
			ErrChannelInviteConflict, err)
	}
	return nil
}

func (grant durableEnrollmentGrant) matches(expected model.OpenEnrollmentGrant) bool {
	return grant.id == expected.ID() && grant.channelID == expected.ChannelID() &&
		grant.verifier == expected.Verifier() && grant.expiresAt == expected.ExpiresAt() &&
		grant.maxUses == expected.MaxUses() && grant.createdAt == expected.CreatedAt()
}

func remainingChannelSeats(roster model.VerifiedRoster, limit uint8) uint8 {
	current := make(map[model.PeerID]model.MemberStatus, limit)
	for _, member := range roster.Members() {
		current[member.PeerID()] = member.Status()
	}
	active := 0
	for _, status := range current {
		if status == model.MemberActive {
			active++
		}
	}
	if active >= int(limit) {
		return 0
	}
	return limit - uint8(active)
}

func mapChannelInviteError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "channel_full") {
		return fmt.Errorf("%w: %v", ErrChannelFull, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		strings.Contains(message, "enrollment grant") {
		return fmt.Errorf("%w: %v", ErrChannelInviteConflict, err)
	}
	return fmt.Errorf("rotate Channel invite: %w", err)
}
