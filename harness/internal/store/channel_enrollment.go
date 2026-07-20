package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"sort"
	"strings"
	"time"
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
		status := ChannelEnrollmentReplayed
		owner, ownerOK := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
		if !ownerOK {
			return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
		}
		if current, ok := authority.roster.CurrentMember(spec.AuthenticatedPeerID); !ok {
			return PrepareChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
		} else if current.Status().Terminal() {
			status = ChannelEnrollmentMemberRevoked
		} else if owner.Status().Terminal() || authority.channel.Status() == model.ChannelClosed {
			status = ChannelEnrollmentChannelClosed
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

type AcceptChannelEnrollmentSpec struct {
	AuthenticatedPeerID  model.PeerID
	Transcript           model.EnrollmentTranscript
	AdvertisedMultiaddrs []string
	Proof                model.Digest
	Signer               ChannelAuthoritySigner
	At                   time.Time
}

type AcceptChannelEnrollmentResult struct {
	Status  ChannelEnrollmentStatus
	Channel model.Channel
	Roster  model.VerifiedRoster
	Member  model.Member
	Receipt model.EnrollmentReceipt
}

// AcceptChannelEnrollment is the owner's single durable acceptance boundary.
// Member, grant use/counter, roster head and signed receipt either all commit
// or all roll back. Replay is checked before mutable lifecycle policies.
func (s *Store) AcceptChannelEnrollment(ctx context.Context,
	spec AcceptChannelEnrollmentSpec,
) (AcceptChannelEnrollmentResult, error) {
	transcript := spec.Transcript
	if s == nil || s.db == nil || ctx == nil || spec.Signer == nil || transcript.IsZero() ||
		spec.AuthenticatedPeerID.IsZero() || spec.Proof.IsZero() ||
		transcript.JoinerPeerID() != spec.AuthenticatedPeerID {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentInput
	}
	at, err := canonicalStoreTime(spec.At)
	addressDigest, addressErr := model.AdvertisedAddressDigest(spec.AdvertisedMultiaddrs)
	joinIdentity, identityErr := transcript.JoinIdentityDigest()
	expectedRequest, requestErr := model.EnrollmentRequestIDForJoinIdentity(joinIdentity)
	if err != nil || addressErr != nil || identityErr != nil ||
		addressDigest != transcript.AdvertisedAddressDigest() {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("%w: advertised address transcript mismatch",
			ErrChannelEnrollmentInput)
	}
	if requestErr != nil || expectedRequest != transcript.RequestID() {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("accept Channel enrollment: begin: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readOwnedInviteAuthority(ctx, tx, transcript.ChannelID())
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if node.PeerID() != authority.channel.OwnerPeerID() ||
		transcript.OwnerPeerID() != authority.channel.OwnerPeerID() {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentOwner
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}

	existing, existingErr := readChannelEnrollmentUse(ctx, tx, authority,
		spec.AuthenticatedPeerID)
	var grant durableEnrollmentGrant
	if existingErr == nil {
		if existing.grantID != transcript.GrantID() {
			return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
		}
		grant, err = readDurableEnrollmentGrant(ctx, tx, existing.grantID)
	} else if errors.Is(existingErr, sql.ErrNoRows) {
		grant, err = readDurableEnrollmentGrant(ctx, tx, transcript.GrantID())
	} else {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(existingErr)
	}
	if err != nil || grant.channelID != transcript.ChannelID() {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
	}
	if existingErr == nil {
		if model.VerifyEnrollmentProof(grant.verifier, transcript, spec.Proof) != nil {
			return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
		}
		predecessor, predecessorErr := enrollmentPredecessor(existing.member)
		if joinIdentity != existing.joinIdentity ||
			existing.receipt.RequestID() != transcript.RequestID() ||
			predecessorErr != nil || predecessor != transcript.RosterHead() ||
			model.VerifyEnrollmentReceiptEvidence(authority.channel.Descriptor(), existing.member,
				existing.receipt) != nil {
			return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
		}
		status := ChannelEnrollmentReplayed
		current, ok := authority.roster.CurrentMember(spec.AuthenticatedPeerID)
		if !ok {
			return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
		}
		owner, ownerOK := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
		if !ownerOK {
			return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
		}
		if current.Status().Terminal() {
			status = ChannelEnrollmentMemberRevoked
		} else if owner.Status().Terminal() || authority.channel.Status() == model.ChannelClosed {
			status = ChannelEnrollmentChannelClosed
		}
		if err := tx.Commit(); err != nil {
			return AcceptChannelEnrollmentResult{}, fmt.Errorf("accept Channel enrollment: commit replay: %w", err)
		}
		return AcceptChannelEnrollmentResult{Status: status, Channel: authority.channel,
			Roster: authority.roster, Member: existing.member, Receipt: existing.receipt}, nil
	}

	if current, ok := authority.roster.CurrentMember(spec.AuthenticatedPeerID); ok {
		if current.Status().Terminal() {
			return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentMemberRevoked
		}
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
	}
	if err := freshEnrollmentAvailability(authority, grant, at); err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if at.Before(authority.channel.UpdatedAt()) {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("%w: acceptance predates roster authority",
			ErrChannelEnrollmentInput)
	}
	if model.VerifyEnrollmentProof(grant.verifier, transcript, spec.Proof) != nil {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
	}
	if transcript.RosterHead() != authority.channel.RosterHead() {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentStale
	}

	previous := authority.channel.RosterHead().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID: authority.channel.ID(), DescriptorDigest: authority.channel.Descriptor().Descriptor().Digest(),
		Revision: authority.channel.RosterHead().Revision() + 1, PreviousDigest: &previous,
		PeerID: spec.AuthenticatedPeerID, OriginEpoch: transcript.JoinerOriginEpoch(),
		DisplayLabel: transcript.JoinerDisplayLabel(), PublicKey: transcript.JoinerPublicKey(),
		Multiaddrs: spec.AdvertisedMultiaddrs, Protocols: model.RequiredMemberProtocols(),
		Limits: transcript.Limits(), Status: model.MemberActive, CreatedAt: at,
	})
	if err != nil {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("%w: joining MemberRecord: %v",
			ErrChannelEnrollmentInput, err)
	}
	message, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	signature, err := spec.Signer.Sign(ctx, message)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("accept Channel enrollment: sign member: %w", err)
	}
	member, err := model.AttachMemberSignature(record, signature)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentOwner
	}
	candidateMembers := authority.roster.Members()
	if len(candidateMembers) >= model.MaxMemberRecordsPerChannel {
		return AcceptChannelEnrollmentResult{}, ErrChannelFull
	}
	candidateMembers = append(candidateMembers, member)
	roster, err := model.NewVerifiedRoster(authority.channel.Descriptor(), candidateMembers)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentOwner
	}
	topic := authority.channel.TopicState()
	if topic == model.TopicJoined {
		topic = model.TopicJoining
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: authority.channel.Descriptor(),
		LocalAlias: authority.channel.LocalAlias(), RosterHead: roster.Head(), Status: model.ChannelActive,
		TopicState: topic, UpdatedAt: at})
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	useID, receiptID, err := model.EnrollmentEvidenceIDs(joinIdentity)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	receiptRecord, err := model.NewEnrollmentReceiptRecord(model.EnrollmentReceiptRecordSpec{
		ReceiptID: receiptID, RequestID: transcript.RequestID(), GrantID: transcript.GrantID(),
		ChannelID: transcript.ChannelID(), MemberPeerID: member.PeerID(),
		JoinIdentityDigest: joinIdentity, MemberHead: member.Head(), AcceptedAt: at,
	})
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	receiptMessage, err := model.EnrollmentReceiptSigningMessage(channel.ID(), receiptRecord.Digest())
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	receiptSignature, err := spec.Signer.Sign(ctx, receiptMessage)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("accept Channel enrollment: sign receipt: %w", err)
	}
	receipt, err := model.AttachEnrollmentReceiptSignature(receiptRecord, receiptSignature)
	if err != nil || model.VerifyEnrollmentReceipt(channel.Descriptor(), member, transcript, receipt) != nil {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentOwner
	}

	if err := insertChannelMember(ctx, tx, member); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES(?,?,?,?,?,?,?,?)`, useID.String(), grant.id.String(), channel.ID().String(),
		member.PeerID().String(), joinIdentity.Bytes(), member.Head().Revision(),
		member.Head().Digest().Bytes(), storeTime(at))
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
		updated_at=? WHERE channel_id=? AND roster_head_revision=? AND roster_head_hash=?`,
		channel.RosterHead().Revision(), channel.RosterHead().Digest().Bytes(), storeTime(at),
		channel.ID().String(), authority.channel.RosterHead().Revision(),
		authority.channel.RosterHead().Digest().Bytes())
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentStale
	}
	if err := syncChannelRosterBindings(ctx, tx, node.PeerID(), channel, roster,
		authority.bindings, at); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if err := insertEnrollmentReceipt(ctx, tx, receipt, useID.String()); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	committed, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channel.ID())
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if committed.channel.RosterHead() != roster.Head() {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, committed); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if err := tx.Commit(); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	return AcceptChannelEnrollmentResult{Status: ChannelEnrollmentAccepted, Channel: committed.channel,
		Roster: committed.roster, Member: member, Receipt: receipt}, nil
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

func replayJoinedChannel(ctx context.Context, tx *sql.Tx, node model.Node,
	spec InstallJoinedChannelSpec, incoming model.VerifiedRoster,
) (InstallJoinedChannelResult, error) {
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(),
		incoming.Descriptor().Descriptor().ID())
	if err != nil || authority.channel.LocalAlias() != spec.LocalAlias ||
		!bytes.Equal(authority.channel.Descriptor().WireJSON().Bytes(), spec.Descriptor.WireJSON().Bytes()) {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	storedMembers := authority.roster.Members()
	incomingMembers := incoming.Members()
	common := len(storedMembers)
	if len(incomingMembers) < common {
		common = len(incomingMembers)
	}
	// Either side may be the longer verified prefix: a delayed response is a
	// read, while a newer response is an atomic suffix apply.
	for index := 0; index < common; index++ {
		if !bytes.Equal(storedMembers[index].WireJSON().Bytes(), incomingMembers[index].WireJSON().Bytes()) {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
	}
	storedReceipt, ownerUse, err := readEnrollmentReceipt(ctx, tx, spec.Receipt.ReceiptID())
	if err != nil || ownerUse.Valid ||
		!bytes.Equal(storedReceipt.WireJSON().Bytes(), spec.Receipt.WireJSON().Bytes()) {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	if len(incoming.Members()) > len(authority.roster.Members()) {
		if spec.At.Before(authority.channel.UpdatedAt()) || authority.channel.Status() == model.ChannelConflicted ||
			authority.channel.Status() == model.ChannelAbandoned {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
		for _, member := range incoming.Members()[len(authority.roster.Members()):] {
			if err := insertChannelMember(ctx, tx, member); err != nil {
				return InstallJoinedChannelResult{}, mapChannelJoinError(err)
			}
		}
		currentLocal, ok := incoming.CurrentMember(node.PeerID())
		if !ok {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
		currentOwner, ownerOK := incoming.CurrentMember(authority.channel.OwnerPeerID())
		if !ownerOK {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
		status, topic := authority.channel.Status(), authority.channel.TopicState()
		if currentOwner.Status() == model.MemberLeft {
			status, topic = model.ChannelClosed, model.TopicLeft
		} else if currentOwner.Status().Terminal() {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		} else if currentLocal.Status().Terminal() {
			status, topic = model.ChannelLeft, model.TopicLeft
		} else if status != model.ChannelActive {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
		channel, err := model.NewChannel(model.ChannelSpec{Descriptor: authority.channel.Descriptor(),
			LocalAlias: authority.channel.LocalAlias(), RosterHead: incoming.Head(), Status: status,
			TopicState: topic, UpdatedAt: spec.At})
		if err != nil {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
		updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
			status=?,topic_state=?,updated_at=? WHERE channel_id=? AND roster_head_revision=?
			AND roster_head_hash=?`, channel.RosterHead().Revision(), channel.RosterHead().Digest().Bytes(),
			string(channel.Status()), string(channel.TopicState()), storeTime(channel.UpdatedAt()),
			channel.ID().String(), authority.channel.RosterHead().Revision(),
			authority.channel.RosterHead().Digest().Bytes())
		if err != nil {
			return InstallJoinedChannelResult{}, mapChannelJoinError(err)
		}
		if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
		if err := syncChannelRosterBindings(ctx, tx, node.PeerID(), channel, incoming,
			authority.bindings, spec.At); err != nil {
			return InstallJoinedChannelResult{}, mapChannelJoinError(err)
		}
		authority, err = readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channel.ID())
		if err != nil || authority.channel.RosterHead() != incoming.Head() {
			return InstallJoinedChannelResult{}, ErrChannelJoinConflict
		}
	}
	status := ChannelEnrollmentReplayed
	owner, ownerOK := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
	if !ownerOK {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	}
	if current, ok := authority.roster.CurrentMember(node.PeerID()); !ok {
		return InstallJoinedChannelResult{}, ErrChannelJoinConflict
	} else if current.Status().Terminal() {
		status = ChannelEnrollmentMemberRevoked
	} else if owner.Status().Terminal() || authority.channel.Status() == model.ChannelClosed {
		status = ChannelEnrollmentChannelClosed
	}
	return InstallJoinedChannelResult{Status: status, Channel: authority.channel,
		Roster: authority.roster}, nil
}

func syncChannelRosterBindings(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	channel model.Channel, roster model.VerifiedRoster, existing []model.PeerBinding, at time.Time,
) error {
	aliases, active, err := deriveEffectiveAliases(localPeer, roster)
	if err != nil {
		return err
	}
	existingByPeer := make(map[model.PeerID]model.PeerBinding, len(existing))
	occupied := make(map[string]model.PeerID, len(existing)+len(active))
	for _, binding := range existing {
		existingByPeer[binding.PeerID()] = binding
		if current, ok := roster.CurrentMember(binding.PeerID()); ok && current.Status().Terminal() {
			occupied[binding.EffectiveAlias()] = binding.PeerID()
		}
	}
	activePeers := make([]model.PeerID, 0, len(active))
	for peerID := range active {
		activePeers = append(activePeers, peerID)
	}
	sort.Slice(activePeers, func(left, right int) bool {
		return activePeers[left].String() < activePeers[right].String()
	})
	for _, peerID := range activePeers {
		alias := aliases[peerID]
		if owner, collision := occupied[alias]; collision && owner != peerID {
			alias, err = uniqueEffectiveAlias(sanitizeEffectiveAliasLabel(active[peerID].DisplayLabel()),
				peerID, occupied)
			if err != nil {
				return err
			}
		}
		aliases[peerID] = alias
		occupied[alias] = peerID
	}

	// Stage aliases that will change before assigning final values. This makes
	// duplicate-label transitions independent of SQL update order.
	staged := make(map[model.PeerID]string)
	unavailableTemporaryAliases := make(map[string]struct{}, len(existing)*2)
	for _, binding := range existing {
		unavailableTemporaryAliases[binding.EffectiveAlias()] = struct{}{}
	}
	for _, alias := range aliases {
		unavailableTemporaryAliases[alias] = struct{}{}
	}
	for alias := range occupied {
		unavailableTemporaryAliases[alias] = struct{}{}
	}
	for _, binding := range existing {
		current, ok := roster.CurrentMember(binding.PeerID())
		if !ok {
			return ErrChannelJoinConflict
		}
		desired := binding.EffectiveAlias()
		if current.Status() == model.MemberActive {
			desired = aliases[binding.PeerID()]
		}
		if desired == binding.EffectiveAlias() {
			continue
		}
		temporary := temporaryBindingAlias(channel.ID(), binding.PeerID(), unavailableTemporaryAliases)
		unavailableTemporaryAliases[temporary] = struct{}{}
		if _, err := tx.ExecContext(ctx, `UPDATE peer_bindings SET effective_alias=?
			WHERE channel_id=? AND peer_id=?`, temporary, channel.ID().String(),
			binding.PeerID().String()); err != nil {
			return fmt.Errorf("stage PeerBinding alias: %w", err)
		}
		staged[binding.PeerID()] = desired
	}

	currentMembers := make(map[model.PeerID]model.Member)
	for _, member := range roster.Members() {
		currentMembers[member.PeerID()] = member
	}
	peers := make([]model.PeerID, 0, len(currentMembers))
	for peerID := range currentMembers {
		if peerID != localPeer {
			peers = append(peers, peerID)
		}
	}
	sort.Slice(peers, func(left, right int) bool { return peers[left].String() < peers[right].String() })
	for _, peerID := range peers {
		member := currentMembers[peerID]
		prior, exists := existingByPeer[peerID]
		if !exists {
			if member.Status() == model.MemberActive {
				if err := insertPendingPeerBinding(ctx, tx, localPeer, channel, roster, peerID,
					aliases[peerID], at); err != nil {
					return err
				}
			}
			continue
		}
		alias := prior.EffectiveAlias()
		if desired, changed := staged[peerID]; changed {
			alias = desired
		} else if member.Status() == model.MemberActive {
			alias = aliases[peerID]
		}
		state := prior.State()
		if member.Status().Terminal() {
			state = model.BindingRevoked
		}
		binding, err := model.NewPeerBinding(localPeer, model.PeerBindingSpec{Channel: channel,
			Roster: roster, PeerID: peerID, EffectiveAlias: alias, State: state,
			Reachability: prior.Reachability(), JoinedAt: prior.JoinedAt(),
			LastSeenAt: bindingLastSeen(prior)})
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
		_, err = tx.ExecContext(ctx, `UPDATE peer_bindings SET effective_alias=?,public_key=?,
			multiaddrs_json=?,protocols_json=?,limits_json=?,member_revision=?,member_record_hash=?,
			state=? WHERE channel_id=? AND peer_id=?`, binding.EffectiveAlias(), binding.PublicKey(),
			multiaddrs.Bytes(), protocols.Bytes(), binding.Limits().Bytes(), binding.MemberHead().Revision(),
			binding.MemberHead().Digest().Bytes(), string(binding.State()), channel.ID().String(),
			peerID.String())
		if err != nil {
			return fmt.Errorf("update joined PeerBinding: %w", err)
		}
	}
	return nil
}

func deriveEffectiveAliases(localPeer model.PeerID,
	roster model.VerifiedRoster,
) (map[model.PeerID]string, map[model.PeerID]model.Member, error) {
	current := make(map[model.PeerID]model.Member)
	for _, member := range roster.Members() {
		current[member.PeerID()] = member
	}
	active := make(map[model.PeerID]model.Member)
	labelCounts := make(map[string]int)
	labels := make(map[model.PeerID]string)
	for peerID, member := range current {
		if peerID == localPeer || member.Status() != model.MemberActive {
			continue
		}
		active[peerID] = member
		label := sanitizeEffectiveAliasLabel(member.DisplayLabel())
		labels[peerID] = label
		labelCounts[label]++
	}
	widths := make(map[model.PeerID]int, len(active))
	aliases := make(map[model.PeerID]string, len(active))
	for peerID := range active {
		if labelCounts[labels[peerID]] > 1 || labels[peerID] == "auto" || labels[peerID] == "team" {
			widths[peerID] = 8
		}
	}
	for attempts := 0; attempts <= model.MaxMembersPerChannel; attempts++ {
		byAlias := make(map[string][]model.PeerID, len(active))
		for peerID := range active {
			alias := labels[peerID]
			if width := widths[peerID]; width != 0 {
				peerText := peerID.String()
				if width > len(peerText) {
					width = len(peerText)
				}
				alias += "~" + peerText[len(peerText)-width:]
			}
			aliases[peerID] = alias
			byAlias[alias] = append(byAlias[alias], peerID)
		}
		unique := true
		for _, peers := range byAlias {
			if len(peers) == 1 {
				continue
			}
			unique = false
			for _, peerID := range peers {
				width := widths[peerID]
				if width == 0 {
					width = 8
				} else {
					width += 4
				}
				widths[peerID] = width
			}
		}
		if unique {
			for peerID, alias := range aliases {
				if strings.TrimSpace(alias) != alias || alias == "" || len(alias) > model.MaxIdentifierBytes {
					return nil, nil, ErrChannelJoinConflict
				}
				_ = peerID
			}
			return aliases, active, nil
		}
	}
	return nil, nil, ErrChannelJoinConflict
}
