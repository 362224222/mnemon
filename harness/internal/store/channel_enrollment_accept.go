package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

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

// AcceptChannelEnrollment signs only after its read snapshot is released, then
// revalidates the exact authority before the indivisible durable acceptance.
// Member, grant use/counter, roster head and receipt still commit or roll back
// together, with exact replay checked before mutable lifecycle policies.
func (s *Store) AcceptChannelEnrollment(ctx context.Context,
	spec AcceptChannelEnrollmentSpec,
) (AcceptChannelEnrollmentResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentInput
	}
	at, joinIdentity, err := validateAcceptEnrollmentInput(spec)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("accept Channel enrollment: begin: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readAcceptEnrollmentAuthority(ctx, tx, spec.Transcript)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	grant, existing, replay, err := readAcceptEnrollmentGrant(ctx, tx, authority, spec)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if replay {
		return replayAcceptedEnrollment(ctx, tx, authority, existing, spec, grant, joinIdentity)
	}
	if err := authorizeFreshEnrollment(authority, grant, spec, at); err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AcceptChannelEnrollmentResult{},
			fmt.Errorf("accept Channel enrollment: commit preparation: %w", err)
	}
	member, err := signJoiningMember(ctx, spec, authority, at)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	roster, channel, err := projectEnrolledChannel(authority, member, at)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	useID, receipt, err := signAcceptedEnrollmentReceipt(ctx, spec, channel, member, joinIdentity, at)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	evidence := acceptedEnrollment{channel: channel, roster: roster, member: member,
		useID: useID, receipt: receipt, joinIdentity: joinIdentity, at: at,
		authority: authority, node: node, grant: grant}
	return s.commitAcceptedEnrollment(ctx, spec, evidence)
}

// validateAcceptEnrollmentInput checks the acceptance request before any
// durable read: completeness, canonical time, advertised address binding and
// the join-identity derivation of the request ID.
func validateAcceptEnrollmentInput(spec AcceptChannelEnrollmentSpec,
) (time.Time, model.Digest, error) {
	transcript := spec.Transcript
	if spec.Signer == nil || transcript.IsZero() || spec.AuthenticatedPeerID.IsZero() ||
		spec.Proof.IsZero() || transcript.JoinerPeerID() != spec.AuthenticatedPeerID {
		return time.Time{}, model.Digest{}, ErrChannelEnrollmentInput
	}
	at, err := canonicalStoreTime(spec.At)
	addressDigest, addressErr := model.AdvertisedAddressDigest(spec.AdvertisedMultiaddrs)
	joinIdentity, identityErr := transcript.JoinIdentityDigest()
	expectedRequest, requestErr := model.EnrollmentRequestIDForJoinIdentity(joinIdentity)
	if err != nil || addressErr != nil || identityErr != nil ||
		addressDigest != transcript.AdvertisedAddressDigest() {
		return time.Time{}, model.Digest{}, fmt.Errorf("%w: advertised address transcript mismatch",
			ErrChannelEnrollmentInput)
	}
	if requestErr != nil || expectedRequest != transcript.RequestID() {
		return time.Time{}, model.Digest{}, ErrChannelEnrollmentProof
	}
	return at, joinIdentity, nil
}

// readAcceptEnrollmentAuthority loads the owner's verified Channel authority
// and proves both the local Node and the transcript name that owner.
func readAcceptEnrollmentAuthority(ctx context.Context, tx *sql.Tx,
	transcript model.EnrollmentTranscript,
) (verifiedChannelAuthority, model.Node, error) {
	authority, node, err := readOwnedInviteAuthority(ctx, tx, transcript.ChannelID())
	if err != nil {
		return verifiedChannelAuthority{}, model.Node{}, mapChannelEnrollmentError(err)
	}
	if node.PeerID() != authority.channel.OwnerPeerID() ||
		transcript.OwnerPeerID() != authority.channel.OwnerPeerID() {
		return verifiedChannelAuthority{}, model.Node{}, ErrChannelEnrollmentOwner
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return verifiedChannelAuthority{}, model.Node{}, mapChannelEnrollmentError(err)
	}
	return authority, node, nil
}

// readAcceptEnrollmentGrant resolves the grant that authorizes this
// acceptance. A committed grant use pins the original grant; otherwise the
// transcript's grant is loaded fresh. The boolean reports the replay case.
func readAcceptEnrollmentGrant(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, spec AcceptChannelEnrollmentSpec,
) (durableEnrollmentGrant, durableChannelEnrollmentUse, bool, error) {
	transcript := spec.Transcript
	existing, existingErr := readChannelEnrollmentUse(ctx, tx, authority,
		spec.AuthenticatedPeerID)
	var grant durableEnrollmentGrant
	var err error
	if existingErr == nil {
		if existing.grantID != transcript.GrantID() {
			return durableEnrollmentGrant{}, durableChannelEnrollmentUse{}, false,
				ErrChannelEnrollmentProof
		}
		grant, err = readDurableEnrollmentGrant(ctx, tx, existing.grantID)
	} else if errors.Is(existingErr, sql.ErrNoRows) {
		grant, err = readDurableEnrollmentGrant(ctx, tx, transcript.GrantID())
	} else {
		return durableEnrollmentGrant{}, durableChannelEnrollmentUse{}, false,
			mapChannelEnrollmentError(existingErr)
	}
	if err != nil || grant.channelID != transcript.ChannelID() {
		return durableEnrollmentGrant{}, durableChannelEnrollmentUse{}, false,
			ErrChannelEnrollmentProof
	}
	return grant, existing, existingErr == nil, nil
}

// replayAcceptedEnrollment re-verifies the committed grant use against the
// replayed transcript and returns the original signed receipt together with
// the latest committed roster.
func replayAcceptedEnrollment(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, existing durableChannelEnrollmentUse,
	spec AcceptChannelEnrollmentSpec, grant durableEnrollmentGrant, joinIdentity model.Digest,
) (AcceptChannelEnrollmentResult, error) {
	transcript := spec.Transcript
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
	status, ok := replayedEnrollmentStatus(authority.channel, authority.roster,
		spec.AuthenticatedPeerID)
	if !ok {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
	}
	if err := tx.Commit(); err != nil {
		return AcceptChannelEnrollmentResult{}, fmt.Errorf("accept Channel enrollment: commit replay: %w", err)
	}
	return AcceptChannelEnrollmentResult{Status: status, Channel: authority.channel,
		Roster: authority.roster, Member: existing.member, Receipt: existing.receipt}, nil
}

// authorizeFreshEnrollment applies the owner's mutable admission policies in
// their committed precedence: membership conflicts, grant availability,
// authority freshness, transcript proof and challenge staleness.
func authorizeFreshEnrollment(authority verifiedChannelAuthority, grant durableEnrollmentGrant,
	spec AcceptChannelEnrollmentSpec, at time.Time,
) error {
	if current, ok := authority.roster.CurrentMember(spec.AuthenticatedPeerID); ok {
		if current.Status().Terminal() {
			return ErrChannelEnrollmentMemberRevoked
		}
		return ErrChannelEnrollmentConflict
	}
	if err := freshEnrollmentAvailability(authority, grant, at); err != nil {
		return err
	}
	if at.Before(authority.channel.UpdatedAt()) {
		return fmt.Errorf("%w: acceptance predates roster authority", ErrChannelEnrollmentInput)
	}
	if model.VerifyEnrollmentProof(grant.verifier, spec.Transcript, spec.Proof) != nil {
		return ErrChannelEnrollmentProof
	}
	if spec.Transcript.RosterHead() != authority.channel.RosterHead() {
		return ErrChannelEnrollmentStale
	}
	return nil
}

// signJoiningMember appends the joiner as the next MemberRecord after the
// committed roster head and binds it with the owner's signature.
func signJoiningMember(ctx context.Context, spec AcceptChannelEnrollmentSpec,
	authority verifiedChannelAuthority, at time.Time,
) (model.Member, error) {
	transcript := spec.Transcript
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
		return model.Member{}, fmt.Errorf("%w: joining MemberRecord: %v",
			ErrChannelEnrollmentInput, err)
	}
	message, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		return model.Member{}, mapChannelEnrollmentError(err)
	}
	signature, err := spec.Signer.Sign(ctx, message)
	if err != nil {
		return model.Member{}, fmt.Errorf("accept Channel enrollment: sign member: %w", err)
	}
	member, err := model.AttachMemberSignature(record, signature)
	if err != nil {
		return model.Member{}, ErrChannelEnrollmentOwner
	}
	return member, nil
}

// projectEnrolledChannel extends the verified roster with the signed joiner
// and rolls the Channel head forward under the owner's topic policy.
func projectEnrolledChannel(authority verifiedChannelAuthority, member model.Member,
	at time.Time,
) (model.VerifiedRoster, model.Channel, error) {
	candidateMembers := authority.roster.Members()
	if len(candidateMembers) >= model.MaxMemberRecordsPerChannel {
		return model.VerifiedRoster{}, model.Channel{}, ErrChannelFull
	}
	candidateMembers = append(candidateMembers, member)
	roster, err := model.NewVerifiedRoster(authority.channel.Descriptor(), candidateMembers)
	if err != nil {
		return model.VerifiedRoster{}, model.Channel{}, ErrChannelEnrollmentOwner
	}
	topic := authority.channel.TopicState()
	if topic == model.TopicJoined {
		topic = model.TopicJoining
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: authority.channel.Descriptor(),
		LocalAlias: authority.channel.LocalAlias(), RosterHead: roster.Head(), Status: model.ChannelActive,
		TopicState: topic, UpdatedAt: at})
	if err != nil {
		return model.VerifiedRoster{}, model.Channel{}, mapChannelEnrollmentError(err)
	}
	return roster, channel, nil
}

// signAcceptedEnrollmentReceipt derives the deterministic evidence IDs, signs
// the receipt with the owner authority and verifies the finished receipt
// against the joining member before anything durable is written.
func signAcceptedEnrollmentReceipt(ctx context.Context, spec AcceptChannelEnrollmentSpec,
	channel model.Channel, member model.Member, joinIdentity model.Digest, at time.Time,
) (model.EnrollmentUseID, model.EnrollmentReceipt, error) {
	transcript := spec.Transcript
	useID, receiptID, err := model.EnrollmentEvidenceIDs(joinIdentity)
	if err != nil {
		return model.EnrollmentUseID{}, model.EnrollmentReceipt{}, mapChannelEnrollmentError(err)
	}
	receiptRecord, err := model.NewEnrollmentReceiptRecord(model.EnrollmentReceiptRecordSpec{
		ReceiptID: receiptID, RequestID: transcript.RequestID(), GrantID: transcript.GrantID(),
		ChannelID: transcript.ChannelID(), MemberPeerID: member.PeerID(),
		JoinIdentityDigest: joinIdentity, MemberHead: member.Head(), AcceptedAt: at,
	})
	if err != nil {
		return model.EnrollmentUseID{}, model.EnrollmentReceipt{}, mapChannelEnrollmentError(err)
	}
	receiptMessage, err := model.EnrollmentReceiptSigningMessage(channel.ID(), receiptRecord.Digest())
	if err != nil {
		return model.EnrollmentUseID{}, model.EnrollmentReceipt{}, mapChannelEnrollmentError(err)
	}
	receiptSignature, err := spec.Signer.Sign(ctx, receiptMessage)
	if err != nil {
		return model.EnrollmentUseID{}, model.EnrollmentReceipt{},
			fmt.Errorf("accept Channel enrollment: sign receipt: %w", err)
	}
	receipt, err := model.AttachEnrollmentReceiptSignature(receiptRecord, receiptSignature)
	if err != nil || model.VerifyEnrollmentReceipt(channel.Descriptor(), member, transcript, receipt) != nil {
		return model.EnrollmentUseID{}, model.EnrollmentReceipt{}, ErrChannelEnrollmentOwner
	}
	return useID, receipt, nil
}

// persistAcceptedEnrollment writes the member, grant use, roster head,
// bindings and receipt inside the owner's final acceptance transaction.
func persistAcceptedEnrollment(ctx context.Context, tx *sql.Tx, node model.Node,
	authority verifiedChannelAuthority, grant durableEnrollmentGrant, evidence acceptedEnrollment,
) error {
	if err := insertChannelMember(ctx, tx, evidence.member); err != nil {
		return mapChannelEnrollmentError(err)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES(?,?,?,?,?,?,?,?)`, evidence.useID.String(), grant.id.String(), evidence.channel.ID().String(),
		evidence.member.PeerID().String(), evidence.joinIdentity.Bytes(), evidence.member.Head().Revision(),
		evidence.member.Head().Digest().Bytes(), storeTime(evidence.at))
	if err != nil {
		return mapChannelEnrollmentError(err)
	}
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
		topic_state=?,updated_at=? WHERE channel_id=? AND roster_head_revision=? AND roster_head_hash=?
		AND status=? AND topic_state=?`,
		evidence.channel.RosterHead().Revision(), evidence.channel.RosterHead().Digest().Bytes(),
		string(evidence.channel.TopicState()), storeTime(evidence.at), evidence.channel.ID().String(),
		authority.channel.RosterHead().Revision(),
		authority.channel.RosterHead().Digest().Bytes(), string(authority.channel.Status()),
		string(authority.channel.TopicState()))
	if err != nil {
		return mapChannelEnrollmentError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrChannelEnrollmentStale
	}
	if err := syncChannelRosterBindings(ctx, tx, node.PeerID(), evidence.channel, evidence.roster,
		authority.bindings, evidence.at); err != nil {
		return mapChannelEnrollmentError(err)
	}
	if err := insertEnrollmentReceipt(ctx, tx, evidence.receipt, evidence.useID.String()); err != nil {
		return mapChannelEnrollmentError(err)
	}
	return nil
}

// verifyCommittedEnrollment re-reads the committed authority and proves the
// roster head and enrollment ledger advanced exactly as signed.
func verifyCommittedEnrollment(ctx context.Context, tx *sql.Tx, node model.Node,
	channelID model.ChannelID, expectedHead model.RecordHead,
) (verifiedChannelAuthority, error) {
	committed, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil {
		return verifiedChannelAuthority{}, mapChannelEnrollmentError(err)
	}
	if committed.channel.RosterHead() != expectedHead {
		return verifiedChannelAuthority{}, ErrChannelEnrollmentConflict
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, committed); err != nil {
		return verifiedChannelAuthority{}, mapChannelEnrollmentError(err)
	}
	return committed, nil
}
