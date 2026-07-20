package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type channelEnrollmentInput struct {
	authenticatedPeer model.PeerID
	transcript        model.EnrollmentTranscript
	addresses         []string
	proof             model.Digest
	at                time.Time
	joinIdentity      model.Digest
	useID             model.EnrollmentUseID
	receiptID         model.EnrollmentReceiptID
}

func (input channelEnrollmentInput) valid() bool {
	return !input.authenticatedPeer.IsZero() && !input.transcript.IsZero() && len(input.addresses) != 0 &&
		!input.proof.IsZero() && !input.at.IsZero() && !input.joinIdentity.IsZero() &&
		!input.useID.IsZero() && !input.receiptID.IsZero()
}

type channelEnrollmentState struct {
	node      model.Node
	authority verifiedChannelAuthority
	grant     durableEnrollmentGrant
	use       durableChannelEnrollmentUse
	useExists bool
	evidence  channelEnrollmentEvidence
}

type channelEnrollmentCandidate struct {
	fresh        bool
	member       model.Member
	receipt      model.EnrollmentReceipt
	channel      model.Channel
	roster       model.VerifiedRoster
	useID        model.EnrollmentUseID
	joinIdentity model.Digest
	result       AcceptChannelEnrollmentResult
}

func freezeChannelEnrollmentInput(st *Store, ctx context.Context,
	spec PrepareChannelEnrollmentSigningSpec,
) (channelEnrollmentInput, error) {
	if st == nil || st.db == nil || ctx == nil || spec.AuthenticatedPeerID.IsZero() ||
		spec.Transcript.IsZero() || spec.Proof.IsZero() || len(spec.AdvertisedMultiaddrs) == 0 {
		return channelEnrollmentInput{}, ErrChannelEnrollmentInput
	}
	transcript, err := model.ParseEnrollmentTranscript(spec.Transcript.CanonicalJSON().Bytes())
	if err != nil || transcript.JoinerPeerID() != spec.AuthenticatedPeerID {
		return channelEnrollmentInput{}, ErrChannelEnrollmentInput
	}
	at, timeErr := canonicalStoreTime(spec.At)
	addresses := append([]string(nil), spec.AdvertisedMultiaddrs...)
	addressDigest, addressErr := model.AdvertisedAddressDigest(addresses)
	joinIdentity, identityErr := transcript.JoinIdentityDigest()
	expectedRequest, requestErr := model.EnrollmentRequestIDForJoinIdentity(joinIdentity)
	if timeErr != nil || addressErr != nil || identityErr != nil ||
		addressDigest != transcript.AdvertisedAddressDigest() {
		return channelEnrollmentInput{}, fmt.Errorf("%w: advertised address transcript mismatch",
			ErrChannelEnrollmentInput)
	}
	if requestErr != nil || expectedRequest != transcript.RequestID() {
		return channelEnrollmentInput{}, ErrChannelEnrollmentProof
	}
	useID, receiptID, err := model.EnrollmentEvidenceIDs(joinIdentity)
	if err != nil {
		return channelEnrollmentInput{}, mapChannelEnrollmentError(err)
	}
	return channelEnrollmentInput{authenticatedPeer: spec.AuthenticatedPeerID,
		transcript: transcript, addresses: addresses, proof: spec.Proof, at: at,
		joinIdentity: joinIdentity, useID: useID, receiptID: receiptID}, nil
}

func readChannelEnrollmentState(ctx context.Context, tx *sql.Tx,
	input channelEnrollmentInput,
) (channelEnrollmentState, error) {
	authority, node, err := readOwnedInviteAuthority(ctx, tx, input.transcript.ChannelID())
	if err != nil {
		return channelEnrollmentState{}, mapChannelEnrollmentError(err)
	}
	if node.PeerID() != authority.channel.OwnerPeerID() ||
		input.transcript.OwnerPeerID() != authority.channel.OwnerPeerID() {
		return channelEnrollmentState{}, ErrChannelEnrollmentOwner
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return channelEnrollmentState{}, mapChannelEnrollmentError(err)
	}
	use, useErr := readChannelEnrollmentUse(ctx, tx, authority, input.authenticatedPeer)
	useExists := useErr == nil
	if useErr != nil && !errors.Is(useErr, sql.ErrNoRows) {
		return channelEnrollmentState{}, mapChannelEnrollmentError(useErr)
	}
	if useExists && (use.grantID != input.transcript.GrantID() || use.useID != input.useID ||
		use.receipt.ReceiptID() != input.receiptID) {
		return channelEnrollmentState{}, ErrChannelEnrollmentProof
	}
	grant, err := readDurableEnrollmentGrant(ctx, tx, input.transcript.GrantID())
	if err != nil || grant.channelID != input.transcript.ChannelID() {
		return channelEnrollmentState{}, ErrChannelEnrollmentProof
	}
	if !useExists {
		if err := verifyUnusedChannelEnrollmentIDs(ctx, tx, input); err != nil {
			return channelEnrollmentState{}, err
		}
	}
	evidence, err := buildChannelEnrollmentEvidence(node, authority, grant, use, useExists)
	if err != nil {
		return channelEnrollmentState{}, err
	}
	return channelEnrollmentState{node: node, authority: authority, grant: grant,
		use: use, useExists: useExists, evidence: evidence}, nil
}

func prepareChannelEnrollmentSigningCandidate(input channelEnrollmentInput,
	state channelEnrollmentState,
) (ChannelEnrollmentSigningPlan, error) {
	if state.useExists {
		if model.VerifyEnrollmentProof(state.grant.verifier, input.transcript, input.proof) != nil {
			return ChannelEnrollmentSigningPlan{}, ErrChannelEnrollmentProof
		}
		result, err := replayChannelEnrollmentResult(input, state)
		return ChannelEnrollmentSigningPlan{replayResult: result}, err
	}
	if err := validateFreshChannelEnrollment(input, state); err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	if model.VerifyEnrollmentProof(state.grant.verifier, input.transcript, input.proof) != nil {
		return ChannelEnrollmentSigningPlan{}, ErrChannelEnrollmentProof
	}
	if input.transcript.RosterHead() != state.authority.channel.RosterHead() {
		return ChannelEnrollmentSigningPlan{}, ErrChannelEnrollmentStale
	}
	record, err := newChannelEnrollmentMemberRecord(input, state.authority)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	receiptRecord, err := newChannelEnrollmentReceiptRecord(input, record)
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, err
	}
	memberMessage, err := model.MemberRecordSigningMessage(record.ChannelID(), record.Digest())
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, mapChannelEnrollmentError(err)
	}
	receiptMessage, err := model.EnrollmentReceiptSigningMessage(record.ChannelID(), receiptRecord.Digest())
	if err != nil {
		return ChannelEnrollmentSigningPlan{}, mapChannelEnrollmentError(err)
	}
	return ChannelEnrollmentSigningPlan{memberRecord: record, receiptRecord: receiptRecord,
		memberMessage: string(memberMessage), receiptMessage: string(receiptMessage)}, nil
}

func validateFreshChannelEnrollment(input channelEnrollmentInput, state channelEnrollmentState) error {
	if current, ok := state.authority.roster.CurrentMember(input.authenticatedPeer); ok {
		if current.Status().Terminal() {
			return ErrChannelEnrollmentMemberRevoked
		}
		return ErrChannelEnrollmentConflict
	}
	if err := freshEnrollmentAvailability(state.authority, state.grant, input.at); err != nil {
		return err
	}
	if input.at.Before(state.authority.channel.UpdatedAt()) {
		return fmt.Errorf("%w: acceptance predates roster authority", ErrChannelEnrollmentInput)
	}
	if len(state.authority.roster.Members()) >= model.MaxMemberRecordsPerChannel {
		return ErrChannelFull
	}
	return nil
}

func newChannelEnrollmentMemberRecord(input channelEnrollmentInput,
	authority verifiedChannelAuthority,
) (model.MemberRecord, error) {
	previous := authority.channel.RosterHead().Digest()
	record, err := model.NewMemberRecord(model.MemberRecordSpec{
		ChannelID: authority.channel.ID(), DescriptorDigest: authority.channel.Descriptor().Descriptor().Digest(),
		Revision: authority.channel.RosterHead().Revision() + 1, PreviousDigest: &previous,
		PeerID: input.authenticatedPeer, OriginEpoch: input.transcript.JoinerOriginEpoch(),
		DisplayLabel: input.transcript.JoinerDisplayLabel(), PublicKey: input.transcript.JoinerPublicKey(),
		Multiaddrs: input.addresses, Protocols: model.RequiredMemberProtocols(),
		Limits: input.transcript.Limits(), Status: model.MemberActive, CreatedAt: input.at,
	})
	if err != nil {
		return model.MemberRecord{}, fmt.Errorf("%w: joining MemberRecord: %v",
			ErrChannelEnrollmentInput, err)
	}
	return record, nil
}

func newChannelEnrollmentReceiptRecord(input channelEnrollmentInput,
	record model.MemberRecord,
) (model.EnrollmentReceiptRecord, error) {
	receipt, err := model.NewEnrollmentReceiptRecord(model.EnrollmentReceiptRecordSpec{
		ReceiptID: input.receiptID, RequestID: input.transcript.RequestID(),
		GrantID: input.transcript.GrantID(), ChannelID: input.transcript.ChannelID(),
		MemberPeerID: record.PeerID(), JoinIdentityDigest: input.joinIdentity,
		MemberHead: record.Head(), AcceptedAt: input.at,
	})
	if err != nil {
		return model.EnrollmentReceiptRecord{}, mapChannelEnrollmentError(err)
	}
	return receipt, nil
}

func replayChannelEnrollmentResult(input channelEnrollmentInput,
	state channelEnrollmentState,
) (AcceptChannelEnrollmentResult, error) {
	predecessor, err := enrollmentPredecessor(state.use.member)
	if input.joinIdentity != state.use.joinIdentity ||
		state.use.receipt.RequestID() != input.transcript.RequestID() || err != nil ||
		predecessor != input.transcript.RosterHead() ||
		model.VerifyEnrollmentReceiptEvidence(state.authority.channel.Descriptor(), state.use.member,
			state.use.receipt) != nil {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentProof
	}
	status, err := channelEnrollmentReplayStatus(input.authenticatedPeer, state.authority)
	if err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	return AcceptChannelEnrollmentResult{Status: status, Channel: state.authority.channel,
		Roster: state.authority.roster, Member: state.use.member, Receipt: state.use.receipt}, nil
}

func channelEnrollmentReplayStatus(peerID model.PeerID,
	authority verifiedChannelAuthority,
) (ChannelEnrollmentStatus, error) {
	current, ok := authority.roster.CurrentMember(peerID)
	owner, ownerOK := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
	if !ok || !ownerOK {
		return "", ErrChannelEnrollmentConflict
	}
	if current.Status().Terminal() {
		return ChannelEnrollmentMemberRevoked, nil
	}
	if owner.Status().Terminal() || authority.channel.Status() == model.ChannelClosed {
		return ChannelEnrollmentChannelClosed, nil
	}
	return ChannelEnrollmentReplayed, nil
}

func buildSignedChannelEnrollmentCandidate(signing ChannelEnrollmentSigningPlan,
	signatures ChannelEnrollmentSignatures, state channelEnrollmentState,
) (channelEnrollmentCandidate, error) {
	if !signing.RequiresSignatures() {
		if len(signatures.MemberSignature) != 0 || len(signatures.ReceiptSignature) != 0 || !state.useExists {
			return channelEnrollmentCandidate{}, ErrChannelEnrollmentOwner
		}
		return channelEnrollmentCandidate{result: signing.replayResult}, nil
	}
	member, err := model.AttachMemberSignature(signing.memberRecord, signatures.MemberSignature)
	if err != nil {
		return channelEnrollmentCandidate{}, ErrChannelEnrollmentOwner
	}
	members := state.authority.roster.Members()
	members = append(members, member)
	roster, err := model.NewVerifiedRoster(state.authority.channel.Descriptor(), members)
	if err != nil {
		return channelEnrollmentCandidate{}, ErrChannelEnrollmentOwner
	}
	channel, err := channelEnrollmentCandidateChannel(state.authority.channel, roster, signing.input.at)
	if err != nil {
		return channelEnrollmentCandidate{}, err
	}
	receipt, err := model.AttachEnrollmentReceiptSignature(signing.receiptRecord,
		signatures.ReceiptSignature)
	if err != nil || model.VerifyEnrollmentReceipt(channel.Descriptor(), member,
		signing.input.transcript, receipt) != nil {
		return channelEnrollmentCandidate{}, ErrChannelEnrollmentOwner
	}
	return channelEnrollmentCandidate{fresh: true, member: member, receipt: receipt,
		channel: channel, roster: roster, useID: signing.input.useID,
		joinIdentity: signing.input.joinIdentity}, nil
}

func channelEnrollmentCandidateChannel(before model.Channel, roster model.VerifiedRoster,
	at time.Time,
) (model.Channel, error) {
	topic := before.TopicState()
	if topic == model.TopicJoined {
		topic = model.TopicJoining
	}
	channel, err := model.NewChannel(model.ChannelSpec{Descriptor: before.Descriptor(),
		LocalAlias: before.LocalAlias(), RosterHead: roster.Head(), Status: model.ChannelActive,
		TopicState: topic, UpdatedAt: at})
	if err != nil {
		return model.Channel{}, mapChannelEnrollmentError(err)
	}
	return channel, nil
}

func applyChannelEnrollmentCandidate(ctx context.Context, tx *sql.Tx, state channelEnrollmentState,
	candidate channelEnrollmentCandidate,
) (AcceptChannelEnrollmentResult, error) {
	if !candidate.fresh {
		return candidate.result, nil
	}
	if err := insertChannelMember(ctx, tx, candidate.member); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES(?,?,?,?,?,?,?,?)`, candidate.useID.String(), state.grant.id.String(),
		candidate.channel.ID().String(), candidate.member.PeerID().String(), candidate.joinIdentity.Bytes(),
		candidate.member.Head().Revision(), candidate.member.Head().Digest().Bytes(),
		storeTime(candidate.member.CreatedAt()))
	if err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if err := advanceChannelEnrollmentHead(ctx, tx, state.authority.channel, candidate.channel); err != nil {
		return AcceptChannelEnrollmentResult{}, err
	}
	if err := syncChannelRosterBindings(ctx, tx, state.node.PeerID(), candidate.channel,
		candidate.roster, state.authority.bindings, candidate.member.CreatedAt()); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	if err := insertEnrollmentReceipt(ctx, tx, candidate.receipt, candidate.useID.String()); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	return readCommittedChannelEnrollment(ctx, tx, state.node, candidate)
}

func advanceChannelEnrollmentHead(ctx context.Context, tx *sql.Tx, before, candidate model.Channel) error {
	updated, err := tx.ExecContext(ctx, `UPDATE channels SET roster_head_revision=?,roster_head_hash=?,
		updated_at=? WHERE channel_id=? AND roster_head_revision=? AND roster_head_hash=?`,
		candidate.RosterHead().Revision(), candidate.RosterHead().Digest().Bytes(), storeTime(candidate.UpdatedAt()),
		candidate.ID().String(), before.RosterHead().Revision(), before.RosterHead().Digest().Bytes())
	if err != nil {
		return mapChannelEnrollmentError(err)
	}
	if changed, changedErr := updated.RowsAffected(); changedErr != nil || changed != 1 {
		return ErrChannelEnrollmentStale
	}
	return nil
}

func readCommittedChannelEnrollment(ctx context.Context, tx *sql.Tx, node model.Node,
	candidate channelEnrollmentCandidate,
) (AcceptChannelEnrollmentResult, error) {
	committed, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), candidate.channel.ID())
	if err != nil || committed.channel.RosterHead() != candidate.roster.Head() {
		return AcceptChannelEnrollmentResult{}, ErrChannelEnrollmentConflict
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, committed); err != nil {
		return AcceptChannelEnrollmentResult{}, mapChannelEnrollmentError(err)
	}
	return AcceptChannelEnrollmentResult{Status: ChannelEnrollmentAccepted, Channel: committed.channel,
		Roster: committed.roster, Member: candidate.member, Receipt: candidate.receipt}, nil
}
