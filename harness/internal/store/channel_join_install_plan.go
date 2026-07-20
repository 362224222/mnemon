package store

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// JoinedChannelInstallPlan is an opaque, Store-bound replica authority plan.
type JoinedChannelInstallPlan struct {
	channelAuthorityPlan
	candidate   joinedChannelInstallCandidate
	reservation joinedChannelReservationFence
	before      joinedChannelReplicaSnapshot
	result      InstallJoinedChannelResult
}

type joinedChannelInstallCandidate struct {
	authenticatedOwner model.PeerID
	ownerOutcome       ChannelEnrollmentStatus
	localAlias         string
	descriptor         model.SignedChannelDescriptor
	transcript         model.EnrollmentTranscript
	receipt            model.EnrollmentReceipt
	roster             model.VerifiedRoster
	at                 time.Time
}

type joinedChannelReplicaSnapshot struct {
	channel model.Channel
	roster  model.VerifiedRoster
}

type joinedChannelReservationFence struct {
	// attempt is deliberately excluded: retries keep one stable request/join
	// identity, and a verified accepted transcript consumes the whole
	// reservation rather than one transport attempt.
	requestID        model.EnrollmentRequestID
	channelID        model.ChannelID
	grantID          model.GrantID
	joinIdentity     model.Digest
	descriptorDigest model.Digest
	localPeerID      model.PeerID
	originEpoch      model.OriginEpoch
	localAlias       string
}

func (plan JoinedChannelInstallPlan) Result() InstallJoinedChannelResult { return plan.result }

func (candidate joinedChannelInstallCandidate) installSpec() InstallJoinedChannelSpec {
	return InstallJoinedChannelSpec{AuthenticatedOwnerPeerID: candidate.authenticatedOwner,
		OwnerOutcome: candidate.ownerOutcome, LocalAlias: candidate.localAlias, Descriptor: candidate.descriptor,
		Transcript: candidate.transcript, Receipt: candidate.receipt,
		Members: candidate.roster.Members(), At: candidate.at}
}

func (fence joinedChannelReservationFence) isZero() bool {
	return fence.requestID.IsZero() || fence.channelID.IsZero() || fence.grantID.IsZero() ||
		fence.joinIdentity.IsZero() || fence.descriptorDigest.IsZero() || fence.localPeerID.IsZero() ||
		fence.originEpoch.IsZero() || fence.localAlias == ""
}

// PrepareJoinedChannelInstall freezes verified remote evidence and its exact
// commit_unknown reservation in a rollback-only transaction.
func (s *Store) PrepareJoinedChannelInstall(ctx context.Context,
	spec InstallJoinedChannelSpec,
) (JoinedChannelInstallPlan, error) {
	candidate, err := validateJoinedChannelInstallSpec(s, ctx, spec)
	if err != nil {
		return JoinedChannelInstallPlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return JoinedChannelInstallPlan{}, fmt.Errorf("prepare joined Channel install: begin: %w", err)
	}
	defer tx.Rollback()
	beforeMesh, err := readChannelAuthorityPlanMesh(ctx, tx)
	if err != nil {
		return JoinedChannelInstallPlan{}, err
	}
	node, err := readNode(ctx, tx)
	if err != nil || node.PeerID() != candidate.transcript.JoinerPeerID() ||
		node.OriginEpoch() != candidate.transcript.JoinerOriginEpoch() {
		return JoinedChannelInstallPlan{}, ErrChannelJoinInput
	}
	reservation, before, err := readJoinedChannelInstallPreimage(ctx, tx, node, candidate)
	if err != nil {
		return JoinedChannelInstallPlan{}, err
	}
	result, err := applyJoinedChannelInstall(ctx, tx, node, candidate, reservation)
	if err != nil {
		return JoinedChannelInstallPlan{}, err
	}
	core, err := finishChannelAuthorityPlan(s, ctx, tx, beforeMesh)
	if err != nil {
		return JoinedChannelInstallPlan{}, err
	}
	result.Installed = false
	return JoinedChannelInstallPlan{channelAuthorityPlan: core, candidate: candidate,
		reservation: reservation, before: before, result: result}, nil
}

// CommitJoinedChannelInstall applies only the frozen preimage and requires an
// exact replica or terminal-only durable outcome before returning success.
func (s *Store) CommitJoinedChannelInstall(ctx context.Context,
	plan JoinedChannelInstallPlan,
) (InstallJoinedChannelResult, error) {
	if !plan.validFor(s) || plan.candidate.roster.IsZero() {
		return InstallJoinedChannelResult{}, ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, false)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	defer tx.Rollback()
	committed, err := joinedChannelInstallEvidence(ctx, tx, plan)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if committed && joinedChannelPlanMatchesRuntime(resolution, plan) {
		return plan.result, nil
	}
	preimage, preimageErr := joinedChannelInstallPreimage(ctx, tx, plan)
	if preimageErr != nil {
		return InstallJoinedChannelResult{}, preimageErr
	}
	if resolution != ChannelAuthorityPlanUnchanged || !preimage {
		return InstallJoinedChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return InstallJoinedChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	result, err := applyJoinedChannelInstall(ctx, tx, node, plan.candidate, plan.reservation)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	after, err := inspectChannelAuthorityPlan(ctx, tx, plan.channelAuthorityPlan)
	if err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if after != ChannelAuthorityPlanCandidate &&
		!(after == ChannelAuthorityPlanUnchanged && !plan.ChangesAuthority()) {
		return InstallJoinedChannelResult{}, ErrChannelAuthorityPlanDiverged
	}
	if err := verifyJoinedChannelInstallPostimage(ctx, tx, plan); err != nil {
		return InstallJoinedChannelResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return InstallJoinedChannelResult{}, mapChannelJoinError(err)
	}
	return result, nil
}

func verifyJoinedChannelInstallPostimage(ctx context.Context, tx *sql.Tx,
	plan JoinedChannelInstallPlan,
) error {
	if plan.result.Channel.ID().IsZero() {
		terminal, err := joinedChannelTerminalOutcomeUnproven(ctx, tx, plan)
		if err != nil {
			return err
		}
		if !terminal {
			return ErrChannelAuthorityPlanDiverged
		}
		return nil
	}
	committed, err := joinedChannelInstallEvidence(ctx, tx, plan)
	if err != nil {
		return err
	}
	if !committed {
		return ErrChannelAuthorityPlanDiverged
	}
	return nil
}

// ResolveJoinedChannelInstall uses exact durable evidence to classify an
// unknown commit without authorizing a stale runtime snapshot.
func (s *Store) ResolveJoinedChannelInstall(ctx context.Context,
	plan JoinedChannelInstallPlan,
) (ChannelAuthorityPlanResolution, error) {
	if !plan.validFor(s) || plan.candidate.roster.IsZero() {
		return "", ErrChannelAuthorityPlan
	}
	tx, resolution, err := s.beginChannelAuthorityPlan(ctx, plan.channelAuthorityPlan, true)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	committed, err := joinedChannelInstallEvidence(ctx, tx, plan)
	if err != nil {
		return "", err
	}
	if committed && joinedChannelPlanMatchesRuntime(resolution, plan) {
		return ChannelAuthorityPlanCandidate, nil
	}
	preimage, err := joinedChannelInstallPreimage(ctx, tx, plan)
	if err != nil {
		return "", err
	}
	if resolution == ChannelAuthorityPlanUnchanged && preimage {
		return ChannelAuthorityPlanUnchanged, nil
	}
	if resolution == ChannelAuthorityPlanUnchanged && plan.result.Channel.ID().IsZero() {
		unproven, evidenceErr := joinedChannelTerminalOutcomeUnproven(ctx, tx, plan)
		if evidenceErr != nil {
			return "", evidenceErr
		}
		if unproven {
			return ChannelAuthorityPlanUnchanged, nil
		}
	}
	return ChannelAuthorityPlanDiverged, nil
}

func joinedChannelPlanMatchesRuntime(resolution ChannelAuthorityPlanResolution,
	plan JoinedChannelInstallPlan,
) bool {
	return resolution == ChannelAuthorityPlanCandidate ||
		(!plan.ChangesAuthority() && resolution == ChannelAuthorityPlanUnchanged)
}

func validateJoinedChannelInstallSpec(s *Store, ctx context.Context,
	spec InstallJoinedChannelSpec,
) (joinedChannelInstallCandidate, error) {
	if s == nil || s.db == nil || ctx == nil || spec.AuthenticatedOwnerPeerID.IsZero() ||
		spec.Descriptor.IsZero() || spec.Transcript.IsZero() || spec.Receipt.IsZero() || len(spec.Members) == 0 {
		return joinedChannelInstallCandidate{}, ErrChannelJoinInput
	}
	candidate, err := freezeJoinedChannelInstallSpec(spec)
	if err != nil || !validJoinedChannelInstallAuthority(candidate) {
		return joinedChannelInstallCandidate{}, ErrChannelJoinInput
	}
	return candidate, nil
}

func freezeJoinedChannelInstallSpec(spec InstallJoinedChannelSpec) (joinedChannelInstallCandidate, error) {
	descriptor, err := model.ParseSignedChannelDescriptor(spec.Descriptor.WireJSON().Bytes())
	if err != nil {
		return joinedChannelInstallCandidate{}, err
	}
	transcript, err := model.ParseEnrollmentTranscript(spec.Transcript.CanonicalJSON().Bytes())
	if err != nil {
		return joinedChannelInstallCandidate{}, err
	}
	receipt, err := model.ParseEnrollmentReceipt(spec.Receipt.WireJSON().Bytes())
	if err != nil {
		return joinedChannelInstallCandidate{}, err
	}
	members := make([]model.Member, len(spec.Members))
	for index, member := range spec.Members {
		members[index], err = model.ParseMember(member.WireJSON().Bytes())
		if err != nil {
			return joinedChannelInstallCandidate{}, err
		}
	}
	roster, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil {
		return joinedChannelInstallCandidate{}, err
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil {
		return joinedChannelInstallCandidate{}, err
	}
	return joinedChannelInstallCandidate{authenticatedOwner: spec.AuthenticatedOwnerPeerID,
		ownerOutcome: spec.OwnerOutcome, localAlias: spec.LocalAlias, descriptor: descriptor, transcript: transcript,
		receipt: receipt, roster: roster, at: at}, nil
}

func validJoinedChannelInstallAuthority(candidate joinedChannelInstallCandidate) bool {
	descriptor := candidate.descriptor.Descriptor()
	if candidate.authenticatedOwner != descriptor.OwnerPeerID() ||
		candidate.transcript.OwnerPeerID() != candidate.authenticatedOwner ||
		candidate.transcript.ChannelID() != descriptor.ID() {
		return false
	}
	member, ok := memberAtHead(candidate.roster, candidate.receipt.MemberHead())
	if !ok || !firstRosterAuthorityForPeer(candidate.roster, member) {
		return false
	}
	if !validJoinedChannelReceiptBinding(candidate, member) ||
		!validJoinedChannelRosterOutcome(candidate) {
		return false
	}
	return candidate.ownerOutcome != ChannelEnrollmentAccepted ||
		model.VerifyEnrollmentReceipt(candidate.descriptor, member, candidate.transcript,
			candidate.receipt) == nil
}

func validJoinedChannelReceiptBinding(candidate joinedChannelInstallCandidate,
	member model.Member,
) bool {
	joinIdentity, identityErr := candidate.transcript.JoinIdentityDigest()
	previous, hasPrevious := member.PreviousDigest()
	return identityErr == nil && member.PeerID() == candidate.transcript.JoinerPeerID() &&
		candidate.receipt.RequestID() == candidate.transcript.RequestID() &&
		candidate.receipt.GrantID() == candidate.transcript.GrantID() &&
		candidate.receipt.JoinIdentityDigest() == joinIdentity && hasPrevious &&
		member.Head().Revision() == candidate.transcript.RosterHead().Revision()+1 &&
		previous == candidate.transcript.RosterHead().Digest() &&
		model.VerifyEnrollmentReceiptEvidence(candidate.descriptor, member, candidate.receipt) == nil
}

func validJoinedChannelRosterOutcome(candidate joinedChannelInstallCandidate) bool {
	members := candidate.roster.Members()
	currentLocal, localOK := candidate.roster.CurrentMember(candidate.transcript.JoinerPeerID())
	currentOwner, ownerOK := candidate.roster.CurrentMember(candidate.authenticatedOwner)
	return localOK && ownerOK && validJoinedChannelOwnerOutcome(candidate.ownerOutcome,
		currentLocal, currentOwner) && !candidate.at.Before(members[len(members)-1].CreatedAt()) &&
		!candidate.at.Before(candidate.receipt.AcceptedAt()) && validJoinedChannelLocalProjection(candidate)
}

func validJoinedChannelOwnerOutcome(outcome ChannelEnrollmentStatus,
	local, owner model.Member,
) bool {
	if local.Status().Terminal() {
		return outcome == ChannelEnrollmentMemberRevoked
	}
	if owner.Status().Terminal() {
		return outcome == ChannelEnrollmentChannelClosed
	}
	return outcome == ChannelEnrollmentAccepted || outcome == ChannelEnrollmentReplayed
}

func validJoinedChannelLocalProjection(candidate joinedChannelInstallCandidate) bool {
	_, err := model.NewChannel(model.ChannelSpec{Descriptor: candidate.descriptor,
		LocalAlias: candidate.localAlias, RosterHead: candidate.roster.Head(), Status: model.ChannelActive,
		TopicState: model.TopicNotJoined, UpdatedAt: candidate.at})
	return err == nil
}

func readJoinedChannelInstallPreimage(ctx context.Context, tx *sql.Tx, node model.Node,
	candidate joinedChannelInstallCandidate,
) (joinedChannelReservationFence, joinedChannelReplicaSnapshot, error) {
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channels WHERE channel_id=?`,
		candidate.descriptor.Descriptor().ID().String()).Scan(&exists); err != nil {
		return joinedChannelReservationFence{}, joinedChannelReplicaSnapshot{}, mapChannelJoinError(err)
	}
	if exists == 0 {
		fence, err := readJoinedChannelReservationFence(ctx, tx, node, candidate)
		return fence, joinedChannelReplicaSnapshot{}, err
	}
	authority, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(),
		candidate.descriptor.Descriptor().ID())
	if err != nil {
		return joinedChannelReservationFence{}, joinedChannelReplicaSnapshot{},
			fmt.Errorf("read joined Channel install preimage: %w", err)
	}
	return joinedChannelReservationFence{}, joinedChannelReplicaSnapshot{
		channel: authority.channel, roster: authority.roster}, nil
}

func readJoinedChannelReservationFence(ctx context.Context, tx *sql.Tx, node model.Node,
	candidate joinedChannelInstallCandidate,
) (joinedChannelReservationFence, error) {
	joinIdentity, err := candidate.transcript.JoinIdentityDigest()
	if err != nil {
		return joinedChannelReservationFence{}, ErrChannelJoinInput
	}
	fence := joinedChannelReservationFence{requestID: candidate.transcript.RequestID(),
		channelID: candidate.transcript.ChannelID(), grantID: candidate.transcript.GrantID(),
		joinIdentity: joinIdentity, descriptorDigest: candidate.descriptor.Descriptor().Digest(),
		localPeerID: node.PeerID(), originEpoch: node.OriginEpoch(), localAlias: candidate.localAlias}
	if err := verifyJoinedChannelReservationFence(ctx, tx, fence); err != nil {
		return joinedChannelReservationFence{}, err
	}
	return fence, nil
}

func verifyJoinedChannelReservationFence(ctx context.Context, tx *sql.Tx,
	fence joinedChannelReservationFence,
) error {
	var channelText, grantText, peerText, epochText, alias, state string
	var joinIdentity, descriptorDigest []byte
	var attempt uint64
	err := tx.QueryRowContext(ctx, `SELECT channel_id,grant_id,join_identity_digest,descriptor_digest,
		local_peer_id,origin_epoch,local_alias,attempt,state FROM channel_join_reservations
		WHERE request_id=?`, fence.requestID.String()).Scan(&channelText, &grantText, &joinIdentity,
		&descriptorDigest, &peerText, &epochText, &alias, &attempt, &state)
	if err != nil || fence.isZero() || channelText != fence.channelID.String() ||
		grantText != fence.grantID.String() || !bytes.Equal(joinIdentity, fence.joinIdentity.Bytes()) ||
		!bytes.Equal(descriptorDigest, fence.descriptorDigest.Bytes()) ||
		peerText != fence.localPeerID.String() || epochText != fence.originEpoch.String() ||
		alias != fence.localAlias || attempt == 0 || attempt > model.MaxSQLiteInteger || state != "commit_unknown" {
		return ErrChannelJoinConflict
	}
	return nil
}
