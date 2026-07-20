package peer

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type authenticatedChannelProtocolFailure struct{ error }

func (failure *authenticatedChannelProtocolFailure) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.error
}

func stableChannelProtocolFailure(err error) bool {
	_, ok := err.(*authenticatedChannelProtocolFailure)
	return ok
}

func receivedChannelFailure(requestID ChannelRequestID, frame ChannelFrame) error {
	if frame.Type() != ChannelFrameProtocolError {
		return nil
	}
	if frame.RequestID() != requestID {
		return newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	payload, ok := frame.Payload().(ProtocolError)
	if !ok {
		return newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	failure := &ChannelProtocolFailure{code: payload.Code(), retryable: payload.Retryable(),
		retryAfter: payload.RetryAfter()}
	return &authenticatedChannelProtocolFailure{error: failure}
}

func readEnrollmentChallenge(ctx context.Context, stream network.Stream,
	requestID ChannelRequestID,
) (EnrollChallenge, error) {
	frame, release, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
	if err != nil {
		return EnrollChallenge{}, enrollmentPrecommitTransportFailure(ctx, err)
	}
	defer release()
	if failure := receivedChannelFailure(requestID, frame); failure != nil {
		return EnrollChallenge{}, failure
	}
	challenge, ok := frame.Payload().(EnrollChallenge)
	if frame.RequestID() != requestID || frame.Type() != ChannelFrameEnrollChallenge || !ok {
		return EnrollChallenge{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return challenge, nil
}

func enrollmentProofFrame(spec frozenJoinChannelSpec, local channelJoinLocalIdentity,
	owner model.PeerID, prepared PreparedChannelJoin, initiation channelEnrollmentInitiation,
	challenge EnrollChallenge,
) (model.EnrollmentTranscript, ChannelFrame, error) {
	payload := spec.token.Payload()
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: payload.Descriptor().Descriptor().ID(), GrantID: payload.GrantID(),
		RequestID: prepared.RequestID(), OwnerPeerID: owner, JoinerPeerID: local.peerID,
		OwnerNonce: challenge.OwnerNonce(), JoinerNonce: initiation.joinerNonce,
		SelectedVersion: challenge.SelectedVersion(), Limits: challenge.Limits(),
		JoinerOriginEpoch: prepared.OriginEpoch(), JoinerDisplayLabel: spec.displayLabel,
		JoinerPublicKey:      append([]byte(nil), local.publicKey...),
		AdvertisedMultiaddrs: append([]string(nil), spec.advertisedMultiaddrs...),
		RosterHead:           challenge.RosterHead(),
	})
	if err != nil {
		return model.EnrollmentTranscript{}, ChannelFrame{},
			newChannelProtocolFailure(ChannelErrorIncompatibleProtocol, 0)
	}
	secret := payload.BearerSecret()
	verifier, verifierErr := model.VerifierForEnrollment(secret,
		payload.Descriptor().Descriptor().ID(), payload.GrantID())
	clear(secret)
	if verifierErr != nil {
		return model.EnrollmentTranscript{}, ChannelFrame{},
			newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	proof, err := model.ComputeEnrollmentProof(verifier, transcript)
	if err != nil {
		return model.EnrollmentTranscript{}, ChannelFrame{},
			newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	proofPayload, err := NewEnrollProof(proof)
	if err != nil {
		return model.EnrollmentTranscript{}, ChannelFrame{},
			newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	frame, err := NewChannelFrame(initiation.frameRequestID, proofPayload)
	if err != nil {
		return model.EnrollmentTranscript{}, ChannelFrame{},
			fmt.Errorf("%w: invalid local enrollment proof frame", ErrChannelEnrollmentProtocol)
	}
	return transcript, frame, nil
}

func readVerifiedChannelEnrollment(stream network.Stream, requestID ChannelRequestID,
	descriptor model.SignedChannelDescriptor, transcript model.EnrollmentTranscript,
	joiner, owner model.PeerID,
) (VerifiedChannelEnrollment, error) {
	frame, release, err := readChannelStreamFrame(stream,
		channelFrameMaximum(ChannelFrameEnrollAccepted))
	if err != nil {
		return VerifiedChannelEnrollment{}, enrollmentOutcomeUnknown(err)
	}
	defer release()
	if failure := receivedChannelFailure(requestID, frame); failure != nil {
		return VerifiedChannelEnrollment{}, failure
	}
	accepted, ok := frame.Payload().(EnrollAccepted)
	if frame.RequestID() != requestID || frame.Type() != ChannelFrameEnrollAccepted || !ok {
		return VerifiedChannelEnrollment{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	members := accepted.RosterSnapshot()
	roster, err := model.NewVerifiedRoster(descriptor, members)
	if err != nil || accepted.MemberRecord().PeerID() != joiner ||
		!validEnrollmentReceiptForStatus(descriptor, transcript, accepted) ||
		!validEnrollmentResultStatus(accepted.Status(), roster, joiner) {
		return VerifiedChannelEnrollment{}, newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return freezeVerifiedChannelEnrollment(owner, accepted.Status(), descriptor,
		transcript, accepted.JoinReceipt(), members)
}

func freezeVerifiedChannelEnrollment(owner model.PeerID, status ChannelEnrollmentStatus,
	descriptor model.SignedChannelDescriptor, transcript model.EnrollmentTranscript,
	receipt model.EnrollmentReceipt, members []model.Member,
) (VerifiedChannelEnrollment, error) {
	frozenDescriptor, err := model.ParseSignedChannelDescriptor(descriptor.WireJSON().Bytes())
	if err != nil {
		return VerifiedChannelEnrollment{}, ErrChannelEnrollmentProtocol
	}
	frozenTranscript, err := model.ParseEnrollmentTranscript(transcript.CanonicalJSON().Bytes())
	if err != nil {
		return VerifiedChannelEnrollment{}, ErrChannelEnrollmentProtocol
	}
	frozenReceipt, err := model.ParseEnrollmentReceipt(receipt.WireJSON().Bytes())
	if err != nil {
		return VerifiedChannelEnrollment{}, ErrChannelEnrollmentProtocol
	}
	frozenMembers := make([]model.Member, len(members))
	for index, member := range members {
		frozenMembers[index], err = model.ParseMember(member.WireJSON().Bytes())
		if err != nil {
			return VerifiedChannelEnrollment{}, ErrChannelEnrollmentProtocol
		}
	}
	roster, err := model.NewVerifiedRoster(frozenDescriptor, frozenMembers)
	if err != nil || owner.IsZero() || !status.Valid() {
		return VerifiedChannelEnrollment{}, ErrChannelEnrollmentProtocol
	}
	return VerifiedChannelEnrollment{owner: owner, status: status, descriptor: frozenDescriptor,
		transcript: frozenTranscript, receipt: frozenReceipt, roster: roster}, nil
}

func validEnrollmentReceiptForStatus(descriptor model.SignedChannelDescriptor,
	transcript model.EnrollmentTranscript, accepted EnrollAccepted,
) bool {
	receipt := accepted.JoinReceipt()
	member := accepted.MemberRecord()
	if accepted.Status() == ChannelEnrollmentAccepted {
		return model.VerifyEnrollmentReceipt(descriptor, member, transcript, receipt) == nil
	}
	identity, err := transcript.JoinIdentityDigest()
	previous, hasPrevious := member.PreviousDigest()
	return err == nil && receipt.RequestID() == transcript.RequestID() &&
		receipt.GrantID() == transcript.GrantID() && receipt.JoinIdentityDigest() == identity &&
		hasPrevious && member.Head().Revision() == transcript.RosterHead().Revision()+1 &&
		previous == transcript.RosterHead().Digest() &&
		model.VerifyEnrollmentReceiptEvidence(descriptor, member, receipt) == nil
}

func (attempt *channelJoinAttempt) markCommitUnknown(ctx context.Context, at time.Time) error {
	if !attempt.prepared.Reserved() {
		return nil
	}
	if at.IsZero() {
		return fmt.Errorf("%w: join clock is unavailable", ErrChannelEnrollmentProtocol)
	}
	if err := attempt.session.MarkChannelJoinCommitUnknown(ctx, at); err != nil {
		return channelJoinControlError(err)
	}
	attempt.release = false
	return nil
}

func (attempt *channelJoinAttempt) allowRelease() {
	if attempt.reservationActive {
		attempt.release = true
	}
}

func (attempt *channelJoinAttempt) install(ctx context.Context, accepted VerifiedChannelEnrollment,
	at time.Time,
) (ChannelJoinResult, error) {
	if at.IsZero() {
		return ChannelJoinResult{}, fmt.Errorf("%w: join clock is unavailable",
			ErrChannelEnrollmentProtocol)
	}
	result, err := attempt.session.InstallAcceptedChannelJoin(ctx, accepted, at)
	if err != nil {
		return ChannelJoinResult{}, channelJoinControlError(err)
	}
	attempt.reservationActive = false
	attempt.release = false
	return result, nil
}

func (attempt *channelJoinAttempt) releaseIfRequired() error {
	if !attempt.reservationActive || !attempt.release {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := attempt.session.ReleaseChannelJoinReservation(ctx); err != nil {
		return fmt.Errorf("%w: release Channel join reservation: %w",
			ErrChannelEnrollmentProtocol, channelJoinControlError(err))
	}
	attempt.reservationActive = false
	attempt.release = false
	return nil
}

func validInstalledChannelJoinResult(accepted VerifiedChannelEnrollment,
	result ChannelJoinResult, expectedLocalAlias string,
) bool {
	descriptor := accepted.Descriptor()
	if !sameChannelJoinRoster(result.Roster(), accepted.Roster()) {
		return false
	}
	if accepted.Status() == ChannelEnrollmentMemberRevoked ||
		accepted.Status() == ChannelEnrollmentChannelClosed {
		return result.Status() == accepted.Status() && !result.Installed() &&
			result.Channel().ID().IsZero()
	}
	return (accepted.Status() == ChannelEnrollmentAccepted ||
		accepted.Status() == ChannelEnrollmentReplayed) &&
		(result.Status() == ChannelEnrollmentAccepted || result.Status() == ChannelEnrollmentReplayed) &&
		(!result.Installed() || result.Status() == ChannelEnrollmentAccepted) &&
		result.Channel().Status() == model.ChannelActive &&
		result.Channel().LocalAlias() == expectedLocalAlias &&
		!result.Channel().ID().IsZero() &&
		result.Channel().ID() == descriptor.Descriptor().ID() &&
		result.Channel().RosterHead() == accepted.Roster().Head() &&
		bytes.Equal(result.Channel().Descriptor().WireJSON().Bytes(), descriptor.WireJSON().Bytes())
}

func sameChannelJoinRoster(left, right model.VerifiedRoster) bool {
	if left.IsZero() || right.IsZero() || left.Head() != right.Head() ||
		!bytes.Equal(left.Descriptor().WireJSON().Bytes(), right.Descriptor().WireJSON().Bytes()) {
		return false
	}
	leftMembers, rightMembers := left.Members(), right.Members()
	if len(leftMembers) != len(rightMembers) {
		return false
	}
	for index := range leftMembers {
		if !bytes.Equal(leftMembers[index].WireJSON().Bytes(), rightMembers[index].WireJSON().Bytes()) {
			return false
		}
	}
	return true
}
