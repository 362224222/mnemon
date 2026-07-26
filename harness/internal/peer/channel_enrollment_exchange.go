package peer

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

func (session *channelJoinSession) exchangeChallenge(
	initFrame ChannelFrame,
) (EnrollChallenge, func(), error) {
	if err := WriteChannelFrame(session.stream, initFrame); err != nil {
		return EnrollChallenge{}, nil, enrollmentPrecommitTransportFailure(session.requestCtx, err)
	}
	frame, release, err := readChannelStreamFrame(session.stream, model.MaxChannelRecordBytes)
	if err != nil {
		return EnrollChallenge{}, nil, enrollmentPrecommitTransportFailure(session.requestCtx, err)
	}
	if failure := receivedChannelFailure(session.requestID, frame); failure != nil {
		session.completeRemoteFailure()
		return EnrollChallenge{}, release, failure
	}
	if frame.RequestID() != session.requestID || frame.Type() != ChannelFrameEnrollChallenge {
		return EnrollChallenge{}, release,
			newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	challenge, ok := frame.Payload().(EnrollChallenge)
	if !ok {
		return EnrollChallenge{}, release,
			newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return challenge, release, nil
}

func (session *channelJoinSession) newProofFrame(challenge EnrollChallenge) (ChannelFrame, error) {
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: session.descriptor.Descriptor().ID(), GrantID: session.payload.GrantID(),
		RequestID: session.prepared.RequestID, OwnerPeerID: session.ownerPeerID,
		JoinerPeerID: session.joinerPeerID, OwnerNonce: challenge.OwnerNonce(),
		JoinerNonce: session.joinerNonce, SelectedVersion: challenge.SelectedVersion(),
		Limits: challenge.Limits(), JoinerOriginEpoch: session.prepared.OriginEpoch,
		JoinerDisplayLabel: session.spec.DisplayLabel, JoinerPublicKey: session.joinerPublicKey,
		AdvertisedMultiaddrs: session.spec.AdvertisedMultiaddrs, RosterHead: challenge.RosterHead(),
	})
	if err != nil {
		return ChannelFrame{}, newChannelProtocolFailure(ChannelErrorIncompatibleProtocol, 0)
	}
	session.transcript = transcript
	secret := session.payload.BearerSecret()
	verifier, verifierErr := model.VerifierForEnrollment(secret,
		session.descriptor.Descriptor().ID(), session.payload.GrantID())
	clear(secret)
	if verifierErr != nil {
		return ChannelFrame{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	proof, err := model.ComputeEnrollmentProof(verifier, transcript)
	if err != nil {
		return ChannelFrame{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	payload, err := NewEnrollProof(proof)
	if err != nil {
		return ChannelFrame{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	frame, err := NewChannelFrame(session.requestID, payload)
	if err != nil {
		return ChannelFrame{}, fmt.Errorf("%w: invalid local enrollment proof frame",
			ErrChannelEnrollmentProtocol)
	}
	return frame, nil
}

func (session *channelJoinSession) sendProof(frame ChannelFrame) error {
	if err := session.requestCtx.Err(); err != nil {
		return enrollmentTransportFailure(err)
	}
	if session.reservationActive {
		if err := session.client.store.MarkJoinedChannelCommitUnknown(session.requestCtx,
			session.prepared.RequestID, session.joinerPeerID, session.prepared.Attempt,
			session.client.clock.Now()); err != nil {
			return joinedChannelStoreFailure(err)
		}
		session.releaseReservation = false
	}
	if err := WriteChannelFrame(session.stream, frame); err != nil {
		return enrollmentOutcomeUnknown(err)
	}
	return nil
}

func (session *channelJoinSession) receiveAccepted() (EnrollAccepted, func(), error) {
	frame, release, err := readChannelStreamFrame(session.stream,
		channelFrameMaximum(ChannelFrameEnrollAccepted))
	if err != nil {
		return EnrollAccepted{}, nil, enrollmentOutcomeUnknown(err)
	}
	if failure := receivedChannelFailure(session.requestID, frame); failure != nil {
		session.completeRemoteFailure()
		return EnrollAccepted{}, release, failure
	}
	if frame.RequestID() != session.requestID || frame.Type() != ChannelFrameEnrollAccepted {
		return EnrollAccepted{}, release,
			newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	accepted, ok := frame.Payload().(EnrollAccepted)
	if !ok {
		return EnrollAccepted{}, release,
			newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return accepted, release, nil
}

func (session *channelJoinSession) completeRemoteFailure() {
	session.completed = true
	if session.reservationActive && !session.prepared.CommitUnknown {
		session.releaseReservation = true
	}
}

func (session *channelJoinSession) installAccepted(
	accepted EnrollAccepted,
) (store.InstallJoinedChannelResult, error) {
	members := accepted.RosterSnapshot()
	roster, err := model.NewVerifiedRoster(session.descriptor, members)
	if err != nil || accepted.MemberRecord().PeerID() != session.joinerPeerID ||
		!validEnrollmentReceiptForStatus(session.descriptor, session.transcript, accepted) ||
		!validEnrollmentResultStatus(accepted.Status(), roster, session.joinerPeerID) {
		return store.InstallJoinedChannelResult{},
			newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	session.completed = true
	installed, err := session.client.store.InstallJoinedChannel(session.requestCtx,
		store.InstallJoinedChannelSpec{
			AuthenticatedOwnerPeerID: session.ownerPeerID, LocalAlias: session.spec.LocalAlias,
			Descriptor: session.descriptor, Transcript: session.transcript,
			Receipt: accepted.JoinReceipt(), Members: members,
			ReservationAttempt: session.prepared.Attempt, At: session.client.clock.Now(),
		})
	if err != nil {
		return store.InstallJoinedChannelResult{}, joinedChannelStoreFailure(err)
	}
	session.reservationActive = false
	if (accepted.Status() == ChannelEnrollmentMemberRevoked &&
		installed.Status != store.ChannelEnrollmentMemberRevoked) ||
		(accepted.Status() == ChannelEnrollmentChannelClosed &&
			installed.Status != store.ChannelEnrollmentChannelClosed) {
		return store.InstallJoinedChannelResult{},
			newChannelProtocolFailure(ChannelErrorRosterConflict, 0)
	}
	return installed, nil
}
