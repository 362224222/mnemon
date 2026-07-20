package peer

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type ChannelEnrollmentOwnerOptions struct {
	Controller ChannelEnrollmentOwnerController
	Clock      channelEnrollmentClock
	Random     io.Reader
}

// ChannelEnrollmentOwner owns wire framing, authenticated transcript
// construction and a bounded admitted-stream budget. mnemond owns every
// durable and runtime authority transition through controller.
type ChannelEnrollmentOwner struct {
	controller ChannelEnrollmentOwnerController
	clock      channelEnrollmentClock
	random     io.Reader
	budget     chan struct{}
}

type channelEnrollmentOwnerRequest struct {
	requestID       ChannelRequestID
	ownerPeerID     model.PeerID
	joinerPeerID    model.PeerID
	joinerPublicKey []byte
	init            EnrollInit
}

var _ ChannelRequestHandler = (*ChannelEnrollmentOwner)(nil)

func NewChannelEnrollmentOwner(options ChannelEnrollmentOwnerOptions) (*ChannelEnrollmentOwner, error) {
	if isNilChannelEnrollmentOwnerController(options.Controller) {
		return nil, fmt.Errorf("%w: owner controller is required", ErrChannelEnrollmentProtocol)
	}
	if options.Clock == nil {
		options.Clock = wallEnrollmentClock{}
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelEnrollmentOwner{controller: options.Controller, clock: options.Clock,
		random: options.Random,
		budget: make(chan struct{}, HermeticLimits().UnknownEnrollmentConnections)}, nil
}

// HandleChannelRequest serves an EnrollInit already admitted by the sole
// ChannelDispatcher. The dispatcher, not this sub-handler, owns the stream
// deadline, cancellation, first-frame reservation and final Close/Reset.
func (owner *ChannelEnrollmentOwner) HandleChannelRequest(ctx context.Context,
	stream network.Stream, initFrame ChannelFrame,
) error {
	if !validChannelEnrollmentOwnerRequest(owner, ctx, stream, initFrame) {
		return ErrChannelEnrollmentProtocol
	}
	request, err := authenticateChannelEnrollmentOwnerRequest(stream, initFrame)
	if err != nil {
		return err
	}
	if !supportsChannelFrameVersion(request.init.SupportedVersions(), ChannelFrameVersion) {
		return owner.writeFailure(stream, initFrame.RequestID(), ChannelErrorIncompatibleProtocol, 0)
	}
	if !owner.acquireBudget() {
		return owner.writeFailure(stream, initFrame.RequestID(), ChannelErrorBusy,
			channelEnrollmentBusyRetry)
	}
	defer owner.releaseBudget()

	at := owner.clock.Now()
	if at.IsZero() {
		return ErrChannelEnrollmentProtocol
	}
	prepared, err := owner.controller.PrepareEnrollmentChallenge(ctx,
		request.challengeControl(at))
	if err != nil {
		return owner.respondControllerFailure(stream, request.requestID, err)
	}
	challenge, err := owner.makeChallenge(prepared)
	if err != nil {
		return err
	}
	if err := writeChannelEnrollmentFrame(stream, request.requestID, challenge); err != nil {
		return err
	}
	proof, err := readChannelEnrollmentProof(stream, request.requestID)
	if err != nil {
		return err
	}
	transcript, err := enrollmentOwnerTranscript(request.ownerPeerID, request.joinerPeerID,
		request.joinerPublicKey, request.init, challenge)
	if err != nil {
		return err
	}
	at = owner.clock.Now()
	if at.IsZero() {
		return ErrChannelEnrollmentProtocol
	}
	accepted, err := owner.controller.AcceptEnrollmentAuthority(ctx,
		request.acceptanceControl(transcript, proof, at))
	if err != nil {
		return owner.respondControllerFailure(stream, request.requestID, err)
	}
	acceptedPayload, err := NewEnrollAccepted(accepted.Status, accepted.Member,
		append([]model.Member(nil), accepted.Roster...), accepted.Receipt)
	if err != nil {
		return fmt.Errorf("%w: controller returned invalid accepted authority",
			ErrChannelEnrollmentProtocol)
	}
	// The controller has committed and installed authority before this write.
	// Any write error intentionally leaves the receipt durable for replay.
	return writeChannelEnrollmentFrame(stream, request.requestID, acceptedPayload)
}

func validChannelEnrollmentOwnerRequest(owner *ChannelEnrollmentOwner, ctx context.Context,
	stream network.Stream, frame ChannelFrame,
) bool {
	return owner != nil && !isNilChannelEnrollmentOwnerController(owner.controller) &&
		owner.clock != nil && owner.random != nil && ctx != nil && stream != nil &&
		stream.Protocol() == ChannelProtocol && stream.Conn() != nil &&
		frame.Type() == ChannelFrameEnrollInit
}

func authenticateChannelEnrollmentOwnerRequest(stream network.Stream,
	frame ChannelFrame,
) (channelEnrollmentOwnerRequest, error) {
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil {
		return channelEnrollmentOwnerRequest{}, err
	}
	joinerPeerID, joinerPublicKey, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || joinerPeerID == ownerPeerID {
		return channelEnrollmentOwnerRequest{}, ErrChannelEnrollmentProtocol
	}
	init, ok := frame.Payload().(EnrollInit)
	if !ok {
		return channelEnrollmentOwnerRequest{}, ErrChannelEnrollmentProtocol
	}
	return channelEnrollmentOwnerRequest{requestID: frame.RequestID(), ownerPeerID: ownerPeerID,
		joinerPeerID: joinerPeerID, joinerPublicKey: append([]byte(nil), joinerPublicKey...),
		init: init}, nil
}

func (request channelEnrollmentOwnerRequest) challengeControl(
	at time.Time,
) ChannelEnrollmentChallengeControl {
	return ChannelEnrollmentChallengeControl{AuthenticatedPeerID: request.joinerPeerID,
		ChannelID: request.init.ChannelID(), GrantID: request.init.GrantID(),
		RequestID: request.init.EnrollmentRequestID(), JoinerOriginEpoch: request.init.OriginEpoch(),
		JoinerPublicKey: append([]byte(nil), request.joinerPublicKey...), At: at}
}

func (request channelEnrollmentOwnerRequest) acceptanceControl(
	transcript model.EnrollmentTranscript, proof EnrollProof, at time.Time,
) ChannelEnrollmentAcceptanceControl {
	return ChannelEnrollmentAcceptanceControl{AuthenticatedPeerID: request.joinerPeerID,
		Transcript: transcript, AdvertisedMultiaddrs: request.init.AdvertisedMultiaddrs(),
		Proof: proof.Proof(), At: at}
}

func readChannelEnrollmentProof(stream network.Stream,
	requestID ChannelRequestID,
) (EnrollProof, error) {
	frame, release, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
	if err != nil {
		return EnrollProof{}, ErrChannelEnrollmentProtocol
	}
	defer release()
	proof, ok := frame.Payload().(EnrollProof)
	if frame.RequestID() != requestID || frame.Type() != ChannelFrameEnrollProof || !ok {
		return EnrollProof{}, ErrChannelEnrollmentProtocol
	}
	return proof, nil
}

func (owner *ChannelEnrollmentOwner) acquireBudget() bool {
	select {
	case owner.budget <- struct{}{}:
		return true
	default:
		return false
	}
}

func (owner *ChannelEnrollmentOwner) releaseBudget() { <-owner.budget }

func (owner *ChannelEnrollmentOwner) makeChallenge(
	prepared ChannelEnrollmentChallengeAuthority,
) (EnrollChallenge, error) {
	ownerNonce := make([]byte, model.EnrollmentNonceBytes)
	if _, err := io.ReadFull(owner.random, ownerNonce); err != nil {
		return EnrollChallenge{}, err
	}
	challenge, err := NewEnrollChallenge(EnrollChallengeSpec{OwnerNonce: ownerNonce,
		SelectedVersion: ChannelFrameVersion, Limits: model.DefaultMemberLimits(),
		RosterHead: prepared.RosterHead})
	if err != nil {
		return EnrollChallenge{}, fmt.Errorf("%w: controller returned invalid challenge authority",
			ErrChannelEnrollmentProtocol)
	}
	return challenge, nil
}

func enrollmentOwnerTranscript(ownerPeerID, joinerPeerID model.PeerID,
	joinerPublicKey []byte, init EnrollInit, challenge EnrollChallenge,
) (model.EnrollmentTranscript, error) {
	transcript, err := model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: init.ChannelID(), GrantID: init.GrantID(), RequestID: init.EnrollmentRequestID(),
		OwnerPeerID: ownerPeerID, JoinerPeerID: joinerPeerID, OwnerNonce: challenge.OwnerNonce(),
		JoinerNonce: init.JoinerNonce(), SelectedVersion: challenge.SelectedVersion(),
		Limits: challenge.Limits(), JoinerOriginEpoch: init.OriginEpoch(),
		JoinerDisplayLabel: init.DisplayLabel(), JoinerPublicKey: append([]byte(nil), joinerPublicKey...),
		AdvertisedMultiaddrs: init.AdvertisedMultiaddrs(), RosterHead: challenge.RosterHead(),
	})
	if err != nil {
		return model.EnrollmentTranscript{}, ErrChannelEnrollmentProtocol
	}
	return transcript, nil
}

func writeChannelEnrollmentFrame(stream network.Stream, requestID ChannelRequestID,
	payload ChannelFramePayload,
) error {
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		return err
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		return err
	}
	return nil
}

func (owner *ChannelEnrollmentOwner) respondControllerFailure(stream network.Stream,
	requestID ChannelRequestID, cause error,
) error {
	code, retryAfter, ok := channelEnrollmentControllerFailure(cause)
	if !ok {
		return fmt.Errorf("%w: controller unavailable", ErrChannelEnrollmentProtocol)
	}
	return owner.writeFailure(stream, requestID, code, retryAfter)
}

func (owner *ChannelEnrollmentOwner) writeFailure(stream network.Stream,
	requestID ChannelRequestID, code ChannelProtocolErrorCode,
	retryAfter time.Duration,
) error {
	payload, err := NewProtocolError(ProtocolErrorSpec{Code: code,
		Retryable: code.retryable(), RetryAfter: retryAfter})
	if err != nil {
		return err
	}
	return writeChannelEnrollmentFrame(stream, requestID, payload)
}

func supportsChannelFrameVersion(versions []uint8, selected uint8) bool {
	for _, version := range versions {
		if version == selected {
			return true
		}
	}
	return false
}
