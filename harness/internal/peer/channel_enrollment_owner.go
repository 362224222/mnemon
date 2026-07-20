package peer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ChannelEnrollmentOwnerStore is the exact owner transaction surface used by
// the Channel handler. mnemond still owns the Store and signer; peer transport
// cannot construct durable membership evidence by itself.
type ChannelEnrollmentOwnerStore interface {
	PrepareChannelEnrollment(context.Context,
		store.PrepareChannelEnrollmentSpec,
	) (store.PrepareChannelEnrollmentResult, error)
	AcceptChannelEnrollment(context.Context,
		store.AcceptChannelEnrollmentSpec,
	) (store.AcceptChannelEnrollmentResult, error)
}

type ChannelEnrollmentOwnerOptions struct {
	Store  ChannelEnrollmentOwnerStore
	Signer store.ChannelAuthoritySigner
	Clock  channelEnrollmentClock
	Random io.Reader
}

// ChannelEnrollmentOwner serves the owner side of /mnemon/channel/1. Its
// bounded semaphore is deliberately independent of libp2p's outer resource
// manager: admitted streams must also have a fixed Store transaction budget.
type ChannelEnrollmentOwner struct {
	store  ChannelEnrollmentOwnerStore
	signer store.ChannelAuthoritySigner
	clock  channelEnrollmentClock
	random io.Reader
	budget chan struct{}
}

type ownerEnrollmentRequest struct {
	frame           ChannelFrame
	init            EnrollInit
	ownerPeerID     model.PeerID
	joinerPeerID    model.PeerID
	joinerPublicKey []byte
}

func NewChannelEnrollmentOwner(options ChannelEnrollmentOwnerOptions) (*ChannelEnrollmentOwner, error) {
	if options.Store == nil || options.Signer == nil {
		return nil, fmt.Errorf("%w: owner Store and signer are required", ErrChannelEnrollmentProtocol)
	}
	if options.Clock == nil {
		options.Clock = wallEnrollmentClock{}
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelEnrollmentOwner{store: options.Store, signer: options.Signer,
		clock: options.Clock, random: options.Random,
		budget: make(chan struct{}, HermeticLimits().UnknownEnrollmentConnections)}, nil
}

// HandleChannelRequest serves an EnrollInit already admitted by the sole
// ChannelDispatcher. The dispatcher, not this sub-handler, owns the stream
// deadline, cancellation, first-frame reservation and final Close/Reset.
func (owner *ChannelEnrollmentOwner) HandleChannelRequest(ctx context.Context,
	stream network.Stream, initFrame ChannelFrame,
) error {
	request, err := owner.validateRequest(ctx, stream, initFrame)
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

	prepared, err := owner.prepare(ctx, request)
	if err != nil {
		return owner.writeStoreFailure(stream, initFrame.RequestID(), err)
	}
	challenge, err := owner.writeChallenge(stream, initFrame.RequestID(), prepared.RosterHead)
	if err != nil {
		return err
	}
	proof, releaseProof, err := owner.readProof(stream, initFrame.RequestID())
	if err != nil {
		return err
	}
	defer releaseProof()
	transcript, err := request.transcript(challenge)
	if err != nil {
		return ErrChannelEnrollmentProtocol
	}
	return owner.acceptAndWrite(ctx, stream, request, transcript, proof)
}

func (owner *ChannelEnrollmentOwner) validateRequest(ctx context.Context, stream network.Stream,
	initFrame ChannelFrame,
) (ownerEnrollmentRequest, error) {
	if owner == nil || ctx == nil || stream == nil || stream.Protocol() != ChannelProtocol ||
		stream.Conn() == nil || initFrame.Type() != ChannelFrameEnrollInit {
		return ownerEnrollmentRequest{}, ErrChannelEnrollmentProtocol
	}
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil {
		return ownerEnrollmentRequest{}, err
	}
	joinerPeerID, joinerPublicKey, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || joinerPeerID == ownerPeerID {
		return ownerEnrollmentRequest{}, ErrChannelEnrollmentProtocol
	}
	init, ok := initFrame.Payload().(EnrollInit)
	if !ok {
		return ownerEnrollmentRequest{}, ErrChannelEnrollmentProtocol
	}
	return ownerEnrollmentRequest{frame: initFrame, init: init, ownerPeerID: ownerPeerID,
		joinerPeerID: joinerPeerID, joinerPublicKey: joinerPublicKey}, nil
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

func (owner *ChannelEnrollmentOwner) prepare(ctx context.Context,
	request ownerEnrollmentRequest,
) (store.PrepareChannelEnrollmentResult, error) {
	init := request.init
	return owner.store.PrepareChannelEnrollment(ctx, store.PrepareChannelEnrollmentSpec{
		ChannelID: init.ChannelID(), GrantID: init.GrantID(), RequestID: init.EnrollmentRequestID(),
		AuthenticatedPeerID: request.joinerPeerID, JoinerOriginEpoch: init.OriginEpoch(),
		JoinerPublicKey: request.joinerPublicKey, At: owner.clock.Now(),
	})
}

func (owner *ChannelEnrollmentOwner) writeChallenge(stream network.Stream,
	requestID ChannelRequestID, rosterHead model.RecordHead,
) (EnrollChallenge, error) {
	ownerNonce := make([]byte, model.EnrollmentNonceBytes)
	if _, err := io.ReadFull(owner.random, ownerNonce); err != nil {
		return EnrollChallenge{}, err
	}
	challenge, err := NewEnrollChallenge(EnrollChallengeSpec{OwnerNonce: ownerNonce,
		SelectedVersion: ChannelFrameVersion, Limits: model.DefaultMemberLimits(), RosterHead: rosterHead})
	if err != nil {
		return EnrollChallenge{}, err
	}
	challengeFrame, err := NewChannelFrame(requestID, challenge)
	if err != nil || WriteChannelFrame(stream, challengeFrame) != nil {
		return EnrollChallenge{}, ErrChannelEnrollmentProtocol
	}
	return challenge, nil
}

func (owner *ChannelEnrollmentOwner) readProof(stream network.Stream,
	requestID ChannelRequestID,
) (EnrollProof, func(), error) {
	proofFrame, releaseProof, err := readChannelStreamFrame(stream, model.MaxChannelRecordBytes)
	if err != nil {
		return EnrollProof{}, nil, ErrChannelEnrollmentProtocol
	}
	if proofFrame.RequestID() != requestID || proofFrame.Type() != ChannelFrameEnrollProof {
		releaseProof()
		return EnrollProof{}, nil, ErrChannelEnrollmentProtocol
	}
	proof, ok := proofFrame.Payload().(EnrollProof)
	if !ok {
		releaseProof()
		return EnrollProof{}, nil, ErrChannelEnrollmentProtocol
	}
	return proof, releaseProof, nil
}

func (request ownerEnrollmentRequest) transcript(challenge EnrollChallenge) (model.EnrollmentTranscript, error) {
	init := request.init
	return model.NewEnrollmentTranscript(model.EnrollmentTranscriptSpec{
		ChannelID: init.ChannelID(), GrantID: init.GrantID(), RequestID: init.EnrollmentRequestID(),
		OwnerPeerID: request.ownerPeerID, JoinerPeerID: request.joinerPeerID,
		OwnerNonce: challenge.OwnerNonce(), JoinerNonce: init.JoinerNonce(),
		SelectedVersion: challenge.SelectedVersion(), Limits: challenge.Limits(),
		JoinerOriginEpoch: init.OriginEpoch(), JoinerDisplayLabel: init.DisplayLabel(),
		JoinerPublicKey: request.joinerPublicKey, AdvertisedMultiaddrs: init.AdvertisedMultiaddrs(),
		RosterHead: challenge.RosterHead(),
	})
}

func (owner *ChannelEnrollmentOwner) acceptAndWrite(ctx context.Context, stream network.Stream,
	request ownerEnrollmentRequest, transcript model.EnrollmentTranscript, proof EnrollProof,
) error {
	accepted, err := owner.store.AcceptChannelEnrollment(ctx, store.AcceptChannelEnrollmentSpec{
		AuthenticatedPeerID: request.joinerPeerID, Transcript: transcript,
		AdvertisedMultiaddrs: request.init.AdvertisedMultiaddrs(), Proof: proof.Proof(),
		Signer: owner.signer, At: owner.clock.Now(),
	})
	if err != nil {
		return owner.writeStoreFailure(stream, request.frame.RequestID(), err)
	}
	status, err := wireEnrollmentStatus(accepted.Status)
	if err != nil {
		return err
	}
	payload, err := NewEnrollAccepted(status, accepted.Member, accepted.Roster.Members(), accepted.Receipt)
	if err != nil {
		return err
	}
	frame, err := NewChannelFrame(request.frame.RequestID(), payload)
	if err != nil {
		return err
	}
	// AcceptChannelEnrollment has committed before this write. Any write error
	// intentionally leaves the receipt durable for stable-request replay.
	return WriteChannelFrame(stream, frame)
}

func (owner *ChannelEnrollmentOwner) writeStoreFailure(stream network.Stream,
	requestID ChannelRequestID, cause error,
) error {
	code, retryAfter, ok := channelStoreFailure(cause)
	if !ok {
		return cause
	}
	return owner.writeFailure(stream, requestID, code, retryAfter)
}

func (owner *ChannelEnrollmentOwner) writeFailure(stream network.Stream,
	requestID ChannelRequestID, code ChannelProtocolErrorCode, retryAfter time.Duration,
) error {
	payload, err := NewProtocolError(ProtocolErrorSpec{Code: code,
		Retryable: code.retryable(), RetryAfter: retryAfter})
	if err != nil {
		return err
	}
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		return err
	}
	return WriteChannelFrame(stream, frame)
}

func channelStoreFailure(cause error) (ChannelProtocolErrorCode, time.Duration, bool) {
	switch {
	case errors.Is(cause, store.ErrChannelEnrollmentOwner):
		return ChannelErrorWrongOwner, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentProof):
		return ChannelErrorBadProof, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExpired):
		return ChannelErrorTokenExpired, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentTokenClosed):
		return ChannelErrorTokenClosed, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentTokenExhausted):
		return ChannelErrorTokenExhausted, 0, true
	case errors.Is(cause, store.ErrChannelFull):
		return ChannelErrorChannelFull, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentChannelClosed):
		return ChannelErrorChannelClosed, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentMemberRevoked):
		return ChannelErrorMemberRevoked, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentStale):
		return ChannelErrorRosterGap, channelEnrollmentGapRetry, true
	case errors.Is(cause, store.ErrChannelEnrollmentConflict),
		errors.Is(cause, store.ErrChannelAuthorityInvariant):
		return ChannelErrorRosterConflict, 0, true
	case errors.Is(cause, store.ErrChannelEnrollmentUnavailable),
		errors.Is(cause, store.ErrChannelEnrollmentInput):
		return ChannelErrorInvalidToken, 0, true
	default:
		return "", 0, false
	}
}

func wireEnrollmentStatus(status store.ChannelEnrollmentStatus) (ChannelEnrollmentStatus, error) {
	switch status {
	case store.ChannelEnrollmentAccepted:
		return ChannelEnrollmentAccepted, nil
	case store.ChannelEnrollmentReplayed:
		return ChannelEnrollmentReplayed, nil
	case store.ChannelEnrollmentMemberRevoked:
		return ChannelEnrollmentMemberRevoked, nil
	case store.ChannelEnrollmentChannelClosed:
		return ChannelEnrollmentChannelClosed, nil
	default:
		return "", fmt.Errorf("%w: unknown durable enrollment status", ErrChannelEnrollmentProtocol)
	}
}
