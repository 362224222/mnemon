package peer

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ChannelEnrollmentJoinerStore is the joiner's one atomic replica boundary.
type ChannelEnrollmentJoinerStore interface {
	PrepareJoinedChannel(context.Context,
		store.PrepareJoinedChannelSpec,
	) (store.PrepareJoinedChannelResult, error)
	MarkJoinedChannelCommitUnknown(context.Context, model.EnrollmentRequestID,
		model.PeerID, uint64, time.Time,
	) error
	ReleaseJoinedChannelReservation(context.Context, model.EnrollmentRequestID,
		model.PeerID, uint64,
	) error
	InstallJoinedChannel(context.Context,
		store.InstallJoinedChannelSpec,
	) (store.InstallJoinedChannelResult, error)
}

type ChannelEnrollmentClientOptions struct {
	Store  ChannelEnrollmentJoinerStore
	Clock  channelEnrollmentClock
	Random io.Reader
}

type ChannelEnrollmentClient struct {
	store  ChannelEnrollmentJoinerStore
	clock  channelEnrollmentClock
	random io.Reader
}

type JoinChannelSpec struct {
	Token                model.EnrollmentToken
	DisplayLabel         string
	AdvertisedMultiaddrs []string
	LocalAlias           string
}

type channelJoinSession struct {
	client  *ChannelEnrollmentClient
	stream  network.Stream
	spec    JoinChannelSpec
	payload model.EnrollmentTokenPayload

	requestCtx       context.Context
	cancel           context.CancelFunc
	stopCancellation func() bool
	completed        bool

	descriptor      model.SignedChannelDescriptor
	ownerPeerID     model.PeerID
	joinerPeerID    model.PeerID
	joinerPublicKey []byte
	prepared        store.PrepareJoinedChannelResult

	requestID          ChannelRequestID
	joinerNonce        []byte
	transcript         model.EnrollmentTranscript
	reservationActive  bool
	releaseReservation bool
}

func NewChannelEnrollmentClient(options ChannelEnrollmentClientOptions) (*ChannelEnrollmentClient, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("%w: joiner Store is required", ErrChannelEnrollmentProtocol)
	}
	if options.Clock == nil {
		options.Clock = wallEnrollmentClock{}
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelEnrollmentClient{store: options.Store, clock: options.Clock,
		random: options.Random}, nil
}

// Join executes the authenticated handshake on an already-open exact Channel
// stream and atomically installs only verified signed evidence. Dial authority
// and peerstore address admission remain the Node reconciler's responsibility.
func (client *ChannelEnrollmentClient) Join(ctx context.Context, stream network.Stream,
	spec JoinChannelSpec,
) (store.InstallJoinedChannelResult, error) {
	if stream == nil {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	session := &channelJoinSession{client: client, stream: stream, spec: spec}
	defer session.finish()
	if err := session.start(ctx); err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	if err := session.prepare(); err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	initFrame, err := session.newInitFrame()
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	challenge, releaseChallenge, err := session.exchangeChallenge(initFrame)
	if releaseChallenge != nil {
		defer releaseChallenge()
	}
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	proofFrame, err := session.newProofFrame(challenge)
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	if err := session.sendProof(proofFrame); err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	accepted, releaseAccepted, err := session.receiveAccepted()
	if releaseAccepted != nil {
		defer releaseAccepted()
	}
	if err != nil {
		return store.InstallJoinedChannelResult{}, err
	}
	return session.installAccepted(accepted)
}

func (session *channelJoinSession) start(ctx context.Context) error {
	client, stream := session.client, session.stream
	if client == nil || client.store == nil || ctx == nil || stream.Conn() == nil ||
		stream.Protocol() != ChannelProtocol || model.VerifyEnrollmentToken(session.spec.Token) != nil {
		return newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	if err := ctx.Err(); err != nil {
		return enrollmentTransportFailure(err)
	}
	deadline := time.Now().Add(HermeticLimits().ChannelRequestTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		_ = stream.Reset()
		return enrollmentTransportFailure(err)
	}
	session.requestCtx, session.cancel = context.WithDeadline(ctx, deadline)
	session.stopCancellation = context.AfterFunc(session.requestCtx,
		func() { _ = stream.SetDeadline(time.Now()) })

	session.payload = session.spec.Token.Payload()
	session.descriptor = session.payload.Descriptor()
	wantOwner := session.descriptor.Descriptor().OwnerPeerID()
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || ownerPeerID != wantOwner {
		return newChannelProtocolFailure(ChannelErrorWrongOwner, 0)
	}
	joinerPeerID, joinerPublicKey, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil || joinerPeerID == ownerPeerID {
		return newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	session.ownerPeerID = ownerPeerID
	session.joinerPeerID = joinerPeerID
	session.joinerPublicKey = joinerPublicKey
	return nil
}

func (session *channelJoinSession) prepare() error {
	prepared, err := session.client.store.PrepareJoinedChannel(session.requestCtx,
		store.PrepareJoinedChannelSpec{
			AuthenticatedLocalPeerID: session.joinerPeerID, LocalPublicKey: session.joinerPublicKey,
			Descriptor: session.descriptor, GrantID: session.payload.GrantID(),
			LocalAlias: session.spec.LocalAlias, At: session.client.clock.Now(),
		})
	if err != nil {
		return joinedChannelStoreFailure(err)
	}
	session.prepared = prepared
	session.reservationActive = prepared.Reserved
	session.releaseReservation = prepared.Reserved && !prepared.CommitUnknown
	return nil
}

func (session *channelJoinSession) newInitFrame() (ChannelFrame, error) {
	session.joinerNonce = make([]byte, model.EnrollmentNonceBytes)
	if _, err := io.ReadFull(session.client.random, session.joinerNonce); err != nil {
		return ChannelFrame{}, fmt.Errorf("%w: joiner nonce unavailable", ErrChannelEnrollmentProtocol)
	}
	requestID, err := NewChannelRequestID(session.client.random)
	if err != nil {
		return ChannelFrame{}, fmt.Errorf("%w: Channel request ID unavailable", ErrChannelEnrollmentProtocol)
	}
	session.requestID = requestID
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: session.descriptor.Descriptor().ID(),
		GrantID: session.payload.GrantID(), EnrollmentRequestID: session.prepared.RequestID,
		JoinerNonce: session.joinerNonce, SupportedVersions: []uint8{ChannelFrameVersion},
		OriginEpoch: session.prepared.OriginEpoch, DisplayLabel: session.spec.DisplayLabel,
		AdvertisedMultiaddrs: session.spec.AdvertisedMultiaddrs})
	if err != nil {
		return ChannelFrame{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	frame, err := NewChannelFrame(requestID, init)
	if err != nil {
		return ChannelFrame{}, fmt.Errorf("%w: invalid local enrollment frame", ErrChannelEnrollmentProtocol)
	}
	return frame, nil
}

func (session *channelJoinSession) finish() {
	if session.reservationActive && session.releaseReservation {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), time.Second)
		_ = session.client.store.ReleaseJoinedChannelReservation(releaseCtx, session.prepared.RequestID,
			session.joinerPeerID, session.prepared.Attempt)
		releaseCancel()
	}
	if session.stopCancellation != nil {
		session.stopCancellation()
	}
	if session.cancel != nil {
		session.cancel()
	}
	if session.completed {
		_ = session.stream.Close()
	} else {
		_ = session.stream.Reset()
	}
}
