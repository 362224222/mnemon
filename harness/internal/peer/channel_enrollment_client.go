package peer

import (
	"bytes"
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

// preparedChannelJoin is the bounded durable reservation for exactly one
// enrollment attempt. It contains no bearer material beyond the caller-owned
// token already required for the exchange.
type preparedChannelJoin struct {
	spec           JoinChannelSpec
	descriptor     model.SignedChannelDescriptor
	localPeerID    model.PeerID
	localPublicKey []byte
	reservation    store.PrepareJoinedChannelResult
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

// prepare reserves the local Channel slot and immutable attempt fence before
// Connect or NewStream can consume remote resources or grant authority.
func (client *ChannelEnrollmentClient) prepare(ctx context.Context, spec JoinChannelSpec,
	localPeerID model.PeerID, localPublicKey []byte,
) (preparedChannelJoin, error) {
	if client == nil || client.store == nil || ctx == nil ||
		localPeerID.IsZero() || len(localPublicKey) == 0 ||
		model.VerifyEnrollmentToken(spec.Token) != nil {
		return preparedChannelJoin{},
			newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	if err := ctx.Err(); err != nil {
		return preparedChannelJoin{}, enrollmentTransportFailure(err)
	}
	descriptor := spec.Token.Payload().Descriptor()
	reservation, err := client.store.PrepareJoinedChannel(ctx, store.PrepareJoinedChannelSpec{
		AuthenticatedLocalPeerID: localPeerID, LocalPublicKey: localPublicKey,
		Descriptor: descriptor, GrantID: spec.Token.Payload().GrantID(),
		LocalAlias: spec.LocalAlias, At: client.clock.Now(),
	})
	if err != nil {
		return preparedChannelJoin{}, joinedChannelStoreFailure(err)
	}
	return preparedChannelJoin{spec: spec, descriptor: descriptor, localPeerID: localPeerID,
		localPublicKey: append([]byte(nil), localPublicKey...), reservation: reservation}, nil
}

// join executes the authenticated handshake using a reservation created before
// the stream was opened, then atomically installs only verified signed
// evidence.
func (client *ChannelEnrollmentClient) join(ctx context.Context, stream network.Stream,
	prepared preparedChannelJoin,
) (store.InstallJoinedChannelResult, error) {
	if client == nil || prepared.reservation.RequestID.IsZero() ||
		prepared.localPeerID.IsZero() || prepared.descriptor.IsZero() {
		return store.InstallJoinedChannelResult{}, newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	session := &channelJoinSession{client: client, stream: stream, spec: prepared.spec,
		descriptor: prepared.descriptor, joinerPeerID: prepared.localPeerID,
		joinerPublicKey: append([]byte(nil), prepared.localPublicKey...),
		prepared:        prepared.reservation, reservationActive: prepared.reservation.Reserved,
		releaseReservation: prepared.reservation.Reserved && !prepared.reservation.CommitUnknown}
	defer session.finish()
	if err := session.start(ctx); err != nil {
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

func (client *ChannelEnrollmentClient) release(prepared preparedChannelJoin) {
	if client == nil || client.store == nil || !prepared.reservation.Reserved ||
		prepared.reservation.CommitUnknown {
		return
	}
	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), time.Second)
	_ = client.store.ReleaseJoinedChannelReservation(releaseCtx, prepared.reservation.RequestID,
		prepared.localPeerID, prepared.reservation.Attempt)
	releaseCancel()
}

func (session *channelJoinSession) start(ctx context.Context) error {
	client, stream := session.client, session.stream
	if client == nil || client.store == nil || ctx == nil || stream == nil || stream.Conn() == nil ||
		stream.Protocol() != ChannelProtocol || model.VerifyEnrollmentToken(session.spec.Token) != nil ||
		session.prepared.RequestID.IsZero() || session.joinerPeerID.IsZero() ||
		session.descriptor.IsZero() {
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
	if session.payload.Descriptor().Descriptor().Digest() !=
		session.descriptor.Descriptor().Digest() {
		return newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	wantOwner := session.descriptor.Descriptor().OwnerPeerID()
	ownerPeerID, _, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil || ownerPeerID != wantOwner {
		return newChannelProtocolFailure(ChannelErrorWrongOwner, 0)
	}
	joinerPeerID, joinerPublicKey, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil || joinerPeerID == ownerPeerID || joinerPeerID != session.joinerPeerID ||
		!bytes.Equal(joinerPublicKey, session.joinerPublicKey) {
		return newChannelProtocolFailure(ChannelErrorInvalidToken, 0)
	}
	session.ownerPeerID = ownerPeerID
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
		session.client.release(preparedChannelJoin{localPeerID: session.joinerPeerID,
			reservation: session.prepared})
	}
	if session.stopCancellation != nil {
		session.stopCancellation()
	}
	if session.cancel != nil {
		session.cancel()
	}
	if session.stream == nil {
		return
	}
	if session.completed {
		_ = session.stream.Close()
	} else {
		_ = session.stream.Reset()
	}
}
