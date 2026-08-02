package peer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrAgencyClient          = errors.New("Mnemon Agency client")
	ErrAgencyClientTransport = fmt.Errorf("%w: transport unavailable", ErrAgencyClient)
	ErrAgencyClientResponse  = fmt.Errorf("%w: invalid response", ErrAgencyClient)
)

// AgencyRemoteFailure reports only a closed transport code. It is neither an
// admission Receipt nor evidence that the remote authority considered the
// semantic candidate.
type AgencyRemoteFailure struct {
	code       AgencyProtocolErrorCode
	retryAfter time.Duration
}

func (failure *AgencyRemoteFailure) Error() string {
	if failure == nil || !failure.code.Valid() {
		return ErrAgencyClient.Error()
	}
	return fmt.Sprintf("%s: %s", ErrAgencyClient, failure.code)
}

func (failure *AgencyRemoteFailure) Unwrap() error { return ErrAgencyClient }
func (failure *AgencyRemoteFailure) Code() AgencyProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}
func (failure *AgencyRemoteFailure) Retryable() bool {
	return failure != nil && failure.code.retryable()
}
func (failure *AgencyRemoteFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.retryAfter
}

type AgencyClientOptions struct{ Host host.Host }

type AgencyClient struct{ host host.Host }

func NewAgencyClient(options AgencyClientOptions) (*AgencyClient, error) {
	if options.Host == nil {
		return nil, fmt.Errorf("%w: live Host is required", ErrAgencyClient)
	}
	local, _, err := secureChannelPeer(options.Host.ID())
	if err != nil || local.IsZero() {
		return nil, fmt.Errorf("%w: Host must use a canonical Ed25519 identity", ErrAgencyClient)
	}
	return &AgencyClient{host: options.Host}, nil
}

// SendDelivery returns either a transport-only ACK or an opaque signed
// admission Receipt. The caller must keep its durable outbox pending for ACK.
func (client *AgencyClient) SendDelivery(ctx context.Context, remote model.PeerID,
	offer AgencyDeliveryOffer,
) (AgencyDeliveryReply, error) {
	if offer.IsZero() {
		return nil, fmt.Errorf("%w: complete Delivery offer is required", ErrAgencyClient)
	}
	request, err := NewAgencyDeliveryFrame(offer)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Delivery offer", ErrAgencyClient)
	}
	session, err := client.open(ctx, remote, AgencyDeliveryProtocol,
		agencyDeliveryRequestTimeout)
	if err != nil {
		return nil, err
	}
	defer session.finish()
	if err := writeAgencyDeliveryStreamFrame(session.stream, request); err != nil {
		return nil, session.transportFailure(err)
	}
	response, release, err := readAgencyDeliveryStreamFrame(session.stream,
		agencyDeliveryReplyBytes)
	if err != nil {
		return nil, session.transportFailure(err)
	}
	defer release()
	if failure, ok := response.Payload().(AgencyProtocolError); response.Type() == AgencyDeliveryFrameError && ok {
		session.completed = true
		return nil, &AgencyRemoteFailure{code: failure.Code(), retryAfter: failure.RetryAfter()}
	}
	reply, ok := response.Payload().(AgencyDeliveryReply)
	if !ok || !validAgencyDeliveryReply(offer, reply) {
		return nil, ErrAgencyClientResponse
	}
	session.completed = true
	return reply, nil
}

func (client *AgencyClient) FetchObject(ctx context.Context, remote model.PeerID,
	request AgencyObjectRequest,
) (AgencyObjectResponse, error) {
	if request.IsZero() {
		return AgencyObjectResponse{}, fmt.Errorf("%w: complete Object request is required", ErrAgencyClient)
	}
	requestFrame, err := NewAgencyObjectFrame(request)
	if err != nil {
		return AgencyObjectResponse{}, fmt.Errorf("%w: invalid Object request", ErrAgencyClient)
	}
	session, err := client.open(ctx, remote, AgencyObjectProtocol, agencyObjectRequestTimeout)
	if err != nil {
		return AgencyObjectResponse{}, err
	}
	defer session.finish()
	if err := writeAgencyObjectStreamFrame(session.stream, requestFrame); err != nil {
		return AgencyObjectResponse{}, session.transportFailure(err)
	}
	response, release, err := readAgencyObjectStreamFrame(session.stream)
	if err != nil {
		return AgencyObjectResponse{}, session.transportFailure(err)
	}
	defer release()
	if failure, ok := response.Payload().(AgencyProtocolError); response.Type() == AgencyObjectFrameError && ok {
		session.completed = true
		return AgencyObjectResponse{}, &AgencyRemoteFailure{code: failure.Code(),
			retryAfter: failure.RetryAfter()}
	}
	object, ok := response.Payload().(AgencyObjectResponse)
	if response.Type() != AgencyObjectFrameObject || !ok ||
		!validAgencyObjectReply(request, object) {
		return AgencyObjectResponse{}, ErrAgencyClientResponse
	}
	session.completed = true
	return object, nil
}

type agencyClientSession struct {
	stream    network.Stream
	ctx       context.Context
	cancel    context.CancelFunc
	stop      func() bool
	completed bool
}

func (client *AgencyClient) open(ctx context.Context, remote model.PeerID,
	protocolID protocol.ID, timeout time.Duration,
) (*agencyClientSession, error) {
	if client == nil || client.host == nil || ctx == nil || ctx.Err() != nil ||
		remote.IsZero() || !agencyProtocol(protocolID) || timeout <= 0 {
		return nil, fmt.Errorf("%w: complete exchange input is required", ErrAgencyClient)
	}
	remoteID, err := canonicalLibp2pID(remote)
	if err != nil || remoteID == client.host.ID() {
		return nil, fmt.Errorf("%w: exact remote Peer is required", ErrAgencyClient)
	}
	requestContext, cancel := context.WithTimeout(ctx, timeout)
	stream, err := client.host.NewStream(requestContext, remoteID, protocolID)
	if err != nil {
		cancel()
		return nil, agencyClientTransportFailure(ctx, err)
	}
	fail := func(cause error) (*agencyClientSession, error) {
		_ = stream.Reset()
		cancel()
		return nil, cause
	}
	if !authenticateAgencyClientStream(client.host, stream, remoteID, protocolID) {
		return fail(ErrAgencyClientResponse)
	}
	deadline, ok := requestContext.Deadline()
	if !ok || stream.SetDeadline(deadline) != nil {
		return fail(ErrAgencyClientTransport)
	}
	stop := context.AfterFunc(requestContext, func() { _ = stream.SetDeadline(time.Now()) })
	return &agencyClientSession{stream: stream, ctx: requestContext,
		cancel: cancel, stop: stop}, nil
}

func authenticateAgencyClientStream(local host.Host, stream network.Stream,
	remoteID libp2ppeer.ID, protocolID protocol.ID,
) bool {
	if local == nil || stream == nil || stream.Scope() == nil || stream.Conn() == nil ||
		stream.Protocol() != protocolID || stream.Conn().LocalPeer() != local.ID() ||
		stream.Conn().RemotePeer() != remoteID || remoteID == local.ID() {
		return false
	}
	remote, _, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	localPeer, _, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	return remoteErr == nil && localErr == nil && remote != localPeer
}

func (session *agencyClientSession) transportFailure(cause error) error {
	if session == nil {
		return ErrAgencyClientTransport
	}
	return agencyClientTransportFailure(session.ctx, cause)
}

func agencyClientTransportFailure(ctx context.Context, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if cause == nil {
		return ErrAgencyClientTransport
	}
	return fmt.Errorf("%w: %v", ErrAgencyClientTransport, cause)
}

func (session *agencyClientSession) finish() {
	if session == nil {
		return
	}
	if session.stop != nil {
		session.stop()
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
