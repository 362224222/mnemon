package peer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	agencyDeliveryRequestTimeout = 15 * time.Second
	agencyObjectRequestTimeout   = 30 * time.Second
	agencyBusyRetry              = time.Second
)

var (
	ErrAgencyServer           = errors.New("Mnemon Agency server")
	agencyServerConstructorMu sync.Mutex
)

// AgencyDeliveryHandler receives transport-validated bytes under an
// authenticated Peer identity. It may block until ctx ends and is invoked
// without a peer-package lock. It alone owns staging, signature verification,
// Artifact requirements, admission, and signed Receipt construction.
type AgencyDeliveryHandler interface {
	HandleAgencyDelivery(context.Context, model.PeerID,
		AgencyDeliveryOffer,
	) (AgencyDeliveryReply, error)
}

type AgencyDeliveryHandlerFunc func(context.Context, model.PeerID,
	AgencyDeliveryOffer,
) (AgencyDeliveryReply, error)

func (handler AgencyDeliveryHandlerFunc) HandleAgencyDelivery(ctx context.Context,
	peerID model.PeerID, offer AgencyDeliveryOffer,
) (AgencyDeliveryReply, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: Delivery handler is unavailable", ErrAgencyServer)
	}
	return handler(ctx, peerID, offer)
}

// AgencyObjectHandler may expose bytes only after checking that the exact
// authenticated Peer and DeliveryID are entitled to the requested digest.
// The peer layer intentionally offers no bare-digest callback.
type AgencyObjectHandler interface {
	HandleAgencyObject(context.Context, model.PeerID,
		AgencyObjectRequest,
	) (AgencyObjectReply, error)
}

type AgencyObjectHandlerFunc func(context.Context, model.PeerID,
	AgencyObjectRequest,
) (AgencyObjectReply, error)

func (handler AgencyObjectHandlerFunc) HandleAgencyObject(ctx context.Context,
	peerID model.PeerID, request AgencyObjectRequest,
) (AgencyObjectReply, error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: Object handler is unavailable", ErrAgencyServer)
	}
	return handler(ctx, peerID, request)
}

type AgencyServerOptions struct {
	Host     host.Host
	Delivery AgencyDeliveryHandler
	Object   AgencyObjectHandler
}

// AgencyServer is the sole owner of both R7 Agency protocols on one Host. It
// only authenticates streams, bounds I/O, and dispatches opaque typed bytes.
type AgencyServer struct {
	host     host.Host
	delivery AgencyDeliveryHandler
	object   AgencyObjectHandler
	ctx      context.Context
	cancel   context.CancelFunc
	budget   chan struct{}

	mu             sync.Mutex
	activeHandlers uint32
	handlersDone   chan struct{}
	closed         bool
	closeOnce      sync.Once
}

func NewAgencyServer(lifetime context.Context,
	options AgencyServerOptions,
) (*AgencyServer, error) {
	if lifetime == nil || lifetime.Err() != nil || options.Host == nil ||
		isNilAgencyValue(options.Delivery) || isNilAgencyValue(options.Object) {
		return nil, fmt.Errorf("%w: live Host and both handlers are required", ErrAgencyServer)
	}
	limit := HermeticLimits().ApplicationProtocolStreams
	if limit <= 0 {
		return nil, fmt.Errorf("%w: invalid stream budget", ErrAgencyServer)
	}
	agencyServerConstructorMu.Lock()
	defer agencyServerConstructorMu.Unlock()
	for _, protocolID := range options.Host.Mux().Protocols() {
		if protocolID == AgencyDeliveryProtocol || protocolID == AgencyObjectProtocol {
			return nil, fmt.Errorf("%w: Host already owns an Agency protocol", ErrAgencyServer)
		}
	}
	ownedContext, cancel := context.WithCancel(lifetime)
	server := &AgencyServer{host: options.Host, delivery: options.Delivery,
		object: options.Object, ctx: ownedContext, cancel: cancel,
		budget: make(chan struct{}, limit)}
	options.Host.SetStreamHandler(AgencyDeliveryProtocol, server.handleDelivery)
	options.Host.SetStreamHandler(AgencyObjectProtocol, server.handleObject)
	return server, nil
}

func (server *AgencyServer) handleDelivery(stream network.Stream) {
	server.handle(stream, AgencyDeliveryProtocol, agencyDeliveryRequestTimeout,
		server.serveDelivery, server.writeDeliveryBusy)
}

func (server *AgencyServer) handleObject(stream network.Stream) {
	server.handle(stream, AgencyObjectProtocol, agencyObjectRequestTimeout,
		server.serveObject, server.writeObjectBusy)
}

func (server *AgencyServer) handle(stream network.Stream, expected protocol.ID,
	timeout time.Duration,
	serve func(context.Context, network.Stream, model.PeerID) error,
	writeBusy func(context.Context, network.Stream) error,
) {
	if !server.begin() {
		resetAgencyStream(stream)
		return
	}
	defer server.finishHandler()
	requestContext, remote, cancel, err := server.bind(stream, expected, timeout)
	if err != nil {
		resetAgencyStream(stream)
		return
	}
	defer cancel()
	if !server.admit() {
		if err := writeBusy(requestContext, stream); err != nil {
			resetAgencyStream(stream)
			return
		}
		_ = stream.Close()
		return
	}
	defer func() { <-server.budget }()
	if err := serve(requestContext, stream, remote); err != nil {
		resetAgencyStream(stream)
		return
	}
	_ = stream.Close()
}

func (server *AgencyServer) bind(stream network.Stream, expected protocol.ID,
	timeout time.Duration,
) (context.Context, model.PeerID, context.CancelFunc, error) {
	if server == nil || stream == nil || stream.Scope() == nil || stream.Conn() == nil ||
		stream.Protocol() != expected || stream.Conn().RemotePeer() == "" ||
		stream.Conn().LocalPeer() == "" || stream.Conn().RemotePeer() == stream.Conn().LocalPeer() {
		return nil, model.PeerID{}, nil, fmt.Errorf("%w: invalid Agency stream", ErrAgencyServer)
	}
	remote, _, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	local, _, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	if remoteErr != nil || localErr != nil || remote == local {
		return nil, model.PeerID{}, nil, fmt.Errorf("%w: unauthenticated Agency stream", ErrAgencyServer)
	}
	deadline := time.Now().Add(timeout)
	if parentDeadline, ok := server.ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return nil, model.PeerID{}, nil, fmt.Errorf("%w: set Agency stream deadline", ErrAgencyServer)
	}
	requestContext, cancel := context.WithDeadline(server.ctx, deadline)
	stop := context.AfterFunc(requestContext, func() { _ = stream.SetDeadline(time.Now()) })
	return requestContext, remote, func() {
		stop()
		cancel()
	}, nil
}

func (server *AgencyServer) serveDelivery(ctx context.Context, stream network.Stream,
	remote model.PeerID,
) error {
	frame, release, err := readAgencyDeliveryStreamFrame(stream, agencyDeliveryFrameBytes)
	if err != nil {
		return fmt.Errorf("%w: read Delivery request: %v", ErrAgencyServer, err)
	}
	defer release()
	offer, ok := frame.Payload().(AgencyDeliveryOffer)
	if frame.Type() != AgencyDeliveryFrameOffer || !ok {
		return fmt.Errorf("%w: first Delivery frame is not an offer", ErrAgencyServer)
	}
	reply, err := server.delivery.HandleAgencyDelivery(ctx, remote, offer)
	if err != nil {
		return fmt.Errorf("%w: Delivery callback: %w", ErrAgencyServer, err)
	}
	if !validAgencyDeliveryReply(offer, reply) {
		return fmt.Errorf("%w: invalid Delivery callback response", ErrAgencyServer)
	}
	response, err := NewAgencyDeliveryFrame(reply)
	if err != nil {
		return fmt.Errorf("%w: construct Delivery response", ErrAgencyServer)
	}
	return writeAgencyDeliveryStreamFrame(stream, response)
}

func validAgencyDeliveryReply(offer AgencyDeliveryOffer, reply AgencyDeliveryReply) bool {
	if isNilAgencyValue(reply) {
		return false
	}
	switch value := reply.(type) {
	case AgencyTransportAck:
		return value.DeliveryID() == offer.DeliveryID() &&
			value.EnvelopeDigest() == offer.EnvelopeDigest()
	case AgencyAdmissionReceipt:
		return value.DeliveryID() == offer.DeliveryID() &&
			value.EnvelopeDigest() == offer.EnvelopeDigest()
	case AgencyProtocolError:
		return !value.IsZero()
	default:
		return false
	}
}

func (server *AgencyServer) serveObject(ctx context.Context, stream network.Stream,
	remote model.PeerID,
) error {
	frame, release, err := readAgencyObjectStreamFrame(stream)
	if err != nil {
		return fmt.Errorf("%w: read Object request: %v", ErrAgencyServer, err)
	}
	defer release()
	request, ok := frame.Payload().(AgencyObjectRequest)
	if frame.Type() != AgencyObjectFrameRequest || !ok {
		return fmt.Errorf("%w: first Object frame is not a request", ErrAgencyServer)
	}
	reply, err := server.object.HandleAgencyObject(ctx, remote, request)
	if err != nil {
		return fmt.Errorf("%w: Object callback: %w", ErrAgencyServer, err)
	}
	if !validAgencyObjectReply(request, reply) {
		return fmt.Errorf("%w: invalid Object callback response", ErrAgencyServer)
	}
	response, err := NewAgencyObjectFrame(reply)
	if err != nil {
		return fmt.Errorf("%w: construct Object response", ErrAgencyServer)
	}
	return writeAgencyObjectStreamFrame(stream, response)
}

func validAgencyObjectReply(request AgencyObjectRequest, reply AgencyObjectReply) bool {
	if isNilAgencyValue(reply) {
		return false
	}
	switch value := reply.(type) {
	case AgencyObjectResponse:
		return value.DeliveryID() == request.DeliveryID() &&
			value.EnvelopeDigest() == request.EnvelopeDigest() &&
			value.ObjectDigest() == request.ObjectDigest()
	case AgencyProtocolError:
		return !value.IsZero()
	default:
		return false
	}
}

func (server *AgencyServer) writeDeliveryBusy(_ context.Context, stream network.Stream) error {
	failure, _ := NewAgencyProtocolError(AgencyProtocolErrorSpec{Code: AgencyErrorBusy,
		RetryAfter: agencyBusyRetry})
	frame, err := NewAgencyDeliveryFrame(failure)
	if err != nil {
		return err
	}
	return writeAgencyDeliveryStreamFrame(stream, frame)
}

func (server *AgencyServer) writeObjectBusy(_ context.Context, stream network.Stream) error {
	failure, _ := NewAgencyProtocolError(AgencyProtocolErrorSpec{Code: AgencyErrorBusy,
		RetryAfter: agencyBusyRetry})
	frame, err := NewAgencyObjectFrame(failure)
	if err != nil {
		return err
	}
	return writeAgencyObjectStreamFrame(stream, frame)
}

func writeAgencyDeliveryStreamFrame(stream network.Stream, frame AgencyDeliveryFrame) error {
	bytes := agencyFrameLengthBytes + len(frame.CanonicalJSON().Bytes())
	release, err := reserveAgencyWrite(stream, bytes)
	if err != nil {
		return err
	}
	defer release()
	return WriteAgencyDeliveryFrame(stream, frame)
}

func writeAgencyObjectStreamFrame(stream network.Stream, frame AgencyObjectFrame) error {
	bytes := agencyFrameLengthBytes + len(frame.CanonicalHeader().Bytes()) + len(frame.body)
	release, err := reserveAgencyWrite(stream, bytes)
	if err != nil {
		return err
	}
	defer release()
	return WriteAgencyObjectFrame(stream, frame)
}

func (server *AgencyServer) admit() bool {
	select {
	case server.budget <- struct{}{}:
		return true
	default:
		return false
	}
}

func (server *AgencyServer) begin() bool {
	if server == nil {
		return false
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.closed || server.ctx == nil || server.ctx.Err() != nil {
		return false
	}
	if server.activeHandlers == 0 {
		server.handlersDone = make(chan struct{})
	}
	server.activeHandlers++
	return true
}

func (server *AgencyServer) finishHandler() {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.activeHandlers == 0 {
		panic("Mnemon Agency server handler accounting underflow")
	}
	server.activeHandlers--
	if server.activeHandlers == 0 {
		close(server.handlersDone)
		server.handlersDone = nil
	}
}

func isNilAgencyValue(value any) bool {
	if value == nil {
		return true
	}
	reflection := reflect.ValueOf(value)
	switch reflection.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflection.IsNil()
	default:
		return false
	}
}

func resetAgencyStream(stream network.Stream) {
	if stream != nil {
		_ = stream.Reset()
	}
}
