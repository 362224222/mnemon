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
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

const eventServerBusyRetry = time.Second

var (
	ErrEventServer = errors.New("Mnemon Events server")

	eventServerConstructorMu sync.Mutex
)

// EventSourceStore is the complete durable surface exposed to the inbound
// Events protocol. In particular, the server cannot query mutable authority
// after either operation or construct publication bytes independently of the
// Store snapshot that advanced the authenticated receiver acknowledgement.
type EventSourceStore interface {
	ReadPeerPullPage(context.Context,
		store.ReadPeerPullPageSpec,
	) (store.PeerPullPage, error)
	CommitPeerPullCursorAck(context.Context,
		store.CommitPeerPullCursorAckSpec,
	) (store.CommitPeerPullCursorAckResult, error)
}

// EventServerClock supplies trusted mnemond time for durable source evidence.
// Transport deadlines deliberately continue to use the process wall clock so
// a test or policy clock cannot freeze or jump socket timeouts.
type EventServerClock interface{ Now() time.Time }

type wallEventServerClock struct{}

func (wallEventServerClock) Now() time.Time { return time.Now() }

type EventServerOptions struct {
	Host   host.Host
	Source EventSourceStore
	Clock  EventServerClock
}

// EventServer is the sole /mnemon/events/1 inbound owner for one Host. Each
// stream carries exactly one request and one response. The stream itself is
// the correlation boundary, while its secure connection is the only source
// of requester identity.
type EventServer struct {
	host   host.Host
	source EventSourceStore
	clock  EventServerClock
	ctx    context.Context
	cancel context.CancelFunc
	budget chan struct{}

	mu             sync.Mutex
	activeHandlers uint32
	handlersDone   chan struct{}
	closed         bool
	closeOnce      sync.Once
}

func NewEventServer(lifetime context.Context, options EventServerOptions) (*EventServer, error) {
	if options.Clock == nil {
		options.Clock = wallEventServerClock{}
	}
	if lifetime == nil || lifetime.Err() != nil || options.Host == nil ||
		isNilEventSourceStore(options.Source) || isNilEventServerClock(options.Clock) {
		return nil, fmt.Errorf("%w: live Host and source Store are required", ErrEventServer)
	}
	limit := HermeticLimits().ApplicationProtocolStreams
	if limit <= 0 {
		return nil, fmt.Errorf("%w: invalid admitted stream limit", ErrEventServer)
	}

	eventServerConstructorMu.Lock()
	defer eventServerConstructorMu.Unlock()
	for _, protocolID := range options.Host.Mux().Protocols() {
		if protocolID == EventsProtocol {
			return nil, fmt.Errorf("%w: Host already owns the Events protocol", ErrEventServer)
		}
	}

	ownedContext, cancel := context.WithCancel(lifetime)
	server := &EventServer{host: options.Host, source: options.Source, clock: options.Clock,
		ctx: ownedContext, cancel: cancel, budget: make(chan struct{}, limit)}
	options.Host.SetStreamHandler(EventsProtocol, server.handle)
	return server, nil
}

func (server *EventServer) handle(stream network.Stream) {
	if !server.begin() {
		if stream != nil {
			_ = stream.Reset()
		}
		return
	}
	defer server.finishHandler()

	if !server.admit() {
		if err := server.writeBusy(stream); err != nil {
			if stream != nil {
				_ = stream.Reset()
			}
			return
		}
		_ = stream.Close()
		return
	}
	defer func() { <-server.budget }()

	if err := server.serve(stream); err != nil {
		if stream != nil {
			_ = stream.Reset()
		}
		return
	}
	_ = stream.Close()
}

func (server *EventServer) begin() bool {
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

func (server *EventServer) finishHandler() {
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.activeHandlers == 0 {
		panic("Mnemon Event server handler accounting underflow")
	}
	server.activeHandlers--
	if server.activeHandlers == 0 {
		close(server.handlersDone)
		server.handlersDone = nil
	}
}

func (server *EventServer) admit() bool {
	select {
	case server.budget <- struct{}{}:
		return true
	default:
		return false
	}
}

func (server *EventServer) serve(stream network.Stream) error {
	if stream == nil || stream.Protocol() != EventsProtocol || stream.Conn() == nil ||
		stream.Conn().RemotePeer() == "" || stream.Conn().LocalPeer() == "" {
		return fmt.Errorf("%w: invalid Events stream", ErrEventServer)
	}
	requester, _, err := secureChannelPeer(stream.Conn().RemotePeer())
	if err != nil {
		return fmt.Errorf("%w: authenticate remote peer", ErrEventServer)
	}
	localPeer, localPublicKey, err := secureChannelPeer(stream.Conn().LocalPeer())
	if err != nil || localPeer == requester {
		return fmt.Errorf("%w: authenticate local peer", ErrEventServer)
	}

	requestContext, cancel, err := server.bindStream(stream)
	if err != nil {
		return err
	}
	defer cancel()

	// Request messages are all small. Applying this fence before reading the
	// typed envelope prevents a peer from using the PullPage response allowance
	// as inbound allocation budget.
	first, release, err := readEventStreamFrame(stream, eventSmallFrameBytes)
	if err != nil {
		return fmt.Errorf("%w: read first frame", ErrEventServer)
	}
	defer release()

	var response EventFramePayload
	switch first.Type() {
	case EventFramePullRequest:
		request, ok := first.Payload().(PullRequest)
		if !ok {
			return fmt.Errorf("%w: invalid PullRequest payload", ErrEventServer)
		}
		response, err = server.pull(requestContext, requester, localPeer, localPublicKey, request)
	case EventFrameCursorAck:
		acknowledgement, ok := first.Payload().(CursorAck)
		if !ok {
			return fmt.Errorf("%w: invalid CursorAck payload", ErrEventServer)
		}
		response, err = server.acknowledge(requestContext, requester, acknowledgement)
	default:
		return fmt.Errorf("%w: first frame is not a request", ErrEventServer)
	}
	if err != nil {
		failure, safe := eventSourceProtocolFailure(err)
		if !safe {
			return fmt.Errorf("%w: source operation failed", ErrEventServer)
		}
		response = failure
	}
	frame, err := NewEventFrame(response)
	if err != nil {
		return fmt.Errorf("%w: construct response", ErrEventServer)
	}
	if err := writeEventStreamFrame(stream, frame); err != nil {
		return fmt.Errorf("%w: write response", ErrEventServer)
	}
	return nil
}

func (server *EventServer) bindStream(stream network.Stream) (context.Context,
	context.CancelFunc, error,
) {
	deadline := time.Now().Add(HermeticLimits().EventRequestTimeout)
	if parentDeadline, ok := server.ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return nil, nil, fmt.Errorf("%w: set stream deadline", ErrEventServer)
	}
	requestContext, cancel := context.WithDeadline(server.ctx, deadline)
	stopCancellation := context.AfterFunc(requestContext,
		func() { _ = stream.SetDeadline(time.Now()) })
	return requestContext, func() {
		stopCancellation()
		cancel()
	}, nil
}

func (server *EventServer) pull(ctx context.Context, requester, localPeer model.PeerID,
	localPublicKey []byte, request PullRequest,
) (EventFramePayload, error) {
	at, err := server.sourceTime()
	if err != nil {
		return nil, err
	}
	page, err := server.source.ReadPeerPullPage(ctx, store.ReadPeerPullPageSpec{
		AuthenticatedPeerID: requester, ChannelID: request.ChannelID(),
		OriginEpoch: request.OriginEpoch(), AfterChannelSequence: request.AfterChannelSequence(),
		Limit: request.Limit(), At: at,
	})
	if err != nil {
		return nil, err
	}
	if err := validateEventSourcePage(localPeer, localPublicKey, request, page); err != nil {
		return nil, err
	}
	payload, err := NewPullPage(PullPageSpec{Publications: page.Publications,
		ScannedChannelSequence: page.ScannedChannelSequence, SourceFloor: page.SourceFloor,
		SourceHead: page.SourceHead, OriginEpoch: page.OriginEpoch})
	if err != nil {
		return nil, fmt.Errorf("%w: source returned an invalid page", store.ErrPeerPullInvariant)
	}
	return payload, nil
}

func (server *EventServer) acknowledge(ctx context.Context, requester model.PeerID,
	acknowledgement CursorAck,
) (EventFramePayload, error) {
	at, err := server.sourceTime()
	if err != nil {
		return nil, err
	}
	_, err = server.source.CommitPeerPullCursorAck(ctx, store.CommitPeerPullCursorAckSpec{
		AuthenticatedPeerID: requester, ChannelID: acknowledgement.ChannelID(),
		OriginEpoch:               acknowledgement.OriginEpoch(),
		ContiguousChannelSequence: acknowledgement.ContiguousChannelSequence(), At: at,
	})
	if err != nil {
		return nil, err
	}
	payload, err := NewEventAck()
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (server *EventServer) sourceTime() (time.Time, error) {
	if server == nil || isNilEventServerClock(server.clock) {
		return time.Time{}, fmt.Errorf("%w: trusted source clock is unavailable", ErrEventServer)
	}
	at := server.clock.Now().Round(0).UTC()
	if at.IsZero() {
		return time.Time{}, fmt.Errorf("%w: trusted source clock returned zero", ErrEventServer)
	}
	return at, nil
}

func eventSourceProtocolFailure(cause error) (EventProtocolError, bool) {
	var specification EventProtocolErrorSpec
	var historyGap store.PeerPullHistoryGap
	switch {
	case errors.As(cause, &historyGap):
		specification = EventProtocolErrorSpec{Code: EventErrorHistoryGap,
			SourceFloor: historyGap.SourceFloor}
	case errors.Is(cause, store.ErrPeerPullEpochMismatch):
		specification = EventProtocolErrorSpec{Code: EventErrorOriginEpochMismatch}
	case errors.Is(cause, store.ErrPeerPullNotOrigin):
		specification = EventProtocolErrorSpec{Code: EventErrorNotOrigin}
	case errors.Is(cause, store.ErrPeerPullMemberRevoked):
		specification = EventProtocolErrorSpec{Code: EventErrorMemberRevoked}
	case errors.Is(cause, store.ErrPeerPullChannelClosed):
		specification = EventProtocolErrorSpec{Code: EventErrorChannelClosed}
	case errors.Is(cause, store.ErrPeerPullNotMember):
		specification = EventProtocolErrorSpec{Code: EventErrorNotMember}
	case errors.Is(cause, store.ErrPeerPullAuthority):
		specification = EventProtocolErrorSpec{Code: EventErrorNotMember}
	case errors.Is(cause, context.DeadlineExceeded):
		specification = EventProtocolErrorSpec{Code: EventErrorBusy, Retryable: true,
			RetryAfter: eventServerBusyRetry}
	default:
		return EventProtocolError{}, false
	}
	payload, err := NewEventProtocolError(specification)
	return payload, err == nil
}

func (server *EventServer) writeBusy(stream network.Stream) error {
	if stream == nil || stream.Protocol() != EventsProtocol || stream.Conn() == nil {
		return fmt.Errorf("%w: invalid overloaded stream", ErrEventServer)
	}
	remotePeer, _, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	localPeer, _, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	if remoteErr != nil || localErr != nil || remotePeer == localPeer {
		return fmt.Errorf("%w: unauthenticated overloaded stream", ErrEventServer)
	}
	deadline := time.Now().Add(eventServerBusyRetry)
	if server != nil && server.ctx != nil {
		if parentDeadline, ok := server.ctx.Deadline(); ok && parentDeadline.Before(deadline) {
			deadline = parentDeadline
		}
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return fmt.Errorf("%w: set overload deadline", ErrEventServer)
	}
	stopCancellation := context.AfterFunc(server.ctx,
		func() { _ = stream.SetDeadline(time.Now()) })
	defer stopCancellation()
	payload, err := NewEventProtocolError(EventProtocolErrorSpec{Code: EventErrorBusy,
		Retryable: true, RetryAfter: eventServerBusyRetry})
	if err != nil {
		return err
	}
	frame, err := NewEventFrame(payload)
	if err != nil {
		return err
	}
	return writeEventStreamFrame(stream, frame)
}

func writeEventStreamFrame(stream network.Stream, frame EventFrame) error {
	if stream == nil || stream.Scope() == nil || frame.IsZero() {
		return fmt.Errorf("%w: live stream scope and response frame are required", ErrEventServer)
	}
	reserved := len(frame.canonical.Bytes())
	if reserved <= 0 || reserved > maxEventFrameBytes() {
		return fmt.Errorf("%w: response frame exceeds its memory bound", ErrEventServer)
	}
	if err := stream.Scope().ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
		return fmt.Errorf("%w: reserve response frame memory: %v", ErrEventServer, err)
	}
	defer stream.Scope().ReleaseMemory(reserved)
	return WriteEventFrame(stream, frame)
}

func isNilEventServerClock(clock EventServerClock) bool {
	if clock == nil {
		return true
	}
	value := reflect.ValueOf(clock)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilEventSourceStore(source EventSourceStore) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
