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
)

var ErrChannelDispatcher = errors.New("Mnemon Channel dispatcher")

var channelDispatcherConstructorMu sync.Mutex

// ChannelRequestHandler consumes a validated first request frame while the
// dispatcher retains ownership of stream deadline, cancellation and closure.
// Implementations may exchange further typed frames on the same stream.
type ChannelRequestHandler interface {
	HandleChannelRequest(context.Context, network.Stream, ChannelFrame) error
}

type ChannelRequestHandlerFunc func(context.Context, network.Stream, ChannelFrame) error

func (handler ChannelRequestHandlerFunc) HandleChannelRequest(ctx context.Context,
	stream network.Stream, frame ChannelFrame,
) error {
	if handler == nil {
		return fmt.Errorf("%w: request handler is unavailable", ErrChannelDispatcher)
	}
	return handler(ctx, stream, frame)
}

type ChannelDispatcherOptions struct {
	Enrollment ChannelRequestHandler
	Member     ChannelRequestHandler
}

// ChannelDispatcher is the sole owner of /mnemon/channel/1 on one Host. A
// single exact handler prevents libp2p's replace-on-register behavior from
// silently disconnecting enrollment, sync or baseline traffic.
type ChannelDispatcher struct {
	host       host.Host
	ctx        context.Context
	cancel     context.CancelFunc
	enrollment ChannelRequestHandler
	member     ChannelRequestHandler

	mu             sync.Mutex
	activeHandlers uint32
	handlersDone   chan struct{}
	closed         bool
	closeOnce      sync.Once
}

func NewChannelDispatcher(lifetime context.Context, nodeHost host.Host,
	options ChannelDispatcherOptions,
) (*ChannelDispatcher, error) {
	if lifetime == nil || lifetime.Err() != nil || nodeHost == nil ||
		isNilChannelRequestHandler(options.Enrollment) {
		return nil, fmt.Errorf("%w: live Host and enrollment handler are required", ErrChannelDispatcher)
	}
	channelDispatcherConstructorMu.Lock()
	defer channelDispatcherConstructorMu.Unlock()
	for _, protocolID := range nodeHost.Mux().Protocols() {
		if protocolID == ChannelProtocol {
			return nil, fmt.Errorf("%w: Host already owns the Channel protocol", ErrChannelDispatcher)
		}
	}
	ownedContext, cancel := context.WithCancel(lifetime)
	dispatcher := &ChannelDispatcher{host: nodeHost, ctx: ownedContext, cancel: cancel,
		enrollment: options.Enrollment, member: options.Member}
	nodeHost.SetStreamHandler(ChannelProtocol, dispatcher.handle)
	return dispatcher, nil
}

func (dispatcher *ChannelDispatcher) handle(stream network.Stream) {
	if !dispatcher.begin() {
		if stream != nil {
			_ = stream.Reset()
		}
		return
	}
	defer dispatcher.finishHandler()
	if err := dispatcher.serve(stream); err != nil {
		if stream != nil {
			_ = stream.Reset()
		}
		return
	}
	_ = stream.Close()
}

func (dispatcher *ChannelDispatcher) begin() bool {
	if dispatcher == nil {
		return false
	}
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.closed || dispatcher.ctx == nil || dispatcher.ctx.Err() != nil {
		return false
	}
	if dispatcher.activeHandlers == 0 {
		dispatcher.handlersDone = make(chan struct{})
	}
	dispatcher.activeHandlers++
	return true
}

func (dispatcher *ChannelDispatcher) finishHandler() {
	dispatcher.mu.Lock()
	defer dispatcher.mu.Unlock()
	if dispatcher.activeHandlers == 0 {
		panic("Mnemon Channel dispatcher handler accounting underflow")
	}
	dispatcher.activeHandlers--
	if dispatcher.activeHandlers == 0 {
		close(dispatcher.handlersDone)
		dispatcher.handlersDone = nil
	}
}

func (dispatcher *ChannelDispatcher) serve(stream network.Stream) error {
	if stream == nil || stream.Protocol() != ChannelProtocol || stream.Conn() == nil {
		return fmt.Errorf("%w: invalid Channel stream", ErrChannelDispatcher)
	}
	deadline := time.Now().Add(HermeticLimits().ChannelRequestTimeout)
	if parentDeadline, ok := dispatcher.ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return fmt.Errorf("%w: set stream deadline: %v", ErrChannelDispatcher, err)
	}
	requestContext, cancel := context.WithDeadline(dispatcher.ctx, deadline)
	defer cancel()
	stopCancellation := context.AfterFunc(requestContext, func() { _ = stream.SetDeadline(time.Now()) })
	defer stopCancellation()

	first, release, err := readChannelStreamFrame(stream, maxChannelFrameBytes())
	if err != nil {
		return fmt.Errorf("%w: read first frame: %v", ErrChannelDispatcher, err)
	}
	defer release()
	var handler ChannelRequestHandler
	switch first.Type() {
	case ChannelFrameEnrollInit:
		handler = dispatcher.enrollment
	case ChannelFrameMemberHello, ChannelFrameSyncRequest, ChannelFrameDataBaseline,
		ChannelFrameLeaveRequest:
		handler = dispatcher.member
	default:
		return fmt.Errorf("%w: invalid first request frame", ErrChannelDispatcher)
	}
	if isNilChannelRequestHandler(handler) {
		return fmt.Errorf("%w: request route is unavailable", ErrChannelDispatcher)
	}
	if err := handler.HandleChannelRequest(requestContext, stream, first); err != nil {
		return fmt.Errorf("%w: request handler: %v", ErrChannelDispatcher, err)
	}
	return nil
}

func (dispatcher *ChannelDispatcher) Close() error {
	return dispatcher.CloseContext(context.Background())
}

// CloseContext seals handler admission, broadcasts cancellation and waits for
// every handler admitted before the seal. A caller may retry the wait with a
// later context after an earlier deadline expires.
func (dispatcher *ChannelDispatcher) CloseContext(ctx context.Context) error {
	if dispatcher == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("%w: shutdown context is unavailable", ErrChannelDispatcher)
	}
	dispatcher.closeOnce.Do(func() {
		channelDispatcherConstructorMu.Lock()
		dispatcher.mu.Lock()
		dispatcher.closed = true
		if dispatcher.cancel != nil {
			dispatcher.cancel()
		}
		if dispatcher.host != nil {
			dispatcher.host.RemoveStreamHandler(ChannelProtocol)
		}
		dispatcher.mu.Unlock()
		channelDispatcherConstructorMu.Unlock()
	})
	dispatcher.mu.Lock()
	drained := dispatcher.handlersDone
	dispatcher.mu.Unlock()
	if drained == nil {
		return nil
	}
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: drain active handlers: %w", ErrChannelDispatcher, ctx.Err())
	}
}

func isNilChannelRequestHandler(handler ChannelRequestHandler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
