package peer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrEventClient          = errors.New("Mnemon Events client")
	ErrEventClientTransport = fmt.Errorf("%w: transport unavailable", ErrEventClient)
	ErrEventClientResponse  = fmt.Errorf("%w: invalid response", ErrEventClient)
)

// EventRemoteFailure is the closed, diagnostic-free failure returned by an
// authenticated origin. Stable codes, rather than remote error strings, drive
// repair retry and terminal decisions.
type EventRemoteFailure struct {
	code        EventProtocolErrorCode
	retryable   bool
	retryAfter  time.Duration
	sourceFloor uint64
}

func (failure *EventRemoteFailure) Error() string {
	if failure == nil || !failure.code.Valid() {
		return ErrEventClient.Error()
	}
	return fmt.Sprintf("%s: %s", ErrEventClient, failure.code)
}

func (failure *EventRemoteFailure) Unwrap() error { return ErrEventClient }
func (failure *EventRemoteFailure) Code() EventProtocolErrorCode {
	if failure == nil {
		return ""
	}
	return failure.code
}
func (failure *EventRemoteFailure) Retryable() bool {
	return failure != nil && failure.retryable
}
func (failure *EventRemoteFailure) RetryAfter() time.Duration {
	if failure == nil {
		return 0
	}
	return failure.retryAfter
}
func (failure *EventRemoteFailure) SourceFloor() uint64 {
	if failure == nil {
		return 0
	}
	return failure.sourceFloor
}

type EventClientOptions struct{ Host host.Host }

// EventClient opens one exact-origin Events stream per request. Connection
// establishment belongs to the mesh reconciler; this client neither discovers
// Peers nor imports remote addresses.
type EventClient struct{ host host.Host }

func NewEventClient(options EventClientOptions) (*EventClient, error) {
	if options.Host == nil {
		return nil, fmt.Errorf("%w: live Host is required", ErrEventClient)
	}
	localPeer, _, err := secureChannelPeer(options.Host.ID())
	if err != nil || localPeer.IsZero() {
		return nil, fmt.Errorf("%w: Host must use a canonical Ed25519 identity", ErrEventClient)
	}
	return &EventClient{host: options.Host}, nil
}

// Pull fetches one bounded page from the publication origin. It authenticates
// every supported or unsupported publication evidence item before returning;
// durable roster and Inbox validation remains the Store's responsibility.
func (client *EventClient) Pull(ctx context.Context, origin model.PeerID,
	request PullRequest,
) (PullPage, error) {
	if request.IsZero() {
		return PullPage{}, fmt.Errorf("%w: complete PullRequest is required", ErrEventClient)
	}
	response, err := client.exchange(ctx, origin, request, eventPullPageFrameBytes)
	if err != nil {
		return PullPage{}, err
	}
	page, ok := response.Payload().(PullPage)
	if response.Type() != EventFramePullPage || !ok {
		return PullPage{}, ErrEventClientResponse
	}
	return page, nil
}

// Acknowledge sends only a cursor already proven contiguous by durable local
// Inbox evidence. The caller owns that proof and the retry schedule.
func (client *EventClient) Acknowledge(ctx context.Context, origin model.PeerID,
	acknowledgement CursorAck,
) error {
	if acknowledgement.IsZero() {
		return fmt.Errorf("%w: complete CursorAck is required", ErrEventClient)
	}
	response, err := client.exchange(ctx, origin, acknowledgement, eventSmallFrameBytes)
	if err != nil {
		return err
	}
	if _, ok := response.Payload().(EventAck); response.Type() != EventFrameAck || !ok {
		return ErrEventClientResponse
	}
	return nil
}

func (client *EventClient) exchange(ctx context.Context, origin model.PeerID,
	request EventFramePayload, responseMaximum int,
) (EventFrame, error) {
	if client == nil || client.host == nil || ctx == nil || origin.IsZero() || request == nil ||
		responseMaximum <= 0 || responseMaximum > maxEventFrameBytes() {
		return EventFrame{}, fmt.Errorf("%w: complete exchange input is required", ErrEventClient)
	}
	if err := ctx.Err(); err != nil {
		return EventFrame{}, err
	}
	originID, err := canonicalLibp2pID(origin)
	if err != nil || originID == client.host.ID() {
		return EventFrame{}, fmt.Errorf("%w: exact remote origin is required", ErrEventClient)
	}
	frame, err := NewEventFrame(request)
	if err != nil {
		return EventFrame{}, fmt.Errorf("%w: invalid local request", ErrEventClient)
	}

	stream, err := client.host.NewStream(ctx, originID, EventsProtocol)
	if err != nil {
		return EventFrame{}, eventClientTransportFailure(ctx)
	}
	completed := false
	defer func() {
		if completed {
			_ = stream.Close()
		} else {
			_ = stream.Reset()
		}
	}()

	remoteKey, err := authenticateEventClientStream(client.host, stream, origin)
	if err != nil {
		return EventFrame{}, ErrEventClientResponse
	}
	requestContext, cancel, err := bindEventClientStream(ctx, stream)
	if err != nil {
		return EventFrame{}, eventClientTransportFailure(ctx)
	}
	defer cancel()

	if err := writeEventClientFrame(stream, frame); err != nil {
		return EventFrame{}, eventClientTransportFailure(requestContext)
	}
	response, release, err := readEventStreamFrame(stream, responseMaximum)
	if err != nil {
		return EventFrame{}, eventClientReadFailure(requestContext, err)
	}
	defer release()
	if response.Type() == EventFrameProtocolError {
		failure, ok := response.Payload().(EventProtocolError)
		if !ok {
			return EventFrame{}, ErrEventClientResponse
		}
		completed = true
		return EventFrame{}, &EventRemoteFailure{code: failure.Code(),
			retryable: failure.Retryable(), retryAfter: failure.RetryAfter(),
			sourceFloor: failure.SourceFloor()}
	}
	switch typed := request.(type) {
	case PullRequest:
		page, ok := response.Payload().(PullPage)
		if response.Type() != EventFramePullPage || !ok ||
			!validEventClientPage(origin, remoteKey, typed, page) {
			return EventFrame{}, ErrEventClientResponse
		}
	case CursorAck:
		if _, ok := response.Payload().(EventAck); response.Type() != EventFrameAck || !ok {
			return EventFrame{}, ErrEventClientResponse
		}
	default:
		return EventFrame{}, fmt.Errorf("%w: request is not a client operation", ErrEventClient)
	}
	completed = true
	return response, nil
}

func authenticateEventClientStream(local host.Host, stream network.Stream,
	origin model.PeerID,
) ([]byte, error) {
	if local == nil || stream == nil || stream.Scope() == nil || stream.Conn() == nil ||
		stream.Protocol() != EventsProtocol || stream.Conn().RemotePeer() == "" ||
		stream.Conn().LocalPeer() != local.ID() {
		return nil, ErrEventClientResponse
	}
	remotePeer, remoteKey, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	localPeer, _, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	if remoteErr != nil || localErr != nil || remotePeer != origin || remotePeer == localPeer {
		return nil, ErrEventClientResponse
	}
	return remoteKey, nil
}

func bindEventClientStream(ctx context.Context, stream network.Stream) (context.Context,
	context.CancelFunc, error,
) {
	deadline := time.Now().Add(HermeticLimits().EventRequestTimeout)
	if parentDeadline, ok := ctx.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return nil, nil, err
	}
	requestContext, cancel := context.WithDeadline(ctx, deadline)
	stopCancellation := context.AfterFunc(requestContext,
		func() { _ = stream.SetDeadline(time.Now()) })
	return requestContext, func() {
		stopCancellation()
		cancel()
	}, nil
}

func writeEventClientFrame(stream network.Stream, frame EventFrame) error {
	if stream == nil || stream.Scope() == nil || frame.IsZero() {
		return ErrEventClientTransport
	}
	reserved := len(frame.CanonicalJSON().Bytes())
	if reserved <= 0 || reserved > eventSmallFrameBytes {
		return ErrEventClientTransport
	}
	if err := stream.Scope().ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
		return ErrEventClientTransport
	}
	defer stream.Scope().ReleaseMemory(reserved)
	return WriteEventFrame(stream, frame)
}

func validEventClientPage(origin model.PeerID, remoteKey []byte, request PullRequest,
	page PullPage,
) bool {
	if origin.IsZero() || len(remoteKey) != 32 || request.IsZero() || page.IsZero() ||
		page.OriginEpoch() != request.OriginEpoch() || page.SourceFloor() == 0 ||
		page.SourceFloor() > model.MaxSQLiteInteger || page.SourceHead() > model.MaxSQLiteInteger ||
		request.AfterChannelSequence() < page.SourceFloor()-1 ||
		page.ScannedChannelSequence() < request.AfterChannelSequence() ||
		page.ScannedChannelSequence() > page.SourceHead() ||
		len(page.Publications()) > int(request.Limit()) {
		return false
	}
	publications := page.Publications()
	if len(publications) == 0 {
		return page.ScannedChannelSequence() == request.AfterChannelSequence() &&
			page.SourceHead() == request.AfterChannelSequence()
	}
	if request.AfterChannelSequence() == model.MaxSQLiteInteger {
		return false
	}
	expected := request.AfterChannelSequence() + 1
	for _, publication := range publications {
		if publication.ChannelID() != request.ChannelID() ||
			publication.OriginPeerID() != origin ||
			publication.OriginEpoch() != request.OriginEpoch() ||
			publication.ChannelSequence() != expected ||
			model.VerifyPublicationEvidence(remoteKey, publication) != nil {
			return false
		}
		expected++
	}
	return expected-1 == page.ScannedChannelSequence()
}

func eventClientTransportFailure(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrEventClientTransport
}

func eventClientReadFailure(ctx context.Context, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	transport := errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) ||
		errors.Is(cause, network.ErrReset) || errors.Is(cause, network.ErrResourceLimitExceeded) ||
		errors.Is(cause, network.ErrResourceScopeClosed)
	var networkFailure net.Error
	if transport || errors.As(cause, &networkFailure) {
		return ErrEventClientTransport
	}
	return ErrEventClientResponse
}
