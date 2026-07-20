package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelMemberClient          = errors.New("Mnemon Channel member client")
	ErrChannelMemberClientTransport = fmt.Errorf("%w: transport unavailable", ErrChannelMemberClient)
	ErrChannelMemberClientResponse  = fmt.Errorf("%w: invalid response", ErrChannelMemberClient)
)

type ChannelMemberClientOptions struct{ Host host.Host }

// ChannelMemberClient performs one authenticated member-control exchange per
// stream. Discovery, retry, durable roster merge and runtime installation
// remain responsibilities of the bounded mesh reconciler.
type ChannelMemberClient struct {
	host           host.Host
	localPeerID    model.PeerID
	localPublicKey []byte
}

func NewChannelMemberClient(options ChannelMemberClientOptions) (*ChannelMemberClient, error) {
	if nilChannelMemberClientHost(options.Host) ||
		HermeticLimits().ChannelRequestTimeout != 10*time.Second {
		return nil, fmt.Errorf("%w: live Host and frozen request limit are required",
			ErrChannelMemberClient)
	}
	localPeerID, localPublicKey, err := secureChannelPeer(options.Host.ID())
	if err != nil || localPeerID.IsZero() {
		return nil, fmt.Errorf("%w: Host must use a canonical Ed25519 identity",
			ErrChannelMemberClient)
	}
	return &ChannelMemberClient{host: options.Host, localPeerID: localPeerID,
		localPublicKey: localPublicKey}, nil
}

func (client *ChannelMemberClient) Hello(ctx context.Context, remote model.PeerID,
	request MemberHello,
) (MemberHelloAck, error) {
	if !client.validRequest(request) {
		return MemberHelloAck{}, fmt.Errorf("%w: local MemberHello identity is invalid",
			ErrChannelMemberClient)
	}
	call, err := client.start(ctx, remote, request)
	if err != nil {
		return MemberHelloAck{}, err
	}
	completed := false
	defer func() { call.close(completed) }()
	response, err := call.read(channelFrameMaximum(ChannelFrameMemberHelloAck))
	if err != nil {
		return MemberHelloAck{}, err
	}
	if failure, responseErr := channelMemberResponseFailure(response, call.requestID); responseErr != nil {
		return MemberHelloAck{}, responseErr
	} else if failure != nil {
		completed = true
		return MemberHelloAck{}, failure
	}
	ack, ok := response.Payload().(MemberHelloAck)
	if response.Type() != ChannelFrameMemberHelloAck || !ok || !validMemberHelloAck(request, ack) {
		return MemberHelloAck{}, ErrChannelMemberClientResponse
	}
	completed = true
	return ack, nil
}

func (client *ChannelMemberClient) Sync(ctx context.Context, remote model.PeerID,
	request SyncRequest,
) (ChannelMemberSyncResult, error) {
	if !client.validRequest(request) {
		return ChannelMemberSyncResult{}, fmt.Errorf("%w: complete SyncRequest is required",
			ErrChannelMemberClient)
	}
	call, err := client.start(ctx, remote, request)
	if err != nil {
		return ChannelMemberSyncResult{}, err
	}
	completed := false
	defer func() { call.close(completed) }()
	state := channelMemberSyncState{channelID: request.ChannelID(), cursor: request.AfterHead()}
	for state.pages < channelMemberSyncPageLimit {
		response, readErr := call.read(channelFrameMaximum(ChannelFrameSyncPage))
		if readErr != nil {
			return ChannelMemberSyncResult{}, readErr
		}
		failure, responseErr := channelMemberResponseFailure(response, call.requestID)
		if responseErr != nil {
			return ChannelMemberSyncResult{}, responseErr
		}
		if failure != nil {
			if state.pages != 0 {
				return ChannelMemberSyncResult{}, ErrChannelMemberClientResponse
			}
			completed = true
			return ChannelMemberSyncResult{}, failure
		}
		page, ok := response.Payload().(SyncPage)
		if response.Type() != ChannelFrameSyncPage || !ok || !state.append(page) {
			return ChannelMemberSyncResult{}, ErrChannelMemberClientResponse
		}
		if !page.More() {
			completed = true
			return state.result(), nil
		}
	}
	return ChannelMemberSyncResult{}, ErrChannelMemberClientResponse
}

func (client *ChannelMemberClient) Baseline(ctx context.Context, remote model.PeerID,
	request DataBaseline,
) (DataBaselineAck, error) {
	if !client.validRequest(request) {
		return DataBaselineAck{}, fmt.Errorf("%w: local DataBaseline identity is invalid",
			ErrChannelMemberClient)
	}
	call, err := client.start(ctx, remote, request)
	if err != nil {
		return DataBaselineAck{}, err
	}
	completed := false
	defer func() { call.close(completed) }()
	response, err := call.read(channelFrameMaximum(ChannelFrameDataBaselineAck))
	if err != nil {
		return DataBaselineAck{}, err
	}
	if failure, responseErr := channelMemberResponseFailure(response, call.requestID); responseErr != nil {
		return DataBaselineAck{}, responseErr
	} else if failure != nil {
		completed = true
		return DataBaselineAck{}, failure
	}
	ack, ok := response.Payload().(DataBaselineAck)
	if response.Type() != ChannelFrameDataBaselineAck || !ok || !sameDataBaseline(request, ack) {
		return DataBaselineAck{}, ErrChannelMemberClientResponse
	}
	completed = true
	return ack, nil
}

type channelMemberClientCall struct {
	stream    network.Stream
	ctx       context.Context
	cancel    context.CancelFunc
	stop      func() bool
	requestID ChannelRequestID
}

func (client *ChannelMemberClient) start(ctx context.Context, remote model.PeerID,
	request ChannelFramePayload,
) (*channelMemberClientCall, error) {
	if client == nil || nilChannelMemberClientHost(client.host) || ctx == nil || remote.IsZero() ||
		!client.validRequest(request) {
		return nil, fmt.Errorf("%w: complete exchange input is required", ErrChannelMemberClient)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	remoteID, err := canonicalLibp2pID(remote)
	if err != nil || remote == client.localPeerID || remoteID == client.host.ID() {
		return nil, fmt.Errorf("%w: exact remote member is required", ErrChannelMemberClient)
	}
	requestID, err := NewChannelRequestID(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("%w: request identity unavailable", ErrChannelMemberClient)
	}
	frame, err := NewChannelFrame(requestID, request)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid local request", ErrChannelMemberClient)
	}
	requestContext, cancel := context.WithTimeout(ctx, HermeticLimits().ChannelRequestTimeout)
	stream, err := client.host.NewStream(requestContext, remoteID, ChannelProtocol)
	if err != nil {
		result := channelMemberTransportFailure(requestContext)
		cancel()
		return nil, result
	}
	call := &channelMemberClientCall{stream: stream, ctx: requestContext,
		cancel: cancel, requestID: requestID}
	if !client.authenticates(stream, remote) {
		call.close(false)
		return nil, ErrChannelMemberClientResponse
	}
	if err := call.bind(); err != nil {
		result := channelMemberTransportFailure(requestContext)
		call.close(false)
		return nil, result
	}
	if err := writeChannelMemberClientFrame(stream, frame); err != nil {
		result := channelMemberTransportFailure(requestContext)
		call.close(false)
		return nil, result
	}
	if err := stream.CloseWrite(); err != nil {
		result := channelMemberTransportFailure(requestContext)
		call.close(false)
		return nil, result
	}
	return call, nil
}

func (client *ChannelMemberClient) validRequest(request ChannelFramePayload) bool {
	if client == nil || request == nil || client.localPeerID.IsZero() || len(client.localPublicKey) != 32 {
		return false
	}
	switch payload := request.(type) {
	case MemberHello:
		member := payload.ActiveMemberRecord()
		return !payload.IsZero() && member.PeerID() == client.localPeerID &&
			bytes.Equal(member.PublicKey(), client.localPublicKey)
	case SyncRequest:
		return !payload.IsZero()
	case DataBaseline:
		return !payload.IsZero() && payload.OriginPeerID() == client.localPeerID
	default:
		return false
	}
}

func (client *ChannelMemberClient) authenticates(stream network.Stream, remote model.PeerID) bool {
	if client == nil || stream == nil || stream.Scope() == nil || stream.Conn() == nil ||
		stream.Protocol() != ChannelProtocol || stream.Conn().LocalPeer() != client.host.ID() {
		return false
	}
	localPeer, localKey, localErr := secureChannelPeer(stream.Conn().LocalPeer())
	remotePeer, _, remoteErr := secureChannelPeer(stream.Conn().RemotePeer())
	return localErr == nil && remoteErr == nil && localPeer == client.localPeerID &&
		bytes.Equal(localKey, client.localPublicKey) && remotePeer == remote && remotePeer != localPeer
}

func (call *channelMemberClientCall) bind() error {
	if call == nil || call.stream == nil || call.ctx == nil {
		return ErrChannelMemberClientTransport
	}
	deadline, ok := call.ctx.Deadline()
	if !ok ||
		time.Until(deadline) > HermeticLimits().ChannelRequestTimeout {
		return ErrChannelMemberClientTransport
	}
	if err := call.stream.SetDeadline(deadline); err != nil {
		return err
	}
	call.stop = context.AfterFunc(call.ctx, func() { _ = call.stream.SetDeadline(time.Now()) })
	return nil
}

func (call *channelMemberClientCall) read(maximum int) (ChannelFrame, error) {
	frame, release, err := readChannelStreamFrame(call.stream, maximum)
	if err != nil {
		return ChannelFrame{}, channelMemberReadFailure(call.ctx, err)
	}
	release()
	return frame, nil
}

func (call *channelMemberClientCall) close(completed bool) {
	if call == nil {
		return
	}
	if call.stop != nil {
		call.stop()
	}
	if call.cancel != nil {
		call.cancel()
	}
	if call.stream == nil {
		return
	}
	if completed {
		_ = call.stream.Close()
	} else {
		_ = call.stream.Reset()
	}
}

func writeChannelMemberClientFrame(stream network.Stream, frame ChannelFrame) error {
	if stream == nil || stream.Scope() == nil || frame.IsZero() {
		return ErrChannelMemberClientTransport
	}
	reserved := len(frame.CanonicalJSON().Bytes())
	if reserved <= 0 || reserved > channelFrameMaximum(frame.Type()) {
		return ErrChannelMemberClientTransport
	}
	if err := stream.Scope().ReserveMemory(reserved, network.ReservationPriorityAlways); err != nil {
		return err
	}
	defer stream.Scope().ReleaseMemory(reserved)
	return WriteChannelFrame(stream, frame)
}

func channelMemberTransportFailure(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
	}
	return ErrChannelMemberClientTransport
}

func channelMemberReadFailure(ctx context.Context, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
	}
	transport := errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) ||
		errors.Is(cause, network.ErrReset) || errors.Is(cause, network.ErrResourceLimitExceeded) ||
		errors.Is(cause, network.ErrResourceScopeClosed)
	var networkFailure net.Error
	if transport || errors.As(cause, &networkFailure) {
		return ErrChannelMemberClientTransport
	}
	return ErrChannelMemberClientResponse
}

func nilChannelMemberClientHost(value host.Host) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}
