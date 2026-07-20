package peer

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelMemberClient          = errors.New("Mnemon Channel member client")
	ErrChannelMemberClientTransport = fmt.Errorf("%w: transport unavailable", ErrChannelMemberClient)
	ErrChannelMemberClientResponse  = fmt.Errorf("%w: invalid authenticated response", ErrChannelMemberClient)
)

type ChannelMemberClientOptions struct {
	Host   host.Host
	Random io.Reader
}

// ChannelMemberClient performs one authenticated member-control operation per
// stream. Address discovery and retry ownership belong to the reconciler.
type ChannelMemberClient struct {
	host   host.Host
	random io.Reader
}

func NewChannelMemberClient(options ChannelMemberClientOptions) (*ChannelMemberClient, error) {
	if options.Host == nil {
		return nil, fmt.Errorf("%w: live Host is required", ErrChannelMemberClient)
	}
	local, _, err := secureChannelPeer(options.Host.ID())
	if err != nil || local.IsZero() {
		return nil, fmt.Errorf("%w: Host must use a canonical Ed25519 identity", ErrChannelMemberClient)
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	return &ChannelMemberClient{host: options.Host, random: options.Random}, nil
}

func (client *ChannelMemberClient) Hello(ctx context.Context, remote model.PeerID,
	remotePublicKey []byte, hello MemberHello,
) (MemberHelloAck, error) {
	session, err := client.open(ctx, remote, remotePublicKey, hello)
	if err != nil {
		return MemberHelloAck{}, err
	}
	defer session.finish()
	response, err := session.read(maxChannelFrameBytes())
	if err != nil {
		return MemberHelloAck{}, err
	}
	ack, ok := response.Payload().(MemberHelloAck)
	if response.Type() != ChannelFrameMemberHelloAck || !ok ||
		!validMemberHelloAck(hello, ack) {
		return MemberHelloAck{}, ErrChannelMemberClientResponse
	}
	session.completed = true
	return ack, nil
}

func (client *ChannelMemberClient) Sync(ctx context.Context, remote model.PeerID,
	remotePublicKey []byte, request SyncRequest,
) ([]SyncPage, error) {
	session, err := client.open(ctx, remote, remotePublicKey, request)
	if err != nil {
		return nil, err
	}
	defer session.finish()
	pages := make([]SyncPage, 0, 4)
	next := request.AfterHead()
	var frozen model.RecordHead
	for {
		response, err := session.read(maxChannelFrameBytes())
		if err != nil {
			return nil, err
		}
		page, ok := response.Payload().(SyncPage)
		if response.Type() != ChannelFrameSyncPage || !ok || page.ChannelID() != request.ChannelID() ||
			len(pages) >= model.MaxMemberRecordsPerChannel ||
			!validMemberSyncPage(next, frozen, page) {
			return nil, ErrChannelMemberClientResponse
		}
		pages = append(pages, page)
		if frozen.IsZero() {
			frozen = page.RosterHead()
		}
		records := page.OwnerSignedRecords()
		if len(records) > 0 {
			next = records[len(records)-1].Head()
		}
		if !page.More() {
			break
		}
	}
	if next != frozen {
		return nil, ErrChannelMemberClientResponse
	}
	session.completed = true
	return pages, nil
}

func (client *ChannelMemberClient) InstallBaseline(ctx context.Context, remote model.PeerID,
	remotePublicKey []byte, baseline DataBaseline,
) (DataBaselineAck, error) {
	session, err := client.open(ctx, remote, remotePublicKey, baseline)
	if err != nil {
		return DataBaselineAck{}, err
	}
	defer session.finish()
	response, err := session.read(maxChannelFrameBytes())
	if err != nil {
		return DataBaselineAck{}, err
	}
	ack, ok := response.Payload().(DataBaselineAck)
	if response.Type() != ChannelFrameDataBaselineAck || !ok ||
		ack.ChannelID() != baseline.ChannelID() || ack.OriginPeerID() != baseline.OriginPeerID() ||
		ack.OriginEpoch() != baseline.OriginEpoch() ||
		ack.BaselineChannelSequence() != baseline.BaselineChannelSequence() {
		return DataBaselineAck{}, ErrChannelMemberClientResponse
	}
	session.completed = true
	return ack, nil
}

type channelMemberClientSession struct {
	stream    network.Stream
	requestID ChannelRequestID
	ctx       context.Context
	cancel    context.CancelFunc
	stop      func() bool
	completed bool
}

func (client *ChannelMemberClient) open(ctx context.Context, remote model.PeerID,
	remotePublicKey []byte, payload ChannelFramePayload,
) (*channelMemberClientSession, error) {
	if !validChannelMemberClientInput(client, ctx, remote, remotePublicKey, payload) {
		return nil, fmt.Errorf("%w: complete operation input is required", ErrChannelMemberClient)
	}
	remoteID, requestID, request, err := client.prepare(remote, payload)
	if err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, HermeticLimits().ChannelRequestTimeout)
	stream, err := client.host.NewStream(requestCtx, remoteID, ChannelProtocol)
	if err != nil {
		cancel()
		return nil, channelMemberClientTransportFailure(ctx, err)
	}
	fail := func(cause error) (*channelMemberClientSession, error) {
		_ = stream.Reset()
		cancel()
		return nil, cause
	}
	if !client.authenticatedStream(stream, remoteID, remote, remotePublicKey) {
		return fail(ErrChannelMemberClientResponse)
	}
	deadline, ok := requestCtx.Deadline()
	if !ok || stream.SetDeadline(deadline) != nil {
		return fail(ErrChannelMemberClientTransport)
	}
	stop := context.AfterFunc(requestCtx, func() { _ = stream.SetDeadline(time.Now()) })
	session := &channelMemberClientSession{stream: stream, requestID: requestID,
		ctx: requestCtx, cancel: cancel, stop: stop}
	if err := writeChannelMemberClientFrame(stream, request); err != nil {
		session.finish()
		return nil, channelMemberClientTransportFailure(requestCtx, err)
	}
	return session, nil
}

func validChannelMemberClientInput(client *ChannelMemberClient, ctx context.Context,
	remote model.PeerID, remotePublicKey []byte, payload ChannelFramePayload,
) bool {
	return client != nil && client.host != nil && client.random != nil && ctx != nil &&
		!remote.IsZero() && len(remotePublicKey) == 32 && payload != nil
}

func (client *ChannelMemberClient) prepare(remote model.PeerID,
	payload ChannelFramePayload,
) (libp2ppeer.ID, ChannelRequestID, ChannelFrame, error) {
	remoteID, err := canonicalLibp2pID(remote)
	if err != nil || remoteID == client.host.ID() {
		return "", ChannelRequestID{}, ChannelFrame{},
			fmt.Errorf("%w: exact remote member is required", ErrChannelMemberClient)
	}
	requestID, err := NewChannelRequestID(client.random)
	if err != nil {
		return "", ChannelRequestID{}, ChannelFrame{},
			fmt.Errorf("%w: request identity unavailable", ErrChannelMemberClient)
	}
	request, err := NewChannelFrame(requestID, payload)
	if err != nil {
		return "", ChannelRequestID{}, ChannelFrame{},
			fmt.Errorf("%w: invalid local request", ErrChannelMemberClient)
	}
	return remoteID, requestID, request, nil
}

func (client *ChannelMemberClient) authenticatedStream(stream network.Stream,
	remoteID libp2ppeer.ID, remote model.PeerID, remotePublicKey []byte,
) bool {
	if stream == nil || stream.Conn() == nil || stream.Conn().LocalPeer() != client.host.ID() ||
		stream.Conn().RemotePeer() != remoteID || stream.Protocol() != ChannelProtocol {
		return false
	}
	secureRemote, secureKey, err := secureChannelPeer(stream.Conn().RemotePeer())
	return err == nil && secureRemote == remote && bytes.Equal(secureKey, remotePublicKey)
}

func (session *channelMemberClientSession) read(maximum int) (ChannelFrame, error) {
	frame, release, err := readChannelStreamFrame(session.stream, maximum)
	if err != nil {
		return ChannelFrame{}, channelMemberClientTransportFailure(session.ctx, err)
	}
	defer release()
	if frame.RequestID() != session.requestID {
		return ChannelFrame{}, ErrChannelMemberClientResponse
	}
	if failure := receivedChannelFailure(session.requestID, frame); failure != nil {
		session.completed = true
		return ChannelFrame{}, failure
	}
	return frame, nil
}

func (session *channelMemberClientSession) finish() {
	if session == nil {
		return
	}
	if session.stop != nil {
		session.stop()
	}
	if session.cancel != nil {
		session.cancel()
	}
	if session.stream != nil {
		if session.completed {
			_ = session.stream.Close()
		} else {
			_ = session.stream.Reset()
		}
	}
}

func writeChannelMemberClientFrame(stream network.Stream, frame ChannelFrame) error {
	if stream == nil || stream.Scope() == nil || frame.IsZero() {
		return ErrChannelMemberClientTransport
	}
	reserved := len(frame.CanonicalJSON().Bytes())
	if reserved <= 0 || reserved > maxChannelFrameBytes() ||
		stream.Scope().ReserveMemory(reserved, network.ReservationPriorityAlways) != nil {
		return ErrChannelMemberClientTransport
	}
	defer stream.Scope().ReleaseMemory(reserved)
	return WriteChannelFrame(stream, frame)
}

func validMemberHelloAck(hello MemberHello, ack MemberHelloAck) bool {
	if ack.ChannelID() != hello.ChannelID() || ack.RosterHead().Revision() < hello.KnownRosterHead().Revision() {
		return false
	}
	records := ack.MissingRecords()
	if len(records) == 0 {
		return ack.RosterHead() == hello.KnownRosterHead()
	}
	previous := hello.KnownRosterHead()
	for _, record := range records {
		digest, ok := record.PreviousDigest()
		if !ok || record.ChannelID() != hello.ChannelID() ||
			record.Head().Revision() != previous.Revision()+1 || digest != previous.Digest() {
			return false
		}
		previous = record.Head()
	}
	return previous == ack.RosterHead()
}

func validMemberSyncPage(after, frozen model.RecordHead, page SyncPage) bool {
	if after.IsZero() || page.RosterHead().Revision() < after.Revision() ||
		!frozen.IsZero() && page.RosterHead() != frozen {
		return false
	}
	records := page.OwnerSignedRecords()
	if len(records) == 0 {
		return !page.More() && page.RosterHead() == after
	}
	previous := after
	for _, record := range records {
		digest, ok := record.PreviousDigest()
		if !ok || record.Head().Revision() != previous.Revision()+1 || digest != previous.Digest() {
			return false
		}
		previous = record.Head()
	}
	return page.More() && previous.Revision() < page.RosterHead().Revision() ||
		!page.More() && previous == page.RosterHead()
}

func channelMemberClientTransportFailure(ctx context.Context, cause error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(cause, ErrChannelMemberClientResponse) {
		return cause
	}
	transport := errors.Is(cause, io.EOF) || errors.Is(cause, io.ErrUnexpectedEOF) ||
		errors.Is(cause, network.ErrReset) || errors.Is(cause, network.ErrResourceLimitExceeded) ||
		errors.Is(cause, network.ErrResourceScopeClosed)
	var networkError net.Error
	if transport || errors.As(cause, &networkError) {
		return ErrChannelMemberClientTransport
	}
	if errors.Is(cause, ErrChannelFrame) {
		return ErrChannelMemberClientResponse
	}
	return ErrChannelMemberClientTransport
}
