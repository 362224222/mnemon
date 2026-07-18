package peer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestEventServerServesAuthenticatedPullAndCursorAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	originIdentity := testkit.NewIdentity(t, "event-server-success-origin")
	requesterIdentity := testkit.NewIdentity(t, "event-server-success-requester")
	originHost := newEventServerTestHost(t, originIdentity)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requesterIdentity)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	channelID := eventServerChannelID(t, "success")
	epoch := originIdentity.OriginEpoch()
	source := &eventServerTestSource{}
	source.read = func(_ context.Context, spec store.ReadPeerPullPageSpec) (store.PeerPullPage, error) {
		return store.PeerPullPage{Publications: []model.SignedPublication{},
			ScannedChannelSequence: spec.AfterChannelSequence, SourceFloor: 1,
			SourceHead: spec.AfterChannelSequence, OriginEpoch: spec.OriginEpoch,
			AcknowledgedSequence: spec.AfterChannelSequence}, nil
	}
	// A deliberately old domain time proves it is not reused as the socket
	// deadline: the live exchange would time out immediately if the two clocks
	// were conflated.
	trustedAt := time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC)
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source,
		Clock: fixedEventServerClock{at: trustedAt}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	pull, err := NewPullRequest(PullRequestSpec{ChannelID: channelID, OriginEpoch: epoch,
		AfterChannelSequence: 4, Limit: 7})
	if err != nil {
		t.Fatal(err)
	}
	response := exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), pull)
	page, ok := response.Payload().(PullPage)
	if response.Type() != EventFramePullPage || !ok || page.OriginEpoch() != epoch ||
		page.ScannedChannelSequence() != 4 || page.SourceFloor() != 1 || page.SourceHead() != 4 ||
		len(page.Publications()) != 0 {
		t.Fatalf("Pull response = %#v", response)
	}
	readSpec := source.lastReadSpec()
	if readSpec.AuthenticatedPeerID != requesterIdentity.PeerID() ||
		readSpec.ChannelID != channelID || readSpec.OriginEpoch != epoch ||
		readSpec.AfterChannelSequence != 4 || readSpec.Limit != 7 || !readSpec.At.Equal(trustedAt) {
		t.Fatalf("authenticated Pull Store input = %#v", readSpec)
	}

	cursor, err := NewCursorAck(CursorAckSpec{ChannelID: channelID, OriginEpoch: epoch,
		ContiguousChannelSequence: 4})
	if err != nil {
		t.Fatal(err)
	}
	response = exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), cursor)
	if _, ok := response.Payload().(EventAck); response.Type() != EventFrameAck || !ok {
		t.Fatalf("CursorAck response = %#v", response)
	}
	ackSpec := source.lastAckSpec()
	if ackSpec.AuthenticatedPeerID != requesterIdentity.PeerID() ||
		ackSpec.ChannelID != channelID || ackSpec.OriginEpoch != epoch ||
		ackSpec.ContiguousChannelSequence != 4 || !ackSpec.At.Equal(trustedAt) {
		t.Fatalf("authenticated CursorAck Store input = %#v", ackSpec)
	}
	if countProtocol(originHost.Mux().Protocols(), EventsProtocol) != 1 {
		t.Fatal("Events server did not own exactly one protocol handler")
	}
}

func TestEventServerRejectsDuplicateWithoutReplacingLiveServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-duplicate-origin")
	requester := testkit.NewIdentity(t, "event-server-duplicate-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	channelID := eventServerChannelID(t, "duplicate")
	firstSource := eventServerEmptySource(origin.OriginEpoch())
	first, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: firstSource})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	replacementSource := eventServerEmptySource(origin.OriginEpoch())
	replacement, err := NewEventServer(ctx,
		EventServerOptions{Host: originHost, Source: replacementSource})
	if replacement != nil || !errors.Is(err, ErrEventServer) {
		t.Fatalf("duplicate Events server = (%p, %v)", replacement, err)
	}

	request, err := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	response := exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	if response.Type() != EventFramePullPage || firstSource.readCalls.Load() != 1 ||
		replacementSource.readCalls.Load() != 0 {
		t.Fatal("rejected duplicate replaced the live Events server")
	}
}

func TestEventServerClosesEachStreamAfterOneRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-single-origin")
	requester := testkit.NewIdentity(t, "event-server-single-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)
	source := eventServerEmptySource(origin.OriginEpoch())
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, err := NewPullRequest(PullRequestSpec{ChannelID: eventServerChannelID(t, "single"),
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewEventFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	stream := openEventServerTestStream(t, ctx, requesterHost, originHost.ID())
	defer stream.Close()
	if err := WriteEventFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	if response, err := ReadEventFrame(stream); err != nil || response.Type() != EventFramePullPage {
		t.Fatalf("first exchange = (%#v, %v)", response, err)
	}
	// A transport may accept a locally buffered write after the remote close,
	// but it must never produce a second response on this correlation stream.
	if err := WriteEventFrame(stream, frame); err == nil {
		if _, err := ReadEventFrame(stream); err == nil {
			t.Fatal("one Events stream served more than one request")
		}
	}
	if source.readCalls.Load() != 1 {
		t.Fatalf("single stream reached source Store %d times", source.readCalls.Load())
	}
}

func TestEventServerMapsOnlyClosedSourceFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-errors-origin")
	requester := testkit.NewIdentity(t, "event-server-errors-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	source := &eventServerTestSource{}
	var sourceMu sync.Mutex
	var sourceErr error
	source.read = func(context.Context, store.ReadPeerPullPageSpec) (store.PeerPullPage, error) {
		sourceMu.Lock()
		defer sourceMu.Unlock()
		return store.PeerPullPage{}, sourceErr
	}
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, err := NewPullRequest(PullRequestSpec{ChannelID: eventServerChannelID(t, "errors"),
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		err       error
		code      EventProtocolErrorCode
		floor     uint64
		retryable bool
	}{
		{name: "history gap", err: store.PeerPullHistoryGap{SourceFloor: 7},
			code: EventErrorHistoryGap, floor: 7},
		{name: "epoch mismatch", err: store.ErrPeerPullEpochMismatch,
			code: EventErrorOriginEpochMismatch},
		{name: "not origin", err: store.ErrPeerPullNotOrigin, code: EventErrorNotOrigin},
		{name: "member revoked", err: store.ErrPeerPullMemberRevoked,
			code: EventErrorMemberRevoked},
		{name: "channel closed", err: store.ErrPeerPullChannelClosed,
			code: EventErrorChannelClosed},
		{name: "not member", err: store.ErrPeerPullNotMember, code: EventErrorNotMember},
		{name: "authority unavailable", err: store.ErrPeerPullAuthority,
			code: EventErrorNotMember},
		{name: "deadline", err: context.DeadlineExceeded, code: EventErrorBusy, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sourceMu.Lock()
			sourceErr = test.err
			sourceMu.Unlock()
			response := exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), request)
			failure, ok := response.Payload().(EventProtocolError)
			if response.Type() != EventFrameProtocolError || !ok || failure.Code() != test.code ||
				failure.SourceFloor() != test.floor || failure.Retryable() != test.retryable ||
				(test.retryable && failure.RetryAfter() != eventServerBusyRetry) {
				t.Fatalf("mapped failure = %#v", response)
			}
		})
	}

	sourceMu.Lock()
	sourceErr = errors.New("private sqlite diagnostic")
	sourceMu.Unlock()
	assertEventServerReset(t, ctx, requesterHost, originHost.ID(), request)
}

func TestEventServerFencesIncompleteAndUnauthenticatedSourcePages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-source-fence-origin")
	requester := testkit.NewIdentity(t, "event-server-source-fence-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	channelID := eventServerChannelID(t, "source-fence")
	request, err := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	source := &eventServerTestSource{}
	var sourceMu sync.Mutex
	page := store.PeerPullPage{Publications: []model.SignedPublication{},
		ScannedChannelSequence: 0, SourceFloor: 1, SourceHead: 1,
		OriginEpoch: origin.OriginEpoch(), AcknowledgedSequence: 0}
	source.read = func(context.Context, store.ReadPeerPullPageSpec) (store.PeerPullPage, error) {
		sourceMu.Lock()
		defer sourceMu.Unlock()
		return page, nil
	}
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	// An empty page cannot conceal a durable source head beyond the requested
	// cursor. The server resets instead of teaching the receiver a false gap-free
	// fixed point.
	assertEventServerReset(t, ctx, requesterHost, originHost.ID(), request)

	wrongSigner := newEventFramePublication(t, channelID, origin.PeerID(),
		origin.OriginEpoch(), 1, 8)
	sourceMu.Lock()
	page.Publications = []model.SignedPublication{wrongSigner}
	page.ScannedChannelSequence = 1
	sourceMu.Unlock()
	assertEventServerReset(t, ctx, requesterHost, originHost.ID(), request)

	valid := signEventServerPublication(t, origin, wrongSigner.Body())
	sourceMu.Lock()
	page.Publications = []model.SignedPublication{valid}
	sourceMu.Unlock()
	response := exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	payload, ok := response.Payload().(PullPage)
	if response.Type() != EventFramePullPage || !ok || len(payload.Publications()) != 1 ||
		!payload.Publications()[0].IsSupported() ||
		!bytes.Equal(payload.Publications()[0].WireJSON().Bytes(), valid.WireJSON().Bytes()) {
		t.Fatalf("authenticated source response = %#v", response)
	}
}

func TestEventServerRejectsInvalidTrustedClockBeforeStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-zero-clock-origin")
	requester := testkit.NewIdentity(t, "event-server-zero-clock-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	source := eventServerEmptySource(origin.OriginEpoch())
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source,
		Clock: fixedEventServerClock{}})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, err := NewPullRequest(PullRequestSpec{ChannelID: eventServerChannelID(t, "zero-clock"),
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	assertEventServerReset(t, ctx, requesterHost, originHost.ID(), request)
	if source.readCalls.Load() != 0 || source.ackCalls.Load() != 0 {
		t.Fatal("invalid trusted clock reached the source Store")
	}
}

func TestEventServerResetsMalformedOversizedAndNonRequestFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-malformed-origin")
	requester := testkit.NewIdentity(t, "event-server-malformed-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)
	source := eventServerEmptySource(origin.OriginEpoch())
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	stream := openEventServerTestStream(t, ctx, requesterHost, originHost.ID())
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(eventSmallFrameBytes+1))
	if _, err := stream.Write(prefix[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEventFrame(stream); err == nil {
		t.Fatal("oversized first frame received a response instead of reset")
	}
	_ = stream.Close()

	ack, err := NewEventAck()
	if err != nil {
		t.Fatal(err)
	}
	assertEventServerReset(t, ctx, requesterHost, originHost.ID(), ack)
	if source.readCalls.Load() != 0 || source.ackCalls.Load() != 0 {
		t.Fatal("invalid first frames reached the source Store")
	}
}

func TestEventServerRejectsNonEd25519TransportIdentityBeforeStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-identity-origin")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterKey, _, err := libp2pcrypto.GenerateKeyPair(libp2pcrypto.RSA, 2048)
	if err != nil {
		t.Fatal(err)
	}
	requesterHost, err := libp2p.New(libp2p.Identity(requesterKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	source := eventServerEmptySource(origin.OriginEpoch())
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	request, err := NewPullRequest(PullRequestSpec{ChannelID: eventServerChannelID(t, "identity"),
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewEventFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	stream, streamErr := requesterHost.NewStream(ctx, originHost.ID(), EventsProtocol)
	if streamErr == nil {
		defer stream.Close()
		if err := stream.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := WriteEventFrame(stream, frame); err == nil {
			if _, err := ReadEventFrame(stream); err == nil {
				t.Fatal("unsupported transport identity received an Events response")
			}
		}
	}
	if source.readCalls.Load() != 0 || source.ackCalls.Load() != 0 {
		t.Fatal("unsupported transport identity reached the source Store")
	}
}

func TestEventServerBoundsWorkAndCloseCancelsAndDrains(t *testing.T) {
	ctx := context.Background()
	origin := testkit.NewIdentity(t, "event-server-bound-origin")
	requester := testkit.NewIdentity(t, "event-server-bound-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	started := make(chan struct{}, HermeticLimits().ApplicationProtocolStreams)
	source := &eventServerTestSource{}
	source.read = func(ctx context.Context, _ store.ReadPeerPullPageSpec) (store.PeerPullPage, error) {
		started <- struct{}{}
		<-ctx.Done()
		return store.PeerPullPage{}, ctx.Err()
	}
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewPullRequest(PullRequestSpec{ChannelID: eventServerChannelID(t, "bound"),
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewEventFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	streams := make([]network.Stream, 0, HermeticLimits().ApplicationProtocolStreams)
	for index := 0; index < HermeticLimits().ApplicationProtocolStreams; index++ {
		stream := openEventServerTestStream(t, ctx, requesterHost, originHost.ID())
		streams = append(streams, stream)
		if err := WriteEventFrame(stream, frame); err != nil {
			t.Fatal(err)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("admitted request %d did not start", index+1)
		}
	}

	overloaded := openEventServerTestStream(t, ctx, requesterHost, originHost.ID())
	if err := WriteEventFrame(overloaded, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadEventFrame(overloaded)
	if err != nil {
		t.Fatal(err)
	}
	failure, ok := response.Payload().(EventProtocolError)
	if response.Type() != EventFrameProtocolError || !ok || failure.Code() != EventErrorBusy ||
		!failure.Retryable() || failure.RetryAfter() != eventServerBusyRetry {
		t.Fatalf("overload response = %#v", response)
	}
	_ = overloaded.Close()

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Events server Close did not cancel and drain admitted work")
	}
	for _, stream := range streams {
		_ = stream.Close()
	}
	if countProtocol(originHost.Mux().Protocols(), EventsProtocol) != 0 {
		t.Fatal("Events server Close retained its protocol handler")
	}
	if err := server.Close(); err != nil {
		t.Fatal("Events server Close was not idempotent")
	}
}

func TestEventServerResponseLossRetriesTheCommittedSourceOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-server-loss-origin")
	requester := testkit.NewIdentity(t, "event-server-loss-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	committed := make(chan struct{}, 1)
	source := eventServerEmptySource(origin.OriginEpoch())
	source.afterRead = func() {
		select {
		case committed <- struct{}{}:
		default:
		}
	}
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	channelID := eventServerChannelID(t, "loss")
	request, err := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewEventFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	first := openEventServerTestStream(t, ctx, requesterHost, originHost.ID())
	if err := WriteEventFrame(first, frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-committed:
	case <-time.After(2 * time.Second):
		t.Fatal("source operation did not commit before simulated response loss")
	}
	_ = first.Reset()

	response := exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), request)
	if response.Type() != EventFramePullPage || source.readCalls.Load() != 2 {
		t.Fatalf("response-loss retry = type %s, calls %d", response.Type(), source.readCalls.Load())
	}

	ackCommitted := make(chan struct{}, 1)
	source.ack = func(_ context.Context,
		spec store.CommitPeerPullCursorAckSpec,
	) (store.CommitPeerPullCursorAckResult, error) {
		select {
		case ackCommitted <- struct{}{}:
		default:
		}
		return store.CommitPeerPullCursorAckResult{
			AcknowledgedSequence: spec.ContiguousChannelSequence,
			Replayed:             source.ackCalls.Load() > 1,
		}, nil
	}
	cursor, err := NewCursorAck(CursorAckSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), ContiguousChannelSequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	cursorFrame, err := NewEventFrame(cursor)
	if err != nil {
		t.Fatal(err)
	}
	firstAck := openEventServerTestStream(t, ctx, requesterHost, originHost.ID())
	if err := WriteEventFrame(firstAck, cursorFrame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ackCommitted:
	case <-time.After(2 * time.Second):
		t.Fatal("CursorAck did not commit before simulated response loss")
	}
	_ = firstAck.Reset()
	response = exchangeEventServerPayload(t, ctx, requesterHost, originHost.ID(), cursor)
	if response.Type() != EventFrameAck || source.ackCalls.Load() != 2 {
		t.Fatalf("CursorAck response-loss retry = type %s, calls %d",
			response.Type(), source.ackCalls.Load())
	}
}

type eventServerTestSource struct {
	read func(context.Context, store.ReadPeerPullPageSpec) (store.PeerPullPage, error)
	ack  func(context.Context,
		store.CommitPeerPullCursorAckSpec,
	) (store.CommitPeerPullCursorAckResult, error)
	afterRead func()

	mu        sync.Mutex
	readSpec  store.ReadPeerPullPageSpec
	ackSpec   store.CommitPeerPullCursorAckSpec
	readCalls atomic.Int32
	ackCalls  atomic.Int32
}

type fixedEventServerClock struct{ at time.Time }

func (clock fixedEventServerClock) Now() time.Time { return clock.at }

func (source *eventServerTestSource) ReadPeerPullPage(ctx context.Context,
	spec store.ReadPeerPullPageSpec,
) (store.PeerPullPage, error) {
	source.mu.Lock()
	source.readSpec = spec
	source.mu.Unlock()
	source.readCalls.Add(1)
	page, err := source.read(ctx, spec)
	if source.afterRead != nil {
		source.afterRead()
	}
	return page, err
}

func (source *eventServerTestSource) CommitPeerPullCursorAck(ctx context.Context,
	spec store.CommitPeerPullCursorAckSpec,
) (store.CommitPeerPullCursorAckResult, error) {
	source.mu.Lock()
	source.ackSpec = spec
	source.mu.Unlock()
	source.ackCalls.Add(1)
	if source.ack == nil {
		return store.CommitPeerPullCursorAckResult{AcknowledgedSequence: spec.ContiguousChannelSequence}, nil
	}
	return source.ack(ctx, spec)
}

func (source *eventServerTestSource) lastReadSpec() store.ReadPeerPullPageSpec {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.readSpec
}

func (source *eventServerTestSource) lastAckSpec() store.CommitPeerPullCursorAckSpec {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.ackSpec
}

func eventServerEmptySource(epoch model.OriginEpoch) *eventServerTestSource {
	source := &eventServerTestSource{}
	source.read = func(_ context.Context, spec store.ReadPeerPullPageSpec) (store.PeerPullPage, error) {
		return store.PeerPullPage{Publications: []model.SignedPublication{},
			ScannedChannelSequence: spec.AfterChannelSequence, SourceFloor: 1,
			SourceHead: spec.AfterChannelSequence, OriginEpoch: epoch,
			AcknowledgedSequence: spec.AfterChannelSequence}, nil
	}
	return source
}

func signEventServerPublication(t testing.TB, identity testkit.Identity,
	body model.PublicationBody,
) model.SignedPublication {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	message, err := model.PublicationSigningMessage(body.Key().ChannelID(), body.Digest())
	if err != nil {
		t.Fatal(err)
	}
	signature, err := privateKey.Sign(message)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := model.AttachSignature(body, signature)
	if err != nil {
		t.Fatal(err)
	}
	return publication
}

func eventServerChannelID(t *testing.T, suffix string) model.ChannelID {
	t.Helper()
	channelID, err := model.ParseChannelID("channel-event-server-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return channelID
}

func newEventServerTestHost(t *testing.T, identity testkit.Identity) host.Host {
	t.Helper()
	privateKey, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	nodeHost, err := libp2p.New(libp2p.Identity(privateKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	return nodeHost
}

func connectEventServerHosts(t *testing.T, ctx context.Context, remote, origin host.Host) {
	t.Helper()
	if err := remote.Connect(ctx, libp2ppeer.AddrInfo{ID: origin.ID(), Addrs: origin.Addrs()}); err != nil {
		t.Fatal(err)
	}
}

func openEventServerTestStream(t *testing.T, ctx context.Context, remote host.Host,
	originID libp2ppeer.ID,
) network.Stream {
	t.Helper()
	stream, err := remote.NewStream(ctx, originID, EventsProtocol)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return stream
}

func exchangeEventServerPayload(t *testing.T, ctx context.Context, remote host.Host,
	originID libp2ppeer.ID, payload EventFramePayload,
) EventFrame {
	t.Helper()
	stream := openEventServerTestStream(t, ctx, remote, originID)
	defer stream.Close()
	frame, err := NewEventFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteEventFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadEventFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func assertEventServerReset(t *testing.T, ctx context.Context, remote host.Host,
	originID libp2ppeer.ID, payload EventFramePayload,
) {
	t.Helper()
	stream := openEventServerTestStream(t, ctx, remote, originID)
	defer stream.Close()
	frame, err := NewEventFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteEventFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadEventFrame(stream); err == nil {
		t.Fatal("unsafe Events failure received a response instead of reset")
	}
}
