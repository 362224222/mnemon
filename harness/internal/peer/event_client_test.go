package peer

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestEventClientPullsAuthenticatedOriginAndAcknowledges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-client-success-origin")
	requester := testkit.NewIdentity(t, "event-client-success-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)

	channelID := eventServerChannelID(t, "client-success")
	unsigned := newEventFramePublication(t, channelID, origin.PeerID(),
		origin.OriginEpoch(), 1, 16)
	publication := signEventServerPublication(t, origin, unsigned.Body())
	source := &eventServerTestSource{}
	source.read = func(_ context.Context, spec store.ReadPeerPullPageSpec) (store.PeerPullPage, error) {
		return store.PeerPullPage{Publications: []model.SignedPublication{publication},
			ScannedChannelSequence: 1, SourceFloor: 1, SourceHead: 1,
			OriginEpoch: spec.OriginEpoch, AcknowledgedSequence: spec.AfterChannelSequence}, nil
	}
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := NewEventClient(EventClientOptions{Host: requesterHost})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), Limit: 4})
	page, err := client.Pull(ctx, origin.PeerID(), request)
	if err != nil || page.ScannedChannelSequence() != 1 || len(page.Publications()) != 1 ||
		!page.Publications()[0].IsSupported() ||
		page.Publications()[0].Digest() != publication.Digest() {
		t.Fatalf("Pull() = (%#v, %v)", page, err)
	}
	ack, _ := NewCursorAck(CursorAckSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), ContiguousChannelSequence: 1})
	if err := client.Acknowledge(ctx, origin.PeerID(), ack); err != nil {
		t.Fatalf("Acknowledge() error = %v", err)
	}
	ackSpec := source.lastAckSpec()
	if ackSpec.AuthenticatedPeerID != requester.PeerID() || ackSpec.ChannelID != channelID ||
		ackSpec.OriginEpoch != origin.OriginEpoch() || ackSpec.ContiguousChannelSequence != 1 {
		t.Fatalf("authenticated ACK = %#v", ackSpec)
	}
}

func TestEventClientReturnsClosedRemoteFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-client-failure-origin")
	requester := testkit.NewIdentity(t, "event-client-failure-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)
	source := &eventServerTestSource{read: func(context.Context,
		store.ReadPeerPullPageSpec,
	) (store.PeerPullPage, error) {
		return store.PeerPullPage{}, store.PeerPullHistoryGap{SourceFloor: 9}
	}}
	server, err := NewEventServer(ctx, EventServerOptions{Host: originHost, Source: source})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, _ := NewEventClient(EventClientOptions{Host: requesterHost})
	request, _ := NewPullRequest(PullRequestSpec{ChannelID: eventServerChannelID(t, "client-failure"),
		OriginEpoch: origin.OriginEpoch(), Limit: 1})
	_, err = client.Pull(ctx, origin.PeerID(), request)
	var failure *EventRemoteFailure
	if !errors.Is(err, ErrEventClient) || !errors.As(err, &failure) ||
		failure.Code() != EventErrorHistoryGap || failure.Retryable() ||
		failure.RetryAfter() != 0 || failure.SourceFloor() != 9 ||
		errors.Is(err, store.ErrPeerPullHistoryGap) {
		t.Fatalf("closed remote failure = %#v (%v)", failure, err)
	}
}

func TestEventClientRejectsMismatchedAndMalformedResponses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	origin := testkit.NewIdentity(t, "event-client-invalid-origin")
	requester := testkit.NewIdentity(t, "event-client-invalid-requester")
	originHost := newEventServerTestHost(t, origin)
	defer originHost.Close()
	requesterHost := newEventServerTestHost(t, requester)
	defer requesterHost.Close()
	connectEventServerHosts(t, ctx, requesterHost, originHost)
	client, _ := NewEventClient(EventClientOptions{Host: requesterHost})
	channelID := eventServerChannelID(t, "client-invalid")
	request, _ := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: origin.OriginEpoch(), Limit: 1})

	wrongEpoch := testkit.NewIdentity(t, "event-client-wrong-epoch").OriginEpoch()
	originHost.SetStreamHandler(EventsProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, release, err := readEventStreamFrame(stream, eventSmallFrameBytes)
		if err != nil {
			_ = stream.Reset()
			return
		}
		release()
		page, _ := NewPullPage(PullPageSpec{OriginEpoch: wrongEpoch,
			SourceFloor: 1, SourceHead: 0, ScannedChannelSequence: 0})
		frame, _ := NewEventFrame(page)
		_ = WriteEventFrame(stream, frame)
	})
	if _, err := client.Pull(ctx, origin.PeerID(), request); !errors.Is(err, ErrEventClientResponse) {
		t.Fatalf("mismatched Pull response error = %v", err)
	}

	originHost.SetStreamHandler(EventsProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, release, err := readEventStreamFrame(stream, eventSmallFrameBytes)
		if err != nil {
			_ = stream.Reset()
			return
		}
		release()
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], 1)
		_, _ = stream.Write(prefix[:])
		_, _ = stream.Write([]byte("{"))
	})
	if _, err := client.Pull(ctx, origin.PeerID(), request); !errors.Is(err, ErrEventClientResponse) {
		t.Fatalf("malformed Pull response error = %v", err)
	}

	originHost.SetStreamHandler(EventsProtocol, func(stream network.Stream) {
		_ = stream.Reset()
	})
	if _, err := client.Pull(ctx, origin.PeerID(), request); !errors.Is(err, ErrEventClientTransport) {
		t.Fatalf("reset Pull response error = %v", err)
	}
}

func TestEventClientFencesInputTransportAndCancellation(t *testing.T) {
	identity := testkit.NewIdentity(t, "event-client-input")
	host := newEventServerTestHost(t, identity)
	defer host.Close()
	client, err := NewEventClient(EventClientOptions{Host: host})
	if err != nil {
		t.Fatal(err)
	}
	channelID := eventServerChannelID(t, "client-input")
	request, _ := NewPullRequest(PullRequestSpec{ChannelID: channelID,
		OriginEpoch: identity.OriginEpoch(), Limit: 1})
	if _, err := client.Pull(context.Background(), identity.PeerID(), request); !errors.Is(err, ErrEventClient) {
		t.Fatalf("self-origin Pull error = %v", err)
	}
	missing := testkit.NewIdentity(t, "event-client-unreachable")
	if _, err := client.Pull(context.Background(), missing.PeerID(), request); !errors.Is(err, ErrEventClientTransport) {
		t.Fatalf("unreachable Pull error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Pull(cancelled, missing.PeerID(), request); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Pull error = %v", err)
	}
	if _, err := NewEventClient(EventClientOptions{}); !errors.Is(err, ErrEventClient) {
		t.Fatalf("empty client error = %v", err)
	}
}
