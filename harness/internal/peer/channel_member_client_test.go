package peer

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelMemberClientRunsTypedHelloAndBaseline(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client-hello-baseline")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dispatcher := fixture.serve(t, ctx, fixture.service(t))
	defer dispatcher.Close()
	client := mustChannelMemberClient(t, fixture.remoteHost)
	ownerPeerID := fixture.channel.Channel().OwnerPeerID()
	hello, err := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if err != nil {
		t.Fatal(err)
	}
	helloAck, err := client.Hello(ctx, ownerPeerID, hello)
	if err != nil || helloAck.ChannelID() != fixture.channel.Channel().ID() ||
		helloAck.RosterHead() != fixture.channel.Roster().Head() ||
		len(helloAck.MissingRecords()) != 0 {
		t.Fatalf("Hello() = (%#v, %v)", helloAck, err)
	}
	baseline, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(),
		OriginEpoch:  fixture.remote.Identity().OriginEpoch(), BaselineChannelSequence: 11})
	if err != nil {
		t.Fatal(err)
	}
	baselineAck, err := client.Baseline(ctx, ownerPeerID, baseline)
	if err != nil || !sameDataBaseline(baseline, baselineAck) {
		t.Fatalf("Baseline() = (%#v, %v)", baselineAck, err)
	}
}

func TestChannelMemberClientRunsFrozenMultiPageSync(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client-sync")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fixture.bootstrapDirect(t, ctx)
	updates := make([]model.Member, 0, 18)
	for revision := 3; revision <= 20; revision++ {
		updates = append(updates,
			fixture.channel.AppendActiveUpdate(t, fixture.remote.Identity().PeerID()).Member())
	}
	merged, err := fixture.store.MergeChannelRoster(ctx, store.MergeChannelRosterSpec{
		ChannelID:                    fixture.channel.Channel().ID(),
		AuthenticatedTransportPeerID: fixture.remote.Identity().PeerID(),
		Records:                      updates, At: fixture.now,
	})
	if err != nil || merged.Status != store.ChannelRosterApplied {
		t.Fatalf("prepare roster = (%#v, %v)", merged, err)
	}
	if _, err := fixture.controller.refresh(ctx, fixture.channel.Channel().ID()); err != nil {
		t.Fatal(err)
	}
	dispatcher := fixture.serve(t, ctx, fixture.service(t))
	defer dispatcher.Close()
	client := mustChannelMemberClient(t, fixture.remoteHost)
	syncRequest, err := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.OwnerMember().Member().Head()})
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.Sync(ctx, fixture.channel.Channel().OwnerPeerID(), syncRequest)
	records := result.OwnerSignedRecords()
	if err != nil || result.ChannelID() != fixture.channel.Channel().ID() ||
		result.RosterHead().Revision() != 20 || len(records) != 19 ||
		records[0].Head().Revision() != 2 || records[len(records)-1].Head().Revision() != 20 {
		t.Fatalf("Sync() = (%#v, %v)", result, err)
	}
}

func TestChannelMemberClientRejectsUnboundOrMalformedResponses(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client-malicious")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connectChannelDispatcherHosts(t, ctx, fixture.remoteHost, fixture.ownerHost)
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost})
	if err != nil {
		t.Fatal(err)
	}
	ownerPeerID := fixture.channel.Channel().OwnerPeerID()
	hello, _ := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	baseline, _ := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.remote.Identity().PeerID(),
		OriginEpoch:  fixture.remote.Identity().OriginEpoch(), BaselineChannelSequence: 7})
	syncRequest, _ := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.Roster().Head()})
	wrongRequestID := channelMemberRequestID(t, 0x72)

	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		ack, _ := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: hello.ChannelID(),
			RosterHead: hello.KnownRosterHead()})
		writeChannelMemberClientResponse(t, stream, wrongRequestID, ack)
	})
	if _, err := client.Hello(ctx, ownerPeerID, hello); !errors.Is(err, ErrChannelMemberClientResponse) {
		t.Fatalf("wrong request identity error = %v", err)
	}
	otherChannelID, _ := model.ParseChannelID("channel-member-client-wrong-response")
	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		ack, _ := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: otherChannelID,
			RosterHead: hello.KnownRosterHead()})
		writeChannelMemberClientResponse(t, stream, request.RequestID(), ack)
	})
	if _, err := client.Hello(ctx, ownerPeerID, hello); !errors.Is(err, ErrChannelMemberClientResponse) {
		t.Fatalf("wrong response Channel error = %v", err)
	}

	hiddenHead, _ := model.NewRecordHead(hello.KnownRosterHead().Revision()+1,
		model.Sum([]byte("member-client-hidden-record")))
	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		ack, _ := NewMemberHelloAck(MemberHelloAckSpec{ChannelID: hello.ChannelID(),
			RosterHead: hiddenHead})
		writeChannelMemberClientResponse(t, stream, request.RequestID(), ack)
	})
	if _, err := client.Hello(ctx, ownerPeerID, hello); !errors.Is(err, ErrChannelMemberClientResponse) {
		t.Fatalf("omitted hello suffix error = %v", err)
	}

	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		ack, _ := NewDataBaselineAck(DataBaselineSpec{ChannelID: baseline.ChannelID(),
			OriginPeerID: baseline.OriginPeerID(), OriginEpoch: baseline.OriginEpoch(),
			BaselineChannelSequence: baseline.BaselineChannelSequence() + 1})
		writeChannelMemberClientResponse(t, stream, request.RequestID(), ack)
	})
	if _, err := client.Baseline(ctx, ownerPeerID, baseline); !errors.Is(err, ErrChannelMemberClientResponse) {
		t.Fatalf("changed baseline tuple error = %v", err)
	}

	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		var prefix [channelFrameLengthBytes]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(channelSmallFrameBytes+1))
		_, _ = stream.Write(prefix[:])
	})
	if _, err := client.Baseline(ctx, ownerPeerID, baseline); !errors.Is(err, ErrChannelMemberClientResponse) ||
		errors.Is(err, ErrChannelMemberClientTransport) {
		t.Fatalf("oversized bounded response error = %v", err)
	}

	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		page, _ := NewSyncPage(SyncPageSpec{ChannelID: syncRequest.ChannelID(),
			RosterHead: syncRequest.AfterHead()})
		writeChannelMemberClientResponse(t, stream, request.RequestID(), page)
	})
	if result, err := client.Sync(ctx, ownerPeerID, syncRequest); err != nil || result.IsZero() {
		t.Fatalf("empty bounded Sync = (%#v, %v)", result, err)
	}
}

func TestChannelMemberClientReturnsOnlyClosedMemberFailures(t *testing.T) {
	fixture := newChannelMemberStoreFixture(t, "member-client-failure")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connectChannelDispatcherHosts(t, ctx, fixture.remoteHost, fixture.ownerHost)
	client, _ := NewChannelMemberClient(ChannelMemberClientOptions{Host: fixture.remoteHost})
	hello, _ := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord: fixture.remote.Member(), KnownRosterHead: fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		failure, _ := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBusy,
			Retryable: true, RetryAfter: time.Second})
		writeChannelMemberClientResponse(t, stream, request.RequestID(), failure)
	})
	_, err := client.Hello(ctx, fixture.channel.Channel().OwnerPeerID(), hello)
	var failure *ChannelMemberRemoteFailure
	if !errors.As(err, &failure) || !errors.Is(err, ErrChannelMemberClient) ||
		errors.Is(err, ErrChannelMemberClientTransport) || errors.Is(err, ErrChannelMemberClientResponse) ||
		failure.Code() != ChannelErrorBusy || !failure.Retryable() || failure.RetryAfter() != time.Second {
		t.Fatalf("remote member failure = (%#v, %v)", failure, err)
	}

	installChannelMemberClientHandler(t, fixture.ownerHost, func(stream network.Stream,
		request ChannelFrame,
	) {
		failure, _ := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorBadProof})
		writeChannelMemberClientResponse(t, stream, request.RequestID(), failure)
	})
	if _, err := client.Hello(ctx, fixture.channel.Channel().OwnerPeerID(), hello); !errors.Is(err,
		ErrChannelMemberClientResponse) {
		t.Fatalf("enrollment-only failure error = %v", err)
	}
}

func TestChannelMemberClientFencesLocalIdentityAndInput(t *testing.T) {
	fixture, ctx, client := newConnectedChannelMemberClient(t, "member-client-input")
	syncRequest, _ := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.Roster().Head()})
	if _, err := client.Sync(ctx, fixture.remote.Identity().PeerID(), syncRequest); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("self remote error = %v", err)
	}
	if _, err := client.Sync(nil, fixture.channel.Channel().OwnerPeerID(), syncRequest); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := client.Sync(ctx, fixture.channel.Channel().OwnerPeerID(), SyncRequest{}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("zero request error = %v", err)
	}
	forgedHello, _ := NewMemberHello(MemberHelloSpec{ChannelID: fixture.channel.Channel().ID(),
		ActiveMemberRecord:    fixture.channel.OwnerMember().Member(),
		KnownRosterHead:       fixture.channel.Roster().Head(),
		OwnerSignedProofChain: fixture.channel.Roster().Members()})
	if _, err := client.Hello(ctx, fixture.channel.Channel().OwnerPeerID(), forgedHello); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("foreign local Hello identity error = %v", err)
	}
	foreignBaseline, _ := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channel.Channel().ID(),
		OriginPeerID: fixture.channel.Channel().OwnerPeerID(),
		OriginEpoch:  fixture.channel.OwnerMember().Identity().OriginEpoch()})
	if _, err := client.Baseline(ctx, fixture.channel.Channel().OwnerPeerID(), foreignBaseline); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("foreign local baseline identity error = %v", err)
	}
	var nilClient *ChannelMemberClient
	if _, err := nilClient.Sync(ctx, fixture.channel.Channel().OwnerPeerID(), syncRequest); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("nil client error = %v", err)
	}
	if _, err := NewChannelMemberClient(ChannelMemberClientOptions{}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("nil Host error = %v", err)
	}
	var typedNil *channelMemberClientNilHost
	if _, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: typedNil}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("typed nil Host error = %v", err)
	}
	rsaHost := newArtifactClientRSAHost(t)
	defer rsaHost.Close()
	if _, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: rsaHost}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("RSA Host error = %v", err)
	}
}

func TestChannelMemberClientBindsRemoteAndDefaultDeadline(t *testing.T) {
	fixture, ctx, _ := newConnectedChannelMemberClient(t, "member-client-remote")
	syncRequest, _ := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.Roster().Head()})
	wrongExpected := testkit.NewIdentity(t, "member-client-wrong-expected")
	deadlineSeen := make(chan time.Duration, 1)
	deadlineClient, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: channelMemberClientDeadlineHost{Host: fixture.remoteHost, seen: deadlineSeen}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := deadlineClient.Sync(context.Background(), wrongExpected.PeerID(), syncRequest); !errors.Is(err, ErrChannelMemberClientTransport) {
		t.Fatalf("default deadline transport error = %v", err)
	}
	remaining := <-deadlineSeen
	if remaining <= 0 || remaining > HermeticLimits().ChannelRequestTimeout {
		t.Fatalf("default request deadline = %v", remaining)
	}

	fixture.ownerHost.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		defer stream.Close()
		_, _ = ReadChannelFrame(stream)
	})
	misdirected, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: channelMemberClientMisdirectHost{Host: fixture.remoteHost, target: fixture.ownerHost.ID()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := misdirected.Sync(ctx, wrongExpected.PeerID(), syncRequest); !errors.Is(err,
		ErrChannelMemberClientResponse) {
		t.Fatalf("wrong secure remote error = %v", err)
	}
}

func TestChannelMemberClientHonorsCallerDeadlineWithoutRetry(t *testing.T) {
	fixture, _, client := newConnectedChannelMemberClient(t, "member-client-deadline")
	syncRequest, _ := NewSyncRequest(SyncRequestSpec{ChannelID: fixture.channel.Channel().ID(),
		AfterHead: fixture.channel.Roster().Head()})
	var streams atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	installChannelMemberClientHandler(t, fixture.ownerHost, func(_ network.Stream, _ ChannelFrame) {
		streams.Add(1)
		startOnce.Do(func() { close(started) })
		<-release
	})
	deadlineContext, deadlineCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer deadlineCancel()
	_, err := client.Sync(deadlineContext, fixture.channel.Channel().OwnerPeerID(), syncRequest)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) || streams.Load() != 1 {
		t.Fatalf("deadline/no-retry = (%d, %v)", streams.Load(), err)
	}
	select {
	case <-started:
	default:
		t.Fatal("deadline test did not reach the remote handler")
	}
}

func newConnectedChannelMemberClient(t *testing.T,
	seed string,
) (*channelMemberStoreFixture, context.Context, *ChannelMemberClient) {
	t.Helper()
	fixture := newChannelMemberStoreFixture(t, seed)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	connectChannelDispatcherHosts(t, ctx, fixture.remoteHost, fixture.ownerHost)
	return fixture, ctx, mustChannelMemberClient(t, fixture.remoteHost)
}

func mustChannelMemberClient(t testing.TB, nodeHost host.Host) *ChannelMemberClient {
	t.Helper()
	client, err := NewChannelMemberClient(ChannelMemberClientOptions{Host: nodeHost})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func installChannelMemberClientHandler(t testing.TB, remote host.Host,
	respond func(network.Stream, ChannelFrame),
) {
	t.Helper()
	remote.SetStreamHandler(ChannelProtocol, func(stream network.Stream) {
		defer stream.Close()
		request, err := ReadChannelFrame(stream)
		if err != nil {
			_ = stream.Reset()
			return
		}
		respond(stream, request)
	})
}

func writeChannelMemberClientResponse(t testing.TB, stream network.Stream,
	requestID ChannelRequestID, payload ChannelFramePayload,
) {
	t.Helper()
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		t.Errorf("construct Channel client response: %v", err)
		return
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Errorf("write Channel client response: %v", err)
	}
}

type channelMemberClientMisdirectHost struct {
	host.Host
	target libp2ppeer.ID
}

func (nodeHost channelMemberClientMisdirectHost) NewStream(ctx context.Context, _ libp2ppeer.ID,
	protocolIDs ...protocol.ID,
) (network.Stream, error) {
	return nodeHost.Host.NewStream(ctx, nodeHost.target, protocolIDs...)
}

type channelMemberClientDeadlineHost struct {
	host.Host
	seen chan<- time.Duration
}

func (nodeHost channelMemberClientDeadlineHost) NewStream(ctx context.Context, _ libp2ppeer.ID,
	_ ...protocol.ID,
) (network.Stream, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		nodeHost.seen <- 0
	} else {
		nodeHost.seen <- time.Until(deadline)
	}
	return nil, errors.New("test dial failure")
}

type channelMemberClientNilHost struct{ host.Host }
