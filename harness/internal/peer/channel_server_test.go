package peer

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestChannelDispatcherRoutesOneProtocolByFirstTypedFrame(t *testing.T) {
	ctx := context.Background()
	ownerIdentity := testkit.NewIdentity(t, "channel-dispatch-owner")
	remoteIdentity := testkit.NewIdentity(t, "channel-dispatch-remote")
	ownerHost := newEnrollmentTestHost(t, ownerIdentity)
	defer ownerHost.Close()
	remoteHost := newEnrollmentTestHost(t, remoteIdentity)
	defer remoteHost.Close()
	connectChannelDispatcherHosts(t, ctx, remoteHost, ownerHost)

	var enrollCalls, memberCalls atomic.Int32
	enrollment := ChannelRequestHandlerFunc(func(_ context.Context, stream network.Stream,
		first ChannelFrame,
	) error {
		enrollCalls.Add(1)
		response, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorInvalidToken})
		if err != nil {
			return err
		}
		frame, err := NewChannelFrame(first.RequestID(), response)
		if err != nil {
			return err
		}
		return WriteChannelFrame(stream, frame)
	})
	member := ChannelRequestHandlerFunc(func(_ context.Context, stream network.Stream,
		first ChannelFrame,
	) error {
		memberCalls.Add(1)
		baseline := first.Payload().(DataBaseline)
		ack, err := NewDataBaselineAck(DataBaselineSpec{ChannelID: baseline.ChannelID(),
			OriginPeerID: baseline.OriginPeerID(), OriginEpoch: baseline.OriginEpoch(),
			BaselineChannelSequence: baseline.BaselineChannelSequence()})
		if err != nil {
			return err
		}
		frame, err := NewChannelFrame(first.RequestID(), ack)
		if err != nil {
			return err
		}
		return WriteChannelFrame(stream, frame)
	})
	dispatcher, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: enrollment, Member: member})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()

	fixture := newChannelFrameFixture(t)
	init := channelDispatcherEnrollInit(t, fixture)
	response := exchangeChannelDispatcherFrame(t, ctx, remoteHost, ownerHost.ID(),
		fixture.frameRequestID, init)
	if response.Type() != ChannelFrameProtocolError || enrollCalls.Load() != 1 || memberCalls.Load() != 0 {
		t.Fatalf("enrollment dispatch = %#v calls %d/%d", response, enrollCalls.Load(), memberCalls.Load())
	}
	baseline, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channelID,
		OriginPeerID: fixture.joiner.modelID, OriginEpoch: fixture.joinerEpoch,
		BaselineChannelSequence: 7})
	if err != nil {
		t.Fatal(err)
	}
	response = exchangeChannelDispatcherFrame(t, ctx, remoteHost, ownerHost.ID(),
		fixture.frameRequestID, baseline)
	if response.Type() != ChannelFrameDataBaselineAck || enrollCalls.Load() != 1 || memberCalls.Load() != 1 {
		t.Fatalf("member dispatch = %#v calls %d/%d", response, enrollCalls.Load(), memberCalls.Load())
	}
	if countProtocol(ownerHost.Mux().Protocols(), ChannelProtocol) != 1 {
		t.Fatal("dispatcher did not own exactly one Channel protocol handler")
	}
}

func TestChannelDispatcherRejectsDuplicateWithoutReplacingLiveHandler(t *testing.T) {
	ctx := context.Background()
	ownerIdentity := testkit.NewIdentity(t, "channel-dispatch-duplicate-owner")
	remoteIdentity := testkit.NewIdentity(t, "channel-dispatch-duplicate-remote")
	ownerHost := newEnrollmentTestHost(t, ownerIdentity)
	defer ownerHost.Close()
	remoteHost := newEnrollmentTestHost(t, remoteIdentity)
	defer remoteHost.Close()
	connectChannelDispatcherHosts(t, ctx, remoteHost, ownerHost)
	var firstCalls, replacementCalls atomic.Int32
	firstHandler := channelDispatcherErrorHandler(&firstCalls)
	first, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: firstHandler})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	replacement, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: channelDispatcherErrorHandler(&replacementCalls)})
	if replacement != nil || !errors.Is(err, ErrChannelDispatcher) {
		t.Fatalf("duplicate dispatcher = (%p,%v)", replacement, err)
	}
	fixture := newChannelFrameFixture(t)
	response := exchangeChannelDispatcherFrame(t, ctx, remoteHost, ownerHost.ID(),
		fixture.frameRequestID, channelDispatcherEnrollInit(t, fixture))
	if response.Type() != ChannelFrameProtocolError || firstCalls.Load() != 1 || replacementCalls.Load() != 0 {
		t.Fatal("rejected duplicate replaced the live Channel handler")
	}
}

func TestChannelDispatcherResetsInvalidFirstFrameAndMissingRoute(t *testing.T) {
	ctx := context.Background()
	ownerIdentity := testkit.NewIdentity(t, "channel-dispatch-reset-owner")
	remoteIdentity := testkit.NewIdentity(t, "channel-dispatch-reset-remote")
	ownerHost := newEnrollmentTestHost(t, ownerIdentity)
	defer ownerHost.Close()
	remoteHost := newEnrollmentTestHost(t, remoteIdentity)
	defer remoteHost.Close()
	connectChannelDispatcherHosts(t, ctx, remoteHost, ownerHost)
	var calls atomic.Int32
	dispatcher, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: channelDispatcherErrorHandler(&calls)})
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Close()
	fixture := newChannelFrameFixture(t)
	challenge, err := NewEnrollChallenge(EnrollChallengeSpec{
		OwnerNonce:      bytes.Repeat([]byte{0x33}, model.EnrollmentNonceBytes),
		SelectedVersion: ChannelFrameVersion, Limits: model.DefaultMemberLimits(),
		RosterHead: fixture.ownerMember.Head()})
	if err != nil {
		t.Fatal(err)
	}
	assertChannelDispatcherReset(t, ctx, remoteHost, ownerHost.ID(), fixture.frameRequestID, challenge)
	baseline, err := NewDataBaseline(DataBaselineSpec{ChannelID: fixture.channelID,
		OriginPeerID: fixture.joiner.modelID, OriginEpoch: fixture.joinerEpoch})
	if err != nil {
		t.Fatal(err)
	}
	assertChannelDispatcherReset(t, ctx, remoteHost, ownerHost.ID(), fixture.frameRequestID, baseline)
	if calls.Load() != 0 {
		t.Fatalf("invalid routes reached enrollment handler %d times", calls.Load())
	}
}

func TestChannelDispatcherCloseCancelsAndDrainsActiveRequest(t *testing.T) {
	ctx := context.Background()
	ownerIdentity := testkit.NewIdentity(t, "channel-dispatch-close-owner")
	remoteIdentity := testkit.NewIdentity(t, "channel-dispatch-close-remote")
	ownerHost := newEnrollmentTestHost(t, ownerIdentity)
	defer ownerHost.Close()
	remoteHost := newEnrollmentTestHost(t, remoteIdentity)
	defer remoteHost.Close()
	connectChannelDispatcherHosts(t, ctx, remoteHost, ownerHost)
	started := make(chan struct{})
	handler := ChannelRequestHandlerFunc(func(ctx context.Context, _ network.Stream, _ ChannelFrame) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	dispatcher, err := NewChannelDispatcher(ctx, ownerHost,
		ChannelDispatcherOptions{Enrollment: handler})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newChannelFrameFixture(t)
	stream := openEnrollmentTestStream(t, ctx, remoteHost, ownerHost.ID())
	frame, err := NewChannelFrame(fixture.frameRequestID, channelDispatcherEnrollInit(t, fixture))
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher handler did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- dispatcher.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher Close did not cancel and drain the active request")
	}
	_ = stream.Close()
	if countProtocol(ownerHost.Mux().Protocols(), ChannelProtocol) != 0 {
		t.Fatal("dispatcher Close retained its Host protocol handler")
	}
}

func channelDispatcherEnrollInit(t *testing.T, fixture channelFrameFixture) EnrollInit {
	t.Helper()
	init, err := NewEnrollInit(EnrollInitSpec{ChannelID: fixture.channelID,
		GrantID: fixture.grantID, EnrollmentRequestID: fixture.requestID,
		JoinerNonce:       bytes.Repeat([]byte{0x44}, model.EnrollmentNonceBytes),
		SupportedVersions: []uint8{ChannelFrameVersion}, OriginEpoch: fixture.joinerEpoch,
		DisplayLabel: "remote", AdvertisedMultiaddrs: fixture.joiningMember.Multiaddrs()})
	if err != nil {
		t.Fatal(err)
	}
	return init
}

func channelDispatcherErrorHandler(calls *atomic.Int32) ChannelRequestHandler {
	return ChannelRequestHandlerFunc(func(_ context.Context, stream network.Stream,
		first ChannelFrame,
	) error {
		calls.Add(1)
		payload, err := NewProtocolError(ProtocolErrorSpec{Code: ChannelErrorInvalidToken})
		if err != nil {
			return err
		}
		frame, err := NewChannelFrame(first.RequestID(), payload)
		if err != nil {
			return err
		}
		return WriteChannelFrame(stream, frame)
	})
}

func exchangeChannelDispatcherFrame(t *testing.T, ctx context.Context,
	remoteHost host.Host, ownerID libp2ppeer.ID, requestID ChannelRequestID,
	payload ChannelFramePayload,
) ChannelFrame {
	t.Helper()
	stream := openEnrollmentTestStream(t, ctx, remoteHost, ownerID)
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	response, err := ReadChannelFrame(stream)
	if err != nil {
		t.Fatal(err)
	}
	if response.RequestID() != requestID {
		t.Fatalf("dispatcher response request ID = %s, want %s",
			response.RequestID().String(), requestID.String())
	}
	return response
}

func assertChannelDispatcherReset(t *testing.T, ctx context.Context, remoteHost host.Host,
	ownerID libp2ppeer.ID, requestID ChannelRequestID, payload ChannelFramePayload,
) {
	t.Helper()
	stream := openEnrollmentTestStream(t, ctx, remoteHost, ownerID)
	defer stream.Close()
	if err := stream.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := NewChannelFrame(requestID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteChannelFrame(stream, frame); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadChannelFrame(stream); err == nil {
		t.Fatal("invalid first Channel frame received a response instead of reset")
	}
}

func connectChannelDispatcherHosts(t *testing.T, ctx context.Context, remote, owner host.Host) {
	t.Helper()
	if err := remote.Connect(ctx, libp2ppeer.AddrInfo{ID: owner.ID(), Addrs: owner.Addrs()}); err != nil {
		t.Fatal(err)
	}
}
