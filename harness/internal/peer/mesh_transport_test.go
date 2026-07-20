package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshTransportOwnsOneManagedHostAndExactHandlers(t *testing.T) {
	runtime := newMeshTransportTestRuntime(t, "mesh-transport-owner")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	host := runtime.managedRuntimeHost()
	assertMeshTransportProtocolCounts(t, host, 0, 0, 0)
	if transport.host != host || transport.memberClient.host != host ||
		transport.eventClient.host != host || transport.artifactClient.host != host ||
		transport.host.ID() != host.ID() {
		t.Fatal("direct transports did not retain the frozen MeshRuntime Host identity")
	}

	ctx, cancel := context.WithCancel(context.Background())
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)
	assertMeshTransportProtocolCounts(t, host, 1, 1, 1)

	cancel()
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatalf("Run() shutdown error = %v", err)
	}
	assertMeshTransportProtocolCounts(t, host, 0, 0, 0)
	if err := transport.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestMeshTransportRejectsDuplicateAndRollsBackPartialRegistration(t *testing.T) {
	t.Run("duplicate", func(t *testing.T) {
		runtime := newMeshTransportTestRuntime(t, "mesh-transport-duplicate")
		first := newMeshTransportTestOwner(t, runtime, nil)
		second := newMeshTransportTestOwner(t, runtime, nil)
		ctx, cancel := context.WithCancel(context.Background())
		firstDone := runMeshTransport(t, first, ctx)
		waitMeshTransportReady(t, first)

		if err := second.Run(context.Background()); !errors.Is(err, ErrChannelDispatcher) {
			t.Fatalf("duplicate Run() error = %v", err)
		}
		assertMeshTransportProtocolCounts(t, runtime.managedRuntimeHost(), 1, 1, 1)
		cancel()
		if err := waitMeshTransportRun(t, firstDone); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("partial Event failure", func(t *testing.T) {
		runtime := newMeshTransportTestRuntime(t, "mesh-transport-partial-event")
		nodeHost := runtime.managedRuntimeHost()
		nodeHost.SetStreamHandler(EventsProtocol, func(stream network.Stream) {
			_ = stream.Reset()
		})
		t.Cleanup(func() { nodeHost.RemoveStreamHandler(EventsProtocol) })
		transport := newMeshTransportTestOwner(t, runtime, nil)
		if err := transport.Run(context.Background()); !errors.Is(err, ErrEventServer) {
			t.Fatalf("partial Run() error = %v", err)
		}
		assertMeshTransportProtocolCounts(t, nodeHost, 0, 1, 0)
	})

	t.Run("partial Artifact failure", func(t *testing.T) {
		runtime := newMeshTransportTestRuntime(t, "mesh-transport-partial")
		nodeHost := runtime.managedRuntimeHost()
		nodeHost.SetStreamHandler(ArtifactsProtocol, func(stream network.Stream) {
			_ = stream.Reset()
		})
		t.Cleanup(func() { nodeHost.RemoveStreamHandler(ArtifactsProtocol) })
		transport := newMeshTransportTestOwner(t, runtime, nil)
		if err := transport.Run(context.Background()); !errors.Is(err, ErrArtifactServer) {
			t.Fatalf("partial Run() error = %v", err)
		}
		assertMeshTransportProtocolCounts(t, nodeHost, 0, 0, 1)
	})

	t.Run("constructor", func(t *testing.T) {
		runtime := newMeshTransportTestRuntime(t, "mesh-transport-constructor")
		if got, err := NewMeshTransport(runtime, MeshTransportOptions{}); got != nil ||
			!errors.Is(err, ErrMeshTransport) {
			t.Fatalf("NewMeshTransport(empty) = (%#v, %v)", got, err)
		}
		assertMeshTransportProtocolCounts(t, runtime.managedRuntimeHost(), 0, 0, 0)
		if err := new(MeshTransport).Close(); !errors.Is(err, ErrMeshTransport) {
			t.Fatalf("zero-value Close() error = %v", err)
		}
	})
}

func TestMeshTransportCloseCancelsAndDrainsAdmittedOutboundCall(t *testing.T) {
	runtime := newMeshTransportTestRuntime(t, "mesh-transport-outbound-drain")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)

	callCtx, callDone, err := transport.beginCall(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- transport.Close() }()
	waitMeshTransportSignal(t, callCtx.Done(), "outbound call cancellation")
	select {
	case err := <-closeDone:
		t.Fatalf("Close() returned before admitted outbound call drained: %v", err)
	default:
	}
	callDone()
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not return after admitted outbound call drained")
	}
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestMeshTransportParentCancellationFencesOutboundAdmission(t *testing.T) {
	runtime := newMeshTransportTestRuntime(t, "mesh-transport-parent-cancel")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)
	remote := testkit.NewIdentity(t, "mesh-transport-parent-cancel-remote").PeerID()

	const calls = 512
	start := make(chan struct{})
	errorsSeen := make(chan error, calls)
	var callers sync.WaitGroup
	callers.Add(calls)
	for index := 0; index < calls; index++ {
		go func() {
			defer callers.Done()
			<-start
			_, err := transport.Pull(context.Background(), remote, PullRequest{})
			errorsSeen <- err
		}()
	}
	cancelled := make(chan struct{})
	go func() {
		<-start
		cancel()
		close(cancelled)
	}()
	close(start)
	callers.Wait()
	close(errorsSeen)
	<-cancelled

	for err := range errorsSeen {
		if !errors.Is(err, ErrEventClient) && !errors.Is(err, ErrMeshTransport) {
			t.Fatalf("Pull() racing parent cancellation error = %v", err)
		}
	}
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatalf("Run() after parent cancellation error = %v", err)
	}
	if err := transport.Readiness(context.Background()); !errors.Is(err, ErrMeshTransport) {
		t.Fatalf("Readiness() after parent cancellation error = %v", err)
	}
}

func TestMeshTransportRuntimeFailCloseStopsWithImmutableCause(t *testing.T) {
	cause := errors.New("asynchronous enrollment transport failure")
	runtime := newMeshTransportFailCloseRuntime(t, "mesh-transport-fail-close", cause)
	transport := newMeshTransportTestOwner(t, runtime, nil)
	runDone := runMeshTransport(t, transport, context.Background())
	waitMeshTransportReady(t, transport)
	nodeHost := runtime.managedRuntimeHost()
	assertMeshTransportProtocolCounts(t, nodeHost, 1, 1, 1)

	failDone := make(chan struct{})
	go func() {
		defer close(failDone)
		runtime.failClosedEnrollmentTransport(cause)
	}()
	runErr := waitMeshTransportRun(t, runDone)
	waitMeshTransportSignal(t, failDone, "runtime fail-close cleanup")
	if !errors.Is(runErr, ErrMeshRuntime) || !errors.Is(runErr, cause) {
		t.Fatalf("Run() fail-close error = %v", runErr)
	}
	assertMeshTransportProtocolCounts(t, nodeHost, 0, 0, 0)
	if err := transport.Readiness(context.Background()); !errors.Is(err, ErrMeshTransport) ||
		!errors.Is(err, cause) {
		t.Fatalf("Readiness() after fail-close error = %v", err)
	}
	remote := testkit.NewIdentity(t, "mesh-transport-fail-close-remote").PeerID()
	if _, err := transport.Pull(context.Background(), remote, PullRequest{}); !errors.Is(err,
		ErrMeshTransport) {
		t.Fatalf("Pull() after fail-close error = %v", err)
	}
	bothReady, cancel := context.WithCancel(context.Background())
	cancel()
	if err := transport.waitStopCause(bothReady); !errors.Is(err, cause) {
		t.Fatalf("simultaneous terminal/context stop cause = %v", err)
	}
	if err := transport.Close(); !errors.Is(err, cause) {
		t.Fatalf("Close() after fail-close error = %v", err)
	}
}

func TestMeshTransportCloseCancelsAndDrainsActiveStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtime, remote, channel := newMeshTransportConnectedRuntime(t,
		"mesh-transport-drain")
	entered := make(chan struct{})
	exited := make(chan struct{})
	source := &eventServerTestSource{read: func(ctx context.Context,
		spec store.ReadPeerPullPageSpec,
	) (store.PeerPullPage, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return store.PeerPullPage{}, ctx.Err()
	}}
	transport := newMeshTransportTestOwner(t, runtime, source)
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)
	remoteHost := newEventServerTestHost(t, remote)
	t.Cleanup(func() { _ = remoteHost.Close() })
	if err := remoteHost.Connect(ctx, libp2ppeer.AddrInfo{ID: runtime.managedRuntimeHost().ID(),
		Addrs: runtime.managedRuntimeHost().Addrs()}); err != nil {
		t.Fatal(err)
	}
	stream, err := remoteHost.NewStream(ctx, runtime.managedRuntimeHost().ID(), EventsProtocol)
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewPullRequest(PullRequestSpec{ChannelID: channel.Channel().ID(),
		OriginEpoch: channel.OwnerMember().Identity().OriginEpoch(), Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := NewEventFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteEventFrame(stream, frame); err != nil {
		t.Fatalf("write blocking PullRequest: %v", err)
	}
	waitMeshTransportSignal(t, entered, "Event source entry")

	closeDone := make(chan error, 1)
	go func() { closeDone <- transport.Close() }()
	waitMeshTransportSignal(t, exited, "Event source cancellation")
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not wait for the active Event stream to drain")
	}
	_ = stream.Close()
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertMeshTransportProtocolCounts(t, runtime.managedRuntimeHost(), 0, 0, 0)
}

type meshTransportEnrollmentController struct{}

func (meshTransportEnrollmentController) PrepareEnrollmentChallenge(context.Context,
	ChannelEnrollmentChallengeControl,
) (ChannelEnrollmentChallengeAuthority, error) {
	return ChannelEnrollmentChallengeAuthority{}, errors.New("unexpected enrollment challenge")
}

func (meshTransportEnrollmentController) AcceptEnrollmentAuthority(context.Context,
	ChannelEnrollmentAcceptanceControl,
) (ChannelEnrollmentAcceptanceAuthority, error) {
	return ChannelEnrollmentAcceptanceAuthority{}, errors.New("unexpected enrollment acceptance")
}

type meshTransportMemberController struct{}

func (meshTransportMemberController) ReconcileMemberHelloGate(context.Context,
	ChannelMemberHelloControl,
) (ChannelMemberHelloAuthority, error) {
	return ChannelMemberHelloAuthority{}, errors.New("unexpected member hello")
}

func (meshTransportMemberController) FreezeMemberRosterForSync(context.Context,
	ChannelMemberSyncControl,
) (ChannelMemberRosterSnapshot, error) {
	return ChannelMemberRosterSnapshot{}, errors.New("unexpected member sync")
}

func (meshTransportMemberController) InstallMemberBaselineGate(context.Context,
	ChannelMemberBaselineControl,
) (ChannelMemberBaselineAuthority, error) {
	return ChannelMemberBaselineAuthority{}, errors.New("unexpected member baseline")
}

type meshTransportArtifactStore struct{}

func (meshTransportArtifactStore) ReadArtifactSourceManifest(context.Context,
	store.ReadArtifactSourceManifestSpec,
) (store.ArtifactSourceManifest, error) {
	return store.ArtifactSourceManifest{}, errors.New("unexpected Artifact manifest read")
}

func (meshTransportArtifactStore) ReadArtifactSourceBlock(context.Context,
	store.ReadArtifactSourceBlockSpec,
) (store.ArtifactSourceBlock, error) {
	return store.ArtifactSourceBlock{}, errors.New("unexpected Artifact block read")
}

func newMeshTransportTestOwner(t *testing.T, runtime *MeshRuntime,
	eventSource EventSourceStore,
) *MeshTransport {
	t.Helper()
	if eventSource == nil {
		eventSource = &eventServerTestSource{read: func(context.Context,
			store.ReadPeerPullPageSpec,
		) (store.PeerPullPage, error) {
			return store.PeerPullPage{}, errors.New("unexpected Event source call")
		}}
	}
	transport, err := NewMeshTransport(runtime, MeshTransportOptions{
		Enrollment:  ChannelEnrollmentOwnerOptions{Controller: meshTransportEnrollmentController{}},
		Member:      ChannelMemberServiceOptions{Controller: meshTransportMemberController{}},
		EventSource: eventSource, ArtifactStore: meshTransportArtifactStore{},
		ArtifactCAS: &artifactServerTestCAS{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return transport
}

func newMeshTransportTestRuntime(t *testing.T, seed string) *MeshRuntime {
	t.Helper()
	identity := testkit.NewIdentity(t, seed)
	st := openPeerMeshStore(t, identity, peerMeshTime(t, "2026-07-20T05:00:00Z"))
	return newTestMeshRuntime(t, context.Background(), identity, readMeshRuntimeAuthority(t, st))
}

func newMeshTransportFailCloseRuntime(t *testing.T, seed string, cause error) *MeshRuntime {
	t.Helper()
	identity := testkit.NewIdentity(t, seed)
	st := openPeerMeshStore(t, identity, peerMeshTime(t, "2026-07-20T05:30:00Z"))
	key, err := identity.Libp2pPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	listen, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewMeshRuntime(context.Background(), key, []ma.Multiaddr{listen},
		readMeshRuntimeAuthority(t, st))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); !errors.Is(err, cause) {
			t.Errorf("close failed MeshRuntime = %v, want %v", err, cause)
		}
	})
	return runtime
}

func newMeshTransportConnectedRuntime(t *testing.T, seed string) (*MeshRuntime,
	testkit.Identity, *testkit.SignedChannel,
) {
	t.Helper()
	createdAt := peerMeshTime(t, "2026-07-20T06:00:00Z")
	owner := testkit.NewIdentity(t, seed+"-owner")
	remote := testkit.NewIdentity(t, seed+"-remote")
	channel := testkit.NewSignedChannelForOwnerAt(t, seed+"-channel", owner, createdAt)
	st := openPeerMeshStore(t, owner, createdAt)
	createPeerMeshChannel(t, st, channel, seed)
	member := channel.AppendActiveIdentity(t, remote)
	mergePeerMeshRoster(t, st, channel, member.Member(), member.Member().CreatedAt())
	baselineAt := member.Member().CreatedAt().Add(time.Second)
	if _, err := st.InstallInboundChannelBaseline(context.Background(),
		store.InstallInboundChannelBaselineSpec{AuthenticatedPeerID: remote.PeerID(),
			Baseline: store.ChannelDataBaseline{ChannelID: channel.Channel().ID(),
				OriginPeerID: remote.PeerID(), OriginEpoch: remote.OriginEpoch()}, At: baselineAt}); err != nil {
		t.Fatal(err)
	}
	reserved, err := st.ReserveOutboundChannelBaseline(context.Background(),
		store.ReserveOutboundChannelBaselineSpec{ChannelID: channel.Channel().ID(),
			TargetPeerID: remote.PeerID(), At: baselineAt.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.ConfirmOutboundChannelBaseline(context.Background(),
		store.ConfirmOutboundChannelBaselineSpec{AuthenticatedPeerID: remote.PeerID(),
			Ack: store.ChannelDataBaselineAck(reserved.Baseline), At: baselineAt.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	return newTestMeshRuntime(t, context.Background(), owner, readMeshRuntimeAuthority(t, st)),
		remote, channel
}

func runMeshTransport(t *testing.T, transport *MeshTransport, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx) }()
	return done
}

func waitMeshTransportReady(t *testing.T, transport *MeshTransport) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := transport.Readiness(ctx); err != nil {
		t.Fatalf("Readiness() error = %v", err)
	}
}

func waitMeshTransportRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("MeshTransport Run did not stop")
		return nil
	}
}

func waitMeshTransportSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func assertMeshTransportProtocolCounts(t *testing.T, nodeHost host.Host,
	channel, events, artifacts int,
) {
	t.Helper()
	protocols := nodeHost.Mux().Protocols()
	counts := map[protocol.ID]int{ChannelProtocol: channel, EventsProtocol: events,
		ArtifactsProtocol: artifacts}
	for expected, want := range counts {
		if got := countProtocol(protocols, expected); got != want {
			t.Fatalf("protocol %s handler count = %d, want %d", expected, got, want)
		}
	}
}
