package peer

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

func TestMeshTransportEnsuresOnlyCurrentActiveChannelTopic(t *testing.T) {
	runtime, _, channel := newMeshTransportTopicRuntime(t, "mesh-transport-topic-active")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)
	channelID := channel.Channel().ID()
	original, err := runtime.Session(channelID)
	if err != nil {
		t.Fatal(err)
	}
	if err := original.Close(); err != nil {
		t.Fatal(err)
	}
	if transport.HasCurrentChannelTopic(channelID) {
		t.Fatal("closed Channel topic session still reports current")
	}
	if err := transport.EnsureChannelTopic(ctx, channelID); err != nil {
		t.Fatalf("EnsureChannelTopic(active) error = %v", err)
	}
	if !transport.HasCurrentChannelTopic(channelID) {
		t.Fatal("active Channel topic is not current")
	}
	replacement, err := runtime.Session(channelID)
	if err != nil || replacement == original || !replacement.IsCurrent() {
		t.Fatalf("ensured Channel topic = (%p, %v), previous %p", replacement, err, original)
	}

	cancel()
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatal(err)
	}
	if transport.HasCurrentChannelTopic(channelID) {
		t.Fatal("stopped transport retained current Channel topic health")
	}
	if err := transport.EnsureChannelTopic(context.Background(), channelID); !errors.Is(err,
		ErrMeshTransport) {
		t.Fatalf("EnsureChannelTopic after stop error = %v", err)
	}
}

func TestMeshTransportEnsureChannelTopicCancellationPreservesTransition(t *testing.T) {
	runtime, st, channel := newMeshTransportTopicRuntime(t,
		"mesh-transport-topic-transition")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()
	runDone := runMeshTransport(t, transport, runCtx)
	waitMeshTransportReady(t, transport)
	remote := channel.AppendActive(t, "mesh-transport-topic-transition-remote")
	mergePeerMeshRoster(t, st, channel, remote.Member(), remote.Member().CreatedAt())
	transition, err := runtime.BeginAuthorityTransition(readMeshRuntimeAuthority(t, st))
	if err != nil {
		t.Fatal(err)
	}
	transitionPending := true
	defer func() {
		if transitionPending {
			_ = transition.Abort()
		}
	}()

	callerCtx, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()
	ensured := make(chan error, 1)
	go func() { ensured <- transport.EnsureChannelTopic(callerCtx, channel.Channel().ID()) }()
	select {
	case err := <-ensured:
		t.Fatalf("Ensure returned during affecting transition: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancelCaller()
	select {
	case err := <-ensured:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled EnsureChannelTopic error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("EnsureChannelTopic ignored caller cancellation")
	}
	select {
	case <-transition.Done():
		t.Fatal("canceled EnsureChannelTopic finalized the authority transition")
	default:
	}
	if transport.HasCurrentChannelTopic(channel.Channel().ID()) {
		t.Fatal("drained affecting transition reported a current topic")
	}
	if err := transition.Abort(); err != nil {
		t.Fatal(err)
	}
	transitionPending = false
	if err := transport.EnsureChannelTopic(context.Background(), channel.Channel().ID()); err != nil ||
		!transport.HasCurrentChannelTopic(channel.Channel().ID()) {
		t.Fatalf("Channel topic after Abort = ensure %v, current %t", err,
			transport.HasCurrentChannelTopic(channel.Channel().ID()))
	}
	stopRun()
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestMeshTransportEnsureChannelTopicRejectsInactiveAndInvalid(t *testing.T) {
	runtime, _, _ := newMeshTransportTopicRuntime(t, "mesh-transport-topic-reject")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)
	owner := testkit.NewIdentity(t, "mesh-transport-topic-absent-owner")
	absent := testkit.NewSignedChannelForOwnerAt(t, "mesh-transport-topic-absent", owner,
		peerMeshTime(t, "2026-07-20T09:10:00Z")).Channel().ID()
	for name, channelID := range map[string]model.ChannelID{"inactive": absent, "invalid": {}} {
		t.Run(name, func(t *testing.T) {
			if err := transport.EnsureChannelTopic(context.Background(), channelID); !errors.Is(err, ErrMeshRuntime) || !errors.Is(err, ErrGossipTopic) {
				t.Fatalf("EnsureChannelTopic error = %v", err)
			}
			if transport.HasCurrentChannelTopic(channelID) {
				t.Fatal("unavailable Channel topic reported current")
			}
		})
	}
	cancel()
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatal(err)
	}
}

func TestMeshTransportForwardsOnlyTypedOperations(t *testing.T) {
	runtime := newMeshTransportTestRuntime(t, "mesh-transport-forward")
	transport := newMeshTransportTestOwner(t, runtime, nil)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := runMeshTransport(t, transport, ctx)
	waitMeshTransportReady(t, transport)
	remote := testkit.NewIdentity(t, "mesh-transport-forward-remote").PeerID()

	if _, err := transport.Hello(ctx, remote, MemberHello{}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("Hello() error = %v", err)
	}
	if _, err := transport.Sync(ctx, remote, SyncRequest{}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("Sync() error = %v", err)
	}
	if _, err := transport.Baseline(ctx, remote, DataBaseline{}); !errors.Is(err,
		ErrChannelMemberClient) {
		t.Fatalf("Baseline() error = %v", err)
	}
	if _, err := transport.Pull(ctx, remote, PullRequest{}); !errors.Is(err, ErrEventClient) {
		t.Fatalf("Pull() error = %v", err)
	}
	if err := transport.Acknowledge(ctx, remote, CursorAck{}); !errors.Is(err, ErrEventClient) {
		t.Fatalf("Acknowledge() error = %v", err)
	}
	if _, err := transport.GetManifest(ctx, remote, GetManifest{}); !errors.Is(err,
		ErrArtifactClient) {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if _, err := transport.GetBlock(ctx, remote, GetBlock{}); !errors.Is(err,
		ErrArtifactClient) {
		t.Fatalf("GetBlock() error = %v", err)
	}

	cancel()
	if err := waitMeshTransportRun(t, runDone); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.Pull(context.Background(), remote, PullRequest{}); !errors.Is(err,
		ErrMeshTransport) {
		t.Fatalf("Pull() after stop error = %v", err)
	}
}

func TestMeshTransportDoesNotExposeRawHost(t *testing.T) {
	hostType := reflect.TypeOf((*host.Host)(nil)).Elem()
	topicSessionType := reflect.TypeOf((*TopicSession)(nil))
	publicType := reflect.TypeOf((*MeshTransport)(nil))
	for index := 0; index < publicType.NumMethod(); index++ {
		method := publicType.Method(index)
		for input := 1; input < method.Type.NumIn(); input++ {
			if current := method.Type.In(input); current == hostType || current == topicSessionType {
				t.Fatalf("%s exposes runtime-internal input %s", method.Name, current)
			}
		}
		for output := 0; output < method.Type.NumOut(); output++ {
			if current := method.Type.Out(output); current == hostType || current == topicSessionType {
				t.Fatalf("%s exposes runtime-internal output %s", method.Name, current)
			}
		}
	}
	optionsType := reflect.TypeOf(MeshTransportOptions{})
	for index := 0; index < optionsType.NumField(); index++ {
		if field := optionsType.Field(index); field.Type == hostType {
			t.Fatalf("MeshTransportOptions.%s exposes raw host.Host", field.Name)
		}
	}
}

func newMeshTransportTopicRuntime(t *testing.T, seed string) (*MeshRuntime, *store.Store,
	*testkit.SignedChannel,
) {
	t.Helper()
	createdAt := peerMeshTime(t, "2026-07-20T09:00:00Z")
	owner := testkit.NewIdentity(t, seed+"-owner")
	channel := testkit.NewSignedChannelForOwnerAt(t, seed+"-channel", owner, createdAt)
	st := openPeerMeshStore(t, owner, createdAt)
	createPeerMeshChannel(t, st, channel, seed)
	runtime := newTestMeshRuntime(t, context.Background(), owner,
		readMeshRuntimeAuthority(t, st))
	return runtime, st, channel
}
