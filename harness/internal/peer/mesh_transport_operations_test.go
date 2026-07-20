package peer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
)

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
	publicType := reflect.TypeOf((*MeshTransport)(nil))
	for index := 0; index < publicType.NumMethod(); index++ {
		method := publicType.Method(index)
		for input := 1; input < method.Type.NumIn(); input++ {
			if method.Type.In(input) == hostType {
				t.Fatalf("%s exposes raw host.Host input", method.Name)
			}
		}
		for output := 0; output < method.Type.NumOut(); output++ {
			if method.Type.Out(output) == hostType {
				t.Fatalf("%s exposes raw host.Host output", method.Name)
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
