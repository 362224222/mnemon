package peer

import (
	"context"
	"errors"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/testkit"
	ma "github.com/multiformats/go-multiaddr"
)

func TestMeshAuthorityTransitionFailsClosedWithoutInstallingCandidate(t *testing.T) {
	t.Parallel()
	fixture := newMeshTransitionFailureFixture(t, "mesh-transition-fail",
		"2026-07-18T08:00:00Z")

	remote := fixture.channel.AppendActive(t, "mesh-transition-fail-remote")
	mergePeerMeshRoster(t, fixture.store, fixture.channel, remote.Member(), remote.Member().CreatedAt())
	transition, err := fixture.runtime.BeginAuthorityTransition(readMeshRuntimeAuthority(t, fixture.store))
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("durable authority diverged")
	if err := transition.FailClosed(cause); !errors.Is(err, cause) {
		t.Fatalf("fail closed error = %v", err)
	}
	if err := transition.FailClosed(cause); !errors.Is(err, cause) {
		t.Fatalf("fail closed replay error = %v", err)
	}
	if transition.Wait() == nil || fixture.runtime.HasCurrentSession(fixture.channel.Channel().ID()) {
		t.Fatal("failed transition retained a live Channel session")
	}
	if _, err := fixture.runtime.Session(fixture.channel.Channel().ID()); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("session after fail closed error = %v", err)
	}
	if transition.snapshot.Channels[0].RosterHead != remote.Member().Head() {
		t.Fatal("test candidate did not contain the unproved roster generation")
	}
	if fixture.runtime.authority.CanConnect(meshRuntimeLibp2pID(t, remote.Identity().PeerID())) {
		t.Fatal("fail closed installed the unproved candidate authority")
	}
	if phase := transition.phase.Load(); phase != meshTransitionFailed {
		t.Fatalf("transition phase = %d, want failed", phase)
	}
	if fixture.runtime.authority.LocalPeerID().String() != fixture.owner.PeerID().String() {
		t.Fatal("fail closed corrupted immutable local identity")
	}
}

func TestMeshAuthorityTransitionInstallFailureClosesWholeHost(t *testing.T) {
	t.Parallel()
	fixture := newMeshTransitionFailureFixture(t, "mesh-install-failure",
		"2026-07-18T09:00:00Z")
	remote := fixture.channel.AppendActive(t, "mesh-install-failure-remote")
	mergePeerMeshRoster(t, fixture.store, fixture.channel, remote.Member(), remote.Member().CreatedAt())
	transition, err := fixture.runtime.BeginAuthorityTransition(readMeshRuntimeAuthority(t, fixture.store))
	if err != nil {
		t.Fatal(err)
	}
	// Inject loss of the Gossip transition token after the durable caller would
	// have committed. MeshRuntime must close the Host, not merely the topic
	// router, because no further control stream may survive an uninstalled
	// durable authority generation.
	fixture.runtime.gossip.mu.Lock()
	fixture.runtime.gossip.transition = nil
	fixture.runtime.gossip.mu.Unlock()
	if err := transition.Install(); !errors.Is(err, ErrMeshRuntime) ||
		!errors.Is(err, ErrGossipTopic) {
		t.Fatalf("injected Mesh install failure = %v", err)
	}
	if transition.phase.Load() != meshTransitionFailed || !fixture.runtime.closed ||
		!fixture.runtime.nodeHost.gater.closed.Load() {
		t.Fatalf("failed install retained Host authority: phase=%d runtime_closed=%v gater_closed=%v",
			transition.phase.Load(), fixture.runtime.closed, fixture.runtime.nodeHost.gater.closed.Load())
	}
	if _, err := fixture.runtime.Session(fixture.channel.Channel().ID()); !errors.Is(err, ErrMeshRuntime) {
		t.Fatalf("Session after failed install error = %v", err)
	}
}

type meshTransitionFailureFixture struct {
	owner   testkit.Identity
	store   *store.Store
	channel *testkit.SignedChannel
	runtime *MeshRuntime
}

func newMeshTransitionFailureFixture(t *testing.T, seed, at string) meshTransitionFailureFixture {
	t.Helper()
	owner := testkit.NewIdentity(t, seed+"-owner")
	st := openPeerMeshStore(t, owner, peerMeshTime(t, at))
	channel := testkit.NewSignedChannelForOwnerAt(t, seed, owner, peerMeshTime(t, at))
	createPeerMeshChannel(t, st, channel, seed)
	key, err := owner.Libp2pPrivateKey()
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
	t.Cleanup(func() { _ = runtime.Close() })
	return meshTransitionFailureFixture{owner: owner, store: st, channel: channel, runtime: runtime}
}
