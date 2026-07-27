package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestGossipCloseContextBoundsBlockedRefreshAndAllowsLaterJoin(t *testing.T) {
	gossip, blocking := startBlockedGossipRefresh(t)
	assertBoundedGossipClose(t, gossip)
	blocking.releaseConnect()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := gossip.CloseContext(joinCtx); err != nil {
		t.Fatalf("join after release: %v", err)
	}
	awaitPeerShutdownSignal(t, blocking.exited, "released refresh dial")
	awaitPeerShutdownSignal(t, gossip.refreshDone, "refresh owner join")
}

func startBlockedGossipRefresh(t *testing.T) (*Gossip, *releaseBlockingConnectHost) {
	t.Helper()
	local := testAuthorityPeer(t, "gossip-bounded-close-local")
	remote := testAuthorityPeer(t, "gossip-bounded-close-remote")
	pending := testAuthorityChannel(t, "gossip-bounded-close",
		model.BindingPending, local, remote)
	authority, _ := NewAuthority(local.modelID)
	if err := authority.Replace(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{pending}}); err != nil {
		t.Fatal(err)
	}
	localHost := newBarePeerHost(t, local)
	t.Cleanup(func() { _ = localHost.Close() })
	remoteHost := newBarePeerHost(t, remote)
	t.Cleanup(func() { _ = remoteHost.Close() })
	blocking := &releaseBlockingConnectHost{Host: localHost, entered: make(chan struct{}),
		release: make(chan struct{}), exited: make(chan struct{})}
	gossip, err := NewGossip(context.Background(), blocking, authority)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		blocking.releaseConnect()
		_ = gossip.Close()
	})
	if err := gossip.CloseContext(nil); !errors.Is(err, ErrGossipTopic) {
		t.Fatalf("nil-context CloseContext() error = %v", err)
	}
	if gossip.closed {
		t.Fatal("nil-context CloseContext sealed Gossip")
	}
	if _, err := gossip.Join(pending.ChannelID); err != nil {
		t.Fatal(err)
	}
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer connectCancel()
	if err := localHost.Connect(connectCtx, libp2ppeer.AddrInfo{
		ID: remoteHost.ID(), Addrs: remoteHost.Addrs(),
	}); err != nil {
		t.Fatal(err)
	}
	active := pending
	active.Bindings = []BindingAuthoritySnapshot{{
		PeerID: remote.modelID, State: model.BindingActive,
	}}
	if err := gossip.Reconcile(NetworkAuthoritySnapshot{LocalPeerID: local.modelID,
		Channels: []ChannelAuthoritySnapshot{active}}); err != nil {
		t.Fatal(err)
	}
	awaitPeerShutdownSignal(t, blocking.entered, "blocked refresh dial")
	return gossip, blocking
}

func assertBoundedGossipClose(t testing.TB, gossip *Gossip) {
	t.Helper()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	started := time.Now()
	err := gossip.CloseContext(shutdownCtx)
	cancel()
	if !errors.Is(err, ErrGossipTopic) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded CloseContext() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded CloseContext() returned after %s", elapsed)
	}
	select {
	case <-gossip.refreshDone:
		t.Fatal("refresh owner joined an attempt that remained blocked")
	default:
	}
}

type releaseBlockingConnectHost struct {
	host.Host
	entered     chan struct{}
	release     chan struct{}
	exited      chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	exitOnce    sync.Once
}

func (blocking *releaseBlockingConnectHost) Connect(ctx context.Context,
	_ libp2ppeer.AddrInfo,
) error {
	blocking.enterOnce.Do(func() { close(blocking.entered) })
	<-blocking.release
	blocking.exitOnce.Do(func() { close(blocking.exited) })
	return ctx.Err()
}

func (blocking *releaseBlockingConnectHost) releaseConnect() {
	blocking.releaseOnce.Do(func() { close(blocking.release) })
}

func awaitPeerShutdownSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not complete", operation)
	}
}
