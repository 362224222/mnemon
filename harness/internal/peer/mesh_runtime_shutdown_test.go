package peer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestMeshRuntimeCloseContextAttemptsHostAfterGossipExhaustsDeadlineAndAllowsLaterJoin(
	t *testing.T,
) {
	gossip, blockingConnect := startBlockedGossipRefresh(t)
	blockingClose := &releaseBlockingCloseHost{
		Host:    gossip.host,
		entered: make(chan struct{}),
		release: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	countingClose := &countingMeshRuntimeCloseHost{
		releaseBlockingCloseHost: blockingClose,
	}
	nodeHost := &NodeHost{host: countingClose, closeDone: make(chan struct{})}
	runtime := &MeshRuntime{gossip: gossip, nodeHost: nodeHost}
	t.Cleanup(func() {
		blockingConnect.releaseConnect()
		blockingClose.releaseClose()
		_ = runtime.Close()
	})

	expiredCtx, cancel := context.WithDeadline(
		context.Background(), time.Now().Add(-time.Second),
	)
	defer cancel()
	err := runtime.CloseContext(expiredCtx)
	for _, target := range []error{
		ErrGossipTopic,
		ErrNodeHost,
		ErrMeshRuntime,
		context.DeadlineExceeded,
	} {
		if !errors.Is(err, target) {
			t.Fatalf("expired CloseContext() error = %v; want %v", err, target)
		}
	}
	awaitPeerShutdownSignal(t, blockingClose.entered, "libp2p Host close start")
	if calls := countingClose.calls.Load(); calls != 1 {
		t.Fatalf("libp2p Host close calls = %d; want 1", calls)
	}
	select {
	case <-gossip.refreshDone:
		t.Fatal("Gossip joined a refresh attempt that remained blocked")
	default:
	}
	select {
	case <-nodeHost.closeDone:
		t.Fatal("NodeHost joined a libp2p close that remained blocked")
	default:
	}

	blockingConnect.releaseConnect()
	awaitPeerShutdownSignal(t, blockingConnect.exited, "released refresh dial")
	awaitPeerShutdownSignal(t, gossip.refreshDone, "Gossip refresh owner join")
	blockingClose.releaseClose()

	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := runtime.CloseContext(joinCtx); err != nil {
		t.Fatalf("join after release: %v", err)
	}
	awaitPeerShutdownSignal(t, blockingClose.exited, "libp2p Host close")
	awaitPeerShutdownSignal(t, nodeHost.closeDone, "NodeHost close owner")
	if calls := countingClose.calls.Load(); calls != 1 {
		t.Fatalf("libp2p Host close calls after join = %d; want 1", calls)
	}
}

type countingMeshRuntimeCloseHost struct {
	*releaseBlockingCloseHost
	calls atomic.Int32
}

func (closing *countingMeshRuntimeCloseHost) Close() error {
	closing.calls.Add(1)
	return closing.releaseBlockingCloseHost.Close()
}
