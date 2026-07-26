package peer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
)

func TestNodeHostCloseContextBoundsLibp2pCloseAndAllowsLaterJoin(t *testing.T) {
	identity := testAuthorityPeer(t, "node-host-bounded-close")
	base := newBarePeerHost(t, identity)
	blocking := &releaseBlockingCloseHost{Host: base, entered: make(chan struct{}),
		release: make(chan struct{}), exited: make(chan struct{})}
	node := &NodeHost{host: blocking, closeDone: make(chan struct{})}
	t.Cleanup(func() {
		blocking.releaseClose()
		_ = node.Close()
	})
	if err := node.CloseContext(nil); !errors.Is(err, ErrNodeHost) {
		t.Fatalf("nil-context CloseContext() error = %v", err)
	}
	select {
	case <-blocking.entered:
		t.Fatal("nil-context CloseContext started libp2p close")
	default:
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := node.CloseContext(shutdownCtx)
	cancel()
	if !errors.Is(err, ErrNodeHost) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("bounded CloseContext() error = %v", err)
	}
	awaitPeerShutdownSignal(t, blocking.entered, "libp2p close start")
	select {
	case <-node.closeDone:
		t.Fatal("NodeHost reported a blocked libp2p close as joined")
	default:
	}

	blocking.releaseClose()
	joinCtx, joinCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer joinCancel()
	if err := node.CloseContext(joinCtx); err != nil {
		t.Fatalf("join after release: %v", err)
	}
	awaitPeerShutdownSignal(t, blocking.exited, "libp2p close")
	awaitPeerShutdownSignal(t, node.closeDone, "NodeHost close owner")

	expired, expire := context.WithCancel(context.Background())
	expire()
	if err := node.CloseContext(expired); err != nil {
		t.Fatalf("completed CloseContext() with expired context = %v", err)
	}
}

type releaseBlockingCloseHost struct {
	host.Host
	entered     chan struct{}
	release     chan struct{}
	exited      chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
	exitOnce    sync.Once
}

func (blocking *releaseBlockingCloseHost) Close() error {
	blocking.enterOnce.Do(func() { close(blocking.entered) })
	<-blocking.release
	err := blocking.Host.Close()
	blocking.exitOnce.Do(func() { close(blocking.exited) })
	return err
}

func (blocking *releaseBlockingCloseHost) releaseClose() {
	blocking.releaseOnce.Do(func() { close(blocking.release) })
}
