package peer

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChannelGateDrainAbortAndRetireRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gate := &channelGate{}
	gate.deliverable.Store(true)
	var admitted atomic.Int64
	var workers sync.WaitGroup
	started := make(chan struct{}, 1)
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for gate.acquire(ctx) {
				admitted.Add(1)
				select {
				case started <- struct{}{}:
				default:
				}
				runtime.Gosched()
				gate.release()
			}
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("gate workers did not start")
	}
	for iteration := 0; iteration < 64; iteration++ {
		drained, deliverable := gate.beginDrain()
		<-drained
		if !deliverable || gate.tryAcquire() {
			t.Fatalf("iteration %d did not close admission", iteration)
		}
		gate.resume(true)
	}
	drained, _ := gate.beginDrain()
	<-drained
	gate.retire()
	cancel()
	workers.Wait()
	if admitted.Load() == 0 || !gate.isRetired() || gate.tryAcquire() {
		t.Fatalf("gate race oracle = admissions %d, retired %v", admitted.Load(), gate.isRetired())
	}
}

func requireGateAdmission(t *testing.T, gate *channelGate, ctx context.Context) {
	t.Helper()
	if !gate.acquire(ctx) {
		t.Fatal("acquire Channel admission")
	}
}
