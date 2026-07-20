package peer

import (
	"testing"
	"time"
)

func TestConnectionGaterExpiryOwnerStopsWhenIdle(t *testing.T) {
	gater := newTestConnectionGater(t, nil)
	gater.mu.Lock()
	gater.ensureExpiryOwnerLocked()
	gater.mu.Unlock()

	done := make(chan struct{})
	go func() {
		gater.expiryWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle expiry owner did not stop")
	}
	gater.mu.Lock()
	running := gater.expiryRunning
	gater.mu.Unlock()
	if running {
		t.Fatal("idle expiry owner retained lifecycle ownership")
	}
}
