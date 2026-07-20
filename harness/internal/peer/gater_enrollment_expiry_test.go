package peer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestConnectionGaterExpiryResetsExactStreamBeforeOwnerCallback(t *testing.T) {
	local := testAuthorityPeer(t, "permit-reset-order-local")
	owner := testAuthorityPeer(t, "permit-reset-order-owner")
	authority, _ := NewAuthority(local.modelID)
	gater := newTestConnectionGater(t, authority)
	start := time.Date(2026, 7, 20, 6, 0, 0, 0, time.UTC)
	gater.pendingTTL = time.Minute
	var clock atomic.Value
	clock.Store(start)
	gater.now = func() time.Time { return clock.Load().(time.Time) }
	var resets atomic.Int32
	callback := make(chan int32, 1)
	token, err := gater.acquireOutboundEnrollmentPermit(context.Background(),
		testEnrollmentTransportPermitSpec(t, owner,
			[]string{"/ip4/127.0.0.1/tcp/44201"}, "reset-order"),
		func(outboundEnrollmentPermitRef, error) { callback <- resets.Load() })
	if err != nil {
		t.Fatal(err)
	}
	gater.mu.Lock()
	gater.outbound.permits[token.key].resetStream = func() error {
		resets.Add(1)
		return nil
	}
	gater.mu.Unlock()
	clock.Store(start.Add(gater.pendingTTL + time.Nanosecond))
	gater.mu.Lock()
	gater.signalExpiryOwnerLocked()
	gater.mu.Unlock()
	select {
	case observed := <-callback:
		if observed != 1 {
			t.Fatalf("owner callback observed %d resets, want exact reset first", observed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expiry owner did not retire the exact stream")
	}
}
