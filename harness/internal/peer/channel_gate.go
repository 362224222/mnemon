package peer

import (
	"context"
	"sync"
	"sync/atomic"
)

// channelGate is an admission fence for one live Channel generation chain.
// Draining closes new admission without holding a mutex while existing work or
// a durable authority CAS completes. A retired gate is never reopened: stale
// validator closures can retain its pointer safely and always fail closed.
type channelGate struct {
	mu               sync.Mutex
	active           int
	draining         bool
	retired          bool
	drainDeliverable bool
	drainDone        chan struct{}
	changed          chan struct{}
	deliverable      atomic.Bool
}

func (gate *channelGate) acquire(ctx context.Context) bool {
	if gate == nil {
		return false
	}
	for {
		gate.mu.Lock()
		if gate.retired {
			gate.mu.Unlock()
			return false
		}
		if !gate.draining {
			if !gate.deliverable.Load() {
				gate.mu.Unlock()
				return false
			}
			gate.active++
			gate.mu.Unlock()
			return true
		}
		changed := gate.changed
		gate.mu.Unlock()
		if ctx == nil || changed == nil {
			return false
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

func (gate *channelGate) tryAcquire() bool {
	if gate == nil {
		return false
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.retired || gate.draining || !gate.deliverable.Load() {
		return false
	}
	gate.active++
	return true
}

func (gate *channelGate) release() {
	if gate == nil {
		return
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.active <= 0 {
		panic("peer.channelGate: release without admission")
	}
	gate.active--
	if gate.draining && gate.active == 0 && gate.drainDone != nil {
		close(gate.drainDone)
		gate.drainDone = nil
	}
}

func (gate *channelGate) beginDrain() (<-chan struct{}, bool) {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.retired {
		done := make(chan struct{})
		close(done)
		return done, false
	}
	if gate.draining {
		if gate.drainDone == nil {
			done := make(chan struct{})
			close(done)
			return done, gate.drainDeliverable
		}
		return gate.drainDone, gate.drainDeliverable
	}
	gate.draining = true
	gate.drainDeliverable = gate.deliverable.Swap(false)
	gate.changed = make(chan struct{})
	done := make(chan struct{})
	if gate.active == 0 {
		close(done)
	} else {
		gate.drainDone = done
	}
	return done, gate.drainDeliverable
}

func (gate *channelGate) resume(deliverable bool) {
	if gate == nil {
		return
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.retired {
		return
	}
	gate.draining = false
	gate.drainDone = nil
	gate.deliverable.Store(deliverable)
	if gate.changed != nil {
		close(gate.changed)
		gate.changed = nil
	}
}

func (gate *channelGate) retire() {
	if gate == nil {
		return
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.active != 0 {
		panic("peer.channelGate: retire before drain completed")
	}
	gate.retired = true
	gate.draining = false
	gate.drainDone = nil
	gate.deliverable.Store(false)
	if gate.changed != nil {
		close(gate.changed)
		gate.changed = nil
	}
}

func (gate *channelGate) isRetired() bool {
	if gate == nil {
		return true
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.retired
}

func channelClosed(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}
