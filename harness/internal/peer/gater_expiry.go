package peer

import "time"

// ensureExpiryOwnerLocked starts at most one owner. The owner exits when there
// are no upgraded unknown connections, so an idle gater retains no goroutine;
// a later admission starts a new generation under the same mutex.
func (gater *ConnectionGater) ensureExpiryOwnerLocked() {
	if gater.expiryRunning || gater.closed.Load() {
		return
	}
	gater.expiryRunning = true
	gater.expiryWG.Add(1)
	go gater.runExpiryOwner()
}

func (gater *ConnectionGater) signalExpiryOwnerLocked() {
	if !gater.expiryRunning || gater.expiryWake == nil {
		return
	}
	select {
	case gater.expiryWake <- struct{}{}:
	default:
	}
}

func (gater *ConnectionGater) runExpiryOwner() {
	defer gater.expiryWG.Done()
	for {
		closes, next, stop := gater.takeExpiryWork()
		if stop {
			return
		}
		// Connection.Close may enter libp2p and synchronously notify this gater.
		// It must never run under the state mutex. shutdown waits for this owner,
		// so no close callback can survive its return.
		closeUnknownConnections(closes)
		if len(closes) != 0 {
			continue
		}
		waitForUnknownExpiry(next, gater.expiryWake)
	}
}

func (gater *ConnectionGater) takeExpiryWork() ([]func() error, time.Time, bool) {
	gater.mu.Lock()
	defer gater.mu.Unlock()
	if gater.closed.Load() || len(gater.unknown) == 0 {
		gater.expiryRunning = false
		return nil, time.Time{}, true
	}
	closes, next := gater.collectExpiredUnknownLocked(time.Now())
	return closes, next, false
}

func closeUnknownConnections(closes []func() error) {
	for _, closeConnection := range closes {
		_ = closeConnection()
	}
}

func waitForUnknownExpiry(next time.Time, wake <-chan struct{}) {
	if next.IsZero() {
		return
	}
	timer := time.NewTimer(time.Until(next))
	select {
	case <-timer.C:
	case <-wake:
		stopAndDrainExpiryTimer(timer)
	}
}

func stopAndDrainExpiryTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

// collectExpiredUnknownLocked removes every due lease in one bounded pass and
// returns only the close calls whose Peers were not promoted before this
// expiry linearization point. unknownMax bounds both the scan and callback set.
func (gater *ConnectionGater) collectExpiredUnknownLocked(now time.Time) ([]func() error, time.Time) {
	closes := make([]func() error, 0, len(gater.unknown))
	var next time.Time
	for connectionID, connection := range gater.unknown {
		if connection.expiresAt.After(now) {
			if next.IsZero() || connection.expiresAt.Before(next) {
				next = connection.expiresAt
			}
			continue
		}
		delete(gater.unknown, connectionID)
		// Promotion wins if its immutable authority revision became visible
		// before this lease-expiry pass. Otherwise only the exact connection
		// that consumed the slot is closed.
		if (gater.authority == nil || !gater.authority.CanConnect(connection.peerID)) &&
			connection.close != nil {
			closes = append(closes, connection.close)
		}
	}
	return closes, next
}
