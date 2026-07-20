package peer

import (
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
)

func (gater *ConnectionGater) releaseUnknown(connectionID string) {
	if gater == nil || connectionID == "" {
		return
	}
	gater.mu.Lock()
	if connection := gater.unknown[connectionID]; connection != nil {
		if connection.timer != nil {
			connection.timer.Stop()
		}
		delete(gater.unknown, connectionID)
	}
	gater.mu.Unlock()
}

// shutdown retires every enrollment reservation before its owning Host stops
// delivering disconnect notifications. The Host close that follows owns the
// physical connections; this method only prevents lease callbacks from
// surviving the NodeHost lifecycle.
func (gater *ConnectionGater) shutdown() {
	if gater == nil {
		return
	}
	gater.mu.Lock()
	gater.closed.Store(true)
	for _, connection := range gater.unknown {
		if connection != nil && connection.timer != nil {
			connection.timer.Stop()
		}
	}
	gater.pending = make(map[unknownReservationKey][]time.Time)
	gater.pendingCount = 0
	gater.unknown = make(map[string]*unknownConnection)
	gater.mu.Unlock()
}

func (gater *ConnectionGater) expireUnknown(connectionID string, expected *unknownConnection) {
	if gater == nil || connectionID == "" || expected == nil {
		return
	}
	gater.mu.Lock()
	current := gater.unknown[connectionID]
	if current != expected {
		gater.mu.Unlock()
		return
	}
	delete(gater.unknown, connectionID)
	// Promotion wins if its immutable authority revision became visible before
	// the lease expiry callback. Otherwise close only the exact connection that
	// consumed this unknown slot; a later connection cannot inherit the timer.
	authorized := gater.authority != nil && gater.authority.CanConnect(current.peerID)
	closeConnection := current.close
	gater.mu.Unlock()
	if !authorized && closeConnection != nil {
		_ = closeConnection()
	}
}

func (gater *ConnectionGater) releasePending(peerID libp2ppeer.ID, addresses network.ConnMultiaddrs) {
	if gater == nil {
		return
	}
	key, ok := unknownKey(peerID, addresses)
	if !ok {
		return
	}
	gater.mu.Lock()
	if reservations := gater.pending[key]; len(reservations) > 0 {
		if len(reservations) == 1 {
			delete(gater.pending, key)
		} else {
			gater.pending[key] = reservations[1:]
		}
		gater.pendingCount--
	}
	gater.mu.Unlock()
}

func (gater *ConnectionGater) prunePendingLocked(now time.Time) {
	for key, reservations := range gater.pending {
		kept := reservations[:0]
		for _, expiresAt := range reservations {
			if expiresAt.After(now) {
				kept = append(kept, expiresAt)
			} else {
				gater.pendingCount--
			}
		}
		if len(kept) == 0 {
			delete(gater.pending, key)
		} else {
			gater.pending[key] = kept
		}
	}
}

func unknownKey(peerID libp2ppeer.ID, addresses network.ConnMultiaddrs) (unknownReservationKey, bool) {
	if peerID == "" || addresses == nil || addresses.LocalMultiaddr() == nil ||
		addresses.RemoteMultiaddr() == nil {
		return unknownReservationKey{}, false
	}
	return unknownReservationKey{peerID: peerID, local: addresses.LocalMultiaddr().String(),
		remote: addresses.RemoteMultiaddr().String()}, true
}
