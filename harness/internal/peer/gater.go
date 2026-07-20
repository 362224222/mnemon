package peer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr"
)

// ConnectionGater enforces coarse physical connection authority plus the
// bounded inbound enrollment exception. Exact Channel stream/topic access is
// deliberately outside this gate.
type ConnectionGater struct {
	authority    *Authority
	unknownMax   int
	pendingTTL   time.Duration
	now          func() time.Time
	mu           sync.Mutex
	pending      map[unknownReservationKey][]time.Time
	pendingCount int
	unknown      map[string]*unknownConnection
	closed       atomic.Bool
}

type unknownConnection struct {
	peerID libp2ppeer.ID
	timer  *time.Timer
	close  func() error
}

type unknownReservationKey struct {
	peerID libp2ppeer.ID
	local  string
	remote string
}

var _ connmgr.ConnectionGater = (*ConnectionGater)(nil)
var _ network.Notifiee = (*ConnectionGater)(nil)

func NewConnectionGater(authority *Authority) *ConnectionGater {
	return &ConnectionGater{authority: authority,
		unknownMax: HermeticLimits().UnknownEnrollmentConnections,
		pendingTTL: HermeticLimits().ChannelRequestTimeout,
		now:        time.Now, pending: make(map[unknownReservationKey][]time.Time),
		unknown: make(map[string]*unknownConnection)}
}

func (gater *ConnectionGater) InterceptPeerDial(peerID libp2ppeer.ID) bool {
	return gater != nil && !gater.closed.Load() && gater.authority != nil && gater.authority.CanDial(peerID)
}

func (gater *ConnectionGater) InterceptAddrDial(peerID libp2ppeer.ID, _ ma.Multiaddr) bool {
	return gater.InterceptPeerDial(peerID)
}

// Identity is unavailable at accept time. The resource manager fences all
// pre-secure connections; the authenticated unknown budget is applied in the
// secured/upgraded stages without accidentally rejecting known members.
func (gater *ConnectionGater) InterceptAccept(network.ConnMultiaddrs) bool {
	return gater != nil && !gater.closed.Load() && gater.authority != nil
}

func (gater *ConnectionGater) InterceptSecured(direction network.Direction,
	peerID libp2ppeer.ID, addresses network.ConnMultiaddrs,
) bool {
	if gater == nil || gater.closed.Load() || gater.authority == nil || peerID == "" ||
		peerID == gater.authority.LocalPeerID() {
		return false
	}
	if direction == network.DirOutbound && gater.authority.CanDial(peerID) {
		return true
	}
	if direction == network.DirInbound && gater.authority.CanConnect(peerID) {
		return true
	}
	if direction != network.DirInbound {
		return false
	}
	key, ok := unknownKey(peerID, addresses)
	if !ok {
		return false
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	if gater.closed.Load() {
		return false
	}
	now := gater.now()
	gater.prunePendingLocked(now)
	if gater.pendingCount+len(gater.unknown) >= gater.unknownMax {
		return false
	}
	// The secured hook has no connection ID. Keep one reservation per call,
	// including concurrent connections with an identical endpoint tuple, and
	// consume exactly one at upgraded time. A set keyed only by the tuple would
	// let duplicate endpoint connections share one slot and exceed the fence.
	gater.pending[key] = append(gater.pending[key], now.Add(gater.pendingTTL))
	gater.pendingCount++
	return true
}

func (gater *ConnectionGater) InterceptUpgraded(connection network.Conn) (bool, control.DisconnectReason) {
	if connection == nil {
		return false, 0
	}
	return gater.admitUpgradedConnection(connection.Stat().Direction, connection.RemotePeer(),
		connection.ID(), connection, connection.Close), 0
}

func (gater *ConnectionGater) admitUpgraded(direction network.Direction,
	peerID libp2ppeer.ID, connectionID string, addresses network.ConnMultiaddrs,
) bool {
	return gater.admitUpgradedConnection(direction, peerID, connectionID, addresses, nil)
}

func (gater *ConnectionGater) admitUpgradedConnection(direction network.Direction,
	peerID libp2ppeer.ID, connectionID string, addresses network.ConnMultiaddrs,
	closeConnection func() error,
) bool {
	if gater == nil || gater.closed.Load() || gater.authority == nil || peerID == "" || connectionID == "" ||
		peerID == gater.authority.LocalPeerID() {
		return false
	}
	if direction == network.DirOutbound && gater.authority.CanDial(peerID) {
		return true
	}
	if direction == network.DirInbound && gater.authority.CanConnect(peerID) {
		gater.releasePending(peerID, addresses)
		return true
	}
	if direction != network.DirInbound {
		return false
	}
	key, ok := unknownKey(peerID, addresses)
	if !ok {
		return false
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	if gater.closed.Load() {
		return false
	}
	gater.prunePendingLocked(gater.now())
	if existing, duplicate := gater.unknown[connectionID]; duplicate {
		return existing.peerID == peerID
	}
	reservations := gater.pending[key]
	if len(reservations) == 0 {
		return false
	}
	if len(reservations) == 1 {
		delete(gater.pending, key)
	} else {
		gater.pending[key] = reservations[1:]
	}
	gater.pendingCount--
	lease := &unknownConnection{peerID: peerID, close: closeConnection}
	gater.unknown[connectionID] = lease
	lease.timer = time.AfterFunc(gater.pendingTTL, func() {
		gater.expireUnknown(connectionID, lease)
	})
	return true
}

// Reconcile releases enrollment slots for Peers that have since gained a
// pending/active binding. It does not close connections or create authority.
func (gater *ConnectionGater) Reconcile() {
	if gater == nil || gater.closed.Load() || gater.authority == nil {
		return
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	for connectionID, connection := range gater.unknown {
		if gater.authority.CanConnect(connection.peerID) {
			if connection.timer != nil {
				connection.timer.Stop()
			}
			delete(gater.unknown, connectionID)
		}
	}
	for key, reservations := range gater.pending {
		if gater.authority.CanConnect(key.peerID) {
			delete(gater.pending, key)
			gater.pendingCount -= len(reservations)
		}
	}
}

// ReconcileConnections applies the current whole-snapshot authority to
// already-upgraded physical connections. ConnectionGater lifecycle hooks only
// govern new connections; without this pass a revoked Peer could retain scarce
// Node connection capacity indefinitely.
func (gater *ConnectionGater) ReconcileConnections(nodeNetwork network.Network) error {
	if gater == nil || gater.authority == nil || nodeNetwork == nil {
		return fmt.Errorf("connection authority reconciliation is unavailable")
	}
	gater.Reconcile()
	var closeErrors []error
	for _, connection := range nodeNetwork.Conns() {
		if connection == nil || gater.allowsExisting(connection) {
			if connection != nil {
				closeErrors = append(closeErrors, gater.reconcileStreams(connection)...)
			}
			continue
		}
		if err := connection.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close Peer %s connection %s: %w",
				connection.RemotePeer(), connection.ID(), err))
		}
	}
	return errors.Join(closeErrors...)
}

func (gater *ConnectionGater) reconcileStreams(connection network.Conn) []error {
	var resetErrors []error
	for _, stream := range connection.GetStreams() {
		if stream == nil || !managedProtocol(stream.Protocol()) ||
			gater.allowsProtocol(connection.RemotePeer(), stream.Protocol(),
				stream.Stat().Direction, connection.ID()) {
			continue
		}
		if err := stream.Reset(); err != nil {
			resetErrors = append(resetErrors, fmt.Errorf("reset Peer %s protocol %s stream %s: %w",
				connection.RemotePeer(), stream.Protocol(), stream.ID(), err))
		}
	}
	return resetErrors
}

func (gater *ConnectionGater) allowsExisting(connection network.Conn) bool {
	if gater == nil || gater.closed.Load() || gater.authority == nil || connection == nil {
		return false
	}
	peerID := connection.RemotePeer()
	if peerID == "" || peerID == gater.authority.LocalPeerID() {
		return false
	}
	if gater.authority.CanConnect(peerID) ||
		(connection.Stat().Direction == network.DirOutbound && gater.authority.CanDial(peerID)) {
		return true
	}
	if connection.Stat().Direction != network.DirInbound {
		return false
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	lease := gater.unknown[connection.ID()]
	return lease != nil && lease.peerID == peerID
}

func (gater *ConnectionGater) allowsProtocol(peerID libp2ppeer.ID, protocolID protocol.ID,
	direction network.Direction, connectionID string,
) bool {
	if gater == nil || gater.closed.Load() || gater.authority == nil || peerID == "" ||
		!managedProtocol(protocolID) {
		return false
	}
	if protocolID != ChannelProtocol {
		return gater.authority.CanOpenDataPlane(peerID)
	}
	if direction == network.DirOutbound {
		return gater.authority.CanDial(peerID)
	}
	if direction != network.DirInbound {
		return false
	}
	if gater.authority.CanConnect(peerID) {
		return true
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	lease := gater.unknown[connectionID]
	return lease != nil && lease.peerID == peerID
}

func (gater *ConnectionGater) UnknownEnrollmentSlots() int {
	if gater == nil {
		return 0
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	gater.prunePendingLocked(gater.now())
	return gater.pendingCount + len(gater.unknown)
}

func (gater *ConnectionGater) UnknownConnections() int {
	if gater == nil {
		return 0
	}
	gater.mu.Lock()
	defer gater.mu.Unlock()
	return len(gater.unknown)
}

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

func (*ConnectionGater) Listen(network.Network, ma.Multiaddr)      {}
func (*ConnectionGater) ListenClose(network.Network, ma.Multiaddr) {}
func (*ConnectionGater) Connected(network.Network, network.Conn)   {}
func (gater *ConnectionGater) Disconnected(_ network.Network, connection network.Conn) {
	if connection != nil {
		gater.releaseUnknown(connection.ID())
	}
}
