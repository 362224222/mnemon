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
	if direction == network.DirInbound && gater.authority.CanUseChannelControl(peerID) {
		return true
	}
	if direction != network.DirInbound {
		return false
	}
	// An Agency route grants the physical connection and only Agency streams.
	// Best-effort reservation of the separate unknown-enrollment budget keeps
	// the pre-existing R5 join path available without turning Agency authority
	// into Channel authority. A saturated enrollment budget must not revoke the
	// independently authorized Agency connection.
	if gater.authority.CanUseAgency(peerID) {
		_ = gater.reserveUnknown(peerID, addresses)
		return true
	}
	return gater.reserveUnknown(peerID, addresses)
}

func (gater *ConnectionGater) reserveUnknown(peerID libp2ppeer.ID,
	addresses network.ConnMultiaddrs,
) bool {
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
	if direction == network.DirInbound && gater.authority.CanUseChannelControl(peerID) {
		gater.releasePending(peerID, addresses)
		return true
	}
	if direction != network.DirInbound {
		return false
	}
	if gater.authority.CanUseAgency(peerID) {
		// Consume a reservation when one was available. Agency transport remains
		// valid without it, but ChannelProtocol stays closed because no unknown
		// lease is installed.
		_ = gater.consumeUnknown(peerID, connectionID, addresses, closeConnection)
		return true
	}
	return gater.consumeUnknown(peerID, connectionID, addresses, closeConnection)
}

func (gater *ConnectionGater) consumeUnknown(peerID libp2ppeer.ID, connectionID string,
	addresses network.ConnMultiaddrs, closeConnection func() error,
) bool {
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
		if gater.authority.CanUseChannelControl(connection.peerID) {
			if connection.timer != nil {
				connection.timer.Stop()
			}
			delete(gater.unknown, connectionID)
		}
	}
	for key, reservations := range gater.pending {
		if gater.authority.CanUseChannelControl(key.peerID) {
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
	if agencyProtocol(protocolID) {
		return gater.authority.CanUseAgency(peerID)
	}
	if protocolID != ChannelProtocol {
		return gater.authority.CanOpenDataPlane(peerID)
	}
	if direction == network.DirOutbound {
		return gater.authority.canOpenOutboundChannelControl(peerID)
	}
	if direction != network.DirInbound {
		return false
	}
	if gater.authority.CanUseChannelControl(peerID) {
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

func (*ConnectionGater) Listen(network.Network, ma.Multiaddr)      {}
func (*ConnectionGater) ListenClose(network.Network, ma.Multiaddr) {}
func (*ConnectionGater) Connected(network.Network, network.Conn)   {}
func (gater *ConnectionGater) Disconnected(_ network.Network, connection network.Conn) {
	if connection != nil {
		gater.releaseUnknown(connection.ID())
	}
}
