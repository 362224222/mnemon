package peer

import (
	"github.com/libp2p/go-libp2p/core/network"
	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

func (gater *ConnectionGater) permitsOutboundEnrollmentPeer(peerID libp2ppeer.ID) bool {
	return gater.permitsOutboundEnrollmentAddress(peerID, nil, false)
}

func (gater *ConnectionGater) permitsOutboundEnrollmentConnection(peerID libp2ppeer.ID,
	addresses network.ConnMultiaddrs,
) bool {
	return addresses != nil &&
		gater.permitsOutboundEnrollmentAddress(peerID, addresses.RemoteMultiaddr(), true)
}

// bindOutboundEnrollmentConnection consumes the physical-connection portion
// of one claimed permit. Secured has no connection ID, so the upgraded hook is
// the linearization point that prevents one slot from authorizing unbounded
// parallel connections to the same owner and address.
func (gater *ConnectionGater) bindOutboundEnrollmentConnection(peerID libp2ppeer.ID,
	connectionID string, addresses network.ConnMultiaddrs,
) bool {
	if gater == nil || peerID == "" || connectionID == "" || addresses == nil ||
		addresses.RemoteMultiaddr() == nil {
		return false
	}
	remote := addresses.RemoteMultiaddr().String()
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	var candidate *outboundEnrollmentPermit
	for _, permit := range gater.outbound.permits {
		if !permit.claimed || permit.key.ownerPeerID != peerID {
			continue
		}
		if _, exact := permit.addresses[remote]; !exact {
			continue
		}
		if permit.connectionID == connectionID {
			candidate = permit
			break
		}
		if permit.connectionID == "" && (candidate == nil || permit.generation < candidate.generation) {
			candidate = permit
		}
	}
	allowed := candidate != nil && (candidate.connectionID == "" || candidate.connectionID == connectionID)
	if allowed {
		candidate.connectionID = connectionID
	}
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}

func (gater *ConnectionGater) permitsBoundOutboundEnrollmentConnection(connection network.Conn) bool {
	if gater == nil || connection == nil || connection.RemotePeer() == "" || connection.ID() == "" ||
		connection.RemoteMultiaddr() == nil {
		return false
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	allowed := false
	for _, permit := range gater.outbound.permits {
		if permit.claimed && permit.key.ownerPeerID == connection.RemotePeer() &&
			permit.connectionID == connection.ID() {
			_, allowed = permit.addresses[connection.RemoteMultiaddr().String()]
			if allowed {
				break
			}
		}
	}
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}

func (gater *ConnectionGater) permitsOutboundEnrollmentStream(connection network.Conn,
	stream network.Stream,
) bool {
	if gater == nil || connection == nil || stream == nil || stream.Protocol() != ChannelProtocol ||
		stream.Stat().Direction != network.DirOutbound || connection.ID() == "" || stream.ID() == "" ||
		connection.RemoteMultiaddr() == nil {
		return false
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	allowed := false
	for _, permit := range gater.outbound.permits {
		if permit.claimed && permit.key.ownerPeerID == connection.RemotePeer() &&
			permit.connectionID == connection.ID() && permit.streamID == stream.ID() {
			_, allowed = permit.addresses[connection.RemoteMultiaddr().String()]
			if allowed {
				break
			}
		}
	}
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}

func (gater *ConnectionGater) allowsExistingStream(connection network.Conn,
	stream network.Stream,
) bool {
	if connection == nil || stream == nil {
		return false
	}
	if gater.allowsProtocol(connection.RemotePeer(), stream.Protocol(),
		stream.Stat().Direction, connection.ID()) {
		return true
	}
	return gater.permitsOutboundEnrollmentStream(connection, stream)
}

func (gater *ConnectionGater) permitsOutboundEnrollmentAddress(peerID libp2ppeer.ID,
	address ma.Multiaddr, exact bool,
) bool {
	if gater == nil || peerID == "" || address == nil && exact {
		return false
	}
	gater.mu.Lock()
	callbacks := gater.pruneOutboundEnrollmentLocked(gater.now())
	allowed := false
	for _, permit := range gater.outbound.permits {
		if !permit.claimed || permit.key.ownerPeerID != peerID {
			continue
		}
		if !exact {
			allowed = true
			break
		}
		if _, allowed = permit.addresses[address.String()]; allowed {
			break
		}
	}
	gater.mu.Unlock()
	runEnrollmentPermitCallbacks(callbacks)
	return allowed
}
