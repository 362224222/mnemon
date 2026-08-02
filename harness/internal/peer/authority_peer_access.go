package peer

import libp2ppeer "github.com/libp2p/go-libp2p/core/peer"

// CanConnect is the coarse physical-connection gate. A pending or active
// binding in any nonterminal Channel, or an enrolled Agency route, is
// sufficient; exact protocol access is always checked separately.
func (authority *Authority) CanConnect(peerID libp2ppeer.ID) bool {
	if authority == nil || peerID == "" || peerID == authority.localPeerID {
		return false
	}
	state := authority.state.Load()
	_, allowed := state.physical[peerID]
	return allowed
}

// CanUseChannelControl admits only a Peer named by a live R5 Channel. Agency
// routes share the physical Host but never inherit R5 enrollment/control
// authority from that shared connection.
func (authority *Authority) CanUseChannelControl(peerID libp2ppeer.ID) bool {
	if authority == nil || peerID == "" || peerID == authority.localPeerID {
		return false
	}
	state := authority.state.Load()
	_, allowed := state.channelPhysical[peerID]
	return allowed
}

func (authority *Authority) canOpenOutboundChannelControl(peerID libp2ppeer.ID) bool {
	if authority == nil || peerID == "" || peerID == authority.localPeerID {
		return false
	}
	state := authority.state.Load()
	if _, allowed := state.channelPhysical[peerID]; allowed {
		return true
	}
	_, allowed := state.outboundEnrollment[peerID]
	return allowed
}

// CanUseAgency admits only a pre-registered R7 Agency Peer. Channel membership
// alone never grants access to the Agency delivery or object protocols.
func (authority *Authority) CanUseAgency(peerID libp2ppeer.ID) bool {
	if authority == nil || peerID == "" || peerID == authority.localPeerID {
		return false
	}
	state := authority.state.Load()
	_, allowed := state.agency[peerID]
	return allowed
}
