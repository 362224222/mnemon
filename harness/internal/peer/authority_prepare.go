package peer

import (
	"fmt"

	libp2ppeer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// Keep the optional overlay at the same small-node bound as the durable R5
// mesh. Physical connection and per-Peer stream budgets remain outer fences.
const maxAgencyPeers = model.MaxChannelsPerNode

// prepare fully validates a candidate without making it visible. Gossip uses
// this split to identify and drain affected Channel gates before the immutable
// revision pointer is installed.
func (authority *Authority) prepare(snapshot NetworkAuthoritySnapshot) (*networkAuthorityState, error) {
	local, err := authority.validateSnapshotIdentityAndBounds(snapshot)
	if err != nil {
		return nil, err
	}
	state := newNetworkAuthorityState(local, snapshot)
	if err := state.addAgencyPeers(local, snapshot.AgencyPeers); err != nil {
		return nil, err
	}
	if err := state.addOutboundEnrollmentPeers(local, snapshot.OutboundEnrollmentPeers); err != nil {
		return nil, err
	}
	if err := state.addChannels(local, snapshot.Channels); err != nil {
		return nil, err
	}
	return state, nil
}

func (authority *Authority) validateSnapshotIdentityAndBounds(
	snapshot NetworkAuthoritySnapshot,
) (libp2ppeer.ID, error) {
	if authority == nil || authority.state.Load() == nil {
		return "", fmt.Errorf("%w: projection is unavailable", ErrNetworkAuthority)
	}
	local, err := canonicalLibp2pID(snapshot.LocalPeerID)
	if err != nil || local != authority.localPeerID {
		return "", fmt.Errorf("%w: snapshot local PeerID changed", ErrNetworkAuthority)
	}
	if len(snapshot.Channels) > model.MaxChannelsPerNode {
		return "", fmt.Errorf("%w: got %d Channels, max %d", ErrNetworkAuthority,
			len(snapshot.Channels), model.MaxChannelsPerNode)
	}
	if len(snapshot.OutboundEnrollmentPeers) > model.MaxChannelsPerNode {
		return "", fmt.Errorf("%w: outbound enrollment permit limit exceeded", ErrNetworkAuthority)
	}
	if len(snapshot.AgencyPeers) > maxAgencyPeers {
		return "", fmt.Errorf("%w: Agency Peer limit exceeded", ErrNetworkAuthority)
	}
	return local, nil
}

func newNetworkAuthorityState(local libp2ppeer.ID,
	snapshot NetworkAuthoritySnapshot,
) *networkAuthorityState {
	return &networkAuthorityState{localPeerID: local,
		channels:           make(map[model.ChannelID]channelAuthorityState, len(snapshot.Channels)),
		channelPhysical:    make(map[libp2ppeer.ID]struct{}),
		agency:             make(map[libp2ppeer.ID]struct{}, len(snapshot.AgencyPeers)),
		physical:           make(map[libp2ppeer.ID]struct{}),
		outboundEnrollment: make(map[libp2ppeer.ID]struct{}, len(snapshot.OutboundEnrollmentPeers))}
}

func (state *networkAuthorityState) addAgencyPeers(local libp2ppeer.ID,
	peers []model.PeerID,
) error {
	for _, candidate := range peers {
		peerID, err := canonicalLibp2pID(candidate)
		if err != nil || peerID == local {
			return fmt.Errorf("%w: invalid Agency Peer", ErrNetworkAuthority)
		}
		if _, duplicate := state.agency[peerID]; duplicate {
			return fmt.Errorf("%w: duplicate Agency Peer", ErrNetworkAuthority)
		}
		state.agency[peerID] = struct{}{}
		state.physical[peerID] = struct{}{}
	}
	return nil
}

func (state *networkAuthorityState) addOutboundEnrollmentPeers(local libp2ppeer.ID,
	permittedPeers []model.PeerID,
) error {
	for _, permitted := range permittedPeers {
		peerID, err := canonicalLibp2pID(permitted)
		if err != nil || peerID == local {
			return fmt.Errorf("%w: invalid outbound enrollment Peer", ErrNetworkAuthority)
		}
		if _, duplicate := state.outboundEnrollment[peerID]; duplicate {
			return fmt.Errorf("%w: duplicate outbound enrollment Peer", ErrNetworkAuthority)
		}
		state.outboundEnrollment[peerID] = struct{}{}
	}
	return nil
}

func (state *networkAuthorityState) addChannels(local libp2ppeer.ID,
	channels []ChannelAuthoritySnapshot,
) error {
	for _, channel := range channels {
		if _, exists := state.channels[channel.ChannelID]; exists {
			return fmt.Errorf("%w: duplicate Channel", ErrNetworkAuthority)
		}
		built, err := buildChannelAuthority(local, channel)
		if err != nil {
			return err
		}
		state.channels[channel.ChannelID] = built
		state.addPhysicalPeers(channel.Status, built.bindings)
	}
	return nil
}

func (state *networkAuthorityState) addPhysicalPeers(status model.ChannelStatus,
	bindings map[libp2ppeer.ID]model.BindingState,
) {
	if status.Terminal() {
		return
	}
	for peerID, binding := range bindings {
		if binding == model.BindingPending || binding == model.BindingActive {
			state.channelPhysical[peerID] = struct{}{}
			state.physical[peerID] = struct{}{}
		}
	}
}
