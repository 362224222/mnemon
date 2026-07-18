package peer

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ProjectNetworkAuthority strictly converts one coherent Store mesh snapshot
// into the bounded, immutable authority consumed by libp2p callbacks. Durable
// terminal Channels remain queryable from Store but grant no runtime network
// authority and are omitted here.
func ProjectNetworkAuthority(mesh store.ChannelMeshAuthority) (NetworkAuthoritySnapshot, error) {
	result := NetworkAuthoritySnapshot{LocalPeerID: mesh.LocalPeerID()}
	for _, durable := range mesh.Channels() {
		channel := durable.Channel()
		switch channel.Status() {
		case model.ChannelLeft, model.ChannelClosed, model.ChannelAbandoned:
			continue
		case model.ChannelActive, model.ChannelLeaving, model.ChannelConflicted:
			// Projected below. Leaving and conflicted Channels retain only
			// physical/control authority; Authority gates their data plane and
			// Gossip topic by status.
		default:
			return NetworkAuthoritySnapshot{}, fmt.Errorf("%w: unknown durable Channel status",
				ErrNetworkAuthority)
		}

		roster := durable.Roster()
		members := roster.Members()
		bindings := durable.Bindings()
		projected := ChannelAuthoritySnapshot{ChannelID: channel.ID(), Status: channel.Status(),
			RosterHead:          channel.RosterHead(),
			VerifiedRosterHeads: make([]model.RecordHead, 0, len(members)),
			Bindings:            make([]BindingAuthoritySnapshot, 0, len(bindings)),
			Members:             make([]MemberAuthoritySnapshot, 0, len(members))}
		for _, member := range members {
			projected.VerifiedRosterHeads = append(projected.VerifiedRosterHeads, member.Head())
			if member.Status() == model.MemberActive {
				// Keep each historical active credential, not just the latest one.
				// A delayed immutable publication names its exact roster head.
				projected.Members = append(projected.Members, MemberAuthoritySnapshot{
					PeerID: member.PeerID(), OriginEpoch: member.OriginEpoch(), Head: member.Head(),
					PublicKey: member.PublicKey(),
				})
			}
		}
		for _, binding := range bindings {
			if binding.State() != model.BindingPending && binding.State() != model.BindingActive {
				return NetworkAuthoritySnapshot{}, fmt.Errorf("%w: Store exported a non-live PeerBinding",
					ErrNetworkAuthority)
			}
			projected.Bindings = append(projected.Bindings, BindingAuthoritySnapshot{
				PeerID: binding.PeerID(), State: binding.State(),
			})
		}
		result.Channels = append(result.Channels, projected)
	}

	// Reuse the callback projection's own strict validator before returning a
	// candidate. This catches bounds, canonical PeerIDs, topic derivation,
	// roster continuity, binding/member coupling and credential/key mismatch.
	authority, err := NewAuthority(result.LocalPeerID)
	if err != nil {
		return NetworkAuthoritySnapshot{}, err
	}
	if _, err := authority.prepare(result); err != nil {
		return NetworkAuthoritySnapshot{}, err
	}
	return result, nil
}
