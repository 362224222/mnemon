package model

import (
	"fmt"
	"time"
)

type PeerBindingSpec struct {
	Channel        Channel
	Roster         VerifiedRoster
	PeerID         PeerID
	EffectiveAlias string
	State          BindingState
	Reachability   Reachability
	JoinedAt       time.Time
	LastSeenAt     *time.Time
}

// PeerBinding is derived authority, never an independently asserted peer
// descriptor. Its identity, epoch, key, addresses, protocols, limits and
// roster head all come from one descriptor-bound owner-signed MemberRecord.
type PeerBinding struct {
	spec        PeerBindingSpec
	member      Member
	rosterHead  RecordHead
	lastSeenAt  time.Time
	hasLastSeen bool
}

func NewPeerBinding(localPeer PeerID, spec PeerBindingSpec) (PeerBinding, error) {
	if localPeer.IsZero() || spec.Channel.ID().IsZero() || spec.Roster.IsZero() || spec.PeerID.IsZero() {
		return PeerBinding{}, invalid("peer binding",
			"local identity, committed Channel, verified roster and remote PeerID are required")
	}
	if spec.Channel.RosterHead() != spec.Roster.Head() ||
		spec.Channel.Descriptor().WireJSON().String() != spec.Roster.Descriptor().WireJSON().String() {
		return PeerBinding{}, invariant("PeerBinding roster does not equal the committed Channel head")
	}
	member, ok := spec.Roster.CurrentMember(spec.PeerID)
	if !ok {
		return PeerBinding{}, fmt.Errorf("peer binding member authority: %w", ErrInvariant)
	}
	if localPeer == member.PeerID() {
		return PeerBinding{}, invariant("self PeerBinding is forbidden")
	}
	if err := validateIdentifier("effective alias", spec.EffectiveAlias); err != nil {
		return PeerBinding{}, err
	}
	if !spec.State.Valid() || !spec.Reachability.Valid() {
		return PeerBinding{}, invalid("peer binding", "unknown state or reachability")
	}
	if member.Status() == MemberActive && spec.State == BindingRevoked ||
		member.Status().Terminal() && spec.State != BindingRevoked {
		return PeerBinding{}, invariant("PeerBinding authority must match latest MemberRecord status")
	}
	joinedAt, err := canonicalTime(spec.JoinedAt)
	if err != nil {
		return PeerBinding{}, err
	}
	if joinedAt.Before(spec.Roster.Descriptor().Descriptor().CreatedAt()) {
		return PeerBinding{}, invariant("PeerBinding join time precedes Channel creation")
	}
	result := PeerBinding{spec: spec, member: member, rosterHead: spec.Roster.Head()}
	result.spec.Channel, result.spec.Roster, result.spec.PeerID = Channel{}, VerifiedRoster{}, PeerID{}
	result.spec.JoinedAt, result.spec.LastSeenAt = joinedAt, nil
	if spec.LastSeenAt != nil {
		lastSeen, err := canonicalTime(*spec.LastSeenAt)
		if err != nil {
			return PeerBinding{}, err
		}
		if lastSeen.Before(joinedAt) {
			return PeerBinding{}, invariant("Peer last-seen time precedes join time")
		}
		result.lastSeenAt, result.hasLastSeen = lastSeen, true
	}
	return result, nil
}

func (binding PeerBinding) ChannelID() ChannelID       { return binding.member.ChannelID() }
func (binding PeerBinding) PeerID() PeerID             { return binding.member.PeerID() }
func (binding PeerBinding) OriginEpoch() OriginEpoch   { return binding.member.OriginEpoch() }
func (binding PeerBinding) EffectiveAlias() string     { return binding.spec.EffectiveAlias }
func (binding PeerBinding) PublicKey() []byte          { return binding.member.PublicKey() }
func (binding PeerBinding) Multiaddrs() []string       { return binding.member.Multiaddrs() }
func (binding PeerBinding) Protocols() []string        { return binding.member.Protocols() }
func (binding PeerBinding) Limits() JSON               { return binding.member.Limits() }
func (binding PeerBinding) MemberHead() RecordHead     { return binding.member.Head() }
func (binding PeerBinding) RosterHead() RecordHead     { return binding.rosterHead }
func (binding PeerBinding) State() BindingState        { return binding.spec.State }
func (binding PeerBinding) Reachability() Reachability { return binding.spec.Reachability }
func (binding PeerBinding) JoinedAt() time.Time        { return binding.spec.JoinedAt }
func (binding PeerBinding) LastSeenAt() (time.Time, bool) {
	return binding.lastSeenAt, binding.hasLastSeen
}
