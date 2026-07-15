package model

import "time"

type PeerBindingSpec struct {
	ChannelID      ChannelID
	PeerID         PeerID
	OriginEpoch    OriginEpoch
	EffectiveAlias string
	PublicKey      []byte
	Multiaddrs     []string
	Protocols      []string
	Limits         JSON
	MemberHead     RecordHead
	State          BindingState
	Reachability   Reachability
	JoinedAt       time.Time
	LastSeenAt     *time.Time
}

type PeerBinding struct {
	spec        PeerBindingSpec
	publicKey   string
	lastSeenAt  time.Time
	hasLastSeen bool
}

func NewPeerBinding(localPeer PeerID, spec PeerBindingSpec) (PeerBinding, error) {
	if localPeer.IsZero() || spec.ChannelID.IsZero() || spec.PeerID.IsZero() ||
		spec.OriginEpoch.IsZero() || spec.MemberHead.IsZero() {
		return PeerBinding{}, invalid("peer binding", "local/remote identity, Channel and member head are required")
	}
	if localPeer == spec.PeerID {
		return PeerBinding{}, invariant("self PeerBinding is forbidden")
	}
	if err := validateIdentifier("effective alias", spec.EffectiveAlias); err != nil {
		return PeerBinding{}, err
	}
	if len(spec.PublicKey) == 0 {
		return PeerBinding{}, invalid("peer public key", "must not be empty")
	}
	if !spec.State.Valid() || !spec.Reachability.Valid() {
		return PeerBinding{}, invalid("peer binding", "unknown state or reachability")
	}
	if spec.Limits.IsZero() || spec.Limits.raw[0] != '{' {
		return PeerBinding{}, invalid("peer limits", "must be a canonical JSON object")
	}
	multiaddrs, err := normalizeRuleStrings("multiaddrs", spec.Multiaddrs)
	if err != nil {
		return PeerBinding{}, err
	}
	protocols, err := normalizeRuleStrings("protocols", spec.Protocols)
	if err != nil {
		return PeerBinding{}, err
	}
	joinedAt, err := canonicalTime(spec.JoinedAt)
	if err != nil {
		return PeerBinding{}, err
	}
	result := PeerBinding{spec: spec, publicKey: string(append([]byte(nil), spec.PublicKey...))}
	result.spec.PublicKey, result.spec.Multiaddrs, result.spec.Protocols = nil, multiaddrs, protocols
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

func (b PeerBinding) ChannelID() ChannelID          { return b.spec.ChannelID }
func (b PeerBinding) PeerID() PeerID                { return b.spec.PeerID }
func (b PeerBinding) OriginEpoch() OriginEpoch      { return b.spec.OriginEpoch }
func (b PeerBinding) EffectiveAlias() string        { return b.spec.EffectiveAlias }
func (b PeerBinding) PublicKey() []byte             { return append([]byte(nil), b.publicKey...) }
func (b PeerBinding) Multiaddrs() []string          { return append([]string(nil), b.spec.Multiaddrs...) }
func (b PeerBinding) Protocols() []string           { return append([]string(nil), b.spec.Protocols...) }
func (b PeerBinding) Limits() JSON                  { return b.spec.Limits }
func (b PeerBinding) MemberHead() RecordHead        { return b.spec.MemberHead }
func (b PeerBinding) State() BindingState           { return b.spec.State }
func (b PeerBinding) Reachability() Reachability    { return b.spec.Reachability }
func (b PeerBinding) JoinedAt() time.Time           { return b.spec.JoinedAt }
func (b PeerBinding) LastSeenAt() (time.Time, bool) { return b.lastSeenAt, b.hasLastSeen }
