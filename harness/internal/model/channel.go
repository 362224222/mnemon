package model

import (
	"sort"
	"strings"
	"time"
)

type ChannelStatus string

const (
	ChannelActive     ChannelStatus = "active"
	ChannelLeaving    ChannelStatus = "leaving"
	ChannelConflicted ChannelStatus = "conflicted"
	ChannelLeft       ChannelStatus = "left"
	ChannelClosed     ChannelStatus = "closed"
	ChannelAbandoned  ChannelStatus = "abandoned"
)

func (s ChannelStatus) Valid() bool {
	switch s {
	case ChannelActive, ChannelLeaving, ChannelConflicted, ChannelLeft, ChannelClosed, ChannelAbandoned:
		return true
	default:
		return false
	}
}

func (s ChannelStatus) Terminal() bool {
	return s == ChannelLeft || s == ChannelClosed || s == ChannelAbandoned
}

type TopicState string

const (
	TopicNotJoined TopicState = "not_joined"
	TopicJoining   TopicState = "joining"
	TopicJoined    TopicState = "joined"
	TopicBlocked   TopicState = "blocked"
	TopicLeft      TopicState = "left"
)

func (s TopicState) Valid() bool {
	return s == TopicNotJoined || s == TopicJoining || s == TopicJoined || s == TopicBlocked || s == TopicLeft
}

type ChannelSpec struct {
	ID             ChannelID
	Name           string
	LocalAlias     string
	OwnerPeerID    PeerID
	OwnerPublicKey []byte
	MemberLimit    uint8
	RosterHead     RecordHead
	Status         ChannelStatus
	TopicState     TopicState
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Channel struct {
	spec           ChannelSpec
	ownerPublicKey string
}

func NewChannel(spec ChannelSpec) (Channel, error) {
	if spec.ID.IsZero() || spec.OwnerPeerID.IsZero() || spec.RosterHead.IsZero() {
		return Channel{}, invalid("channel", "ID, owner and roster head are required")
	}
	if err := validateRuleText("channel name", spec.Name, MaxLabelBytes); err != nil {
		return Channel{}, err
	}
	if err := validateIdentifier("channel local alias", spec.LocalAlias); err != nil {
		return Channel{}, err
	}
	if len(spec.OwnerPublicKey) == 0 {
		return Channel{}, invalid("owner public key", "must not be empty")
	}
	if spec.MemberLimit < 2 || spec.MemberLimit > MaxMembersPerChannel {
		return Channel{}, limit("member limit", int(spec.MemberLimit), MaxMembersPerChannel)
	}
	if !spec.Status.Valid() || !spec.TopicState.Valid() || !validChannelTopic(spec.Status, spec.TopicState) {
		return Channel{}, invariant("Channel status and topic state combination is invalid")
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Channel{}, err
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return Channel{}, err
	}
	if updatedAt.Before(createdAt) {
		return Channel{}, invariant("Channel update time precedes creation time")
	}
	publicKey := string(append([]byte(nil), spec.OwnerPublicKey...))
	spec.OwnerPublicKey = nil
	spec.CreatedAt, spec.UpdatedAt = createdAt, updatedAt
	return Channel{spec: spec, ownerPublicKey: publicKey}, nil
}

func validChannelTopic(status ChannelStatus, topic TopicState) bool {
	switch status {
	case ChannelActive:
		return true
	case ChannelConflicted:
		return topic == TopicBlocked || topic == TopicLeft
	case ChannelLeaving, ChannelLeft, ChannelClosed, ChannelAbandoned:
		return topic == TopicLeft
	default:
		return false
	}
}

func (c Channel) ID() ChannelID          { return c.spec.ID }
func (c Channel) Name() string           { return c.spec.Name }
func (c Channel) LocalAlias() string     { return c.spec.LocalAlias }
func (c Channel) OwnerPeerID() PeerID    { return c.spec.OwnerPeerID }
func (c Channel) OwnerPublicKey() []byte { return append([]byte(nil), c.ownerPublicKey...) }
func (c Channel) MemberLimit() uint8     { return c.spec.MemberLimit }
func (c Channel) RosterHead() RecordHead { return c.spec.RosterHead }
func (c Channel) Status() ChannelStatus  { return c.spec.Status }
func (c Channel) TopicState() TopicState { return c.spec.TopicState }
func (c Channel) CreatedAt() time.Time   { return c.spec.CreatedAt }
func (c Channel) UpdatedAt() time.Time   { return c.spec.UpdatedAt }

type MemberStatus string

const (
	MemberActive  MemberStatus = "active"
	MemberLeft    MemberStatus = "left"
	MemberRevoked MemberStatus = "revoked"
)

func (s MemberStatus) Valid() bool    { return s == MemberActive || s == MemberLeft || s == MemberRevoked }
func (s MemberStatus) Terminal() bool { return s == MemberLeft || s == MemberRevoked }

type MemberSpec struct {
	ChannelID      ChannelID
	Head           RecordHead
	PreviousDigest *Digest
	PeerID         PeerID
	OriginEpoch    OriginEpoch
	DisplayLabel   string
	PublicKey      []byte
	Multiaddrs     []string
	Status         MemberStatus
	SignedRecord   JSON
	OwnerSignature []byte
	CreatedAt      time.Time
}

type Member struct {
	spec           MemberSpec
	previousDigest Digest
	hasPrevious    bool
	publicKey      string
	ownerSignature string
}

func NewMember(spec MemberSpec) (Member, error) {
	if spec.ChannelID.IsZero() || spec.Head.IsZero() || spec.PeerID.IsZero() || spec.OriginEpoch.IsZero() {
		return Member{}, invalid("member", "Channel, record head, PeerID and epoch are required")
	}
	if err := validateRuleText("display label", spec.DisplayLabel, MaxLabelBytes); err != nil {
		return Member{}, err
	}
	if !spec.Status.Valid() || len(spec.PublicKey) == 0 || len(spec.OwnerSignature) == 0 {
		return Member{}, invalid("member", "status, public key and owner signature are required")
	}
	if spec.SignedRecord.IsZero() || spec.SignedRecord.raw[0] != '{' {
		return Member{}, invalid("signed member record", "must be a canonical JSON object")
	}
	if spec.Head.Revision() == 1 && spec.PreviousDigest != nil {
		return Member{}, invariant("genesis MemberRecord cannot have a previous digest")
	}
	if spec.Head.Revision() > 1 && (spec.PreviousDigest == nil || spec.PreviousDigest.IsZero()) {
		return Member{}, invariant("non-genesis MemberRecord requires a previous digest")
	}
	multiaddrs, err := normalizeRuleStrings("multiaddrs", spec.Multiaddrs)
	if err != nil {
		return Member{}, err
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Member{}, err
	}
	result := Member{spec: spec, publicKey: string(append([]byte(nil), spec.PublicKey...)),
		ownerSignature: string(append([]byte(nil), spec.OwnerSignature...))}
	result.spec.PublicKey, result.spec.OwnerSignature = nil, nil
	result.spec.Multiaddrs, result.spec.CreatedAt = multiaddrs, createdAt
	if spec.PreviousDigest != nil {
		result.previousDigest, result.hasPrevious = *spec.PreviousDigest, true
		result.spec.PreviousDigest = nil
	}
	return result, nil
}

func (m Member) ChannelID() ChannelID           { return m.spec.ChannelID }
func (m Member) Head() RecordHead               { return m.spec.Head }
func (m Member) PreviousDigest() (Digest, bool) { return m.previousDigest, m.hasPrevious }
func (m Member) PeerID() PeerID                 { return m.spec.PeerID }
func (m Member) OriginEpoch() OriginEpoch       { return m.spec.OriginEpoch }
func (m Member) DisplayLabel() string           { return m.spec.DisplayLabel }
func (m Member) PublicKey() []byte              { return append([]byte(nil), m.publicKey...) }
func (m Member) Multiaddrs() []string           { return append([]string(nil), m.spec.Multiaddrs...) }
func (m Member) Status() MemberStatus           { return m.spec.Status }
func (m Member) SignedRecord() JSON             { return m.spec.SignedRecord }
func (m Member) OwnerSignature() []byte         { return append([]byte(nil), m.ownerSignature...) }
func (m Member) CreatedAt() time.Time           { return m.spec.CreatedAt }

type BindingState string

const (
	BindingPending BindingState = "pending"
	BindingActive  BindingState = "active"
	BindingRevoked BindingState = "revoked"
)

func (s BindingState) Valid() bool {
	return s == BindingPending || s == BindingActive || s == BindingRevoked
}

type Reachability string

const (
	ReachabilityUnknown     Reachability = "unknown"
	ReachabilityReachable   Reachability = "reachable"
	ReachabilityUnreachable Reachability = "unreachable"
)

func (r Reachability) Valid() bool {
	return r == ReachabilityUnknown || r == ReachabilityReachable || r == ReachabilityUnreachable
}

func validateRuleText(field, value string, max int) error {
	if err := validateText(field, value, max, false); err != nil {
		return err
	}
	if strings.TrimSpace(value) != value {
		return invalid(field, "must not have surrounding whitespace")
	}
	return nil
}

func normalizeRuleStrings(field string, values []string) ([]string, error) {
	result := append([]string{}, values...)
	for _, value := range result {
		if err := validateIdentifier(field, value); err != nil {
			return nil, err
		}
	}
	sort.Strings(result)
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, invalid(field, "contains a duplicate value")
		}
	}
	return result, nil
}
