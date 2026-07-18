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
	Descriptor SignedChannelDescriptor
	LocalAlias string
	RosterHead RecordHead
	Status     ChannelStatus
	TopicState TopicState
	UpdatedAt  time.Time
}

type Channel struct {
	spec ChannelSpec
}

func NewChannel(spec ChannelSpec) (Channel, error) {
	if spec.Descriptor.IsZero() || spec.RosterHead.IsZero() {
		return Channel{}, invalid("channel", "signed descriptor and roster head are required")
	}
	if err := VerifyChannelDescriptor(spec.Descriptor); err != nil {
		return Channel{}, err
	}
	if err := validateIdentifier("channel local alias", spec.LocalAlias); err != nil {
		return Channel{}, err
	}
	if !spec.Status.Valid() || !spec.TopicState.Valid() || !validChannelTopic(spec.Status, spec.TopicState) {
		return Channel{}, invariant("Channel status and topic state combination is invalid")
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return Channel{}, err
	}
	if updatedAt.Before(spec.Descriptor.Descriptor().CreatedAt()) {
		return Channel{}, invariant("Channel update time precedes creation time")
	}
	spec.UpdatedAt = updatedAt
	return Channel{spec: spec}, nil
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

func (c Channel) ID() ChannelID                       { return c.spec.Descriptor.Descriptor().ID() }
func (c Channel) Name() string                        { return c.spec.Descriptor.Descriptor().Name() }
func (c Channel) LocalAlias() string                  { return c.spec.LocalAlias }
func (c Channel) OwnerPeerID() PeerID                 { return c.spec.Descriptor.Descriptor().OwnerPeerID() }
func (c Channel) OwnerPublicKey() []byte              { return c.spec.Descriptor.Descriptor().OwnerPublicKey() }
func (c Channel) Descriptor() SignedChannelDescriptor { return c.spec.Descriptor }
func (c Channel) MemberLimit() uint8                  { return c.spec.Descriptor.Descriptor().MemberLimit() }
func (c Channel) RosterHead() RecordHead              { return c.spec.RosterHead }
func (c Channel) Status() ChannelStatus               { return c.spec.Status }
func (c Channel) TopicState() TopicState              { return c.spec.TopicState }
func (c Channel) CreatedAt() time.Time                { return c.spec.Descriptor.Descriptor().CreatedAt() }
func (c Channel) UpdatedAt() time.Time                { return c.spec.UpdatedAt }

type MemberStatus string

const (
	MemberActive  MemberStatus = "active"
	MemberLeft    MemberStatus = "left"
	MemberRevoked MemberStatus = "revoked"
)

func (s MemberStatus) Valid() bool    { return s == MemberActive || s == MemberLeft || s == MemberRevoked }
func (s MemberStatus) Terminal() bool { return s == MemberLeft || s == MemberRevoked }

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
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return invalid(field, "must be a sanitized single line")
		}
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
