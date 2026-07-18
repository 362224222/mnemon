package model

import (
	"bytes"
	"fmt"
	"time"
)

// VerifiedRoster is a complete, continuous owner-signed roster prefix. It is
// the only model value that can identify a current MemberRecord; verifying an
// isolated historical record is deliberately insufficient for authority.
type VerifiedRoster struct {
	descriptor SignedChannelDescriptor
	members    []Member
	current    map[PeerID]Member
	head       RecordHead
}

// NewVerifiedRoster verifies the descriptor, every owner signature, the
// global hash chain and each peer's lifecycle before exposing current member
// authority. Callers loading durable state must also compare Head with the
// Channel row's committed roster head in the same snapshot.
func NewVerifiedRoster(descriptor SignedChannelDescriptor, members []Member) (VerifiedRoster, error) {
	if err := VerifyChannelDescriptor(descriptor); err != nil || len(members) == 0 {
		return VerifiedRoster{}, invalid("verified roster", "descriptor and a non-empty roster are required")
	}
	current := make(map[PeerID]Member, descriptor.Descriptor().MemberLimit())
	active := make(map[PeerID]struct{}, descriptor.Descriptor().MemberLimit())
	verified := make([]Member, len(members))
	var previous Member
	var lastCreatedAt time.Time
	ownerClosed := false
	for index, member := range members {
		expectedRevision := uint64(index + 1)
		if ownerClosed {
			return VerifiedRoster{}, invariant("Channel owner leave must be the final roster record")
		}
		if err := VerifyMember(descriptor, member); err != nil {
			return VerifiedRoster{}, fmt.Errorf("verified roster revision %d: %w", expectedRevision, err)
		}
		if member.PeerID() == descriptor.Descriptor().OwnerPeerID() &&
			member.Status() == MemberRevoked {
			return VerifiedRoster{}, invariant("Channel owner may leave but cannot revoke itself")
		}
		if member.PeerID() == descriptor.Descriptor().OwnerPeerID() && member.Status() == MemberLeft {
			ownerClosed = true
		}
		if member.Head().Revision() != expectedRevision {
			return VerifiedRoster{}, invariant("verified roster revisions must be contiguous and ordered")
		}
		previousDigest, hasPrevious := member.PreviousDigest()
		if index == 0 {
			if hasPrevious {
				return VerifiedRoster{}, invariant("verified roster genesis has a previous digest")
			}
		} else if !hasPrevious || previousDigest != previous.Head().Digest() {
			return VerifiedRoster{}, invariant("verified roster hash chain is discontinuous")
		}
		if !lastCreatedAt.IsZero() && member.CreatedAt().Before(lastCreatedAt) {
			return VerifiedRoster{}, invariant("verified roster creation times regress")
		}
		prior, seen := current[member.PeerID()]
		if !seen {
			if member.Status() != MemberActive {
				return VerifiedRoster{}, invariant("a peer's first roster record must be active")
			}
			active[member.PeerID()] = struct{}{}
		} else {
			if prior.Status().Terminal() {
				return VerifiedRoster{}, invariant("terminal member authority cannot be extended")
			}
			if prior.OriginEpoch() != member.OriginEpoch() ||
				!bytes.Equal(prior.PublicKey(), member.PublicKey()) {
				return VerifiedRoster{}, invariant("member identity and origin epoch are immutable")
			}
			if member.Status().Terminal() {
				delete(active, member.PeerID())
			}
		}
		if len(active) > int(descriptor.Descriptor().MemberLimit()) {
			return VerifiedRoster{}, limit("active Channel members", len(active),
				int(descriptor.Descriptor().MemberLimit()))
		}
		verified[index] = member
		current[member.PeerID()] = member
		previous = member
		lastCreatedAt = member.CreatedAt()
	}
	return VerifiedRoster{descriptor: descriptor, members: verified, current: current,
		head: previous.Head()}, nil
}

func (roster VerifiedRoster) Descriptor() SignedChannelDescriptor { return roster.descriptor }
func (roster VerifiedRoster) Head() RecordHead                    { return roster.head }
func (roster VerifiedRoster) Members() []Member {
	return append([]Member(nil), roster.members...)
}
func (roster VerifiedRoster) CurrentMember(peerID PeerID) (Member, bool) {
	member, ok := roster.current[peerID]
	return member, ok
}
func (roster VerifiedRoster) IsZero() bool {
	return roster.descriptor.IsZero() || roster.head.IsZero() || len(roster.members) == 0
}
