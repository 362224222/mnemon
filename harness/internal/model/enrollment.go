package model

import "time"

// OpenEnrollmentGrant is the durable, secret-free half of an invite. The
// bearer secret and encoded token never cross this model or enter SQLite.
type OpenEnrollmentGrant struct {
	id        GrantID
	channelID ChannelID
	verifier  Digest
	expiresAt time.Time
	maxUses   uint8
	createdAt time.Time
}

func NewOpenEnrollmentGrant(id GrantID, channelID ChannelID, verifier Digest,
	expiresAt time.Time, maxUses uint8, createdAt time.Time,
) (OpenEnrollmentGrant, error) {
	if id.IsZero() || channelID.IsZero() || verifier.IsZero() {
		return OpenEnrollmentGrant{}, invalid("enrollment grant", "ID, Channel and verifier are required")
	}
	if maxUses == 0 || maxUses >= MaxMembersPerChannel {
		return OpenEnrollmentGrant{}, limit("enrollment grant uses", int(maxUses), MaxMembersPerChannel-1)
	}
	created, err := canonicalTime(createdAt)
	if err != nil {
		return OpenEnrollmentGrant{}, err
	}
	expires, err := canonicalTime(expiresAt)
	if err != nil {
		return OpenEnrollmentGrant{}, err
	}
	if !expires.After(created) {
		return OpenEnrollmentGrant{}, invariant("enrollment grant must expire after creation")
	}
	return OpenEnrollmentGrant{id: id, channelID: channelID, verifier: verifier,
		expiresAt: expires, maxUses: maxUses, createdAt: created}, nil
}

func (grant OpenEnrollmentGrant) ID() GrantID          { return grant.id }
func (grant OpenEnrollmentGrant) ChannelID() ChannelID { return grant.channelID }
func (grant OpenEnrollmentGrant) Verifier() Digest     { return grant.verifier }
func (grant OpenEnrollmentGrant) ExpiresAt() time.Time { return grant.expiresAt }
func (grant OpenEnrollmentGrant) MaxUses() uint8       { return grant.maxUses }
func (grant OpenEnrollmentGrant) CreatedAt() time.Time { return grant.createdAt }
func (grant OpenEnrollmentGrant) IsZero() bool {
	return grant.id.IsZero() || grant.channelID.IsZero() || grant.verifier.IsZero()
}
