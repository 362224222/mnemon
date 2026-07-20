package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"hash"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	channelEnrollmentAuthorityEvidenceDomain = "mnemon/r5/channel-enrollment-authority-evidence/1"
	channelEnrollmentGrantEvidenceDomain     = "mnemon/r5/channel-enrollment-grant-evidence/1"
	channelEnrollmentUseEvidenceDomain       = "mnemon/r5/channel-enrollment-use-evidence/1"
)

type channelEnrollmentEvidence struct {
	authority  model.Digest
	grant      channelEnrollmentGrantFence
	usePresent bool
	use        model.Digest
}

func (evidence channelEnrollmentEvidence) valid() bool {
	return !evidence.authority.IsZero() && evidence.grant.valid() &&
		(evidence.usePresent == !evidence.use.IsZero())
}

type channelEnrollmentGrantFence struct {
	id             model.GrantID
	channelID      model.ChannelID
	verifierDigest model.Digest
	expiresAt      time.Time
	maxUses        uint8
	usedUses       uint8
	status         string
	createdAt      time.Time
	closedAt       time.Time
	hasClosedAt    bool
	useCount       uint8
}

func (fence channelEnrollmentGrantFence) valid() bool {
	return !fence.id.IsZero() && !fence.channelID.IsZero() && !fence.verifierDigest.IsZero() &&
		!fence.expiresAt.IsZero() && !fence.createdAt.IsZero() && fence.maxUses != 0 && fence.status != ""
}

func buildChannelEnrollmentEvidence(node model.Node, authority verifiedChannelAuthority,
	grant durableEnrollmentGrant, use durableChannelEnrollmentUse, useExists bool,
) (channelEnrollmentEvidence, error) {
	authorityDigest, err := fingerprintChannelEnrollmentAuthority(node, authority)
	if err != nil {
		return channelEnrollmentEvidence{}, err
	}
	grantFence, err := freezeChannelEnrollmentGrant(grant)
	if err != nil {
		return channelEnrollmentEvidence{}, err
	}
	var useDigest model.Digest
	if useExists {
		useDigest, err = fingerprintChannelEnrollmentUse(use)
		if err != nil {
			return channelEnrollmentEvidence{}, err
		}
	}
	return channelEnrollmentEvidence{authority: authorityDigest, grant: grantFence,
		usePresent: useExists, use: useDigest}, nil
}

func freezeChannelEnrollmentGrant(grant durableEnrollmentGrant) (channelEnrollmentGrantFence, error) {
	verifierMaterial := append([]byte(channelEnrollmentGrantEvidenceDomain+"\x00"), grant.verifier.Bytes()...)
	fence := channelEnrollmentGrantFence{id: grant.id, channelID: grant.channelID,
		verifierDigest: model.Sum(verifierMaterial), expiresAt: grant.expiresAt,
		maxUses: grant.maxUses, usedUses: grant.usedUses, status: grant.status,
		createdAt: grant.createdAt, useCount: grant.useCount}
	if grant.closedAt.Valid {
		closedAt, err := parseCanonicalStoreTime(grant.closedAt.String)
		if err != nil {
			return channelEnrollmentGrantFence{}, ErrChannelEnrollmentConflict
		}
		fence.closedAt, fence.hasClosedAt = closedAt, true
	}
	if !fence.valid() {
		return channelEnrollmentGrantFence{}, ErrChannelEnrollmentConflict
	}
	return fence, nil
}

func fingerprintChannelEnrollmentAuthority(node model.Node,
	authority verifiedChannelAuthority,
) (model.Digest, error) {
	if node.PeerID().IsZero() || node.OriginEpoch().IsZero() || authority.channel.ID().IsZero() ||
		authority.roster.IsZero() {
		return model.Digest{}, ErrChannelEnrollmentConflict
	}
	digest := sha256.New()
	writeAuthorityField(digest, []byte(channelEnrollmentAuthorityEvidenceDomain))
	writeAuthorityField(digest, []byte(node.PeerID().String()))
	writeAuthorityField(digest, []byte(node.OriginEpoch().String()))
	writeExactChannelEnrollmentProjection(digest, authority)
	return model.DigestFromBytes(digest.Sum(nil))
}

func writeExactChannelEnrollmentProjection(digest hash.Hash, authority verifiedChannelAuthority) {
	channel := authority.channel
	writeAuthorityField(digest, channel.Descriptor().WireJSON().Bytes())
	for _, value := range []string{channel.ID().String(), channel.LocalAlias(), string(channel.Status()),
		string(channel.TopicState()), storeTime(channel.CreatedAt()), storeTime(channel.UpdatedAt())} {
		writeAuthorityField(digest, []byte(value))
	}
	writeAuthorityUint(digest, channel.RosterHead().Revision())
	writeAuthorityField(digest, channel.RosterHead().Digest().Bytes())
	members := authority.roster.Members()
	writeAuthorityUint(digest, uint64(len(members)))
	for _, member := range members {
		writeAuthorityField(digest, member.WireJSON().Bytes())
	}
	bindings := append([]model.PeerBinding(nil), authority.bindings...)
	sort.Slice(bindings, func(left, right int) bool {
		return bindings[left].PeerID().String() < bindings[right].PeerID().String()
	})
	writeAuthorityUint(digest, uint64(len(bindings)))
	for _, binding := range bindings {
		writeExactChannelEnrollmentBinding(digest, binding)
	}
}

func writeExactChannelEnrollmentBinding(digest hash.Hash, binding model.PeerBinding) {
	writeAuthorityBinding(digest, binding)
	for _, value := range []string{binding.ChannelID().String(), binding.EffectiveAlias(),
		string(binding.Reachability()), storeTime(binding.JoinedAt())} {
		writeAuthorityField(digest, []byte(value))
	}
	lastSeen, ok := binding.LastSeenAt()
	if ok {
		writeAuthorityField(digest, []byte(storeTime(lastSeen)))
	} else {
		writeAuthorityField(digest, nil)
	}
}

func fingerprintChannelEnrollmentUse(use durableChannelEnrollmentUse) (model.Digest, error) {
	if use.useID.IsZero() || use.grantID.IsZero() || use.joinIdentity.IsZero() ||
		use.member.IsZero() || use.receipt.IsZero() || use.usedAt.IsZero() {
		return model.Digest{}, ErrChannelEnrollmentConflict
	}
	digest := sha256.New()
	writeAuthorityField(digest, []byte(channelEnrollmentUseEvidenceDomain))
	for _, value := range []string{use.useID.String(), use.grantID.String(), storeTime(use.usedAt)} {
		writeAuthorityField(digest, []byte(value))
	}
	writeAuthorityField(digest, use.joinIdentity.Bytes())
	writeAuthorityField(digest, use.member.WireJSON().Bytes())
	writeAuthorityField(digest, use.receipt.WireJSON().Bytes())
	return model.DigestFromBytes(digest.Sum(nil))
}

func verifyUnusedChannelEnrollmentIDs(ctx context.Context, tx *sql.Tx,
	input channelEnrollmentInput,
) error {
	var rows int
	err := tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM enrollment_grant_uses WHERE channel_id=? AND member_peer_id=?) +
		(SELECT COUNT(*) FROM enrollment_grant_uses WHERE use_id=?) +
		(SELECT COUNT(*) FROM enrollment_receipts WHERE receipt_id=?)`,
		input.transcript.ChannelID().String(), input.authenticatedPeer.String(), input.useID.String(),
		input.receiptID.String()).Scan(&rows)
	if err != nil {
		return fmt.Errorf("inspect unused Channel enrollment evidence: %w", err)
	}
	if rows != 0 {
		return ErrChannelEnrollmentConflict
	}
	return nil
}
