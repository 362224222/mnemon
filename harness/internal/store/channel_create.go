package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelCreateInput    = errors.New("invalid Channel create input")
	ErrChannelCreateConflict = errors.New("Channel create conflicts with durable state")
	ErrNodeChannelLimit      = errors.New("node Channel limit reached")
)

type CreateChannelSpec struct {
	Channel model.Channel
	Genesis model.Member
	Token   model.EnrollmentToken
}

type CreateChannelResult struct {
	Created bool
	Channel model.Channel
	GrantID model.GrantID
}

// CreateChannel atomically persists the signed descriptor, owner genesis,
// local publication epoch and first bearer-secret-free enrollment verifier.
// The signed token crosses this transient boundary so its exact grant can be
// derived, but only the verifier is written to SQLite.
func (s *Store) CreateChannel(ctx context.Context, spec CreateChannelSpec) (CreateChannelResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Genesis.IsZero() || spec.Token.IsZero() {
		return CreateChannelResult{}, ErrChannelCreateInput
	}
	channel, grant, err := validateCreateChannelSpec(spec)
	if err != nil {
		return CreateChannelResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CreateChannelResult{}, fmt.Errorf("create Channel: begin: %w", err)
	}
	defer tx.Rollback()
	node, err := readNode(ctx, tx)
	if err != nil || !localGenesisOwner(node, channel, spec.Genesis) {
		return CreateChannelResult{}, fmt.Errorf("%w: local Node is not the genesis owner: %v",
			ErrChannelCreateInput, err)
	}

	var exists int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM channels WHERE channel_id=?", channel.ID().String()).
		Scan(&exists); err != nil {
		return CreateChannelResult{}, fmt.Errorf("create Channel: inspect replay: %w", err)
	}
	if exists != 0 {
		result, err := replayCreateChannel(ctx, tx, node.PeerID(), spec, grant)
		if err != nil {
			return CreateChannelResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return CreateChannelResult{}, fmt.Errorf("create Channel: commit replay read: %w", err)
		}
		return result, nil
	}

	if err := insertCreatedChannel(ctx, tx, node, channel, spec.Genesis, grant); err != nil {
		return CreateChannelResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CreateChannelResult{}, mapChannelCreateError(err)
	}
	return CreateChannelResult{Created: true, Channel: channel, GrantID: grant.ID()}, nil
}

func validateCreateChannelSpec(spec CreateChannelSpec) (model.Channel, model.OpenEnrollmentGrant, error) {
	channel := spec.Channel
	grant, grantErr := model.NewOpenEnrollmentGrantForToken(spec.Token, channel.CreatedAt())
	genesisAddresses, genesisAddressErr := model.AdvertisedAddressDigest(spec.Genesis.Multiaddrs())
	tokenAddresses, tokenAddressErr := model.AdvertisedAddressDigest(spec.Token.Payload().OwnerMultiaddrs())
	if channel.ID().IsZero() || channel.Status() != model.ChannelActive ||
		channel.TopicState() != model.TopicNotJoined || grantErr != nil || grant.ChannelID() != channel.ID() ||
		!bytes.Equal(spec.Token.Payload().Descriptor().WireJSON().Bytes(), channel.Descriptor().WireJSON().Bytes()) ||
		genesisAddressErr != nil || tokenAddressErr != nil || genesisAddresses != tokenAddresses ||
		channel.UpdatedAt() != channel.CreatedAt() || grant.CreatedAt() != channel.CreatedAt() ||
		grant.MaxUses() != channel.MemberLimit()-1 {
		return model.Channel{}, model.OpenEnrollmentGrant{},
			fmt.Errorf("%w: inconsistent Channel, topic or grant", ErrChannelCreateInput)
	}
	roster, err := model.NewVerifiedRoster(channel.Descriptor(), []model.Member{spec.Genesis})
	if err != nil || roster.Head() != channel.RosterHead() {
		return model.Channel{}, model.OpenEnrollmentGrant{},
			fmt.Errorf("%w: genesis authority: %v", ErrChannelCreateInput, err)
	}
	return channel, grant, nil
}

func localGenesisOwner(node model.Node, channel model.Channel, genesis model.Member) bool {
	return node.PeerID() == channel.OwnerPeerID() && node.PeerID() == genesis.PeerID() &&
		node.OriginEpoch() == genesis.OriginEpoch()
}

func insertCreatedChannel(ctx context.Context, tx *sql.Tx, node model.Node, channel model.Channel,
	genesis model.Member, grant model.OpenEnrollmentGrant,
) error {
	descriptor := channel.Descriptor().Descriptor()
	_, err := tx.ExecContext(ctx, `INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,
		owner_public_key,descriptor_json,descriptor_digest,descriptor_signature,member_limit,
		roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, channel.ID().String(), channel.Name(), channel.LocalAlias(),
		channel.OwnerPeerID().String(), channel.OwnerPublicKey(), descriptor.CanonicalJSON().Bytes(),
		descriptor.Digest().Bytes(), channel.Descriptor().OwnerSignature(), channel.MemberLimit(),
		channel.RosterHead().Revision(), channel.RosterHead().Digest().Bytes(), string(channel.Status()),
		string(channel.TopicState()), storeTime(channel.CreatedAt()), storeTime(channel.UpdatedAt()))
	if err != nil {
		return mapChannelCreateError(err)
	}
	if err := insertChannelMember(ctx, tx, genesis); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at) VALUES(?,?,?,1,0,?)`,
		channel.ID().String(), node.PeerID().String(), node.OriginEpoch().String(), storeTime(channel.CreatedAt()))
	if err != nil {
		return fmt.Errorf("create Channel: initialize publication epoch: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO enrollment_grants(grant_id,channel_id,verifier,expires_at,
		max_uses,used_uses,status,created_at,closed_at) VALUES(?,?,?,?,?,0,'open',?,NULL)`,
		grant.ID().String(), channel.ID().String(), grant.Verifier().Bytes(),
		storeTime(grant.ExpiresAt()), grant.MaxUses(), storeTime(grant.CreatedAt()))
	if err != nil {
		return mapChannelCreateError(fmt.Errorf("insert enrollment grant: %w", err))
	}
	return nil
}

func insertChannelMember(ctx context.Context, tx *sql.Tx, member model.Member) error {
	multiaddrs, err := model.JSONFrom(member.Multiaddrs())
	if err != nil {
		return fmt.Errorf("create Channel: encode member multiaddrs: %w", err)
	}
	protocols, err := model.JSONFrom(member.Protocols())
	if err != nil {
		return fmt.Errorf("create Channel: encode member protocols: %w", err)
	}
	var previous any
	if digest, ok := member.PreviousDigest(); ok {
		previous = digest.Bytes()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_members(channel_id,revision,record_hash,
		previous_hash,member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,
		protocols_json,limits_json,status,signed_record_json,owner_signature,created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, member.ChannelID().String(), member.Head().Revision(),
		member.Head().Digest().Bytes(), previous, member.PeerID().String(), member.OriginEpoch().String(),
		member.DisplayLabel(), member.PublicKey(), multiaddrs.Bytes(), protocols.Bytes(), member.Limits().Bytes(),
		string(member.Status()), member.SignedRecord().Bytes(), member.OwnerSignature(), storeTime(member.CreatedAt()))
	if err != nil {
		return fmt.Errorf("create Channel: insert MemberRecord: %w", err)
	}
	return nil
}

func replayCreateChannel(ctx context.Context, tx *sql.Tx, localPeer model.PeerID,
	spec CreateChannelSpec, grant model.OpenEnrollmentGrant,
) (CreateChannelResult, error) {
	authority, err := readVerifiedChannelAuthority(ctx, tx, localPeer, spec.Channel.ID())
	if err != nil {
		return CreateChannelResult{}, fmt.Errorf("%w: %v", ErrChannelCreateConflict, err)
	}
	members := authority.roster.Members()
	if len(members) == 0 || !bytes.Equal(authority.channel.Descriptor().WireJSON().Bytes(),
		spec.Channel.Descriptor().WireJSON().Bytes()) || authority.channel.LocalAlias() != spec.Channel.LocalAlias() ||
		!bytes.Equal(members[0].WireJSON().Bytes(), spec.Genesis.WireJSON().Bytes()) {
		return CreateChannelResult{}, ErrChannelCreateConflict
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return CreateChannelResult{}, fmt.Errorf("%w: %v", ErrChannelCreateConflict, err)
	}
	var channelText, expiryText, createdText, status string
	var verifier []byte
	var maxUses, usedUses uint8
	var closedText sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT channel_id,verifier,expires_at,max_uses,used_uses,status,
		created_at,closed_at FROM enrollment_grants WHERE grant_id=?`, grant.ID().String()).
		Scan(&channelText, &verifier, &expiryText, &maxUses, &usedUses, &status, &createdText, &closedText)
	if err != nil || channelText != spec.Channel.ID().String() ||
		!bytes.Equal(verifier, grant.Verifier().Bytes()) || expiryText != storeTime(grant.ExpiresAt()) ||
		maxUses != grant.MaxUses() ||
		createdText != storeTime(grant.CreatedAt()) {
		return CreateChannelResult{}, fmt.Errorf("%w: enrollment grant replay differs: %v",
			ErrChannelCreateConflict, err)
	}
	var useCount uint8
	var lastUseText sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrollment_grant_uses WHERE grant_id=?`,
		grant.ID().String()).Scan(&useCount); err != nil {
		return CreateChannelResult{}, fmt.Errorf("%w: enrollment grant ledger is inconsistent: %v",
			ErrChannelCreateConflict, err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT MAX(used_at) FROM enrollment_grant_uses WHERE grant_id=?`,
		grant.ID().String()).Scan(&lastUseText); err != nil || useCount != usedUses ||
		!validEnrollmentGrantState(status, usedUses, maxUses, closedText, grant.CreatedAt(),
			grant.ExpiresAt(), lastUseText) {
		return CreateChannelResult{}, fmt.Errorf("%w: enrollment grant ledger is inconsistent: %v",
			ErrChannelCreateConflict, err)
	}
	return CreateChannelResult{Channel: authority.channel, GrantID: grant.ID()}, nil
}

func validEnrollmentGrantState(status string, usedUses, maxUses uint8, closedText sql.NullString,
	createdAt, expiresAt time.Time, lastUseText sql.NullString,
) bool {
	if usedUses > maxUses || !expiresAt.After(createdAt) || (usedUses == 0) != !lastUseText.Valid {
		return false
	}
	var lastUse time.Time
	if lastUseText.Valid {
		var err error
		lastUse, err = parseCanonicalStoreTime(lastUseText.String)
		if err != nil || lastUse.Before(createdAt) || !lastUse.Before(expiresAt) {
			return false
		}
	}
	if status == "open" {
		return usedUses < maxUses && !closedText.Valid
	}
	if !closedText.Valid {
		return false
	}
	closedAt, err := parseCanonicalStoreTime(closedText.String)
	if err != nil || closedAt.Before(createdAt) || (lastUseText.Valid && closedAt.Before(lastUse)) {
		return false
	}
	switch status {
	case "exhausted":
		return usedUses == maxUses && lastUseText.Valid && closedAt.Equal(lastUse)
	case "closed":
		return usedUses < maxUses && closedAt.Before(expiresAt)
	case "expired":
		return usedUses < maxUses && !closedAt.Before(expiresAt)
	default:
		return false
	}
}

func mapChannelCreateError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "node_channel_limit") {
		return fmt.Errorf("%w: %v", ErrNodeChannelLimit, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		strings.Contains(message, "conflicts with reserved join") {
		return fmt.Errorf("%w: %v", ErrChannelCreateConflict, err)
	}
	return fmt.Errorf("create Channel: %w", err)
}
