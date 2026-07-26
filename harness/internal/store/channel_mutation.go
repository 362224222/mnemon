package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrChannelMutationInput    = errors.New("invalid Channel mutation operation")
	ErrChannelMutationMismatch = errors.New("Channel mutation operation key conflicts with request")
	ErrChannelMutationConflict = errors.New("Channel mutation operation conflicts with durable state")
)

type ChannelMutationKind string

const (
	ChannelMutationCreate ChannelMutationKind = "create"
	ChannelMutationInvite ChannelMutationKind = "invite"
)

func (kind ChannelMutationKind) Valid() bool {
	return kind == ChannelMutationCreate || kind == ChannelMutationInvite
}

// ChannelMutationOperation is the complete non-secret idempotency authority
// supplied by the authenticated local route. The raw operation key never
// crosses into Store.
type ChannelMutationOperation struct {
	Kind             ChannelMutationKind
	OperationKeyHash model.Digest
	RequestDigest    model.Digest
}

func (operation ChannelMutationOperation) valid() bool {
	return operation.Kind.Valid() && !operation.OperationKeyHash.IsZero() &&
		!operation.RequestDigest.IsZero()
}

func optionalChannelMutationOperation(operation *ChannelMutationOperation,
	want ChannelMutationKind,
) (ChannelMutationOperation, bool, error) {
	if operation == nil {
		return ChannelMutationOperation{}, false, nil
	}
	if !operation.valid() || operation.Kind != want {
		return ChannelMutationOperation{}, false, ErrChannelMutationInput
	}
	return *operation, true, nil
}

// ChannelMutationAuthority is enough to reconstruct the exact signed invite
// from a caller-resupplied operation secret. It contains no bearer material.
type ChannelMutationAuthority struct {
	kind            ChannelMutationKind
	channel         model.Channel
	grantID         model.GrantID
	tokenPayload    model.Digest
	grantVerifier   model.EnrollmentVerifier
	grantExpiresAt  time.Time
	grantMaxUses    uint8
	grantUsedUses   uint8
	grantStatus     string
	grantCreatedAt  time.Time
	ownerMultiaddrs []string
	replayed        bool
}

func (authority ChannelMutationAuthority) Kind() ChannelMutationKind { return authority.kind }
func (authority ChannelMutationAuthority) Channel() model.Channel    { return authority.channel }
func (authority ChannelMutationAuthority) GrantID() model.GrantID    { return authority.grantID }
func (authority ChannelMutationAuthority) TokenPayloadDigest() model.Digest {
	return authority.tokenPayload
}
func (authority ChannelMutationAuthority) GrantVerifier() model.EnrollmentVerifier {
	return authority.grantVerifier
}
func (authority ChannelMutationAuthority) GrantExpiresAt() time.Time { return authority.grantExpiresAt }
func (authority ChannelMutationAuthority) GrantMaxUses() uint8       { return authority.grantMaxUses }
func (authority ChannelMutationAuthority) GrantUsedUses() uint8      { return authority.grantUsedUses }
func (authority ChannelMutationAuthority) GrantStatus() string       { return authority.grantStatus }
func (authority ChannelMutationAuthority) GrantCreatedAt() time.Time { return authority.grantCreatedAt }
func (authority ChannelMutationAuthority) OwnerMultiaddrs() []string {
	return append([]string(nil), authority.ownerMultiaddrs...)
}
func (authority ChannelMutationAuthority) Replayed() bool { return authority.replayed }
func (authority ChannelMutationAuthority) IsZero() bool {
	return !authority.kind.Valid() || authority.channel.ID().IsZero() ||
		authority.grantID.IsZero() || authority.tokenPayload.IsZero() ||
		authority.grantVerifier.IsZero()
}

// ReadChannelMutation classifies only the caller-stable key and request
// digest. A missing key is fresh; a matching row is a semantic replay; and a
// changed kind or digest fails closed before current Channel policy is read.
func (s *Store) ReadChannelMutation(ctx context.Context,
	operation ChannelMutationOperation,
) (ChannelMutationAuthority, bool, error) {
	if s == nil || s.db == nil || ctx == nil || !operation.valid() {
		return ChannelMutationAuthority{}, false, ErrChannelMutationInput
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("read Channel mutation: begin: %w", err)
	}
	defer tx.Rollback()
	authority, found, err := readChannelMutation(ctx, tx, operation)
	if err != nil {
		return ChannelMutationAuthority{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("read Channel mutation: commit read: %w", err)
	}
	return authority, found, nil
}

func readChannelMutation(ctx context.Context, tx *sql.Tx,
	operation ChannelMutationOperation,
) (ChannelMutationAuthority, bool, error) {
	var conflict int
	conflictErr := tx.QueryRowContext(ctx, `SELECT 1 FROM channel_leave_operations
		WHERE operation_key_hash=?`, operation.OperationKeyHash.Bytes()).Scan(&conflict)
	if conflictErr == nil {
		return ChannelMutationAuthority{}, false, ErrChannelMutationMismatch
	}
	if !errors.Is(conflictErr, sql.ErrNoRows) {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: inspect operation key scope: %v",
				ErrChannelMutationConflict, conflictErr)
	}
	var requestBytes, payloadDigestBytes, addressesJSON []byte
	var kindText, channelText, grantText, committedText string
	err := tx.QueryRowContext(ctx, `SELECT request_digest,kind,channel_id,grant_id,
		token_payload_digest,owner_multiaddrs_json,committed_at FROM channel_mutation_operations
		WHERE operation_key_hash=?`, operation.OperationKeyHash.Bytes()).Scan(
		&requestBytes, &kindText, &channelText, &grantText, &payloadDigestBytes,
		&addressesJSON, &committedText)
	if errors.Is(err, sql.ErrNoRows) {
		return ChannelMutationAuthority{}, false, nil
	}
	if err != nil {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: read operation: %v", ErrChannelMutationConflict, err)
	}
	requestDigest, err := model.DigestFromBytes(requestBytes)
	storedKind := ChannelMutationKind(kindText)
	if err != nil || !storedKind.Valid() {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: invalid operation identity", ErrChannelMutationConflict)
	}
	if storedKind != operation.Kind || requestDigest != operation.RequestDigest {
		return ChannelMutationAuthority{}, false, ErrChannelMutationMismatch
	}
	channelID, channelErr := model.ParseChannelID(channelText)
	grantID, grantErr := model.ParseGrantID(grantText)
	payloadDigest, payloadErr := model.DigestFromBytes(payloadDigestBytes)
	if channelErr != nil || grantErr != nil || payloadErr != nil || payloadDigest.IsZero() {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: invalid result identity", ErrChannelMutationConflict)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: read Node: %v", ErrChannelMutationConflict, err)
	}
	verified, err := readVerifiedChannelAuthority(ctx, tx, node.PeerID(), channelID)
	if err != nil || verified.channel.OwnerPeerID() != node.PeerID() {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: read Channel authority: %v", ErrChannelMutationConflict, err)
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, verified); err != nil {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: verify enrollment ledger: %v", ErrChannelMutationConflict, err)
	}
	grant, err := readDurableEnrollmentGrant(ctx, tx, grantID)
	if err != nil || grant.channelID != channelID || committedText != storeTime(grant.createdAt) {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: read grant authority: %v", ErrChannelMutationConflict, err)
	}
	addresses, err := decodeChannelMutationAddresses(addressesJSON)
	if err != nil || !ownerAddressesExisted(verified, addresses, grant.createdAt) {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: invalid owner address authority", ErrChannelMutationConflict)
	}
	if storedKind == ChannelMutationCreate &&
		(grant.createdAt != verified.channel.CreatedAt() ||
			grant.maxUses != verified.channel.MemberLimit()-1) {
		return ChannelMutationAuthority{}, false,
			fmt.Errorf("%w: create result differs from genesis", ErrChannelMutationConflict)
	}
	return ChannelMutationAuthority{kind: storedKind, channel: verified.channel,
		grantID: grant.id, tokenPayload: payloadDigest, grantVerifier: grant.verifier,
		grantExpiresAt: grant.expiresAt,
		grantMaxUses:   grant.maxUses, grantUsedUses: grant.usedUses, grantStatus: grant.status,
		grantCreatedAt: grant.createdAt, ownerMultiaddrs: addresses, replayed: true}, true, nil
}

func insertChannelMutation(ctx context.Context, tx *sql.Tx, operation ChannelMutationOperation,
	token model.EnrollmentToken, committedAt time.Time,
) (ChannelMutationAuthority, error) {
	if !operation.valid() || token.IsZero() {
		return ChannelMutationAuthority{}, ErrChannelMutationInput
	}
	payload := token.Payload()
	addresses, err := model.JSONFrom(payload.OwnerMultiaddrs())
	if err != nil {
		return ChannelMutationAuthority{}, fmt.Errorf("%w: owner addresses: %v",
			ErrChannelMutationInput, err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO channel_mutation_operations(
		operation_key_hash,request_digest,kind,channel_id,grant_id,token_payload_digest,
		owner_multiaddrs_json,committed_at)
		VALUES(?,?,?,?,?,?,?,?)`, operation.OperationKeyHash.Bytes(), operation.RequestDigest.Bytes(),
		string(operation.Kind), payload.Descriptor().Descriptor().ID().String(),
		payload.GrantID().String(), payload.Digest().Bytes(), addresses.Bytes(), storeTime(committedAt))
	if err != nil {
		return ChannelMutationAuthority{},
			fmt.Errorf("%w: insert operation: %v", ErrChannelMutationConflict, err)
	}
	authority, found, err := readChannelMutation(ctx, tx, operation)
	if err != nil {
		return ChannelMutationAuthority{}, err
	}
	if !found || authority.IsZero() {
		return ChannelMutationAuthority{},
			fmt.Errorf("%w: inserted operation is not readable", ErrChannelMutationConflict)
	}
	authority.replayed = false
	return authority, nil
}

func decodeChannelMutationAddresses(raw []byte) ([]string, error) {
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return nil, ErrChannelMutationConflict
	}
	var addresses []string
	if err := json.Unmarshal(raw, &addresses); err != nil || len(addresses) == 0 {
		return nil, ErrChannelMutationConflict
	}
	if _, err := model.AdvertisedAddressDigest(addresses); err != nil {
		return nil, ErrChannelMutationConflict
	}
	encoded, err := model.JSONFrom(addresses)
	if err != nil || !bytes.Equal(encoded.Bytes(), raw) {
		return nil, ErrChannelMutationConflict
	}
	return addresses, nil
}

func ownerAddressesExisted(authority verifiedChannelAuthority, addresses []string,
	grantCreatedAt time.Time,
) bool {
	want, err := model.AdvertisedAddressDigest(addresses)
	if err != nil {
		return false
	}
	for _, member := range authority.roster.Members() {
		got, addressErr := model.AdvertisedAddressDigest(member.Multiaddrs())
		if member.PeerID() == authority.channel.OwnerPeerID() &&
			!member.CreatedAt().After(grantCreatedAt) && addressErr == nil && got == want {
			return true
		}
	}
	return false
}
