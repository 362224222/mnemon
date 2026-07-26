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
	ErrChannelInviteInput       = errors.New("invalid Channel invite input")
	ErrChannelInviteOwner       = errors.New("local Node is not the Channel owner")
	ErrChannelInviteUnavailable = errors.New("Channel invite is unavailable")
	ErrChannelInviteConflict    = errors.New("Channel invite conflicts with durable state")
	ErrChannelInviteStale       = errors.New("Channel invite authority snapshot is stale")
	ErrChannelFull              = errors.New("Channel member capacity reached")
)

type ChannelInviteOpenGrantFence struct {
	Present bool
	GrantID model.GrantID
}

type RotateChannelInviteSpec struct {
	ChannelID          model.ChannelID
	Token              model.EnrollmentToken
	At                 time.Time
	ExpectedRosterHead model.RecordHead
	ExpectedOpenGrant  ChannelInviteOpenGrantFence
	Operation          *ChannelMutationOperation
}

type RotateChannelInviteResult struct {
	Created        bool
	GrantID        model.GrantID
	ReplacedGrant  model.GrantID
	RemainingSeats uint8
	Status         string
	Mutation       ChannelMutationAuthority
}

type CloseChannelInviteResult struct {
	Changed bool
	GrantID model.GrantID
	Status  string
}

type channelInviteRotation struct {
	spec         RotateChannelInviteSpec
	at           time.Time
	grant        model.OpenEnrollmentGrant
	operation    ChannelMutationOperation
	hasOperation bool
}

type freshChannelInviteRotation struct {
	open      durableEnrollmentGrant
	hasOpen   bool
	remaining uint8
}

// RotateChannelInvite atomically retires the current open grant and installs
// one bearer-secret-free replacement derived from an owner-signed token.
func (s *Store) RotateChannelInvite(ctx context.Context,
	spec RotateChannelInviteSpec,
) (RotateChannelInviteResult, error) {
	if s == nil || s.db == nil {
		return RotateChannelInviteResult{}, ErrChannelInviteInput
	}
	rotation, err := validateChannelInviteRotation(ctx, spec)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RotateChannelInviteResult{}, fmt.Errorf("rotate Channel invite: begin: %w", err)
	}
	defer tx.Rollback()
	if result, replayed, err := replayChannelInviteOperation(ctx, tx, rotation); err != nil {
		return RotateChannelInviteResult{}, err
	} else if replayed {
		if err := tx.Commit(); err != nil {
			return RotateChannelInviteResult{},
				fmt.Errorf("rotate Channel invite: commit operation replay: %w", err)
		}
		return result, nil
	}
	authority, node, err := readOwnedInviteAuthority(ctx, tx, spec.ChannelID)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	if err := validateChannelInviteOwner(authority, node, spec.Token); err != nil {
		return RotateChannelInviteResult{}, err
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return RotateChannelInviteResult{}, err
	}
	if result, replayed, err := replayExactChannelInvite(ctx, tx, authority, rotation); err != nil {
		return RotateChannelInviteResult{}, err
	} else if replayed {
		if err := tx.Commit(); err != nil {
			return RotateChannelInviteResult{},
				fmt.Errorf("rotate Channel invite: commit replay read: %w", err)
		}
		return result, nil
	}
	fresh, err := prepareFreshChannelInvite(ctx, tx, authority, rotation)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	result, err := installFreshChannelInvite(ctx, tx, rotation, fresh)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return RotateChannelInviteResult{}, mapChannelInviteError(err)
	}
	return result, nil
}

func validateChannelInviteRotation(ctx context.Context,
	spec RotateChannelInviteSpec,
) (channelInviteRotation, error) {
	if ctx == nil || spec.ChannelID.IsZero() || spec.Token.IsZero() {
		return channelInviteRotation{}, ErrChannelInviteInput
	}
	if spec.ExpectedRosterHead.IsZero() ||
		spec.ExpectedOpenGrant.Present != !spec.ExpectedOpenGrant.GrantID.IsZero() {
		return channelInviteRotation{}, fmt.Errorf("%w: noncanonical authority fence",
			ErrChannelInviteInput)
	}
	at, timeErr := canonicalStoreTime(spec.At)
	grant, grantErr := model.NewOpenEnrollmentGrantForToken(spec.Token, spec.At)
	if timeErr != nil || grantErr != nil || grant.CreatedAt() != at ||
		grant.ChannelID() != spec.ChannelID {
		return channelInviteRotation{}, fmt.Errorf("%w: invalid creation time or signed token",
			ErrChannelInviteInput)
	}
	operation, hasOperation, err := optionalChannelMutationOperation(
		spec.Operation, ChannelMutationInvite)
	if err != nil {
		return channelInviteRotation{}, err
	}
	return channelInviteRotation{spec: spec, at: at, grant: grant,
		operation: operation, hasOperation: hasOperation}, nil
}

func replayChannelInviteOperation(ctx context.Context, tx *sql.Tx,
	rotation channelInviteRotation,
) (RotateChannelInviteResult, bool, error) {
	if !rotation.hasOperation {
		return RotateChannelInviteResult{}, false, nil
	}
	mutation, found, err := readChannelMutation(ctx, tx, rotation.operation)
	if err != nil || !found {
		return RotateChannelInviteResult{}, found, err
	}
	return RotateChannelInviteResult{GrantID: mutation.GrantID(),
		Status: mutation.GrantStatus(), Mutation: mutation}, true, nil
}

func validateChannelInviteOwner(authority verifiedChannelAuthority, node model.Node,
	token model.EnrollmentToken,
) error {
	if node.PeerID() != authority.channel.OwnerPeerID() {
		return ErrChannelInviteOwner
	}
	if !bytes.Equal(token.Payload().Descriptor().WireJSON().Bytes(),
		authority.channel.Descriptor().WireJSON().Bytes()) {
		return ErrChannelInviteInput
	}
	return nil
}

func prepareFreshChannelInvite(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, rotation channelInviteRotation,
) (freshChannelInviteRotation, error) {
	open, hasOpen, err := readFencedOpenChannelInvite(ctx, tx, authority, rotation)
	if err != nil {
		return freshChannelInviteRotation{}, err
	}
	if err := validateFreshChannelInviteAuthority(ctx, tx, authority, rotation, hasOpen); err != nil {
		return freshChannelInviteRotation{}, err
	}
	remaining := remainingChannelSeats(authority.roster, authority.channel.MemberLimit())
	if remaining == 0 {
		return freshChannelInviteRotation{}, ErrChannelFull
	}
	if rotation.grant.MaxUses() > remaining {
		return freshChannelInviteRotation{}, fmt.Errorf(
			"%w: grant uses %d exceed %d remaining seats",
			ErrChannelInviteInput, rotation.grant.MaxUses(), remaining)
	}
	if hasOpen && !rotation.at.After(open.createdAt) {
		return freshChannelInviteRotation{}, fmt.Errorf(
			"%w: rotation does not advance current grant", ErrChannelInviteInput)
	}
	return freshChannelInviteRotation{open: open, hasOpen: hasOpen, remaining: remaining}, nil
}

func validateFreshChannelInviteAuthority(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, rotation channelInviteRotation, hasOpen bool,
) error {
	if authority.channel.Status() != model.ChannelActive {
		return ErrChannelInviteUnavailable
	}
	if err := validateChannelInviteOwnerAddresses(authority, rotation.spec.Token); err != nil {
		return err
	}
	if rotation.at.Before(authority.channel.UpdatedAt()) {
		return fmt.Errorf("%w: invite predates Channel authority", ErrChannelInviteInput)
	}
	lifecycleAt, err := latestEnrollmentGrantLifecycle(ctx, tx, rotation.spec.ChannelID)
	if err != nil {
		return err
	}
	if !lifecycleAt.IsZero() && (rotation.at.Before(lifecycleAt) ||
		(!hasOpen && rotation.at.Equal(lifecycleAt))) {
		return fmt.Errorf("%w: invite predates durable grant lifecycle", ErrChannelInviteInput)
	}
	return nil
}

func validateChannelInviteOwnerAddresses(authority verifiedChannelAuthority,
	token model.EnrollmentToken,
) error {
	owner, exists := authority.roster.CurrentMember(authority.channel.OwnerPeerID())
	ownerAddresses, ownerErr := model.AdvertisedAddressDigest(owner.Multiaddrs())
	tokenAddresses, tokenErr := model.AdvertisedAddressDigest(token.Payload().OwnerMultiaddrs())
	if !exists || owner.Status() != model.MemberActive || ownerErr != nil ||
		tokenErr != nil || ownerAddresses != tokenAddresses {
		return fmt.Errorf("%w: token owner addresses differ from current roster",
			ErrChannelInviteInput)
	}
	return nil
}

func installFreshChannelInvite(ctx context.Context, tx *sql.Tx,
	rotation channelInviteRotation, fresh freshChannelInviteRotation,
) (RotateChannelInviteResult, error) {
	replaced, err := retireOpenChannelInvite(ctx, tx, rotation.at, fresh)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO enrollment_grants(grant_id,channel_id,verifier,expires_at,
		max_uses,used_uses,status,created_at,closed_at) VALUES(?,?,?,?,?,0,'open',?,NULL)`,
		rotation.grant.ID().String(), rotation.spec.ChannelID.String(),
		rotation.grant.Verifier().Bytes(), storeTime(rotation.grant.ExpiresAt()),
		rotation.grant.MaxUses(), storeTime(rotation.grant.CreatedAt()))
	if err != nil {
		return RotateChannelInviteResult{}, mapChannelInviteError(err)
	}
	mutation, err := insertChannelInviteMutation(ctx, tx, rotation)
	if err != nil {
		return RotateChannelInviteResult{}, err
	}
	return RotateChannelInviteResult{Created: true, GrantID: rotation.grant.ID(),
		ReplacedGrant: replaced, RemainingSeats: fresh.remaining,
		Status: "open", Mutation: mutation}, nil
}

func insertChannelInviteMutation(ctx context.Context, tx *sql.Tx,
	rotation channelInviteRotation,
) (ChannelMutationAuthority, error) {
	if !rotation.hasOperation {
		return ChannelMutationAuthority{}, nil
	}
	return insertChannelMutation(ctx, tx, rotation.operation, rotation.spec.Token, rotation.at)
}

// CloseChannelInvite retires one exact grant without letting replay close a successor.
func (s *Store) CloseChannelInvite(ctx context.Context, channelID model.ChannelID,
	grantID model.GrantID, at time.Time,
) (CloseChannelInviteResult, error) {
	if s == nil || s.db == nil || ctx == nil || channelID.IsZero() || grantID.IsZero() {
		return CloseChannelInviteResult{}, ErrChannelInviteInput
	}
	closedAt, err := canonicalStoreTime(at)
	if err != nil {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close time: %v", ErrChannelInviteInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CloseChannelInviteResult{}, fmt.Errorf("close Channel invite: begin: %w", err)
	}
	defer tx.Rollback()
	authority, node, err := readOwnedInviteAuthority(ctx, tx, channelID)
	if err != nil {
		return CloseChannelInviteResult{}, err
	}
	if node.PeerID() != authority.channel.OwnerPeerID() {
		return CloseChannelInviteResult{}, ErrChannelInviteOwner
	}
	if err := verifyOwnedChannelEnrollmentLedger(ctx, tx, authority); err != nil {
		return CloseChannelInviteResult{}, err
	}
	grant, err := readDurableEnrollmentGrant(ctx, tx, grantID)
	if err != nil || grant.channelID != channelID {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: grant is not owned by Channel: %v",
			ErrChannelInviteConflict, err)
	}
	if grant.status != "open" {
		if err := tx.Commit(); err != nil {
			return CloseChannelInviteResult{},
				fmt.Errorf("close Channel invite: commit replay read: %w", err)
		}
		return CloseChannelInviteResult{GrantID: grantID, Status: grant.status}, nil
	}
	if authority.channel.Status() != model.ChannelActive {
		return CloseChannelInviteResult{}, ErrChannelInviteUnavailable
	}
	if closedAt.Before(grant.createdAt) {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close predates grant", ErrChannelInviteInput)
	}
	if closedAt.Before(authority.channel.UpdatedAt()) {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close predates Channel authority",
			ErrChannelInviteInput)
	}
	lastUseAt, err := latestEnrollmentGrantUse(ctx, tx, grantID)
	if err != nil {
		return CloseChannelInviteResult{}, err
	}
	if !lastUseAt.IsZero() && closedAt.Before(lastUseAt) {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: close predates durable grant use",
			ErrChannelInviteInput)
	}
	status := "closed"
	if !closedAt.Before(grant.expiresAt) {
		status = "expired"
	}
	result, err := tx.ExecContext(ctx, `UPDATE enrollment_grants SET status=?,closed_at=?
		WHERE grant_id=? AND status='open'`, status, storeTime(closedAt), grantID.String())
	if err != nil {
		return CloseChannelInviteResult{}, mapChannelInviteError(err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return CloseChannelInviteResult{}, fmt.Errorf("%w: exact open grant was not retired: %v",
			ErrChannelInviteConflict, err)
	}
	if err := tx.Commit(); err != nil {
		return CloseChannelInviteResult{}, mapChannelInviteError(err)
	}
	return CloseChannelInviteResult{Changed: true, GrantID: grantID, Status: status}, nil
}

func remainingChannelSeats(roster model.VerifiedRoster, limit uint8) uint8 {
	current := make(map[model.PeerID]model.MemberStatus, limit)
	for _, member := range roster.Members() {
		current[member.PeerID()] = member.Status()
	}
	active := 0
	for _, status := range current {
		if status == model.MemberActive {
			active++
		}
	}
	if active >= int(limit) {
		return 0
	}
	return limit - uint8(active)
}

func mapChannelInviteError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	if strings.Contains(message, "channel_full") {
		return fmt.Errorf("%w: %v", ErrChannelFull, err)
	}
	if strings.Contains(message, "UNIQUE constraint failed") ||
		strings.Contains(message, "FOREIGN KEY constraint failed") ||
		strings.Contains(message, "enrollment grant") {
		return fmt.Errorf("%w: %v", ErrChannelInviteConflict, err)
	}
	return fmt.Errorf("rotate Channel invite: %w", err)
}
