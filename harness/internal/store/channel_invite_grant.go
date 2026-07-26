package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type durableEnrollmentGrant struct {
	id        model.GrantID
	channelID model.ChannelID
	verifier  model.EnrollmentVerifier
	expiresAt time.Time
	maxUses   uint8
	usedUses  uint8
	status    string
	createdAt time.Time
	closedAt  sql.NullString
	useCount  uint8
}

// Exact-grant replay precedes all mutable Channel and open-grant policy.
func replayExactChannelInvite(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, rotation channelInviteRotation,
) (RotateChannelInviteResult, bool, error) {
	existing, err := readDurableEnrollmentGrant(ctx, tx, rotation.grant.ID())
	if errors.Is(err, sql.ErrNoRows) {
		return RotateChannelInviteResult{}, false, nil
	}
	if err != nil {
		return RotateChannelInviteResult{}, false, err
	}
	if !existing.matches(rotation.grant) {
		return RotateChannelInviteResult{}, false, ErrChannelInviteConflict
	}
	mutation, err := insertChannelInviteMutation(ctx, tx, rotation)
	if err != nil {
		return RotateChannelInviteResult{}, false, err
	}
	remaining := remainingChannelSeats(authority.roster, authority.channel.MemberLimit())
	return RotateChannelInviteResult{GrantID: rotation.grant.ID(), RemainingSeats: remaining,
		Status: existing.status, Mutation: mutation}, true, nil
}

func retireOpenChannelInvite(ctx context.Context, tx *sql.Tx, at time.Time,
	fresh freshChannelInviteRotation,
) (model.GrantID, error) {
	if !fresh.hasOpen {
		return model.GrantID{}, nil
	}
	status := "closed"
	if !at.Before(fresh.open.expiresAt) {
		status = "expired"
	}
	result, err := tx.ExecContext(ctx, `UPDATE enrollment_grants SET status=?,closed_at=?
		WHERE grant_id=? AND status='open'`,
		status, storeTime(at), fresh.open.id.String())
	if err != nil {
		return model.GrantID{}, fmt.Errorf("rotate Channel invite: retire current grant: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return model.GrantID{}, fmt.Errorf("%w: exact open grant was not retired: %v",
			ErrChannelInviteStale, err)
	}
	return fresh.open.id, nil
}

func readDurableEnrollmentGrant(ctx context.Context, tx *sql.Tx,
	grantID model.GrantID,
) (durableEnrollmentGrant, error) {
	var grant durableEnrollmentGrant
	var idText, channelText, expiresText, status, createdText string
	var verifier []byte
	err := tx.QueryRowContext(ctx, `SELECT grant_id,channel_id,verifier,expires_at,max_uses,used_uses,
		status,created_at,closed_at FROM enrollment_grants WHERE grant_id=?`, grantID.String()).Scan(
		&idText, &channelText, &verifier, &expiresText, &grant.maxUses, &grant.usedUses, &status,
		&createdText, &grant.closedAt)
	if err != nil {
		return durableEnrollmentGrant{}, err
	}
	grant.id, err = model.ParseGrantID(idText)
	if err != nil || grant.id != grantID {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable grant ID: %v",
			ErrChannelInviteConflict, err)
	}
	grant.channelID, err = model.ParseChannelID(channelText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable grant Channel: %v",
			ErrChannelInviteConflict, err)
	}
	grant.verifier, err = model.EnrollmentVerifierFromStoredBytes(verifier)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable verifier: %v",
			ErrChannelInviteConflict, err)
	}
	grant.expiresAt, err = parseCanonicalStoreTime(expiresText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable expiry: %v",
			ErrChannelInviteConflict, err)
	}
	grant.createdAt, err = parseCanonicalStoreTime(createdText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: invalid durable creation time: %v",
			ErrChannelInviteConflict, err)
	}
	grant.status = status
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM enrollment_grant_uses WHERE grant_id=?`,
		grantID.String()).Scan(&grant.useCount); err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: inspect grant use ledger: %v",
			ErrChannelInviteConflict, err)
	}
	var lastUseText sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT MAX(used_at) FROM enrollment_grant_uses WHERE grant_id=?`,
		grantID.String()).Scan(&lastUseText); err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: inspect latest grant use: %v",
			ErrChannelInviteConflict, err)
	}
	if grant.useCount != grant.usedUses || !validEnrollmentGrantState(grant.status, grant.usedUses,
		grant.maxUses, grant.closedAt, grant.createdAt, grant.expiresAt, lastUseText) {
		return durableEnrollmentGrant{}, ErrChannelInviteConflict
	}
	return grant, nil
}

func readOpenEnrollmentGrant(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (durableEnrollmentGrant, error) {
	var idText string
	if err := tx.QueryRowContext(ctx, `SELECT grant_id FROM enrollment_grants
		WHERE channel_id=? AND status='open'`, channelID.String()).Scan(&idText); err != nil {
		return durableEnrollmentGrant{}, err
	}
	id, err := model.ParseGrantID(idText)
	if err != nil {
		return durableEnrollmentGrant{}, fmt.Errorf("%w: open grant ID: %v",
			ErrChannelInviteConflict, err)
	}
	return readDurableEnrollmentGrant(ctx, tx, id)
}

func readFencedOpenChannelInvite(ctx context.Context, tx *sql.Tx,
	authority verifiedChannelAuthority, rotation channelInviteRotation,
) (durableEnrollmentGrant, bool, error) {
	open, err := readOpenEnrollmentGrant(ctx, tx, rotation.spec.ChannelID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return durableEnrollmentGrant{}, false, err
	}
	hasOpen := err == nil
	expected := rotation.spec.ExpectedOpenGrant
	if authority.roster.Head() != rotation.spec.ExpectedRosterHead ||
		hasOpen != expected.Present || (hasOpen && open.id != expected.GrantID) {
		return durableEnrollmentGrant{}, false, ErrChannelInviteStale
	}
	return open, hasOpen, nil
}

func latestEnrollmentGrantLifecycle(ctx context.Context, tx *sql.Tx,
	channelID model.ChannelID,
) (time.Time, error) {
	var latest sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT MAX(lifecycle_at) FROM (
		SELECT created_at AS lifecycle_at FROM enrollment_grants WHERE channel_id=?
		UNION ALL
		SELECT closed_at AS lifecycle_at FROM enrollment_grants WHERE channel_id=? AND closed_at IS NOT NULL
		UNION ALL
		SELECT uses.used_at AS lifecycle_at FROM enrollment_grant_uses uses
		JOIN enrollment_grants grants ON grants.grant_id=uses.grant_id WHERE grants.channel_id=?
	)`, channelID.String(), channelID.String(), channelID.String()).Scan(&latest)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: inspect grant lifecycle: %v",
			ErrChannelInviteConflict, err)
	}
	if !latest.Valid {
		return time.Time{}, nil
	}
	parsed, err := parseCanonicalStoreTime(latest.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid grant lifecycle time: %v",
			ErrChannelInviteConflict, err)
	}
	return parsed, nil
}

func latestEnrollmentGrantUse(ctx context.Context, tx *sql.Tx,
	grantID model.GrantID,
) (time.Time, error) {
	var latest sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT MAX(used_at) FROM enrollment_grant_uses WHERE grant_id=?`,
		grantID.String()).Scan(&latest); err != nil {
		return time.Time{}, fmt.Errorf("%w: inspect grant uses: %v",
			ErrChannelInviteConflict, err)
	}
	if !latest.Valid {
		return time.Time{}, nil
	}
	parsed, err := parseCanonicalStoreTime(latest.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid grant use time: %v",
			ErrChannelInviteConflict, err)
	}
	return parsed, nil
}

func (grant durableEnrollmentGrant) matches(expected model.OpenEnrollmentGrant) bool {
	return grant.id == expected.ID() && grant.channelID == expected.ChannelID() &&
		grant.verifier == expected.Verifier() && grant.expiresAt == expected.ExpiresAt() &&
		grant.maxUses == expected.MaxUses() && grant.createdAt == expected.CreatedAt()
}
