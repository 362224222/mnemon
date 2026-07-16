package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrProfileDeactivationConflict = errors.New("Profile deactivation conflicts with durable state")
	ErrProfileDeactivationBusy     = errors.New("Profile authority is in use")
)

// DeactivationResult reports the durable authority after a managed Profile is
// disabled. The selected Host, Runtime and asset revision remain staged for a
// repair or exact reactivation; only Agent admission authority is withdrawn.
type DeactivationResult struct {
	Changed bool
	Node    model.Node
	Profile model.Profile
}

// PreflightProfileDeactivation re-reads one exact Profile generation and the
// complete authority-busy predicate in a single read transaction. It is the
// online half of a conditional mutation shutdown: callers must separately
// fence new Agent admission while this check runs, and DeactivateProfile still
// repeats the same checks after the daemon releases its Store writer.
//
// ErrProfileDeactivationBusy is returned with the exact observed authority so
// a closed controller can report the typed retryable condition without
// weakening the generation fence. No durable state is changed.
func (s *Store) PreflightProfileDeactivation(ctx context.Context,
	expected model.Profile,
) (LocalAuthority, error) {
	if s == nil || s.db == nil {
		return LocalAuthority{}, errors.New("preflight Profile deactivation: nil store")
	}
	if ctx == nil {
		return LocalAuthority{}, errors.New("preflight Profile deactivation: nil context")
	}
	if expected.ID().IsZero() {
		return LocalAuthority{}, fmt.Errorf("%w: expected Profile is unavailable",
			ErrProfileDeactivationConflict)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return LocalAuthority{}, fmt.Errorf("preflight Profile deactivation: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readExactProfileDeactivationAuthority(ctx, tx, expected)
	if err != nil {
		return LocalAuthority{}, err
	}
	busy, err := profileAuthorityBusy(ctx, tx, authority.Profile.ID())
	if err != nil {
		return LocalAuthority{}, fmt.Errorf(
			"preflight Profile deactivation: inspect active Agent authority: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return LocalAuthority{}, fmt.Errorf("preflight Profile deactivation: commit read: %w", err)
	}
	if busy {
		return authority, ErrProfileDeactivationBusy
	}
	return authority, nil
}

// DeactivateProfile atomically withdraws one exact Profile authority. It is a
// setup/eject recovery boundary, not a force stop: live claims, Agent runs or
// operations must first quiesce under mnemond ownership. The expected Profile
// carries the exact durable update generation as an ABA fence.
func (s *Store) DeactivateProfile(ctx context.Context, expected model.Profile,
	at time.Time,
) (DeactivationResult, error) {
	if s == nil || s.db == nil {
		return DeactivationResult{}, errors.New("deactivate Profile: nil store")
	}
	if ctx == nil {
		return DeactivationResult{}, errors.New("deactivate Profile: nil context")
	}
	if expected.ID().IsZero() {
		return DeactivationResult{}, fmt.Errorf("%w: expected Profile is unavailable",
			ErrProfileDeactivationConflict)
	}
	at = at.Round(0).UTC()
	if at.IsZero() {
		return DeactivationResult{}, fmt.Errorf("%w: zero deactivation time",
			ErrProfileDeactivationConflict)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: begin: %w", err)
	}
	defer tx.Rollback()

	authority, err := readExactProfileDeactivationAuthority(ctx, tx, expected)
	if err != nil {
		return DeactivationResult{}, err
	}
	node, profile := authority.Node, authority.Profile
	if !profile.Enabled() {
		return DeactivationResult{Node: node, Profile: profile}, nil
	}
	busy, err := profileAuthorityBusy(ctx, tx, profile.ID())
	if err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: inspect active Agent authority: %w", err)
	}
	if busy {
		return DeactivationResult{}, ErrProfileDeactivationBusy
	}
	if !at.After(node.UpdatedAt()) || !at.After(profile.UpdatedAt()) {
		return DeactivationResult{}, fmt.Errorf("%w: deactivation time does not advance durable update time",
			ErrProfileDeactivationConflict)
	}

	nodeSpec := node.Spec()
	nodeSpec.UpdatedAt = at
	deactivatedNode, err := model.NewNode(nodeSpec)
	if err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: Node projection: %w", err)
	}
	profileSpec := profile.Spec()
	profileSpec.Enabled = false
	profileSpec.UpdatedAt = at
	deactivatedProfile, err := model.NewProfile(profileSpec)
	if err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: projection: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "UPDATE node SET updated_at = ? WHERE singleton = 1",
		storeTime(deactivatedNode.UpdatedAt())); err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: update Node: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE profiles SET enabled = 0, updated_at = ?
		WHERE profile_id = ? AND enabled = 1`, storeTime(deactivatedProfile.UpdatedAt()),
		deactivatedProfile.ID().String())
	if err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: update authority: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return DeactivationResult{}, fmt.Errorf("%w: Profile authority changed concurrently",
			ErrProfileDeactivationConflict)
	}
	if err := tx.Commit(); err != nil {
		return DeactivationResult{}, fmt.Errorf("deactivate Profile: commit: %w", err)
	}
	return DeactivationResult{Changed: true, Node: deactivatedNode, Profile: deactivatedProfile}, nil
}

func readExactProfileDeactivationAuthority(ctx context.Context, q rowQuerier,
	expected model.Profile,
) (LocalAuthority, error) {
	node, err := readNode(ctx, q)
	if err != nil {
		return LocalAuthority{}, deactivationReadError("Node", err)
	}
	profile, err := readProfile(ctx, q)
	if err != nil {
		return LocalAuthority{}, deactivationReadError("Profile", err)
	}
	if !sameProfileIdentity(profile, expected) || !sameProfileAuthority(profile, expected) {
		return LocalAuthority{}, fmt.Errorf("%w: expected Teamwork Profile authority differs",
			ErrProfileDeactivationConflict)
	}
	if !profile.UpdatedAt().Equal(expected.UpdatedAt()) {
		return LocalAuthority{}, fmt.Errorf("%w: expected Teamwork Profile generation differs",
			ErrProfileDeactivationConflict)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return LocalAuthority{}, fmt.Errorf("%w: durable Node/Profile authority is inconsistent",
			ErrProfileDeactivationConflict)
	}
	return LocalAuthority{Node: node, Profile: profile}, nil
}

func deactivationReadError(kind string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s is not initialized", ErrProfileDeactivationConflict, kind)
	}
	return fmt.Errorf("deactivate Profile: read %s: %w", kind, err)
}
