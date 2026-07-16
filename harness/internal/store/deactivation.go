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

	node, err := readNode(ctx, tx)
	if err != nil {
		return DeactivationResult{}, deactivationReadError("Node", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return DeactivationResult{}, deactivationReadError("Profile", err)
	}
	if !sameProfileIdentity(profile, expected) || !sameProfileAuthority(profile, expected) {
		return DeactivationResult{}, fmt.Errorf("%w: expected Teamwork Profile authority differs",
			ErrProfileDeactivationConflict)
	}
	if !profile.UpdatedAt().Equal(expected.UpdatedAt()) {
		return DeactivationResult{}, fmt.Errorf("%w: expected Teamwork Profile generation differs",
			ErrProfileDeactivationConflict)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return DeactivationResult{}, fmt.Errorf("%w: durable Node/Profile authority is inconsistent",
			ErrProfileDeactivationConflict)
	}
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

func deactivationReadError(kind string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s is not initialized", ErrProfileDeactivationConflict, kind)
	}
	return fmt.Errorf("deactivate Profile: read %s: %w", kind, err)
}
