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
	ErrProfileActivationConflict = errors.New("Profile activation conflicts with durable state")
	ErrProfileActivationBusy     = errors.New("Profile authority is in use")
	ErrProfileHostMismatch       = errors.New("enabled Profile Host cannot be switched")
)

// ActivationResult reports the durable authority after activation. Changed is
// false for an exact replay, which also preserves the original update times.
type ActivationResult struct {
	Changed bool
	Node    model.Node
	Profile model.Profile
}

// ActivateProfile publishes a successfully staged and self-checked Profile.
// Identity and credentials remain immutable. An enabled Host adapter cannot be
// switched in place; setup must first eject/disable it. Asset and budget
// changes are allowed only while no Agent authority is live. expectedUpdatedAt
// fences the exact durable generation observed by the caller.
func (s *Store) ActivateProfile(ctx context.Context, desired model.Profile, expectedUpdatedAt,
	at time.Time,
) (ActivationResult, error) {
	if s == nil || s.db == nil {
		return ActivationResult{}, errors.New("activate Profile: nil store")
	}
	if ctx == nil {
		return ActivationResult{}, errors.New("activate Profile: nil context")
	}
	if desired.ID().IsZero() || !desired.Enabled() {
		return ActivationResult{}, fmt.Errorf("%w: desired Profile must be enabled", ErrProfileActivationConflict)
	}
	at = at.Round(0).UTC()
	if at.IsZero() {
		return ActivationResult{}, fmt.Errorf("%w: zero activation time", ErrProfileActivationConflict)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ActivationResult{}, fmt.Errorf("activate Profile: begin: %w", err)
	}
	defer tx.Rollback()

	node, err := readNode(ctx, tx)
	if err != nil {
		return ActivationResult{}, activationReadError("Node", err)
	}
	profile, err := readProfile(ctx, tx)
	if err != nil {
		return ActivationResult{}, activationReadError("Profile", err)
	}
	if !sameProfileIdentity(profile, desired) {
		return ActivationResult{}, fmt.Errorf("%w: Teamwork Profile identity differs", ErrProfileActivationConflict)
	}
	if node.ActiveAssetRevision() != profile.ActiveAssetRevision() {
		return ActivationResult{}, fmt.Errorf("%w: durable Node/Profile authority is inconsistent", ErrProfileActivationConflict)
	}
	if profile.Enabled() && (profile.Host() != desired.Host() || profile.Runtime() != desired.Runtime()) {
		return ActivationResult{}, ErrProfileHostMismatch
	}
	expectedUpdatedAt = expectedUpdatedAt.Round(0).UTC()
	if expectedUpdatedAt.IsZero() || !profile.UpdatedAt().Equal(expectedUpdatedAt) {
		return ActivationResult{}, fmt.Errorf("%w: expected Teamwork Profile generation differs",
			ErrProfileActivationConflict)
	}

	// Enabling a disabled Profile grants Agent authority even when all adapter
	// fields are byte-equivalent, so it requires the same quiescence gate as an
	// asset, budget or adapter update.
	authorityChanged := !profile.Enabled() || !sameProfileAuthority(profile, desired)
	if profile.Enabled() && !authorityChanged {
		return ActivationResult{Node: node, Profile: profile}, nil
	}
	if authorityChanged {
		busy, err := profileAuthorityBusy(ctx, tx, desired.ID())
		if err != nil {
			return ActivationResult{}, err
		}
		if busy {
			return ActivationResult{}, ErrProfileActivationBusy
		}
	}
	if !at.After(node.UpdatedAt()) || !at.After(profile.UpdatedAt()) {
		return ActivationResult{}, fmt.Errorf("%w: activation time does not advance durable update time", ErrProfileActivationConflict)
	}

	nodeSpec := node.Spec()
	nodeSpec.ActiveAssetRevision = desired.ActiveAssetRevision()
	nodeSpec.UpdatedAt = at
	activatedNode, err := model.NewNode(nodeSpec)
	if err != nil {
		return ActivationResult{}, fmt.Errorf("activate Profile: Node projection: %w", err)
	}
	profileSpec := profile.Spec()
	profileSpec.Host = desired.Host()
	profileSpec.Runtime = desired.Runtime()
	profileSpec.ActiveAssetRevision = desired.ActiveAssetRevision()
	profileSpec.HandlingBudget = desired.HandlingBudget()
	profileSpec.Enabled = true
	profileSpec.UpdatedAt = at
	activatedProfile, err := model.NewProfile(profileSpec)
	if err != nil {
		return ActivationResult{}, fmt.Errorf("activate Profile: projection: %w", err)
	}

	if _, err := tx.ExecContext(ctx, "UPDATE node SET active_asset_rev = ?, updated_at = ? WHERE singleton = 1",
		activatedNode.ActiveAssetRevision(), storeTime(activatedNode.UpdatedAt())); err != nil {
		return ActivationResult{}, fmt.Errorf("activate Profile: update Node: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE profiles SET host = ?, runtime_kind = ?, active_asset_rev = ?,
		handling_budget_json = ?, enabled = 1, updated_at = ? WHERE profile_id = ?`,
		string(activatedProfile.Host()), string(activatedProfile.Runtime()), activatedProfile.ActiveAssetRevision(),
		activatedProfile.HandlingBudget().Bytes(), storeTime(activatedProfile.UpdatedAt()), activatedProfile.ID().String()); err != nil {
		return ActivationResult{}, fmt.Errorf("activate Profile: update authority: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ActivationResult{}, fmt.Errorf("activate Profile: commit: %w", err)
	}
	return ActivationResult{Changed: true, Node: activatedNode, Profile: activatedProfile}, nil
}

func activationReadError(kind string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s is not initialized", ErrProfileActivationConflict, kind)
	}
	return fmt.Errorf("activate Profile: read %s: %w", kind, err)
}

func sameProfileAuthority(a, b model.Profile) bool {
	return a.Host() == b.Host() && a.Runtime() == b.Runtime() &&
		a.ActiveAssetRevision() == b.ActiveAssetRevision() &&
		a.HandlingBudget().String() == b.HandlingBudget().String()
}

func profileAuthorityBusy(ctx context.Context, q rowQuerier, id model.ProfileID) (bool, error) {
	var busy int
	err := q.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM agent_handlings WHERE profile_id = ? AND status = 'claimed'
		UNION ALL
		SELECT 1 FROM agent_runs WHERE profile_id = ? AND (
			status IN ('starting','running','runtime_finished')
			OR (launcher='mnemond-wake' AND completion_receipt_json IS NULL)
		)
		UNION ALL
		SELECT 1 FROM operations WHERE profile_id = ? AND status = 'started'
	)`, id.String(), id.String(), id.String()).Scan(&busy)
	if err != nil {
		return false, fmt.Errorf("activate Profile: inspect active Agent authority: %w", err)
	}
	return busy == 1, nil
}
