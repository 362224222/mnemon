package selector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CreateOwnerSelection persists an immutable descriptor but does not seed or
// run it. The consuming owner-control boundary is responsible for authenticating
// the caller; this method is intentionally not reachable through an Agent View.
func (s *Store) CreateOwnerSelection(ctx context.Context, descriptor SelectionDescriptor,
	self ParticipantID,
) (SelectionSnapshot, error) {
	if ctx == nil {
		return SelectionSnapshot{}, errors.New("create owner selection: nil context")
	}
	if err := validateProviderActivation(descriptor, self); err != nil {
		return SelectionSnapshot{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return SelectionSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionSnapshot{}, fmt.Errorf("create owner selection: begin: %w", err)
	}
	defer tx.Rollback()
	if existing, err := loadSelectionTx(ctx, tx, descriptor.id); err == nil {
		if existing.self == self {
			return existing, nil
		}
		return SelectionSnapshot{}, ErrConflict
	} else if !errors.Is(err, ErrNotFound) {
		return SelectionSnapshot{}, err
	}
	now, err := s.trustedNow()
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if now.Before(descriptor.createdAt) {
		return SelectionSnapshot{}, fmt.Errorf("selection creation is in the future: %w", ErrActivation)
	}
	if !now.Before(descriptor.expiresAt) {
		return SelectionSnapshot{}, fmt.Errorf("selection expires before creation: %w", ErrActivation)
	}
	if err := prepareSelectionCapacityTx(ctx, tx, now); err != nil {
		return SelectionSnapshot{}, err
	}
	stamp := formatProviderTime(now)
	if _, err := tx.ExecContext(ctx, `INSERT INTO selections(
		selection_id, descriptor_json, local_participant, phase, revision, created_at, updated_at)
		VALUES(?, ?, ?, 'awaiting_seed', 1, ?, ?)`, descriptor.id.String(),
		descriptor.canonical, self.String(), stamp, stamp); err != nil {
		return SelectionSnapshot{}, fmt.Errorf("create owner selection: insert: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return SelectionSnapshot{}, fmt.Errorf("create owner selection: commit: %w", err)
	}
	return SelectionSnapshot{descriptor: descriptor, self: self,
		phase: PhaseAwaitingSeed, revision: 1}, nil
}

// SeedSelection activates an owner-created descriptor with one independently
// accepted local Principal opinion. Exact replay is idempotent; a different
// seed for the same selection fails closed.
func (s *Store) SeedSelection(ctx context.Context, id SelectionID,
	seed AcceptedSeedOpinion,
) (SelectionSnapshot, error) {
	if ctx == nil || !seed.valid() {
		return SelectionSnapshot{}, fmt.Errorf("seed selection input is incomplete: %w", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return SelectionSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SelectionSnapshot{}, fmt.Errorf("seed selection: begin: %w", err)
	}
	defer tx.Rollback()
	snapshot, err := loadSelectionTx(ctx, tx, id)
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if snapshot.phase != PhaseAwaitingSeed {
		if sameSeed(snapshot.seed, seed) {
			return snapshot, nil
		}
		return SelectionSnapshot{}, ErrConflict
	}
	now, err := s.trustedNow()
	if err != nil {
		return SelectionSnapshot{}, err
	}
	if now.Before(snapshot.descriptor.createdAt) {
		return SelectionSnapshot{}, fmt.Errorf("selection creation is in the future: %w", ErrNotActive)
	}
	if !now.Before(snapshot.descriptor.expiresAt) {
		if err := discardAwaitingSelectionTx(ctx, tx, snapshot); err != nil {
			return SelectionSnapshot{}, err
		}
		if err := tx.Commit(); err != nil {
			return SelectionSnapshot{}, fmt.Errorf("seed selection: expire: %w", err)
		}
		return SelectionSnapshot{}, fmt.Errorf("selection expired before seed: %w", ErrNotActive)
	}
	nextRevision := snapshot.revision + 1
	result, err := tx.ExecContext(ctx, `UPDATE selections SET
		phase = 'active', seed_principal_id = ?, seed_event_id = ?, seed_event_digest = ?,
		initial_preference = ?, current_preference = ?, revision = ?, updated_at = ?
		WHERE selection_id = ? AND phase = 'awaiting_seed' AND revision = ?`,
		seed.principal.String(), seed.event.ID().String(), seed.event.Digest().String(),
		seed.preference.String(), seed.preference.String(), nextRevision, formatProviderTime(now),
		id.String(), snapshot.revision)
	if err != nil {
		return SelectionSnapshot{}, fmt.Errorf("seed selection: update: %w", err)
	}
	if err := requireOneRow(result); err != nil {
		return SelectionSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return SelectionSnapshot{}, fmt.Errorf("seed selection: commit: %w", err)
	}
	state, _ := NewSelectionState(id, seed.preference)
	snapshot.phase, snapshot.seed, snapshot.state = PhaseActive, seed, state
	snapshot.revision = nextRevision
	return snapshot, nil
}

func (s *Store) Selection(ctx context.Context, id SelectionID) (SelectionSnapshot, error) {
	if ctx == nil {
		return SelectionSnapshot{}, errors.New("read selection: nil context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireOpen(); err != nil {
		return SelectionSnapshot{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return SelectionSnapshot{}, fmt.Errorf("read selection: begin: %w", err)
	}
	defer tx.Rollback()
	return loadSelectionTx(ctx, tx, id)
}

func sameSeed(left, right AcceptedSeedOpinion) bool {
	return left.valid() && right.valid() && left.principal == right.principal &&
		left.event.ID() == right.event.ID() && left.event.Digest() == right.event.Digest() &&
		left.preference == right.preference
}

func requireOneRow(result sql.Result) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("selector CAS cardinality: %w", err)
	}
	if changed != 1 {
		return ErrConflict
	}
	return nil
}
