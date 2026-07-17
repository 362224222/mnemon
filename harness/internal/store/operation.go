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
	ErrOperationMismatch = errors.New("operation identity reused with different request")
	ErrOperationPending  = errors.New("operation is already in progress")
	ErrOperationFence    = errors.New("operation lease fence rejected")
)

type OperationReservation struct {
	Operation model.Operation
	Replayed  bool
	Acquired  bool
}

// ReserveOperation atomically creates or replays a started operation. Only
// the same client identity and request may reclaim an expired lease; another
// started operation bound to the same context remains pending.
func (s *Store) ReserveOperation(ctx context.Context, requested model.Operation, now time.Time) (OperationReservation, error) {
	if s == nil || s.db == nil || ctx == nil {
		return OperationReservation{}, errors.New("reserve operation: nil store or context")
	}
	if requested.Status() != model.OperationStarted {
		return OperationReservation{}, errors.New("reserve operation: requested value is not started")
	}
	if _, hasCapture := requested.Capture(); hasCapture {
		return OperationReservation{}, errors.New("reserve operation: first reservation cannot contain capture state")
	}
	now = now.Round(0).UTC()
	leaseUntil, ok := requested.LeaseUntil()
	if now.IsZero() || !ok || !leaseUntil.After(now) {
		return OperationReservation{}, errors.New("reserve operation: lease must end after trusted now")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationReservation{}, fmt.Errorf("reserve operation: begin: %w", err)
	}
	defer tx.Rollback()
	existing, err := readOperationByClientKey(ctx, tx, requested.ProfileID(), requested.ClientKeyHash())
	if err == nil {
		if !sameOperationRequest(existing, requested) {
			return OperationReservation{}, ErrOperationMismatch
		}
		if existing.Status().Terminal() {
			if err := tx.Commit(); err != nil {
				return OperationReservation{}, fmt.Errorf("reserve operation: replay commit: %w", err)
			}
			return OperationReservation{Operation: existing, Replayed: true}, nil
		}
		if existing.AgentRunID() != requested.AgentRunID() {
			return OperationReservation{}, fmt.Errorf("%w: started operation belongs to AgentRun %s", ErrOperationMismatch, existing.AgentRunID().String())
		}
		if err := requireEnabledProfile(ctx, tx, requested.ProfileID()); err != nil {
			return OperationReservation{}, err
		}
		if err := requireOperationAgentRun(ctx, tx, existing, true); err != nil {
			return OperationReservation{}, err
		}
		existingLease, _ := existing.LeaseUntil()
		if existingLease.After(now) {
			if existing.LeaseOwner() != requested.LeaseOwner() {
				return OperationReservation{Operation: existing, Replayed: true}, ErrOperationPending
			}
			if err := tx.Commit(); err != nil {
				return OperationReservation{}, fmt.Errorf("reserve operation: owner replay commit: %w", err)
			}
			return OperationReservation{Operation: existing, Replayed: true, Acquired: true}, nil
		}
		if existing.LeaseOwner() != requested.LeaseOwner() || !existingLease.Equal(leaseUntil) {
			result, err := tx.ExecContext(ctx, `UPDATE operations SET lease_owner=?,lease_until=?
				WHERE operation_id=? AND status='started' AND lease_owner=? AND lease_until=? AND lease_until<=?`,
				requested.LeaseOwner(), storeTime(leaseUntil), existing.ID().String(), existing.LeaseOwner(),
				storeTime(existingLease), storeTime(now))
			if err != nil {
				return OperationReservation{}, fmt.Errorf("reserve operation: reclaim lease: %w", err)
			}
			if exactlyOne(result) != nil {
				return OperationReservation{}, ErrOperationFence
			}
			existing, err = operationWithLease(existing, requested.LeaseOwner(), leaseUntil)
			if err != nil {
				return OperationReservation{}, err
			}
		}
		if err := tx.Commit(); err != nil {
			return OperationReservation{}, fmt.Errorf("reserve operation: reclaim commit: %w", err)
		}
		return OperationReservation{Operation: existing, Replayed: true, Acquired: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return OperationReservation{}, err
	}
	if err := requireEnabledProfile(ctx, tx, requested.ProfileID()); err != nil {
		return OperationReservation{}, err
	}
	if err := requireOperationAgentRun(ctx, tx, requested, false); err != nil {
		return OperationReservation{}, err
	}
	if contextHash, hasContext := requested.ContextHash(); hasContext {
		var otherID string
		err := tx.QueryRowContext(ctx,
			"SELECT operation_id FROM operations WHERE profile_id = ? AND context_hash = ? AND status = 'started' LIMIT 1",
			requested.ProfileID().String(), contextHash.Bytes()).Scan(&otherID)
		if err == nil {
			return OperationReservation{}, fmt.Errorf("%w: context owned by %s", ErrOperationPending, otherID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return OperationReservation{}, fmt.Errorf("reserve operation: inspect context: %w", err)
		}
	}
	if err := insertOperation(ctx, tx, requested); err != nil {
		return OperationReservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return OperationReservation{}, fmt.Errorf("reserve operation: commit: %w", err)
	}
	return OperationReservation{Operation: requested, Acquired: true}, nil
}

// CheckpointOperationCapture writes the verified capture closure exactly once
// under the active operation lease. Identical retry is a replay; different
// bytes are rejected before SQLite's immutable trigger is reached.
func (s *Store) CheckpointOperationCapture(ctx context.Context, id model.OperationID, owner string,
	now time.Time, capture model.JSON,
) (bool, error) {
	if s == nil || s.db == nil || ctx == nil || id.IsZero() || capture.IsZero() {
		return false, errors.New("checkpoint operation capture: incomplete input")
	}
	captureRoots, err := parseOperationCapture(capture)
	if err != nil {
		return false, fmt.Errorf("checkpoint operation capture: %w", err)
	}
	captureBytes := capture.Bytes()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("checkpoint operation capture: begin: %w", err)
	}
	defer tx.Rollback()
	operation, err := readOperationByID(ctx, tx, id)
	if err != nil {
		return false, fmt.Errorf("checkpoint operation capture: %w", err)
	}
	if err := requireOperationFence(operation, owner, now); err != nil {
		return false, err
	}
	for _, captured := range captureRoots {
		root, err := requireVerifiedArtifactRoot(ctx, tx, captured.RootDigest)
		if err != nil || root.ManifestDigest != captured.ManifestDigest {
			return false, fmt.Errorf("checkpoint operation capture: verified root/manifest mismatch: %w", ErrCaptureMismatch)
		}
	}
	if existing, ok := operation.Capture(); ok {
		if existing.String() != capture.String() {
			return false, ErrOperationMismatch
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("checkpoint operation capture: replay commit: %w", err)
		}
		return true, nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE operations SET capture_json=? WHERE operation_id=?
		AND status='started' AND lease_owner=? AND lease_until>? AND capture_json IS NULL`,
		captureBytes, id.String(), owner, storeTime(now))
	if err != nil {
		return false, fmt.Errorf("checkpoint operation capture: update: %w", err)
	}
	if exactlyOne(result) != nil {
		return false, ErrOperationFence
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("checkpoint operation capture: commit: %w", err)
	}
	return false, nil
}

// RejectOperation durably closes a fenced operation without changing its
// associated handling. The canonical result carries the stable error union.
func (s *Store) RejectOperation(ctx context.Context, id model.OperationID, owner string, now time.Time,
	result model.JSON,
) (model.Operation, error) {
	if s == nil || s.db == nil || ctx == nil || id.IsZero() {
		return model.Operation{}, errors.New("reject operation: incomplete input")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Operation{}, fmt.Errorf("reject operation: begin: %w", err)
	}
	defer tx.Rollback()
	operation, err := readOperationByID(ctx, tx, id)
	if err != nil {
		return model.Operation{}, fmt.Errorf("reject operation: %w", err)
	}
	if operation.Status().Terminal() {
		return operation, nil
	}
	if err := requireOperationFence(operation, owner, now); err != nil {
		return model.Operation{}, err
	}
	if result.IsZero() || len(result.Bytes()) == 0 || result.Bytes()[0] != '{' {
		return model.Operation{}, errors.New("reject operation: result must be a canonical object")
	}
	now = now.Round(0).UTC()
	updated, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND status='started'
		AND lease_owner=? AND lease_until>?`, result.Bytes(), storeTime(now), id.String(), owner, storeTime(now))
	if err != nil {
		return model.Operation{}, fmt.Errorf("reject operation: update: %w", err)
	}
	if exactlyOne(updated) != nil {
		return model.Operation{}, ErrOperationFence
	}
	rejected, err := operationTerminal(operation, model.OperationRejected, result, now)
	if err != nil {
		return model.Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return model.Operation{}, fmt.Errorf("reject operation: commit: %w", err)
	}
	return rejected, nil
}

func requireOperationFence(operation model.Operation, owner string, now time.Time) error {
	now = now.Round(0).UTC()
	leaseUntil, hasLease := operation.LeaseUntil()
	if operation.Status() != model.OperationStarted || !hasLease || operation.LeaseOwner() != owner ||
		now.IsZero() || now.Before(operation.CreatedAt()) || !leaseUntil.After(now) {
		return ErrOperationFence
	}
	return nil
}

func sameOperationRequest(a, b model.Operation) bool {
	aContext, aHas := a.ContextHash()
	bContext, bHas := b.ContextHash()
	return a.ProfileID() == b.ProfileID() && a.ClientKeyHash() == b.ClientKeyHash() &&
		a.Kind() == b.Kind() && a.RequestDigest() == b.RequestDigest() && aHas == bHas && (!aHas || aContext == bContext)
}

func requireEnabledProfile(ctx context.Context, q rowQuerier, id model.ProfileID) error {
	var enabled int
	var profileAsset, nodeAsset string
	if err := q.QueryRowContext(ctx, `SELECT p.enabled,p.active_asset_rev,n.active_asset_rev
		FROM profiles p JOIN node n ON n.singleton=1 WHERE p.profile_id=?`, id.String()).
		Scan(&enabled, &profileAsset, &nodeAsset); err != nil {
		return fmt.Errorf("operation Profile: %w", err)
	}
	if enabled != 1 || profileAsset != nodeAsset {
		return errors.New("operation Profile is disabled or asset authority drifted")
	}
	return nil
}

func requireOperationAgentRun(ctx context.Context, q rowQuerier, operation model.Operation,
	allowRuntimeFinished bool,
) error {
	var runProfile, runRuntime, profileRuntime, status string
	err := q.QueryRowContext(ctx, `SELECT r.profile_id,r.runtime_kind,p.runtime_kind,r.status
		FROM agent_runs r JOIN profiles p ON p.profile_id=r.profile_id WHERE r.run_id=?`,
		operation.AgentRunID().String()).Scan(&runProfile, &runRuntime, &profileRuntime, &status)
	if err != nil {
		return fmt.Errorf("operation AgentRun: %w", err)
	}
	active := status == "starting" || status == "running" ||
		(allowRuntimeFinished && status == "runtime_finished")
	if runProfile != operation.ProfileID().String() || runRuntime != profileRuntime || !active {
		return errors.New("operation AgentRun is not active authority for its Profile/runtime")
	}
	return nil
}

func insertOperation(ctx context.Context, tx *sql.Tx, operation model.Operation) error {
	contextBytes := any(nil)
	if contextHash, ok := operation.ContextHash(); ok {
		contextBytes = contextHash.Bytes()
	}
	leaseUntil, _ := operation.LeaseUntil()
	_, err := tx.ExecContext(ctx, "INSERT INTO operations(operation_id, profile_id, agent_run_id, client_key_hash, context_hash, kind, request_digest, status, lease_owner, lease_until, capture_json, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, 'started', ?, ?, ?, ?)",
		operation.ID().String(), operation.ProfileID().String(), operation.AgentRunID().String(), operation.ClientKeyHash().Bytes(), contextBytes,
		string(operation.Kind()), operation.RequestDigest().Bytes(), operation.LeaseOwner(), storeTime(leaseUntil), nil,
		storeTime(operation.CreatedAt()))
	if err != nil {
		return fmt.Errorf("reserve operation: insert: %w", err)
	}
	return nil
}
