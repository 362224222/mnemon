package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrManagedOperationInput     = errors.New("invalid managed operation input")
	ErrManagedProfileAuthority   = errors.New("managed operation Profile authority is unavailable")
	ErrManagedContextRequired    = errors.New("managed context is required")
	ErrManagedContextStale       = errors.New("managed context is stale")
	ErrManagedActionNotAllowed   = errors.New("managed action is not allowed by current")
	ErrManagedOperationInvariant = errors.New("managed operation durable invariant violated")
)

// ManagedOperationSpec contains only transport-authenticated identity and
// server-owned lease inputs. In particular it has no caller-selected
// Operation, AgentRun, Handling, Work, actor, or mode field.
type ManagedOperationSpec struct {
	Profile          model.Profile
	ClientKeyHash    model.Digest
	RequestDigest    model.Digest
	Kind             model.OperationKind
	LeaseOwner       string
	At               time.Time
	LeaseUntil       time.Time
	ClaimContextHash model.Digest
	HasClaimContext  bool
}

// ManagedOperationReservation returns the authority resolved in the reserve
// transaction. Run and Handling are intentionally absent for terminal replay:
// replay must not need to revalidate an expired managed context.
type ManagedOperationReservation struct {
	Operation   model.Operation
	Run         model.AgentRun
	Handling    model.Handling
	HasHandling bool
	Replayed    bool
	Acquired    bool
}

// ReserveManagedOperation is the only Store reservation boundary for an
// authenticated Agent action. It derives the Operation identity, resolves an
// opaque claim hash to the exact current Run/Handling, and creates an
// operation-scoped Run for a contextless Teamwork offer. Those decisions and
// the started Operation are committed atomically.
func (s *Store) ReserveManagedOperation(ctx context.Context,
	spec ManagedOperationSpec,
) (ManagedOperationReservation, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ManagedOperationReservation{}, fmt.Errorf("%w: nil store or context", ErrManagedOperationInput)
	}
	at, leaseUntil, err := validateManagedOperationSpec(spec)
	if err != nil {
		return ManagedOperationReservation{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: begin: %w", err)
	}
	defer tx.Rollback()

	profile, err := requireAuthenticatedManagedProfile(ctx, tx, spec.Profile)
	if err != nil {
		return ManagedOperationReservation{}, err
	}
	operationID, err := managedOperationID(profile.ID(), spec.ClientKeyHash)
	if err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("%w: derive operation ID: %v", ErrManagedOperationInvariant, err)
	}

	existing, err := readOperationByClientKey(ctx, tx, profile.ID(), spec.ClientKeyHash)
	if err == nil {
		if existing.ID() != operationID || existing.Kind() != spec.Kind ||
			existing.RequestDigest() != spec.RequestDigest {
			return ManagedOperationReservation{}, ErrOperationMismatch
		}
		if existing.Status().Terminal() {
			if err := tx.Commit(); err != nil {
				return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: terminal replay: %w", err)
			}
			return ManagedOperationReservation{Operation: existing, Replayed: true}, nil
		}
		if err := requireActiveManagedProfile(ctx, tx, profile, at); err != nil {
			return ManagedOperationReservation{}, err
		}
		return reserveExistingManagedOperation(ctx, tx, existing, profile, spec, at, leaseUntil)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: inspect identity: %w", err)
	}
	if err := requireActiveManagedProfile(ctx, tx, profile, at); err != nil {
		return ManagedOperationReservation{}, err
	}

	if spec.HasClaimContext {
		run, handling, err := requireManagedClaimContext(ctx, tx, profile,
			spec.ClaimContextHash, spec.Kind, at)
		if err != nil {
			return ManagedOperationReservation{}, err
		}
		// runtime_finished retains authority only for an Operation that was
		// durably started before the Runtime completion boundary. A fresh key
		// after that boundary cannot turn a finished turn into new work.
		if run.Status() == model.AgentRunRuntimeFinished {
			return ManagedOperationReservation{}, ErrManagedContextStale
		}
		if err := requireNoStartedManagedContext(ctx, tx, profile.ID(), spec.ClaimContextHash); err != nil {
			return ManagedOperationReservation{}, err
		}
		operation, err := newManagedStartedOperation(operationID, profile.ID(), run.ID(),
			spec.ClientKeyHash, &spec.ClaimContextHash, spec.Kind, spec.RequestDigest,
			spec.LeaseOwner, at, leaseUntil)
		if err != nil {
			return ManagedOperationReservation{}, err
		}
		if err := insertOperation(ctx, tx, operation); err != nil {
			return ManagedOperationReservation{}, err
		}
		if err := tx.Commit(); err != nil {
			return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: commit outcome: %w", err)
		}
		return ManagedOperationReservation{Operation: operation, Run: run, Handling: handling,
			HasHandling: true, Acquired: true}, nil
	}

	if spec.Kind != model.OperationTeamworkOffer {
		return ManagedOperationReservation{}, ErrManagedContextRequired
	}
	if err := requireNoActiveManagedClaim(ctx, tx, profile.ID()); err != nil {
		return ManagedOperationReservation{}, err
	}
	run, err := newManagedInitiateRun(profile, operationID, at)
	if err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("%w: build initiate AgentRun: %v", ErrManagedOperationInvariant, err)
	}
	if err := insertAgentRun(ctx, tx, run); err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: insert initiate AgentRun: %w", err)
	}
	operation, err := newManagedStartedOperation(operationID, profile.ID(), run.ID(),
		spec.ClientKeyHash, nil, spec.Kind, spec.RequestDigest, spec.LeaseOwner, at, leaseUntil)
	if err != nil {
		return ManagedOperationReservation{}, err
	}
	if err := insertOperation(ctx, tx, operation); err != nil {
		return ManagedOperationReservation{}, err
	}
	if err := tx.Commit(); err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: commit initiate: %w", err)
	}
	return ManagedOperationReservation{Operation: operation, Run: run, Acquired: true}, nil
}

func validateManagedOperationSpec(spec ManagedOperationSpec) (time.Time, time.Time, error) {
	if spec.Profile.ID().IsZero() || spec.ClientKeyHash.IsZero() || spec.RequestDigest.IsZero() ||
		!spec.Kind.Valid() || spec.LeaseOwner == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: Profile, key, request, kind and owner are required", ErrManagedOperationInput)
	}
	if _, err := model.ParseRunID(spec.LeaseOwner); err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: lease owner: %v", ErrManagedOperationInput, err)
	}
	if spec.HasClaimContext == spec.ClaimContextHash.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: claim context presence is inconsistent", ErrManagedOperationInput)
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: invalid trusted time", ErrManagedOperationInput)
	}
	leaseUntil, err := canonicalStoreTime(spec.LeaseUntil)
	if err != nil || !leaseUntil.After(at) {
		return time.Time{}, time.Time{}, fmt.Errorf("%w: lease must end after trusted time", ErrManagedOperationInput)
	}
	return at, leaseUntil, nil
}

func requireAuthenticatedManagedProfile(ctx context.Context, q rowQuerier,
	authenticated model.Profile,
) (model.Profile, error) {
	profile, err := readProfile(ctx, q)
	if err != nil || !sameProfileIdentity(profile, authenticated) ||
		!sameProfileAuthority(profile, authenticated) {
		return model.Profile{}, fmt.Errorf("%w: authenticated Profile changed", ErrManagedProfileAuthority)
	}
	return profile, nil
}

func requireActiveManagedProfile(ctx context.Context, q rowQuerier, profile model.Profile,
	at time.Time,
) error {
	node, err := readNode(ctx, q)
	if err != nil || !profile.Enabled() || profile.ActiveAssetRevision() != node.ActiveAssetRevision() ||
		at.Before(profile.UpdatedAt()) || at.Before(node.UpdatedAt()) {
		return fmt.Errorf("%w: Profile is disabled, drifted, or ahead of trusted time", ErrManagedProfileAuthority)
	}
	return nil
}

func reserveExistingManagedOperation(ctx context.Context, tx *sql.Tx, operation model.Operation,
	profile model.Profile, spec ManagedOperationSpec, at, leaseUntil time.Time,
) (ManagedOperationReservation, error) {
	result := ManagedOperationReservation{Operation: operation, Replayed: true}
	contextHash, hasContext := operation.ContextHash()
	if hasContext {
		if !spec.HasClaimContext || contextHash != spec.ClaimContextHash {
			return ManagedOperationReservation{}, ErrManagedContextStale
		}
		run, handling, err := requireManagedClaimContext(ctx, tx, profile, contextHash, spec.Kind, at)
		if err != nil {
			return ManagedOperationReservation{}, err
		}
		if run.ID() != operation.AgentRunID() {
			return ManagedOperationReservation{}, fmt.Errorf("%w: started operation is bound to another AgentRun", ErrOperationMismatch)
		}
		result.Run, result.Handling, result.HasHandling = run, handling, true
	} else {
		if spec.HasClaimContext || operation.Kind() != model.OperationTeamworkOffer {
			return ManagedOperationReservation{}, ErrOperationMismatch
		}
		run, err := requireManagedInitiateRun(ctx, tx, profile, operation)
		if err != nil {
			return ManagedOperationReservation{}, err
		}
		result.Run = run
	}

	lease, ok := operation.LeaseUntil()
	if !ok || at.Before(operation.CreatedAt()) {
		return ManagedOperationReservation{}, ErrOperationFence
	}
	if lease.After(at) {
		if operation.LeaseOwner() != spec.LeaseOwner {
			return result, ErrOperationPending
		}
		if err := tx.Commit(); err != nil {
			return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: active replay: %w", err)
		}
		result.Acquired = true
		return result, nil
	}

	updated, err := tx.ExecContext(ctx, `UPDATE operations SET lease_owner=?,lease_until=?
		WHERE operation_id=? AND status='started' AND lease_owner=? AND lease_until=? AND lease_until<=?`,
		spec.LeaseOwner, storeTime(leaseUntil), operation.ID().String(), operation.LeaseOwner(),
		storeTime(lease), storeTime(at))
	if err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: reclaim lease: %w", err)
	}
	if exactlyOne(updated) != nil {
		return ManagedOperationReservation{}, ErrOperationFence
	}
	reclaimed, err := operationWithLease(operation, spec.LeaseOwner, leaseUntil)
	if err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("%w: rebuild reclaimed operation: %v", ErrManagedOperationInvariant, err)
	}
	if err := tx.Commit(); err != nil {
		return ManagedOperationReservation{}, fmt.Errorf("reserve managed operation: reclaim commit: %w", err)
	}
	result.Operation, result.Acquired = reclaimed, true
	return result, nil
}

func requireManagedClaimContext(ctx context.Context, tx *sql.Tx, profile model.Profile,
	contextHash model.Digest, kind model.OperationKind, at time.Time,
) (model.AgentRun, model.Handling, error) {
	handling, err := scanAgentHandling(tx.QueryRowContext(ctx, handlingSelect+
		" WHERE profile_id=? AND status='claimed' AND claim_token_hash=?",
		profile.ID().String(), contextHash.Bytes()))
	if err != nil {
		return model.AgentRun{}, model.Handling{}, managedContextReadError("claim", err)
	}
	lease, hasLease := handling.LeaseUntil()
	fence, hasFence := handling.ClaimTokenHash()
	if !hasLease || !hasFence || fence != contextHash || !lease.After(at) ||
		at.Before(handling.UpdatedAt()) {
		return model.AgentRun{}, model.Handling{}, ErrManagedContextStale
	}
	runID, err := requireActiveAgentRunForClaim(ctx, tx, profile, handling)
	if err != nil {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: exact AgentRun is unavailable", ErrManagedContextStale)
	}
	run, err := readAgentRun(ctx, tx, runID)
	if err != nil || at.Before(run.StartedAt()) {
		return model.AgentRun{}, model.Handling{}, managedContextReadError("AgentRun", err)
	}
	receipt, ok := run.CurrentReadReceipt()
	if !ok {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: current was not durably read", ErrManagedContextStale)
	}
	current, err := model.ParseCurrentReadReceipt(receipt.Bytes())
	if err != nil {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: invalid current-read receipt", ErrManagedContextStale)
	}
	if current.RunID() != run.ID() || current.ProfileID() != profile.ID() ||
		current.HandlingID() != handling.ID() || current.HandlingAttempt() != handling.Attempts() ||
		run.HandlingRecovery() != handling.RecoveryCount() ||
		current.SourceEvent().EventID() != handling.EventID() || current.ReadAt().Before(run.StartedAt()) ||
		current.ReadAt().After(at) || !current.ReadAt().Before(lease) {
		return model.AgentRun{}, model.Handling{}, fmt.Errorf("%w: current-read binding differs", ErrManagedContextStale)
	}
	if !managedCurrentAllows(current, kind) {
		return model.AgentRun{}, model.Handling{}, ErrManagedActionNotAllowed
	}
	return run, handling, nil
}

func managedContextReadError(subject string, err error) error {
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s is unavailable", ErrManagedContextStale, subject)
	}
	return fmt.Errorf("%w: invalid %s evidence", ErrManagedContextStale, subject)
}

func managedCurrentAllows(current model.CurrentReadReceipt, kind model.OperationKind) bool {
	for _, action := range current.AllowedActions() {
		if action == kind {
			return true
		}
	}
	return false
}

func requireNoStartedManagedContext(ctx context.Context, q rowQuerier, profile model.ProfileID,
	contextHash model.Digest,
) error {
	var operationID string
	err := q.QueryRowContext(ctx, `SELECT operation_id FROM operations
		WHERE profile_id=? AND context_hash=? AND status='started' LIMIT 1`,
		profile.String(), contextHash.Bytes()).Scan(&operationID)
	if err == nil {
		return fmt.Errorf("%w: context already has operation %s", ErrOperationPending, operationID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("reserve managed operation: inspect context slot: %w", err)
	}
	return nil
}

func requireNoActiveManagedClaim(ctx context.Context, q rowQuerier, profile model.ProfileID) error {
	var claimed int
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_handlings
		WHERE profile_id=? AND status='claimed')`, profile.String()).Scan(&claimed); err != nil {
		return fmt.Errorf("reserve managed operation: inspect active claim: %w", err)
	}
	if claimed == 1 {
		return ErrManagedContextRequired
	}
	return nil
}

func requireManagedInitiateRun(ctx context.Context, q rowQuerier, profile model.Profile,
	operation model.Operation,
) (model.AgentRun, error) {
	run, err := readAgentRun(ctx, q, operation.AgentRunID())
	if err != nil || run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() ||
		!run.Status().OperationAuthority() {
		return model.AgentRun{}, fmt.Errorf("%w: initiate AgentRun is unavailable", ErrManagedOperationInvariant)
	}
	if _, hasHandling := run.HandlingID(); hasHandling {
		return model.AgentRun{}, fmt.Errorf("%w: contextless operation uses a handling-bound AgentRun", ErrManagedOperationInvariant)
	}
	wantCause, err := managedInitiateCause(operation.ID())
	if err != nil || run.Cause().String() != wantCause.String() {
		return model.AgentRun{}, fmt.Errorf("%w: initiate AgentRun cause differs", ErrManagedOperationInvariant)
	}
	return run, nil
}

func newManagedInitiateRun(profile model.Profile, operation model.OperationID,
	at time.Time,
) (model.AgentRun, error) {
	runID, err := newServerAgentRunID()
	if err != nil {
		return model.AgentRun{}, err
	}
	cause, err := managedInitiateCause(operation)
	if err != nil {
		return model.AgentRun{}, err
	}
	empty, err := model.NewJSON([]byte(`{}`))
	if err != nil {
		return model.AgentRun{}, err
	}
	return model.NewAgentRun(model.AgentRunSpec{ID: runID, ProfileID: profile.ID(), Cause: cause,
		Launcher: "external-operation", Runtime: profile.Runtime(), LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: model.AgentRunRunning, StartedAt: at})
}

func managedInitiateCause(operation model.OperationID) (model.JSON, error) {
	return model.JSONFrom(struct {
		Kind        string `json:"kind"`
		OperationID string `json:"operation_id"`
	}{"external_initiate", operation.String()})
}

func newManagedStartedOperation(id model.OperationID, profile model.ProfileID,
	run model.RunID, key model.Digest, contextHash *model.Digest, kind model.OperationKind,
	request model.Digest, owner string, at, leaseUntil time.Time,
) (model.Operation, error) {
	operation, err := model.NewOperation(model.OperationSpec{ID: id, ProfileID: profile,
		AgentRunID: run, ClientKeyHash: key, ContextHash: contextHash, Kind: kind,
		RequestDigest: request, Status: model.OperationStarted, LeaseOwner: owner,
		LeaseUntil: &leaseUntil, CreatedAt: at})
	if err != nil {
		return model.Operation{}, fmt.Errorf("%w: build started operation: %v", ErrManagedOperationInput, err)
	}
	return operation, nil
}

func managedOperationID(profile model.ProfileID, key model.Digest) (model.OperationID, error) {
	material := append([]byte("mnemon/r5/managed-operation/v1\x00"+profile.String()+"\x00"), key.Bytes()...)
	digest := model.Sum(material)
	return model.ParseOperationID("operation-" + base64.RawURLEncoding.EncodeToString(digest.Bytes()))
}
