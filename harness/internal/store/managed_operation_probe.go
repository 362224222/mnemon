package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ManagedOperationProbeSpec carries only the authenticated operation identity.
// It deliberately excludes live context, time and lease inputs so a terminal
// receipt can be found before any live admission gate is consulted.
type ManagedOperationProbeSpec struct {
	Profile          model.Profile
	ClientKeyHash    model.Digest
	RequestDigest    model.Digest
	Kind             model.OperationKind
	ClaimContextHash model.Digest
	HasClaimContext  bool
}

// ManagedOperationProbe returns only an exact terminal match. A missing or
// exact started operation is a miss and carries no live execution authority.
type ManagedOperationProbe struct {
	Operation model.Operation
	Found     bool
}

// ProbeManagedOperation reads one operation by authenticated Profile and
// opaque client key without acquiring a lease or mutating durable state.
func (s *Store) ProbeManagedOperation(ctx context.Context,
	spec ManagedOperationProbeSpec,
) (ManagedOperationProbe, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Profile.ID().IsZero() ||
		spec.ClientKeyHash.IsZero() || spec.RequestDigest.IsZero() || !spec.Kind.Valid() ||
		spec.HasClaimContext == spec.ClaimContextHash.IsZero() {
		return ManagedOperationProbe{}, fmt.Errorf("%w: Profile, key, request, kind and context tuple are required",
			ErrManagedOperationInput)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ManagedOperationProbe{}, fmt.Errorf("probe managed operation: begin: %w", err)
	}
	defer tx.Rollback()

	profile, err := requireAuthenticatedManagedProfile(ctx, tx, spec.Profile)
	if err != nil {
		return ManagedOperationProbe{}, err
	}
	wantID, err := managedOperationID(profile.ID(), spec.ClientKeyHash)
	if err != nil {
		return ManagedOperationProbe{}, fmt.Errorf("%w: derive operation ID: %v",
			ErrManagedOperationInvariant, err)
	}
	operation, err := readOperationByClientKey(ctx, tx, profile.ID(), spec.ClientKeyHash)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return ManagedOperationProbe{}, fmt.Errorf("probe managed operation: empty read: %w", err)
		}
		return ManagedOperationProbe{}, nil
	}
	if err != nil {
		return ManagedOperationProbe{}, fmt.Errorf("probe managed operation: inspect identity: %w", err)
	}
	if !sameManagedOperationRequest(operation, wantID, profile.ID(), spec.ClientKeyHash, spec.RequestDigest,
		spec.Kind, spec.ClaimContextHash, spec.HasClaimContext) {
		return ManagedOperationProbe{}, ErrOperationMismatch
	}
	if err := tx.Commit(); err != nil {
		return ManagedOperationProbe{}, fmt.Errorf("probe managed operation: read: %w", err)
	}
	if !operation.Status().Terminal() {
		return ManagedOperationProbe{}, nil
	}
	return ManagedOperationProbe{Operation: operation, Found: true}, nil
}

func sameManagedOperationRequest(operation model.Operation, wantID model.OperationID,
	profileID model.ProfileID, clientKey, requestDigest model.Digest, kind model.OperationKind,
	contextHash model.Digest, hasContext bool,
) bool {
	durableContext, durableHasContext := operation.ContextHash()
	return operation.ID() == wantID && operation.ProfileID() == profileID &&
		operation.ClientKeyHash() == clientKey &&
		operation.Kind() == kind && operation.RequestDigest() == requestDigest &&
		durableHasContext == hasContext && (!hasContext || durableContext == contextHash)
}
