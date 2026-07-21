package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrDeadlineResolution = errors.New("deadline winner cannot be resolved from the supplied authority")

const workExpiredMessage = "Work deadline reached before action commit"

// DeadlineResolutionSpec is the narrow handoff from the home controller to
// the Store. Expiry contains an already-admitted and signed controller Event;
// it is not an Event construction surface. ContextHash must come from the
// server-resolved managed context that selected the competing action.
type DeadlineResolutionSpec struct {
	Scope       LocalAdmissionScope
	Expiry      LocalAcceptanceItem
	Action      LocalOperationAuthority
	ContextHash model.Digest
}

type DeadlineResolutionResult struct {
	Receipt  model.JSON
	Replayed bool
}

// ResolveDeadlineWinner commits a due home expiry and the competing action's
// stable work_expired rejection in one SQLite transaction. A terminal replay
// returns before revalidating the now-stale admission snapshot, preserving the
// original receipt after response loss or restart.
func (s *Store) ResolveDeadlineWinner(ctx context.Context, spec DeadlineResolutionSpec,
	trustedNow time.Time,
) (DeadlineResolutionResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Action.ID.IsZero() ||
		!spec.Action.Kind.Valid() || spec.Action.RequestDigest.IsZero() {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: incomplete Store, context, or action", ErrDeadlineResolution)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: begin: %w", err)
	}
	defer tx.Rollback()

	operation, receipt, replayed, err := readDeadlineOperation(ctx, tx, spec.Action)
	if err != nil {
		return DeadlineResolutionResult{}, err
	}
	if replayed {
		if err := tx.Commit(); err != nil {
			return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: replay read: %w", err)
		}
		return DeadlineResolutionResult{Receipt: receipt, Replayed: true}, nil
	}

	trustedNow, err = validateDeadlineAction(ctx, tx, operation, spec, trustedNow)
	if err != nil {
		return DeadlineResolutionResult{}, err
	}
	controllerSpec, event, err := validateDeadlineExpiry(ctx, tx, spec, trustedNow)
	if err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := persistAcceptedBatch(ctx, tx, controllerSpec, []model.Event{event}, trustedNow); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := rejectDeadlineAction(ctx, tx, operation, spec.Action.LeaseOwner, receipt, trustedNow); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: commit: %w", err)
	}
	return DeadlineResolutionResult{Receipt: receipt}, nil
}

func readDeadlineOperation(ctx context.Context, tx *sql.Tx, authority LocalOperationAuthority,
) (model.Operation, model.JSON, bool, error) {
	operation, err := readOperationByID(ctx, tx, authority.ID)
	if err != nil {
		return model.Operation{}, model.JSON{}, false,
			fmt.Errorf("resolve deadline winner: operation: %w", err)
	}
	if operation.ProfileID() != model.TeamworkProfileID() || operation.Kind() != authority.Kind ||
		operation.RequestDigest() != authority.RequestDigest {
		return model.Operation{}, model.JSON{}, false, ErrOperationMismatch
	}
	receipt, err := buildWorkExpiredReceipt(operation.ID())
	if err != nil {
		return model.Operation{}, model.JSON{}, false,
			fmt.Errorf("resolve deadline winner: receipt: %w", err)
	}
	if operation.Status() == model.OperationRejected {
		stored, ok := operation.Result()
		if !ok || stored.String() != receipt.String() {
			return model.Operation{}, model.JSON{}, false, ErrOperationTerminal
		}
		return operation, stored, true, nil
	}
	if operation.Status() != model.OperationStarted {
		return model.Operation{}, model.JSON{}, false, ErrOperationTerminal
	}
	return operation, receipt, false, nil
}

func validateDeadlineAction(ctx context.Context, tx *sql.Tx, operation model.Operation,
	spec DeadlineResolutionSpec, trustedNow time.Time,
) (time.Time, error) {
	if !deadlineCompetingHomeAction(operation.Kind()) {
		return time.Time{}, fmt.Errorf("%w: %s is not a competing home action",
			ErrDeadlineResolution, operation.Kind())
	}
	contextHash, hasContext := operation.ContextHash()
	if !hasContext || spec.ContextHash.IsZero() || contextHash != spec.ContextHash {
		return time.Time{}, fmt.Errorf("%w: action context does not match the started operation",
			ErrDeadlineResolution)
	}
	canonical, err := canonicalStoreTime(trustedNow)
	if err != nil || canonical.IsZero() || canonical.UnixNano() <= 0 ||
		!time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) {
		return time.Time{}, fmt.Errorf("%w: trusted time is not an exact positive Unix nanosecond",
			ErrDeadlineResolution)
	}
	if err := requireOperationFence(operation, spec.Action.LeaseOwner, canonical); err != nil {
		return time.Time{}, err
	}
	if err := requireOperationAgentRun(ctx, tx, operation, true); err != nil {
		return time.Time{}, err
	}
	return canonical, nil
}

func validateDeadlineExpiry(ctx context.Context, tx *sql.Tx, spec DeadlineResolutionSpec,
	trustedNow time.Time,
) (LocalAcceptanceSpec, model.Event, error) {
	controllerSpec := LocalAcceptanceSpec{Scope: spec.Scope,
		Items: []LocalAcceptanceItem{spec.Expiry}, Controller: true}
	event := spec.Expiry.Publication.Event()
	if spec.Scope.Count() != 1 || spec.Expiry.Work == nil || event.Type() != model.EventReviewExpired ||
		!event.AcceptedAt().Equal(trustedNow) || !event.CreatedAt().Equal(trustedNow) {
		return LocalAcceptanceSpec{}, model.Event{}, fmt.Errorf(
			"%w: exact single expiry at trusted commit time is required", ErrDeadlineResolution)
	}
	originPublicKey, err := validateAdmissionAuthority(ctx, tx, controllerSpec, model.Operation{}, trustedNow)
	if err != nil {
		return LocalAcceptanceSpec{}, model.Event{}, err
	}
	event, err = validateLocalPublication(spec.Expiry.Publication, originPublicKey)
	if err != nil {
		return LocalAcceptanceSpec{}, model.Event{}, err
	}
	if err := validateDeadlineEventAuthority(ctx, tx, spec, controllerSpec, event); err != nil {
		return LocalAcceptanceSpec{}, model.Event{}, err
	}
	if err := validateDueDeadlineWork(ctx, tx, spec.Expiry, event, trustedNow); err != nil {
		return LocalAcceptanceSpec{}, model.Event{}, err
	}
	return controllerSpec, event, nil
}

func validateDeadlineEventAuthority(ctx context.Context, tx *sql.Tx, spec DeadlineResolutionSpec,
	controllerSpec LocalAcceptanceSpec, event model.Event,
) error {
	if event.ActorPrincipal() != spec.Scope.Profile().Principal() {
		return fmt.Errorf("%w: expiry actor drift", ErrAdmissionConflict)
	}
	expectedScope, err := spec.Scope.EventScope(0, event.Scope().WorkRef())
	if err != nil || event.Scope() != expectedScope {
		return fmt.Errorf("%w: expiry scope drift", ErrAdmissionConflict)
	}
	if event.Scope().OriginPeerID() != spec.Scope.Node().PeerID() ||
		event.Scope().WorkRef().HomePeerID() != spec.Scope.Node().PeerID() {
		return fmt.Errorf("%w: only the local Work home may resolve expiry", ErrDeadlineResolution)
	}
	if err := validateWorkItem(spec.Expiry, event); err != nil {
		return err
	}
	if err := validateParticipantBinding(ctx, tx, spec.Expiry, event); err != nil {
		return err
	}
	if err := validateLocalCausality(ctx, tx, event); err != nil {
		return err
	}
	if err := validateLocalCausalSemantics(ctx, tx, model.Operation{}, event, managedAcceptanceState{}); err != nil {
		return err
	}
	if err := validateOperationEvents(nil, []model.Event{event}, false); err != nil {
		return err
	}
	_, err = validateAcceptanceArtifacts(ctx, tx, model.Operation{}, controllerSpec, []model.Event{event})
	return err
}

func validateDueDeadlineWork(ctx context.Context, tx *sql.Tx, item LocalAcceptanceItem,
	event model.Event, trustedNow time.Time,
) error {
	current, err := readReviewWork(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return fmt.Errorf("resolve deadline winner: current Work: %w", err)
	}
	mutation := *item.Work
	if current.Version() != mutation.ExpectedVersion || current.State() != mutation.ExpectedState ||
		!current.State().DeadlineEligible() || trustedNow.UnixNano() < current.DeadlineUnixNano() {
		return fmt.Errorf("%w: Work is stale, terminal, or not due", ErrDeadlineResolution)
	}
	return requireExactDeadlineCause(ctx, tx, event, current)
}

func rejectDeadlineAction(ctx context.Context, tx *sql.Tx, operation model.Operation,
	leaseOwner string, receipt model.JSON, trustedNow time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND status='started'
		AND lease_owner=? AND lease_until>?`, receipt.Bytes(), storeTime(trustedNow), operation.ID().String(),
		leaseOwner, storeTime(trustedNow))
	if err != nil || exactlyOne(result) != nil {
		return ErrOperationFence
	}
	_, err = operationTerminal(operation, model.OperationRejected, receipt, trustedNow)
	return err
}

func deadlineCompetingHomeAction(kind model.OperationKind) bool {
	return kind == model.OperationTeamworkRework || kind == model.OperationTeamworkClose ||
		kind == model.OperationTeamworkCancel
}

func buildWorkExpiredReceipt(operation model.OperationID) (model.JSON, error) {
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		OperationID: operation, Code: "work_expired", Message: workExpiredMessage,
	})
	if err != nil {
		return model.JSON{}, err
	}
	return receipt.JSON(), nil
}

func requireExactDeadlineCause(ctx context.Context, tx *sql.Tx, expiry model.Event,
	current model.ReviewWork,
) error {
	causes := expiry.CausedBy()
	if len(causes) != 1 {
		return fmt.Errorf("%w: expiry must cite exactly the current Work update", ErrDeadlineResolution)
	}
	want, err := exactDeadlineCause(ctx, tx, current)
	if err != nil {
		return err
	}
	if causes[0] != want {
		return fmt.Errorf("%w: expiry does not cite the current Work update", ErrDeadlineResolution)
	}
	return nil
}

func deadlineCurrentEventType(state model.WorkState) model.EventType {
	switch state {
	case model.WorkOffered:
		return model.EventReviewOffered
	case model.WorkActive:
		return model.EventReviewAccepted
	case model.WorkRework:
		return model.EventReviewReworkRequested
	default:
		return ""
	}
}
