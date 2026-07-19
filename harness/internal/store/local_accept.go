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
	ErrOperationTerminal = errors.New("operation is already terminal without a committed receipt")
	ErrCaptureMismatch   = errors.New("operation capture does not match produced Artifact roots")
)

type LocalOperationAuthority struct {
	ID            model.OperationID
	Kind          model.OperationKind
	RequestDigest model.Digest
	LeaseOwner    string
}

type LocalDerivationParent struct {
	ChannelID       model.ChannelID
	WorkRef         model.WorkRef
	ExpectedVersion uint64
	UpdatedByEvent  model.EventID
}

// A local-origin Event always addresses remote audience members and therefore
// cannot enqueue an AgentHandling at its own origin. Imported Inbox apply and
// the internal derivation-resume controller own those separate queue writes.
type LocalAcceptanceItem struct {
	Publication model.SignedPublication
	Work        *WorkMutation
}

// LocalAcceptanceSpec contains only already-admitted immutable domain values
// and server-owned authority references. Identity, actor, source, time,
// sequence, roster and projection remain frozen in Scope and are re-read.
type LocalAcceptanceSpec struct {
	Scope      LocalAdmissionScope
	Items      []LocalAcceptanceItem
	Controller bool
	Operation  *LocalOperationAuthority
	// AuthorizedReferences is the server-resolved current-read/controller
	// authority, sorted by digest. Stage 3 binds this same set to the durable
	// AgentRun current_read_receipt rather than accepting it from HTTP.
	AuthorizedReferences []model.Digest
	Derivation           *LocalDerivationParent
	// semanticControllerBatch is set only by the package-private Peer Inbox
	// transaction. It admits the one closed two-Event deadline decision
	// (review.expired followed by its request receipt) without widening the
	// ordinary public controller boundary.
	semanticControllerBatch bool
}

type LocalAcceptanceResult struct {
	Receipt  model.JSON
	Replayed bool
}

// CommitLocalAcceptance uses a server clock value distinct from Event time.
// Event accepted_at may be frozen before filesystem capture; lease freshness
// and durable commit timestamps must be checked at the actual commit attempt.
func (s *Store) CommitLocalAcceptance(ctx context.Context, spec LocalAcceptanceSpec,
	trustedNow time.Time,
) (LocalAcceptanceResult, error) {
	return s.commitLocalAcceptance(ctx, spec, trustedNow, false)
}

func (s *Store) commitLocalAcceptance(ctx context.Context, spec LocalAcceptanceSpec,
	trustedNow time.Time, managed bool,
) (LocalAcceptanceResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return LocalAcceptanceResult{}, errors.New("commit local acceptance: nil store or context")
	}
	if spec.Controller == (spec.Operation != nil) {
		return LocalAcceptanceResult{}, errors.New("commit local acceptance: choose controller or operation authority")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: begin: %w", err)
	}
	defer tx.Rollback()

	replay, replayed, err := readLocalAcceptanceReplayTx(ctx, tx, spec, managed)
	if err != nil {
		return LocalAcceptanceResult{}, err
	}
	if replayed {
		if err := tx.Commit(); err != nil {
			return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: replay read: %w", err)
		}
		return replay, nil
	}
	receipt, err := applyLocalAcceptanceTx(ctx, tx, spec, trustedNow, managed)
	if err != nil {
		return LocalAcceptanceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: commit: %w", err)
	}
	return LocalAcceptanceResult{Receipt: receipt}, nil
}

// readLocalAcceptanceReplayTx keeps response-loss replay at the public Store
// boundary. The transaction core below deliberately accepts only fresh work:
// a caller composing it into a larger transaction cannot use a terminal
// operation to skip current authority validation or recreate durable effects.
func readLocalAcceptanceReplayTx(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	managed bool,
) (LocalAcceptanceResult, bool, error) {
	if spec.Operation == nil {
		return LocalAcceptanceResult{}, false, nil
	}
	operation, err := readLocalAcceptanceOperation(ctx, tx, spec.Operation)
	if err != nil {
		return LocalAcceptanceResult{}, false, err
	}
	if operation.Status() == model.OperationCommitted {
		receipt, ok := operation.Result()
		if !ok {
			return LocalAcceptanceResult{}, false, ErrOperationTerminal
		}
		if managed {
			if err := validateManagedTerminalAcceptance(ctx, tx, operation, receipt); err != nil {
				return LocalAcceptanceResult{}, false, err
			}
		}
		return LocalAcceptanceResult{Receipt: receipt, Replayed: true}, true, nil
	}
	if operation.Status() != model.OperationStarted {
		return LocalAcceptanceResult{}, false, ErrOperationTerminal
	}
	return LocalAcceptanceResult{}, false, nil
}

func readLocalAcceptanceOperation(ctx context.Context, tx *sql.Tx,
	authority *LocalOperationAuthority,
) (model.Operation, error) {
	operation, err := readOperationByID(ctx, tx, authority.ID)
	if err != nil {
		return model.Operation{}, fmt.Errorf("commit local acceptance: operation: %w", err)
	}
	if operation.ProfileID() != model.TeamworkProfileID() || operation.Kind() != authority.Kind ||
		operation.RequestDigest() != authority.RequestDigest {
		return model.Operation{}, ErrOperationMismatch
	}
	return operation, nil
}

// applyLocalAcceptanceTx is the complete fresh local-admission boundary for a
// caller-owned transaction. It re-reads every durable authority and performs
// every validation and write that CommitLocalAcceptance uses, but never
// commits or rolls back tx. The caller must roll back the entire enclosing
// transaction after any error.
func applyLocalAcceptanceTx(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	trustedNow time.Time, managed bool,
) (model.JSON, error) {
	if ctx == nil || tx == nil {
		return model.JSON{}, errors.New("commit local acceptance: nil context or transaction")
	}
	if spec.Controller == (spec.Operation != nil) {
		return model.JSON{}, errors.New("commit local acceptance: choose controller or operation authority")
	}
	var operation model.Operation
	if spec.Operation != nil {
		var err error
		operation, err = readLocalAcceptanceOperation(ctx, tx, spec.Operation)
		if err != nil {
			return model.JSON{}, err
		}
		if operation.Status() != model.OperationStarted {
			return model.JSON{}, ErrOperationTerminal
		}
	}
	if len(spec.Items) == 0 || len(spec.Items) > model.MaxChildWorks || spec.Scope.Count() != uint8(len(spec.Items)) {
		return model.JSON{}, errors.New("commit local acceptance: batch must contain exactly Scope count 1..7")
	}
	trustedNow = trustedNow.Round(0).UTC()
	acceptedAt := spec.Items[0].Publication.Event().AcceptedAt()
	if trustedNow.IsZero() || acceptedAt.IsZero() || trustedNow.Before(acceptedAt) {
		return model.JSON{}, errors.New("commit local acceptance: trusted commit time is invalid")
	}
	if spec.Operation != nil {
		if err := requireOperationFence(operation, spec.Operation.LeaseOwner, trustedNow); err != nil {
			return model.JSON{}, err
		}
	}
	originPublicKey, err := validateAdmissionAuthority(ctx, tx, spec, operation, acceptedAt)
	if err != nil {
		return model.JSON{}, err
	}
	managedAuthority := managedAcceptanceState{}
	if managed {
		managedAuthority, err = prepareManagedAcceptance(ctx, tx, spec, operation, trustedNow)
		if err != nil {
			return model.JSON{}, err
		}
	}

	events := make([]model.Event, len(spec.Items))
	for index, item := range spec.Items {
		event, err := validateLocalPublication(item.Publication, originPublicKey)
		if err != nil {
			return model.JSON{}, err
		}
		if !event.AcceptedAt().Equal(acceptedAt) || event.ActorPrincipal() != spec.Scope.Profile().Principal() {
			return model.JSON{}, fmt.Errorf("%w: Event time or actor drift", ErrAdmissionConflict)
		}
		expectedScope, err := spec.Scope.EventScope(uint8(index), event.Scope().WorkRef())
		if err != nil || event.Scope() != expectedScope {
			return model.JSON{}, fmt.Errorf("%w: Event %d scope drift", ErrAdmissionConflict, index)
		}
		if err := validateWorkItem(item, event); err != nil {
			return model.JSON{}, err
		}
		if err := validateParticipantBinding(ctx, tx, item, event); err != nil {
			return model.JSON{}, err
		}
		if err := validateLocalCausality(ctx, tx, event); err != nil {
			return model.JSON{}, err
		}
		if err := validateLocalCausalSemantics(ctx, tx, operation, event); err != nil {
			return model.JSON{}, err
		}
		if err := validateDeadlinePrecedence(ctx, tx, item, event, trustedNow); err != nil {
			return model.JSON{}, err
		}
		events[index] = event
	}
	if err := validateOperationEvents(spec.Operation, events, spec.semanticControllerBatch); err != nil {
		return model.JSON{}, err
	}
	if managed {
		if err := validateManagedAcceptanceEvents(managedAuthority, operation, events); err != nil {
			return model.JSON{}, err
		}
		spec.AuthorizedReferences = managedAuthority.authorizedReferences
		spec.Derivation = managedAuthority.derivation
	}
	capture, err := validateAcceptanceArtifacts(ctx, tx, operation, spec, events)
	if err != nil {
		return model.JSON{}, err
	}
	derivations, parent, err := prepareLocalDerivations(ctx, tx, operation, spec, events)
	if err != nil {
		return model.JSON{}, err
	}
	receipt, err := buildAcceptanceReceipt(ctx, tx, operation.ID(), events, spec.Items, capture)
	if err != nil {
		return model.JSON{}, fmt.Errorf("commit local acceptance: receipt: %w", err)
	}

	for index, item := range spec.Items {
		event := events[index]
		if err := insertAcceptedEvent(ctx, tx, item.Publication); err != nil {
			return model.JSON{}, err
		}
		if item.Work != nil {
			if err := applyWorkMutation(ctx, tx, *item.Work, event); err != nil {
				return model.JSON{}, err
			}
			if item.Work.Work.State().Terminal() {
				if err := reconcileWorkDerivationDisposition(ctx, tx, item.Work.Work.Ref()); err != nil {
					return model.JSON{}, err
				}
			}
		}
		for _, ref := range event.Artifacts() {
			if _, err := insertEventArtifactPin(ctx, tx, ref.RootDigest(), event.ID(), event.AcceptedAt()); err != nil {
				return model.JSON{}, err
			}
		}
		if err := insertPublicationEvidence(ctx, tx, event); err != nil {
			return model.JSON{}, err
		}
		if err := advancePublicationHead(ctx, tx, event, trustedNow); err != nil {
			return model.JSON{}, err
		}
	}
	if err := advanceNodeOriginSequence(ctx, tx, spec.Scope, trustedNow); err != nil {
		return model.JSON{}, err
	}

	if spec.Operation != nil {
		result, err := tx.ExecContext(ctx, `UPDATE operations SET status='committed',lease_owner=NULL,
			lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND status='started'
			AND lease_owner=? AND lease_until>?`, receipt.Bytes(), storeTime(trustedNow), operation.ID().String(),
			spec.Operation.LeaseOwner, storeTime(trustedNow))
		if err != nil || exactlyOne(result) != nil {
			return model.JSON{}, ErrOperationFence
		}
		committed, err := operationTerminal(operation, model.OperationCommitted, receipt, trustedNow)
		if err != nil {
			return model.JSON{}, err
		}
		if err := insertLocalProvenance(ctx, tx, committed, events); err != nil {
			return model.JSON{}, err
		}
		if err := insertLocalDerivations(ctx, tx, committed, parent, derivations); err != nil {
			return model.JSON{}, err
		}
		if managed {
			if err := completeManagedAcceptance(ctx, tx, committed, managedAuthority,
				receipt, events, trustedNow); err != nil {
				return model.JSON{}, err
			}
		}
	}
	return receipt, nil
}

func validateDeadlinePrecedence(ctx context.Context, tx *sql.Tx, item LocalAcceptanceItem,
	event model.Event, trustedNow time.Time,
) error {
	if item.Work == nil || item.Work.ExpectedVersion == 0 || event.Type() == model.EventReviewExpired {
		return nil
	}
	current, err := readReviewWork(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return fmt.Errorf("commit local acceptance: deadline current Work: %w", err)
	}
	if current.State().DeadlineEligible() && trustedNow.UnixNano() >= current.DeadlineUnixNano() {
		return fmt.Errorf("%w: due Work must commit review.expired before %s",
			ErrDeadlineResolution, event.Type())
	}
	return nil
}
