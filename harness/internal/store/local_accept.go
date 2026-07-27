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
	operation, acceptedAt, trustedNow, err := prepareLocalAcceptanceTx(ctx, tx, spec, trustedNow)
	if err != nil {
		return model.JSON{}, err
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
	events, err := validateAcceptanceItems(ctx, tx, spec, operation, originPublicKey,
		acceptedAt, trustedNow, managedAuthority)
	if err != nil {
		return model.JSON{}, err
	}
	if err := validateOperationEvents(spec.Operation, events, spec.semanticControllerBatch); err != nil {
		return model.JSON{}, err
	}
	if managed {
		spec, managedAuthority, err = bindManagedAcceptanceAuthority(spec, managedAuthority, operation, events)
		if err != nil {
			return model.JSON{}, err
		}
	}
	artifacts, err := validateAcceptanceArtifacts(ctx, tx, operation, spec, events,
		trustedNow)
	if err != nil {
		return model.JSON{}, err
	}
	derivations, parent, err := prepareLocalDerivations(ctx, tx, operation, spec, events)
	if err != nil {
		return model.JSON{}, err
	}
	receipt, err := buildAcceptanceReceipt(ctx, tx, operation.ID(), events,
		spec.Items, artifacts.capture)
	if err != nil {
		return model.JSON{}, fmt.Errorf("commit local acceptance: receipt: %w", err)
	}
	if err := persistAcceptedBatch(ctx, tx, spec, operation, artifacts,
		events, trustedNow); err != nil {
		return model.JSON{}, err
	}
	if spec.Operation != nil {
		if err := commitAcceptanceOperation(ctx, tx, spec, operation, receipt, events,
			artifacts, parent, derivations, managedAuthority, managed,
			trustedNow); err != nil {
			return model.JSON{}, err
		}
	}
	return receipt, nil
}

// validateAcceptanceItems checks every publication in Scope order against the
// frozen batch identity: signature, actor, exact Event scope, Work mutation
// projection, participant binding, causality and deadline precedence.
func validateAcceptanceItems(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, originPublicKey []byte, acceptedAt, trustedNow time.Time,
	managedAuthority managedAcceptanceState,
) ([]model.Event, error) {
	events := make([]model.Event, len(spec.Items))
	for index, item := range spec.Items {
		event, err := validateLocalPublication(item.Publication, originPublicKey)
		if err != nil {
			return nil, err
		}
		if !event.AcceptedAt().Equal(acceptedAt) || event.ActorPrincipal() != spec.Scope.Profile().Principal() {
			return nil, fmt.Errorf("%w: Event time or actor drift", ErrAdmissionConflict)
		}
		expectedScope, err := spec.Scope.EventScope(uint8(index), event.Scope().WorkRef())
		if err != nil || event.Scope() != expectedScope {
			return nil, fmt.Errorf("%w: Event %d scope drift", ErrAdmissionConflict, index)
		}
		if err := validateWorkItem(item, event); err != nil {
			return nil, err
		}
		if err := validateParticipantBinding(ctx, tx, item, event); err != nil {
			return nil, err
		}
		if err := validateLocalCausality(ctx, tx, event); err != nil {
			return nil, err
		}
		if err := validateLocalCausalSemantics(ctx, tx, operation, event, managedAuthority); err != nil {
			return nil, err
		}
		if err := validateDeadlinePrecedence(ctx, tx, item, event, trustedNow); err != nil {
			return nil, err
		}
		events[index] = event
	}
	return events, nil
}

// persistAcceptedBatch appends every accepted Event with its Work mutation,
// Artifact pins and publication evidence, then advances the durable
// publication head and the Node origin sequence.
func persistAcceptedBatch(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, artifacts acceptanceArtifactAuthority,
	events []model.Event, trustedNow time.Time,
) error {
	for index, item := range spec.Items {
		event := events[index]
		if err := insertAcceptedEvent(ctx, tx, item.Publication); err != nil {
			return err
		}
		for _, ref := range event.Artifacts() {
			var err error
			if ref.Role() == model.ArtifactProduced && spec.Operation != nil {
				_, err = insertOperationAcceptanceEventArtifactPin(ctx, tx,
					artifacts, operation, ref.RootDigest(), event.ID(),
					event.AcceptedAt())
			} else {
				_, err = insertEventArtifactPin(ctx, tx, ref.RootDigest(),
					event.ID(), event.AcceptedAt())
			}
			if err != nil {
				return err
			}
		}
		if item.Work != nil {
			if err := applyAcceptedWorkMutation(ctx, tx, *item.Work, event); err != nil {
				return err
			}
		}
		if err := insertAcceptedPublicationEvidence(ctx, tx, event,
			operation, artifacts); err != nil {
			return err
		}
		if err := advancePublicationHead(ctx, tx, event, trustedNow); err != nil {
			return err
		}
	}
	return advanceNodeOriginSequence(ctx, tx, spec.Scope, trustedNow)
}

// applyAcceptedWorkMutation writes one accepted Work mutation and reconciles
// derivation disposition when the mutation reaches a terminal Work state.
func applyAcceptedWorkMutation(ctx context.Context, tx *sql.Tx, mutation WorkMutation,
	event model.Event,
) error {
	if err := applyWorkMutation(ctx, tx, mutation, event); err != nil {
		return err
	}
	if mutation.Work.State().Terminal() {
		return reconcileWorkDerivationDisposition(ctx, tx, mutation.Work.Ref())
	}
	return nil
}

// commitAcceptanceOperation fences the started operation into its committed
// terminal state and records provenance, derivations and, for the managed
// boundary, the settlement evidence for the accepted Events.
func commitAcceptanceOperation(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, receipt model.JSON, events []model.Event,
	artifacts acceptanceArtifactAuthority, parent model.ReviewWork,
	derivations []model.WorkDerivation,
	managedAuthority managedAcceptanceState, managed bool, trustedNow time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE operations SET status='committed',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND status='started'
		AND lease_owner=? AND lease_until>?`, receipt.Bytes(), storeTime(trustedNow), operation.ID().String(),
		spec.Operation.LeaseOwner, storeTime(trustedNow))
	if err != nil || exactlyOne(result) != nil {
		return ErrOperationFence
	}
	committed, err := operationTerminal(operation, model.OperationCommitted, receipt, trustedNow)
	if err != nil {
		return err
	}
	if err := insertLocalProvenance(ctx, tx, committed, events, artifacts); err != nil {
		return err
	}
	if err := insertLocalDerivations(ctx, tx, committed, parent, derivations); err != nil {
		return err
	}
	if managed {
		return completeManagedAcceptance(ctx, tx, committed, managedAuthority,
			receipt, events, trustedNow)
	}
	return nil
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
