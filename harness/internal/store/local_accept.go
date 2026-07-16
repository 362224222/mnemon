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

	var operation model.Operation
	if spec.Operation != nil {
		operation, err = readOperationByID(ctx, tx, spec.Operation.ID)
		if err != nil {
			return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: operation: %w", err)
		}
		if operation.ProfileID() != model.TeamworkProfileID() || operation.Kind() != spec.Operation.Kind ||
			operation.RequestDigest() != spec.Operation.RequestDigest {
			return LocalAcceptanceResult{}, ErrOperationMismatch
		}
		if operation.Status() == model.OperationCommitted {
			receipt, ok := operation.Result()
			if !ok {
				return LocalAcceptanceResult{}, ErrOperationTerminal
			}
			if managed {
				if err := validateManagedTerminalAcceptance(ctx, tx, operation, receipt); err != nil {
					return LocalAcceptanceResult{}, err
				}
			}
			if err := tx.Commit(); err != nil {
				return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: replay read: %w", err)
			}
			return LocalAcceptanceResult{Receipt: receipt, Replayed: true}, nil
		}
		if operation.Status() != model.OperationStarted {
			return LocalAcceptanceResult{}, ErrOperationTerminal
		}
	}
	if len(spec.Items) == 0 || len(spec.Items) > model.MaxChildWorks || spec.Scope.Count() != uint8(len(spec.Items)) {
		return LocalAcceptanceResult{}, errors.New("commit local acceptance: batch must contain exactly Scope count 1..7")
	}
	trustedNow = trustedNow.Round(0).UTC()
	acceptedAt := spec.Items[0].Publication.Event().AcceptedAt()
	if trustedNow.IsZero() || acceptedAt.IsZero() || trustedNow.Before(acceptedAt) {
		return LocalAcceptanceResult{}, errors.New("commit local acceptance: trusted commit time is invalid")
	}
	if spec.Operation != nil {
		if err := requireOperationFence(operation, spec.Operation.LeaseOwner, trustedNow); err != nil {
			return LocalAcceptanceResult{}, err
		}
	}
	originPublicKey, err := validateAdmissionAuthority(ctx, tx, spec, operation, acceptedAt)
	if err != nil {
		return LocalAcceptanceResult{}, err
	}
	managedAuthority := managedAcceptanceState{}
	if managed {
		managedAuthority, err = prepareManagedAcceptance(ctx, tx, spec, operation, trustedNow)
		if err != nil {
			return LocalAcceptanceResult{}, err
		}
	}

	events := make([]model.Event, len(spec.Items))
	for index, item := range spec.Items {
		event, err := validateLocalPublication(item.Publication, originPublicKey)
		if err != nil {
			return LocalAcceptanceResult{}, err
		}
		if !event.AcceptedAt().Equal(acceptedAt) || event.ActorPrincipal() != spec.Scope.Profile().Principal() {
			return LocalAcceptanceResult{}, fmt.Errorf("%w: Event time or actor drift", ErrAdmissionConflict)
		}
		expectedScope, err := spec.Scope.EventScope(uint8(index), event.Scope().WorkRef())
		if err != nil || event.Scope() != expectedScope {
			return LocalAcceptanceResult{}, fmt.Errorf("%w: Event %d scope drift", ErrAdmissionConflict, index)
		}
		if err := validateWorkItem(item, event); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := validateParticipantBinding(ctx, tx, item, event); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := validateLocalCausality(ctx, tx, event); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := validateLocalCausalSemantics(ctx, tx, operation, event); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := validateDeadlinePrecedence(ctx, tx, item, event, trustedNow); err != nil {
			return LocalAcceptanceResult{}, err
		}
		events[index] = event
	}
	if err := validateOperationEvents(spec.Operation, events); err != nil {
		return LocalAcceptanceResult{}, err
	}
	if managed {
		if err := validateManagedAcceptanceEvents(managedAuthority, operation, events); err != nil {
			return LocalAcceptanceResult{}, err
		}
		spec.AuthorizedReferences = managedAuthority.authorizedReferences
		spec.Derivation = managedAuthority.derivation
	}
	capture, err := validateAcceptanceArtifacts(ctx, tx, operation, spec, events)
	if err != nil {
		return LocalAcceptanceResult{}, err
	}
	derivations, parent, err := prepareLocalDerivations(ctx, tx, operation, spec, events)
	if err != nil {
		return LocalAcceptanceResult{}, err
	}
	receipt, err := buildAcceptanceReceipt(ctx, tx, operation.ID(), events, spec.Items, capture)
	if err != nil {
		return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: receipt: %w", err)
	}

	for index, item := range spec.Items {
		event := events[index]
		if err := insertAcceptedEvent(ctx, tx, item.Publication); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if item.Work != nil {
			if err := applyWorkMutation(ctx, tx, *item.Work, event); err != nil {
				return LocalAcceptanceResult{}, err
			}
			if item.Work.Work.State().Terminal() {
				if err := reconcileWorkDerivationDisposition(ctx, tx, item.Work.Work.Ref()); err != nil {
					return LocalAcceptanceResult{}, err
				}
			}
		}
		for _, ref := range event.Artifacts() {
			if _, err := insertEventArtifactPin(ctx, tx, ref.RootDigest(), event.ID(), event.AcceptedAt()); err != nil {
				return LocalAcceptanceResult{}, err
			}
		}
		if err := insertPublicationEvidence(ctx, tx, event); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := advancePublicationHead(ctx, tx, event, trustedNow); err != nil {
			return LocalAcceptanceResult{}, err
		}
	}
	if err := advanceNodeOriginSequence(ctx, tx, spec.Scope, trustedNow); err != nil {
		return LocalAcceptanceResult{}, err
	}

	if spec.Operation != nil {
		result, err := tx.ExecContext(ctx, `UPDATE operations SET status='committed',lease_owner=NULL,
			lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND status='started'
			AND lease_owner=? AND lease_until>?`, receipt.Bytes(), storeTime(trustedNow), operation.ID().String(),
			spec.Operation.LeaseOwner, storeTime(trustedNow))
		if err != nil || exactlyOne(result) != nil {
			return LocalAcceptanceResult{}, ErrOperationFence
		}
		committed, err := operationTerminal(operation, model.OperationCommitted, receipt, trustedNow)
		if err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := insertLocalProvenance(ctx, tx, committed, events); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if err := insertLocalDerivations(ctx, tx, committed, parent, derivations); err != nil {
			return LocalAcceptanceResult{}, err
		}
		if managed {
			if err := completeManagedAcceptance(ctx, tx, committed, managedAuthority,
				receipt, events, trustedNow); err != nil {
				return LocalAcceptanceResult{}, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return LocalAcceptanceResult{}, fmt.Errorf("commit local acceptance: commit: %w", err)
	}
	return LocalAcceptanceResult{Receipt: receipt}, nil
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
