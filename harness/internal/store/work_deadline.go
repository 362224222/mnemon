package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	workExpiryEventIDDomain = "mnemon:r5:work-expiry-event:v1"
	workExpiryBlockedReason = "target binding baseline unavailable at expiry"
)

type workExpiryDeliveryFence struct {
	binding           model.BindingState
	baselineAt        string
	baselineConfirmed bool
}

type workExpiryOperationFence struct {
	operation model.Operation
	run       model.AgentRun
	current   model.JSON
	found     bool
}

// WorkExpiryPreparation is the opaque prepare side of a prepare/sign/commit
// protocol. Every field is re-read by CommitWorkExpiry after Event signing.
type WorkExpiryPreparation struct {
	scope      LocalAdmissionScope
	work       model.ReviewWork
	cause      model.EventKey
	acceptedAt time.Time
	eventID    model.EventID
	delivery   workExpiryDeliveryFence
	operation  workExpiryOperationFence
}

func (p WorkExpiryPreparation) Scope() LocalAdmissionScope { return p.scope }
func (p WorkExpiryPreparation) Work() model.ReviewWork     { return p.work }
func (p WorkExpiryPreparation) Cause() model.EventKey      { return p.cause }
func (p WorkExpiryPreparation) AcceptedAt() time.Time      { return p.acceptedAt }
func (p WorkExpiryPreparation) EventID() model.EventID     { return p.eventID }

type WorkExpiryCommitSpec struct {
	Preparation WorkExpiryPreparation
	Expiry      LocalAcceptanceItem
}

type WorkExpiryCommitResult struct {
	rejectedOperation model.OperationID
	rejectionReceipt  model.JSON
	hasRejection      bool
}

func (r WorkExpiryCommitResult) RejectedOperation() (model.OperationID, model.JSON, bool) {
	return r.rejectedOperation, r.rejectionReceipt, r.hasRejection
}

// WorkExpiryEventID derives the only Event identity accepted for a prepared
// expiry. The current Work version and exact updater cause make retries stable
// without allowing a later Work generation to reuse the identity.
func WorkExpiryEventID(preparation WorkExpiryPreparation) (model.EventID, error) {
	if preparation.work.Ref().IsZero() || preparation.work.Version() == 0 ||
		preparation.cause.IsZero() || preparation.scope.Node().OriginEpoch().IsZero() ||
		preparation.scope.Node().PeerID() != preparation.work.Ref().HomePeerID() ||
		preparation.cause.OriginPeerID() != preparation.work.Ref().HomePeerID() ||
		preparation.cause.OriginEpoch() != preparation.scope.Node().OriginEpoch() ||
		preparation.cause.EventID() != preparation.work.UpdatedBy() {
		return model.EventID{}, fmt.Errorf("%w: incomplete expiry preparation", ErrWorkDeadlineInput)
	}
	identity, err := model.JSONFrom(struct {
		Cause       model.EventKey    `json:"cause"`
		Domain      string            `json:"domain"`
		OriginEpoch model.OriginEpoch `json:"origin_epoch"`
		Version     uint64            `json:"work_version"`
		Work        model.WorkRef     `json:"work"`
	}{preparation.cause, workExpiryEventIDDomain, preparation.scope.Node().OriginEpoch(),
		preparation.work.Version(), preparation.work.Ref()})
	if err != nil {
		return model.EventID{}, fmt.Errorf("%w: encode expiry identity: %v", ErrWorkDeadlineInvariant, err)
	}
	id, err := model.ParseEventID("event-work-expiry-" + model.Sum(identity.Bytes()).String())
	if err != nil {
		return model.EventID{}, fmt.Errorf("%w: derive expiry Event ID: %v", ErrWorkDeadlineInvariant, err)
	}
	return id, nil
}

// PrepareWorkExpiry re-reads a scan candidate and freezes every authority that
// may change while the controller constructs and signs review.expired.
func (s *Store) PrepareWorkExpiry(ctx context.Context, candidate WorkDeadlineCandidate,
	acceptedAt time.Time,
) (WorkExpiryPreparation, error) {
	if s == nil || s.db == nil || ctx == nil || candidate.work.Ref().IsZero() || candidate.cause.IsZero() {
		return WorkExpiryPreparation{}, fmt.Errorf("%w: incomplete Store or candidate", ErrWorkDeadlineInput)
	}
	acceptedAt, err := canonicalStoreTime(acceptedAt)
	if err != nil || acceptedAt.UnixNano() <= 0 {
		return WorkExpiryPreparation{}, fmt.Errorf("%w: trusted expiry time", ErrWorkDeadlineInput)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return WorkExpiryPreparation{}, fmt.Errorf("prepare Work expiry: begin: %w", err)
	}
	defer tx.Rollback()
	scope, err := prepareWorkDeadlineScopeTx(ctx, tx, candidate.work.ChannelID(), 1)
	if err != nil {
		return WorkExpiryPreparation{}, err
	}
	current, err := readReviewWork(ctx, tx, candidate.work.Ref())
	if err != nil || !validWorkExpiryCandidate(current, candidate, scope, acceptedAt) {
		return WorkExpiryPreparation{}, fmt.Errorf("%w: Work is changed, remote, exhausted, or not due",
			ErrWorkDeadlineStale)
	}
	cause, err := exactDeadlineCause(ctx, tx, current)
	if err != nil {
		return WorkExpiryPreparation{}, fmt.Errorf("%w: invalid current Work cause: %v",
			ErrWorkDeadlineInvariant, err)
	}
	if cause != candidate.cause {
		return WorkExpiryPreparation{}, fmt.Errorf("%w: current Work cause changed", ErrWorkDeadlineStale)
	}
	delivery, err := readWorkExpiryDeliveryFence(ctx, tx, scope, current.Participants().ReviewerPeerID())
	if err != nil {
		return WorkExpiryPreparation{}, err
	}
	operation, err := readWorkExpiryOperationFence(ctx, tx, current, acceptedAt)
	if err != nil {
		return WorkExpiryPreparation{}, err
	}
	preparation := WorkExpiryPreparation{scope: scope, work: current, cause: cause,
		acceptedAt: acceptedAt, delivery: delivery, operation: operation}
	preparation.eventID, err = WorkExpiryEventID(preparation)
	if err != nil {
		return WorkExpiryPreparation{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkExpiryPreparation{}, fmt.Errorf("prepare Work expiry: commit read: %w", err)
	}
	return preparation, nil
}

func validWorkExpiryCandidate(current model.ReviewWork, candidate WorkDeadlineCandidate,
	scope LocalAdmissionScope, acceptedAt time.Time,
) bool {
	return sameDeadlineWork(current, candidate.work) &&
		current.Ref().HomePeerID() == scope.Node().PeerID() && current.State().DeadlineEligible() &&
		current.Version() < model.MaxSQLiteInteger && acceptedAt.UnixNano() >= current.DeadlineUnixNano() &&
		!acceptedAt.Before(scope.Node().UpdatedAt())
}

// CommitWorkExpiry atomically persists the signed expiry, canonical Work CAS,
// publication/delivery evidence, pins/derivation settlement, and an optional
// stable work_expired rejection for the exact competing managed Operation.
func (s *Store) CommitWorkExpiry(ctx context.Context, spec WorkExpiryCommitSpec,
	trustedNow time.Time,
) (WorkExpiryCommitResult, error) {
	preparation := spec.Preparation
	if s == nil || s.db == nil || ctx == nil || spec.Expiry.Work == nil ||
		preparation.eventID.IsZero() || preparation.scope.Count() != 1 {
		return WorkExpiryCommitResult{}, fmt.Errorf("%w: incomplete Store, preparation, or expiry", ErrWorkDeadlineInput)
	}
	trustedNow, err := canonicalStoreTime(trustedNow)
	if err != nil || trustedNow.UnixNano() <= 0 || trustedNow.Before(preparation.acceptedAt) {
		return WorkExpiryCommitResult{}, fmt.Errorf("%w: trusted commit time", ErrWorkDeadlineInput)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkExpiryCommitResult{}, fmt.Errorf("commit Work expiry: begin: %w", err)
	}
	defer tx.Rollback()
	event, err := validateWorkExpiryPublicationTx(ctx, tx, spec)
	if err != nil {
		return WorkExpiryCommitResult{}, err
	}
	delivery, operation, err := validateWorkExpiryFencesTx(ctx, tx, spec, event)
	if err != nil {
		return WorkExpiryCommitResult{}, err
	}
	result, err := applyPreparedWorkExpiryTx(ctx, tx, spec, event, delivery, operation, trustedNow)
	if err != nil {
		return WorkExpiryCommitResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return WorkExpiryCommitResult{}, fmt.Errorf("commit Work expiry: commit: %w", err)
	}
	return result, nil
}

func validateWorkExpiryPublicationTx(ctx context.Context, tx *sql.Tx,
	spec WorkExpiryCommitSpec,
) (model.Event, error) {
	preparation := spec.Preparation
	controller := LocalAcceptanceSpec{Scope: preparation.scope,
		Items: []LocalAcceptanceItem{spec.Expiry}, Controller: true}
	publicKey, err := validateWorkDeadlineAdmissionAuthority(ctx, tx, controller,
		preparation.acceptedAt)
	if err != nil {
		return model.Event{}, err
	}
	event, err := validateLocalPublication(spec.Expiry.Publication, publicKey)
	if err != nil {
		return model.Event{}, err
	}
	if err := validatePreparedExpiryEvent(preparation, spec.Expiry, event); err != nil {
		return model.Event{}, err
	}
	if err := validateParticipantBinding(ctx, tx, spec.Expiry, event); err != nil {
		return model.Event{}, err
	}
	if err := validateLocalCausality(ctx, tx, event); err != nil {
		return model.Event{}, err
	}
	if err := validateLocalCausalSemantics(ctx, tx, model.Operation{}, event, managedAcceptanceState{}); err != nil {
		return model.Event{}, err
	}
	if err := validateOperationEvents(nil, []model.Event{event}, false); err != nil {
		return model.Event{}, err
	}
	if _, err := validateAcceptanceArtifacts(ctx, tx, model.Operation{}, controller,
		[]model.Event{event}); err != nil {
		return model.Event{}, err
	}
	return event, nil
}

func validateWorkExpiryFencesTx(ctx context.Context, tx *sql.Tx, spec WorkExpiryCommitSpec,
	event model.Event,
) (workExpiryDeliveryFence, workExpiryOperationFence, error) {
	preparation := spec.Preparation
	current, err := readReviewWork(ctx, tx, preparation.work.Ref())
	mutation := *spec.Expiry.Work
	if err != nil || !sameDeadlineWork(current, preparation.work) ||
		mutation.ExpectedVersion != current.Version() || mutation.ExpectedState != current.State() ||
		!current.State().DeadlineEligible() || current.Version() >= model.MaxSQLiteInteger ||
		preparation.acceptedAt.UnixNano() < current.DeadlineUnixNano() {
		return workExpiryDeliveryFence{}, workExpiryOperationFence{},
			fmt.Errorf("%w: Work changed before expiry commit", ErrWorkDeadlineStale)
	}
	cause, err := exactDeadlineCause(ctx, tx, current)
	if err != nil {
		return workExpiryDeliveryFence{}, workExpiryOperationFence{}, fmt.Errorf("%w: invalid current Work cause: %v",
			ErrWorkDeadlineInvariant, err)
	}
	causes := event.CausedBy()
	if cause != preparation.cause || len(causes) != 1 || causes[0] != cause {
		return workExpiryDeliveryFence{}, workExpiryOperationFence{},
			fmt.Errorf("%w: exact expiry cause changed", ErrWorkDeadlineStale)
	}
	delivery, err := readWorkExpiryDeliveryFence(ctx, tx, preparation.scope,
		current.Participants().ReviewerPeerID())
	if err != nil || delivery != preparation.delivery {
		return workExpiryDeliveryFence{}, workExpiryOperationFence{},
			fmt.Errorf("%w: delivery authority changed", ErrWorkDeadlineStale)
	}
	operation, err := readWorkExpiryOperationFence(ctx, tx, current, preparation.acceptedAt)
	if err != nil {
		return workExpiryDeliveryFence{}, workExpiryOperationFence{}, err
	}
	if !sameWorkExpiryOperationFence(operation, preparation.operation) {
		return workExpiryDeliveryFence{}, workExpiryOperationFence{},
			fmt.Errorf("%w: competing Operation changed", ErrWorkDeadlineStale)
	}
	return delivery, operation, nil
}

func applyPreparedWorkExpiryTx(ctx context.Context, tx *sql.Tx, spec WorkExpiryCommitSpec,
	event model.Event, delivery workExpiryDeliveryFence, operation workExpiryOperationFence,
	trustedNow time.Time,
) (WorkExpiryCommitResult, error) {
	preparation := spec.Preparation
	if err := applyWorkExpiryEffects(ctx, tx, preparation.scope, spec.Expiry, event,
		delivery, trustedNow); err != nil {
		return WorkExpiryCommitResult{}, err
	}
	result := WorkExpiryCommitResult{}
	if operation.found {
		receipt, err := buildWorkExpiredReceipt(operation.operation.ID())
		if err != nil {
			return WorkExpiryCommitResult{}, fmt.Errorf("commit Work expiry: rejection receipt: %w", err)
		}
		if err := rejectStartedManagedOperation(ctx, tx, operation.operation, receipt, trustedNow); err != nil {
			return WorkExpiryCommitResult{}, err
		}
		result = WorkExpiryCommitResult{operation.operation.ID(), receipt, true}
	}
	return result, nil
}

func validatePreparedExpiryEvent(preparation WorkExpiryPreparation, item LocalAcceptanceItem,
	event model.Event,
) error {
	wantID, err := WorkExpiryEventID(preparation)
	wantScope, scopeErr := preparation.scope.EventScope(0, preparation.work.Ref())
	if err != nil || scopeErr != nil || wantID != preparation.eventID || event.ID() != wantID ||
		event.Type() != model.EventReviewExpired || event.Scope() != wantScope ||
		event.ActorPrincipal() != preparation.scope.Profile().Principal() ||
		!event.AcceptedAt().Equal(preparation.acceptedAt) || !event.CreatedAt().Equal(preparation.acceptedAt) ||
		event.Scope().OriginPeerID() != preparation.work.Ref().HomePeerID() {
		return fmt.Errorf("%w: signed expiry does not match its preparation", ErrWorkDeadlineStale)
	}
	return validateWorkItem(item, event)
}

func sameDeadlineWork(left, right model.ReviewWork) bool {
	return left.Ref() == right.Ref() && left.ChannelID() == right.ChannelID() &&
		left.Participants() == right.Participants() && left.Version() == right.Version() &&
		left.Iteration() == right.Iteration() && left.DeadlineUnixNano() == right.DeadlineUnixNano() &&
		left.State() == right.State() && left.StateData().String() == right.StateData().String() &&
		left.UpdatedBy() == right.UpdatedBy() && left.UpdatedAt().Equal(right.UpdatedAt())
}

// applyWorkExpiryEffects is the single transaction-ordered expiry writer used
// by both timer expiry and the deadline/action race boundary.
func applyWorkExpiryEffects(ctx context.Context, tx *sql.Tx, scope LocalAdmissionScope,
	item LocalAcceptanceItem, event model.Event, delivery workExpiryDeliveryFence,
	committedAt time.Time,
) error {
	if item.Work == nil || event.Type() != model.EventReviewExpired {
		return fmt.Errorf("%w: expiry effects require one Work mutation", ErrWorkDeadlineInput)
	}
	if err := insertAcceptedEvent(ctx, tx, item.Publication); err != nil {
		return err
	}
	if err := applyWorkMutation(ctx, tx, *item.Work, event); err != nil {
		return err
	}
	if err := reconcileWorkDerivationDisposition(ctx, tx, item.Work.Work.Ref()); err != nil {
		return err
	}
	for _, ref := range event.Artifacts() {
		if _, err := insertEventArtifactPin(ctx, tx, ref.RootDigest(), event.ID(), event.AcceptedAt()); err != nil {
			return err
		}
	}
	status, reason := delivery.status()
	if err := insertPublicationEvidenceDisposition(ctx, tx, event, status, reason); err != nil {
		return err
	}
	if err := advancePublicationHead(ctx, tx, event, committedAt); err != nil {
		return err
	}
	return advanceNodeOriginSequence(ctx, tx, scope, committedAt)
}
