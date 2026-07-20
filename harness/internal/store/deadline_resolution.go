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

	operation, err := readOperationByID(ctx, tx, spec.Action.ID)
	if err != nil {
		return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: operation: %w", err)
	}
	if operation.ProfileID() != model.TeamworkProfileID() || operation.Kind() != spec.Action.Kind ||
		operation.RequestDigest() != spec.Action.RequestDigest {
		return DeadlineResolutionResult{}, ErrOperationMismatch
	}
	receipt, err := buildWorkExpiredReceipt(operation.ID())
	if err != nil {
		return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: receipt: %w", err)
	}
	if operation.Status() == model.OperationRejected {
		stored, ok := operation.Result()
		if !ok || stored.String() != receipt.String() {
			return DeadlineResolutionResult{}, ErrOperationTerminal
		}
		if err := tx.Commit(); err != nil {
			return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: replay read: %w", err)
		}
		return DeadlineResolutionResult{Receipt: stored, Replayed: true}, nil
	}
	if operation.Status() != model.OperationStarted {
		return DeadlineResolutionResult{}, ErrOperationTerminal
	}
	if !deadlineCompetingHomeAction(operation.Kind()) {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: %s is not a competing home action", ErrDeadlineResolution, operation.Kind())
	}
	contextHash, hasContext := operation.ContextHash()
	if !hasContext || spec.ContextHash.IsZero() || contextHash != spec.ContextHash {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: action context does not match the started operation", ErrDeadlineResolution)
	}

	trustedNow = trustedNow.Round(0).UTC()
	nowUnixNano := trustedNow.UnixNano()
	if trustedNow.IsZero() || nowUnixNano <= 0 || !time.Unix(0, nowUnixNano).UTC().Equal(trustedNow) {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: trusted time is not an exact positive Unix nanosecond", ErrDeadlineResolution)
	}
	if err := requireOperationFence(operation, spec.Action.LeaseOwner, trustedNow); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := requireOperationAgentRun(ctx, tx, operation, true); err != nil {
		return DeadlineResolutionResult{}, err
	}

	event := spec.Expiry.Publication.Event()
	if spec.Scope.Count() != 1 || spec.Expiry.Work == nil || event.Type() != model.EventReviewExpired ||
		!event.AcceptedAt().Equal(trustedNow) || !event.CreatedAt().Equal(trustedNow) {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: exact single expiry at trusted commit time is required", ErrDeadlineResolution)
	}
	controllerSpec := LocalAcceptanceSpec{
		Scope:      spec.Scope,
		Items:      []LocalAcceptanceItem{spec.Expiry},
		Controller: true,
	}
	originPublicKey, err := validateAdmissionAuthority(ctx, tx, controllerSpec, model.Operation{}, trustedNow)
	if err != nil {
		return DeadlineResolutionResult{}, err
	}
	event, err = validateLocalPublication(spec.Expiry.Publication, originPublicKey)
	if err != nil {
		return DeadlineResolutionResult{}, err
	}
	if event.ActorPrincipal() != spec.Scope.Profile().Principal() {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: expiry actor drift", ErrAdmissionConflict)
	}
	expectedScope, err := spec.Scope.EventScope(0, event.Scope().WorkRef())
	if err != nil || event.Scope() != expectedScope {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: expiry scope drift", ErrAdmissionConflict)
	}
	if event.Scope().OriginPeerID() != spec.Scope.Node().PeerID() ||
		event.Scope().WorkRef().HomePeerID() != spec.Scope.Node().PeerID() {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: only the local Work home may resolve expiry", ErrDeadlineResolution)
	}
	if err := validateWorkItem(spec.Expiry, event); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := validateParticipantBinding(ctx, tx, spec.Expiry, event); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := validateLocalCausality(ctx, tx, event); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := validateLocalCausalSemantics(ctx, tx, model.Operation{}, event); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := validateOperationEvents(nil, []model.Event{event}, false); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if _, err := validateAcceptanceArtifacts(ctx, tx, model.Operation{}, controllerSpec, []model.Event{event}); err != nil {
		return DeadlineResolutionResult{}, err
	}

	current, err := readReviewWork(ctx, tx, event.Scope().WorkRef())
	if err != nil {
		return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: current Work: %w", err)
	}
	mutation := *spec.Expiry.Work
	if current.Version() != mutation.ExpectedVersion || current.State() != mutation.ExpectedState ||
		!current.State().DeadlineEligible() || trustedNow.UnixNano() < current.DeadlineUnixNano() {
		return DeadlineResolutionResult{}, fmt.Errorf("%w: Work is stale, terminal, or not due", ErrDeadlineResolution)
	}
	if err := requireExactDeadlineCause(ctx, tx, event, current); err != nil {
		return DeadlineResolutionResult{}, err
	}

	if err := insertAcceptedEvent(ctx, tx, spec.Expiry.Publication); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := applyWorkMutation(ctx, tx, mutation, event); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := reconcileWorkDerivationDisposition(ctx, tx, mutation.Work.Ref()); err != nil {
		return DeadlineResolutionResult{}, err
	}
	for _, ref := range event.Artifacts() {
		if _, err := insertEventArtifactPin(ctx, tx, ref.RootDigest(), event.ID(), event.AcceptedAt()); err != nil {
			return DeadlineResolutionResult{}, err
		}
	}
	if err := insertPublicationEvidence(ctx, tx, event); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := advancePublicationHead(ctx, tx, event, trustedNow); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := advanceNodeOriginSequence(ctx, tx, spec.Scope, trustedNow); err != nil {
		return DeadlineResolutionResult{}, err
	}

	result, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND status='started'
		AND lease_owner=? AND lease_until>?`, receipt.Bytes(), storeTime(trustedNow), operation.ID().String(),
		spec.Action.LeaseOwner, storeTime(trustedNow))
	if err != nil || exactlyOne(result) != nil {
		return DeadlineResolutionResult{}, ErrOperationFence
	}
	if _, err := operationTerminal(operation, model.OperationRejected, receipt, trustedNow); err != nil {
		return DeadlineResolutionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return DeadlineResolutionResult{}, fmt.Errorf("resolve deadline winner: commit: %w", err)
	}
	return DeadlineResolutionResult{Receipt: receipt}, nil
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
	var originText, epochText, channelText, homeText, workText, eventTypeText, sourceText, acceptedText string
	err := tx.QueryRowContext(ctx, `SELECT origin_peer_id,origin_epoch,channel_id,work_home_peer_id,work_id,
		event_type,source,accepted_at
		FROM events WHERE event_id=?`, current.UpdatedBy().String()).
		Scan(&originText, &epochText, &channelText, &homeText, &workText, &eventTypeText, &sourceText, &acceptedText)
	acceptedAt, timeErr := parseCanonicalStoreTime(acceptedText)
	if err != nil || channelText != current.ChannelID().String() || homeText != current.Ref().HomePeerID().String() ||
		workText != current.Ref().WorkID().String() || originText != current.Ref().HomePeerID().String() ||
		model.EventType(eventTypeText) != deadlineCurrentEventType(current.State()) ||
		sourceText != string(model.EventSourceLocal) || timeErr != nil || !acceptedAt.Equal(current.UpdatedAt()) {
		return fmt.Errorf("%w: current Work update Event is not exact", ErrDeadlineResolution)
	}
	origin, err := model.ParsePeerID(originText)
	if err != nil {
		return err
	}
	epoch, err := model.ParseOriginEpoch(epochText)
	if err != nil {
		return err
	}
	want, err := model.NewEventKey(origin, epoch, current.UpdatedBy())
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
