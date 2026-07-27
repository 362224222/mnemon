package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrManagedAcceptanceInvariant = errors.New("managed acceptance durable invariant violated")

// ManagedAcceptanceSpec contains admitted values but no Agent-selected
// Handling, Run, source Event, Work version, Artifact reference authority, or
// derivation parent. CommitManagedAcceptance reconstructs all of those from
// the operation and its write-once current receipt.
type ManagedAcceptanceSpec struct {
	Scope     LocalAdmissionScope
	Items     []LocalAcceptanceItem
	Operation LocalOperationAuthority
}

// CommitManagedAcceptance is the atomic domain-acceptance boundary used by
// the Agent controller. A committed Event, Operation receipt, Handling
// outcome and AgentRun outcome become visible together or not at all.
func (s *Store) CommitManagedAcceptance(ctx context.Context, spec ManagedAcceptanceSpec,
	trustedNow time.Time,
) (LocalAcceptanceResult, error) {
	authority := spec.Operation
	return s.commitLocalAcceptance(ctx, LocalAcceptanceSpec{
		Scope: spec.Scope, Items: spec.Items, Operation: &authority,
	}, trustedNow, true)
}

type managedAcceptanceState struct {
	run                  model.AgentRun
	handling             model.Handling
	hasHandling          bool
	current              model.CurrentReadReceipt
	hasCurrent           bool
	actionWork           model.ReviewWork
	hasActionWork        bool
	authorizedReferences []model.Digest
	derivation           *LocalDerivationParent
}

func prepareManagedAcceptance(ctx context.Context, tx *sql.Tx, spec LocalAcceptanceSpec,
	operation model.Operation, trustedNow time.Time,
) (managedAcceptanceState, error) {
	if spec.Operation == nil || operation.ID().IsZero() || operation.Status() != model.OperationStarted {
		return managedAcceptanceState{}, fmt.Errorf("%w: started operation authority is required",
			ErrManagedAcceptanceInvariant)
	}
	if len(spec.AuthorizedReferences) != 0 || spec.Derivation != nil {
		return managedAcceptanceState{}, fmt.Errorf("%w: caller supplied derived acceptance authority",
			ErrManagedAcceptanceInvariant)
	}
	profile := spec.Scope.Profile()
	contextHash, hasContext := operation.ContextHash()
	if !hasContext {
		if operation.Kind() != model.OperationTeamworkOffer {
			return managedAcceptanceState{}, ErrManagedContextRequired
		}
		run, err := requireManagedInitiateRun(ctx, tx, profile, operation)
		if err != nil {
			return managedAcceptanceState{}, err
		}
		return managedAcceptanceState{run: run}, nil
	}

	run, handling, err := requireManagedClaimContext(ctx, tx, profile, contextHash,
		operation.Kind(), trustedNow)
	if err != nil {
		return managedAcceptanceState{}, err
	}
	if run.ID() != operation.AgentRunID() {
		return managedAcceptanceState{}, fmt.Errorf("%w: operation Run differs from current claim",
			ErrManagedAcceptanceInvariant)
	}
	raw, ok := run.CurrentReadReceipt()
	if !ok {
		return managedAcceptanceState{}, fmt.Errorf("%w: current receipt is absent",
			ErrManagedAcceptanceInvariant)
	}
	current, err := model.ParseCurrentReadReceipt(raw.Bytes())
	if err != nil {
		return managedAcceptanceState{}, fmt.Errorf("%w: current receipt is invalid: %v",
			ErrManagedAcceptanceInvariant, err)
	}
	state := managedAcceptanceState{run: run, handling: handling, hasHandling: true,
		current: current, hasCurrent: true}
	actionWork, err := readReviewWork(ctx, tx, current.ActionWork())
	if err != nil {
		return managedAcceptanceState{}, fmt.Errorf("%w: current action Work is unavailable",
			ErrManagedAcceptanceInvariant)
	}
	state.actionWork, state.hasActionWork = actionWork, true
	if operation.Kind() == model.OperationTeamworkOffer {
		if actionWork.Version() != current.ActionWorkVersion() ||
			actionWork.UpdatedBy() != current.SourceEvent().EventID() ||
			(actionWork.State() != model.WorkActive && actionWork.State() != model.WorkRework) {
			return managedAcceptanceState{}, fmt.Errorf("%w: nested offer parent differs from current receipt",
				ErrManagedAcceptanceInvariant)
		}
		state.derivation = &LocalDerivationParent{ChannelID: actionWork.ChannelID(), WorkRef: actionWork.Ref(),
			ExpectedVersion: actionWork.Version(), UpdatedByEvent: actionWork.UpdatedBy()}
	}
	return state, nil
}

func validateManagedAcceptanceEvents(authority managedAcceptanceState, operation model.Operation,
	events []model.Event,
) error {
	if len(events) != 1 {
		return fmt.Errorf("%w: accepted operation must have exactly one Event",
			ErrManagedAcceptanceInvariant)
	}
	event := events[0]
	if !authority.hasHandling {
		if operation.Kind() != model.OperationTeamworkOffer {
			return fmt.Errorf("%w: contextless operation is not an offer", ErrManagedAcceptanceInvariant)
		}
		if len(event.CausedBy()) != 0 {
			return fmt.Errorf("%w: contextless offer claimed current causality",
				ErrManagedAcceptanceInvariant)
		}
		return nil
	}
	if !authority.hasCurrent || authority.current.RunID() != operation.AgentRunID() {
		return fmt.Errorf("%w: context operation lacks exact current authority",
			ErrManagedAcceptanceInvariant)
	}
	source := authority.current.SourceEvent()
	causes := event.CausedBy()
	if len(causes) != 1 || event.AcceptedAt().Before(authority.current.ReadAt()) {
		return fmt.Errorf("%w: Event is not caused by the exact current source",
			ErrManagedAcceptanceInvariant)
	}
	if causes[0] != source && !managedParentResumeActionWorkCause(authority, operation, event) {
		return fmt.Errorf("%w: Event is not caused by the exact current source",
			ErrManagedAcceptanceInvariant)
	}
	if operation.Kind() == model.OperationTeamworkOffer {
		if authority.derivation == nil {
			return fmt.Errorf("%w: nested offer lacks derived parent", ErrManagedAcceptanceInvariant)
		}
		return nil
	}
	if event.Scope().WorkRef() != authority.current.ActionWork() {
		return fmt.Errorf("%w: action Event targets a different current Work",
			ErrManagedAcceptanceInvariant)
	}
	facts, err := decodeClosedEventPayload(event)
	if err != nil || facts.WorkVersion != authority.current.ActionWorkVersion() {
		return fmt.Errorf("%w: action Event uses a different current Work version",
			ErrManagedAcceptanceInvariant)
	}
	return nil
}

func managedParentResumeActionWorkCause(authority managedAcceptanceState,
	operation model.Operation, event model.Event,
) bool {
	if !managedParentResumeCauseEligible(authority, operation, event) {
		return false
	}
	projection := authority.current.Projection()
	action := projection.ActionWork()
	source := projection.SourceEvent()
	return eventCausesParentResumeActionWork(event, authority.actionWork, action) &&
		parentResumeActionWorkMatchesCurrent(authority.actionWork, action) &&
		currentSourceIsParentResumeChild(projection.ChildResults(), source.WorkRef()) &&
		source.WorkRef() != action.Ref()
}

func managedParentResumeCauseEligible(authority managedAcceptanceState,
	operation model.Operation, event model.Event,
) bool {
	if !authority.hasCurrent || !authority.hasActionWork ||
		operation.Kind() != model.OperationTeamworkDeliver ||
		event.Type() != model.EventReviewDeliveryReady ||
		len(authority.current.Projection().ChildResults()) == 0 {
		return false
	}
	return true
}

func eventCausesParentResumeActionWork(event model.Event, work model.ReviewWork,
	action model.CurrentWork,
) bool {
	causes := event.CausedBy()
	if len(causes) != 1 ||
		causes[0].EventID() != work.UpdatedBy() ||
		causes[0].OriginPeerID() != work.Ref().HomePeerID() ||
		event.Scope().WorkRef() != action.Ref() ||
		work.Ref() != action.Ref() ||
		work.ChannelID() != event.Scope().ChannelID() {
		return false
	}
	return true
}

func parentResumeActionWorkMatchesCurrent(work model.ReviewWork,
	action model.CurrentWork,
) bool {
	return action.LocalRole() == model.CurrentReviewer &&
		work.Version() == action.Version() &&
		work.Iteration() == action.Iteration() &&
		work.State() == action.State() &&
		work.DeadlineUnixNano() == action.DeadlineUnixNano() &&
		work.StateData().String() == action.StateData().String()
}

func currentSourceIsParentResumeChild(children []model.CurrentChildResult,
	source model.WorkRef,
) bool {
	for _, child := range children {
		if child.WorkRef() == source {
			return true
		}
	}
	return false
}

func managedAuthorizedReferences(authority managedAcceptanceState,
	events []model.Event,
) ([]model.Digest, error) {
	referenced := make(map[model.Digest]struct{})
	for _, event := range events {
		for _, ref := range event.Artifacts() {
			if ref.Role() == model.ArtifactReferenced {
				referenced[ref.RootDigest()] = struct{}{}
			}
		}
	}
	if len(referenced) == 0 {
		return nil, nil
	}
	if !authority.hasCurrent {
		return nil, fmt.Errorf("%w: referenced roots require current authority", ErrArtifactReference)
	}
	current := make(map[model.Digest]struct{}, len(authority.current.ArtifactRefs()))
	for _, ref := range authority.current.ArtifactRefs() {
		current[ref.RootDigest()] = struct{}{}
	}
	for root := range referenced {
		if _, ok := current[root]; !ok {
			return nil, fmt.Errorf("%w: referenced root lacks current authority", ErrArtifactReference)
		}
	}
	return sortedDigests(referenced), nil
}

func completeManagedAcceptance(ctx context.Context, tx *sql.Tx, operation model.Operation,
	authority managedAcceptanceState, receipt model.JSON, events []model.Event, trustedNow time.Time,
) error {
	if operation.Status() != model.OperationCommitted || receipt.IsZero() || len(events) == 0 ||
		authority.run.ID() != operation.AgentRunID() {
		return fmt.Errorf("%w: incomplete committed action evidence", ErrManagedAcceptanceInvariant)
	}
	if authority.hasHandling {
		fence, _ := authority.handling.ClaimTokenHash()
		lease, _ := authority.handling.LeaseUntil()
		result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status='completed',
			claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition='teamwork_action',
			outcome_event_id=?,last_error=NULL,updated_at=? WHERE handling_id=? AND profile_id=?
			AND event_id=? AND status='claimed' AND claim_owner=? AND claim_token_hash=?
			AND lease_until=? AND attempts=? AND recovery_count=? AND updated_at<=?`, events[0].ID().String(), storeTime(trustedNow),
			authority.handling.ID().String(), authority.handling.ProfileID().String(),
			authority.handling.EventID().String(), authority.handling.ClaimOwner(), fence.Bytes(),
			storeTime(lease), authority.handling.Attempts(), authority.handling.RecoveryCount(), storeTime(trustedNow))
		if err != nil || exactlyOne(result) != nil {
			return fmt.Errorf("%w: Handling completion fence failed", ErrManagedAcceptanceInvariant)
		}
	}
	query := `UPDATE agent_runs SET status='outcome_accepted',finished_at=COALESCE(finished_at,?),
		outcome_receipt_json=?,error=NULL
		WHERE run_id=? AND profile_id=?
		AND status IN ('starting','running','runtime_finished') AND outcome_receipt_json IS NULL
		`
	args := []any{storeTime(trustedNow), receipt.Bytes(), authority.run.ID().String(),
		authority.run.ProfileID().String()}
	if authority.hasHandling {
		fence, _ := authority.run.ClaimFenceHash()
		lease, _ := authority.run.LeaseUntil()
		query += ` AND handling_id=? AND handling_attempt=? AND handling_recovery=?
			AND claim_fence_hash=? AND lease_until=?
			AND current_read_receipt_json IS NOT NULL`
		args = append(args, authority.handling.ID().String(), authority.run.HandlingAttempt(),
			authority.run.HandlingRecovery(), fence.Bytes(), storeTime(lease))
	} else {
		query += ` AND handling_id IS NULL AND current_read_receipt_json IS NULL`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil || exactlyOne(result) != nil {
		return fmt.Errorf("%w: AgentRun completion fence failed", ErrManagedAcceptanceInvariant)
	}
	return nil
}

func validateManagedTerminalAcceptance(ctx context.Context, tx *sql.Tx, operation model.Operation,
	receipt model.JSON,
) error {
	run, err := readAgentRun(ctx, tx, operation.AgentRunID())
	if err != nil || run.Status() != model.AgentRunOutcomeAccepted {
		return fmt.Errorf("%w: committed operation lacks completed AgentRun", ErrManagedAcceptanceInvariant)
	}
	outcome, hasOutcome := run.OutcomeReceipt()
	if !hasOutcome || outcome.String() != receipt.String() {
		return fmt.Errorf("%w: AgentRun receipts differ from operation", ErrManagedAcceptanceInvariant)
	}
	contextHash, hasContext := operation.ContextHash()
	handlingID, runHasHandling := run.HandlingID()
	if hasContext != runHasHandling {
		return fmt.Errorf("%w: operation context and Run handling shape differ",
			ErrManagedAcceptanceInvariant)
	}
	if !hasContext {
		return nil
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if err != nil || handling.Status() != model.HandlingCompleted ||
		handling.LastDisposition() != "teamwork_action" || handling.Attempts() != run.HandlingAttempt() {
		return fmt.Errorf("%w: committed operation lacks completed Handling",
			ErrManagedAcceptanceInvariant)
	}
	if handling.RecoveryCount() != run.HandlingRecovery() {
		return fmt.Errorf("%w: committed Handling recovery generation differs",
			ErrManagedAcceptanceInvariant)
	}
	if fence, ok := run.ClaimFenceHash(); !ok || fence != contextHash {
		return fmt.Errorf("%w: committed Run differs from operation context",
			ErrManagedAcceptanceInvariant)
	}
	if _, ok := handling.OutcomeEventID(); !ok {
		return fmt.Errorf("%w: completed Handling lacks outcome Event",
			ErrManagedAcceptanceInvariant)
	}
	return nil
}
