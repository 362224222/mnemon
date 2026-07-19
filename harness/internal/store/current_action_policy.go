package store

import (
	"context"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func validateStoredCurrentActionAuthority(ctx context.Context, q rowQuerier,
	policy model.TeamworkActionPolicy, receipt model.CurrentReadReceipt, sourceEvent model.Event,
	work model.ReviewWork, role model.CurrentRole, durableBrief model.CurrentBrief,
) error {
	workUpdate, err := readExactCurrentWorkUpdate(ctx, q, work)
	if err != nil {
		return err
	}
	if receipt.ActionWorkUpdatedBy() != work.UpdatedBy() {
		workUpdate, err = readCurrentSourceEvent(ctx, q, receipt.ActionWorkUpdatedBy())
		if err != nil {
			return fmt.Errorf("%w: replay frozen Work update Event: %v", ErrCurrentReadInvariant, err)
		}
	}
	return validateStoredCurrentWorkProjection(policy, receipt.Projection(), sourceEvent,
		workUpdate, work, role, durableBrief, receipt.ActionWorkUpdatedBy(),
		receipt.ActionWorkUpdatedAt())
}

func deriveFreshCurrentPolicyActions(ctx context.Context, q rowQuerier,
	policy model.TeamworkActionPolicy, role model.CurrentRole, sourceEvent model.Event,
	work model.ReviewWork,
) ([]model.OperationKind, error) {
	if _, err := readExactCurrentWorkUpdate(ctx, q, work); err != nil {
		return nil, err
	}
	return deriveCurrentPolicyActions(policy, role, sourceEvent, work)
}

func readExactCurrentWorkUpdate(ctx context.Context, q rowQuerier,
	work model.ReviewWork,
) (model.Event, error) {
	update, err := readCurrentSourceEvent(ctx, q, work.UpdatedBy())
	if err != nil {
		return model.Event{}, fmt.Errorf("%w: Work update Event: %v", ErrCurrentReadInvariant, err)
	}
	facts, err := decodeClosedEventPayload(update)
	if err != nil {
		return model.Event{}, fmt.Errorf("%w: Work update Event: %v", ErrCurrentReadInvariant, err)
	}
	exact, err := currentWorkIsExactSource(update, work, facts)
	if err != nil || !exact {
		return model.Event{}, fmt.Errorf("%w: Work updater does not produce its durable snapshot",
			ErrCurrentReadInvariant)
	}
	return update, nil
}

func deriveCurrentPolicyActions(policy model.TeamworkActionPolicy, role model.CurrentRole,
	event model.Event, work model.ReviewWork,
) ([]model.OperationKind, error) {
	facts, err := decodeClosedEventPayload(event)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCurrentReadInvariant, err)
	}
	exactUpdate, err := currentWorkIsExactSource(event, work, facts)
	if err != nil {
		return nil, err
	}
	return deriveCurrentActions(policy, role, event.Type(), work.State(), work.Iteration(), exactUpdate)
}

func deriveCurrentActions(policy model.TeamworkActionPolicy, role model.CurrentRole,
	eventType model.EventType, state model.WorkState, iteration uint8, exactUpdate bool,
) ([]model.OperationKind, error) {
	domain, err := policy.ActionsForContexts(
		deriveCurrentActionContexts(role, eventType, state, iteration, exactUpdate))
	if err != nil {
		return nil, fmt.Errorf("%w: Teamwork Action policy: %v", ErrCurrentReadInvariant, err)
	}
	if len(domain) == 0 {
		return currentResolutionActions(), nil
	}
	return append(domain, model.OperationResolveRetry), nil
}

func currentResolutionActions() []model.OperationKind {
	return []model.OperationKind{model.OperationResolveNoAction, model.OperationResolveRetry,
		model.OperationResolveReject}
}

func deriveCurrentActionContexts(role model.CurrentRole, eventType model.EventType,
	state model.WorkState, iteration uint8, exactUpdate bool,
) []model.TeamworkActionContext {
	if !exactUpdate {
		return nil
	}
	if role == model.CurrentReviewer {
		switch {
		case state == model.WorkOffered && eventType == model.EventReviewOffered:
			return []model.TeamworkActionContext{model.TeamworkActionContextReviewerOffered}
		case state == model.WorkActive && eventType == model.EventReviewAccepted:
			return []model.TeamworkActionContext{model.TeamworkActionContextReviewerActive}
		case state == model.WorkRework && eventType == model.EventReviewReworkRequested:
			return []model.TeamworkActionContext{model.TeamworkActionContextReviewerRework}
		default:
			return nil
		}
	}
	if role != model.CurrentInitiator {
		return nil
	}
	contexts := make([]model.TeamworkActionContext, 0, 3)
	if state == model.WorkDelivered && eventType == model.EventReviewDelivered {
		contexts = append(contexts, model.TeamworkActionContextHomeDelivered)
		if iteration == 1 {
			contexts = append(contexts, model.TeamworkActionContextHomeDeliveredIteration1)
		}
	}
	if !state.Terminal() {
		contexts = append(contexts, model.TeamworkActionContextHomeNonterminal)
	}
	return contexts
}

func validateStoredCurrentWorkProjection(policy model.TeamworkActionPolicy,
	projection model.CurrentProjection, sourceEvent, workUpdate model.Event,
	work model.ReviewWork, role model.CurrentRole, durableBrief model.CurrentBrief,
	workUpdatedBy model.EventID, workUpdatedAt time.Time,
) error {
	projectedWork := projection.ActionWork()
	projectedBrief, ok := projectedWork.Brief()
	if !ok || projectedWork.Ref() != work.Ref() ||
		projectedWork.DeadlineUnixNano() != work.DeadlineUnixNano() || projectedWork.LocalRole() != role ||
		projectedBrief.Content() != durableBrief.Content() ||
		projectedBrief.DeadlineUnixNano() != durableBrief.DeadlineUnixNano() ||
		!sameCurrentArtifactRoots(projectedBrief.ArtifactRefs(), durableBrief.ArtifactRefs()) {
		return fmt.Errorf("%w: stored projection Work brief differs from durable authority",
			ErrCurrentReadInvariant)
	}
	if err := validateFrozenCurrentWorkPosition(projectedWork, work, workUpdatedBy, workUpdatedAt); err != nil {
		return err
	}
	frozenWork, err := reconstructStoredCurrentWork(projectedWork, work, workUpdatedBy, workUpdatedAt)
	if err != nil {
		return err
	}
	updateFacts, err := decodeClosedEventPayload(workUpdate)
	if err != nil {
		return fmt.Errorf("%w: frozen Work update Event: %v", ErrCurrentReadInvariant, err)
	}
	exactUpdate, err := currentWorkIsExactSource(workUpdate, frozenWork, updateFacts)
	if err != nil || !exactUpdate {
		return fmt.Errorf("%w: frozen Work updater does not produce its snapshot",
			ErrCurrentReadInvariant)
	}
	actions, err := deriveCurrentPolicyActions(policy, role, sourceEvent, frozenWork)
	if err != nil {
		return err
	}
	if !sameCurrentActionKinds(projection.AllowedActions(), actions) {
		return fmt.Errorf("%w: stored projection actions differ from the managed policy",
			ErrCurrentReadInvariant)
	}
	return nil
}

func validateFrozenCurrentWorkPosition(projected model.CurrentWork, durable model.ReviewWork,
	updatedBy model.EventID, updatedAt time.Time,
) error {
	if projected.Version() > durable.Version() {
		return fmt.Errorf("%w: frozen Work version leads durable authority", ErrCurrentReadInvariant)
	}
	if projected.Version() < durable.Version() {
		if durable.UpdatedAt().Before(updatedAt) {
			return fmt.Errorf("%w: durable Work time precedes its frozen history",
				ErrCurrentReadInvariant)
		}
		return nil
	}
	if projected.Iteration() != durable.Iteration() || projected.State() != durable.State() ||
		projected.StateData().String() != durable.StateData().String() ||
		updatedBy != durable.UpdatedBy() || !updatedAt.Equal(durable.UpdatedAt()) {
		return fmt.Errorf("%w: equal-version frozen Work differs from durable authority",
			ErrCurrentReadInvariant)
	}
	return nil
}

// reconstructStoredCurrentWork joins the private receipt's frozen updater
// evidence to the public Work snapshot. Replay therefore recovers the original
// exact/stale branch without consulting mutable current Work state.
func reconstructStoredCurrentWork(projected model.CurrentWork, durable model.ReviewWork,
	updatedBy model.EventID, updatedAt time.Time,
) (model.ReviewWork, error) {
	work, err := model.NewReviewWork(model.ReviewWorkSpec{Ref: projected.Ref(),
		ChannelID: durable.ChannelID(), Participants: durable.Participants(), Version: projected.Version(),
		Iteration: projected.Iteration(), DeadlineUnixNano: projected.DeadlineUnixNano(),
		State: projected.State(), StateData: projected.StateData(), UpdatedBy: updatedBy,
		UpdatedAt: updatedAt})
	if err != nil {
		return model.ReviewWork{}, fmt.Errorf("%w: reconstruct frozen Work: %v",
			ErrCurrentReadInvariant, err)
	}
	return work, nil
}

func sameCurrentActionKinds(left, right []model.OperationKind) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
