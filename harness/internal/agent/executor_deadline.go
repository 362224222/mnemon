package agent

import (
	"context"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func (e *TeamworkActionExecutor) resolveDeadline(ctx context.Context,
	authority executionDeadlineAuthority, at time.Time,
) (OperationResponse, *ControlError) {
	transition, err := teamwork.PlanHomeTransition(teamwork.HomeTransitionSpec{Work: authority.work,
		ActorPeerID: authority.scope.node.PeerID(), ExpectedVersion: authority.current.ActionWorkVersion(),
		EventType: model.EventReviewExpired, NowUnixNano: at.UnixNano()})
	if err != nil || transition.AuthoritativeEventType() != model.EventReviewExpired {
		return OperationResponse{}, NewControlError(CodeWorkConflict,
			"current Work changed before deadline resolution")
	}
	eventID, err := derivedDeadlineEventID(authority.operation.ID)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not derive deadline Event identity")
	}
	eventScope, err := authority.scope.eventScope(0, authority.work.Ref())
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not derive deadline Event scope")
	}
	audience, err := model.NewAudience([]model.PeerID{authority.work.Participants().ReviewerPeerID()})
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"deadline Work audience is invalid")
	}
	stamp, err := event.NewAdmissionStamp(event.AdmissionStampSpec{Node: authority.scope.node,
		Profile: authority.scope.profile, EventID: eventID, ChannelID: authority.scope.channelID,
		WorkRef: authority.work.Ref(), OriginSequence: eventScope.OriginSequence(),
		ChannelSequence: eventScope.ChannelSequence(), OriginMember: eventScope.OriginMember(),
		PublicationRoster: eventScope.PublicationRoster(), Audience: audience,
		WorkVersion: authority.work.Version(), Iteration: authority.work.Iteration(),
		WorkDeadlineUnixNano: authority.work.DeadlineUnixNano(),
		CausedBy:             []model.EventKey{authority.current.SourceEvent()}})
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not bind deadline authority")
	}
	factory, err := event.NewFactory(executionClock{at}, e.signer)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"Teamwork deadline Event factory is unavailable")
	}
	bundle, err := factory.AdmitController(ctx, stamp, event.ExpiredDecision{})
	if err != nil || bundle.Event().Type() != model.EventReviewExpired ||
		!bundle.Event().AcceptedAt().Equal(at) {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not admit deadline Event")
	}
	nextSpec := authority.work.Spec()
	nextSpec.Version, nextSpec.Iteration = transition.NextVersion(), transition.NextIteration()
	nextSpec.State, nextSpec.StateData = transition.NextState(), bundle.Event().Payload()
	nextSpec.UpdatedBy, nextSpec.UpdatedAt = bundle.Event().ID(), bundle.Event().AcceptedAt()
	next, err := model.NewReviewWork(nextSpec)
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not project expired Work")
	}
	mutation, err := store.NewWorkTransition(next, authority.work.Version(), authority.work.State())
	if err != nil {
		return OperationResponse{}, NewControlError(CodeInternal,
			"server could not freeze expired Work")
	}
	result, err := e.backend.ResolveDeadline(ctx, executionDeadlineSpec{scope: authority.scope,
		expiry:    store.LocalAcceptanceItem{Publication: bundle.Publication(), Work: &mutation},
		operation: authority.operation, contextHash: authority.contextHash}, at)
	if err != nil {
		apiErr := mapTeamworkExecutionError(err)
		return OperationResponse{}, operationAPIError(apiErr.Code, apiErr.Message,
			authority.operation.ID, false)
	}
	apiErr := decodeManagedRejectionReceipt(result.Receipt, authority.operation.ID, result.Replayed)
	if apiErr.Code != CodeWorkExpired {
		return OperationResponse{}, operationAPIError(CodeInternal,
			"deadline rejection receipt is invalid", authority.operation.ID, result.Replayed)
	}
	return OperationResponse{}, apiErr
}
