package agent

import (
	"context"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type currentActionPreparation struct {
	work        model.ReviewWork
	audience    model.Audience
	scope       executionScope
	participant bool
}

func (e *TeamworkActionExecutor) prepareCurrentAction(ctx context.Context,
	spec TeamworkExecutionSpec, current model.CurrentReadReceipt,
) (currentActionPreparation, *ControlError) {
	work, err := e.backend.GetReviewWork(ctx, current.ActionWork())
	if err != nil || work.Version() != current.ActionWorkVersion() {
		return currentActionPreparation{}, NewControlError(CodeWorkConflict,
			"current Work changed before admission")
	}
	participant := spec.Action.handler.mechanic.actor == actionActorParticipant
	localActor := work.Ref().HomePeerID()
	target := work.Participants().ReviewerPeerID()
	if participant {
		localActor, target = work.Participants().ReviewerPeerID(), work.Ref().HomePeerID()
	}
	audience, err := model.NewAudience([]model.PeerID{target})
	if err != nil {
		return currentActionPreparation{}, NewControlError(CodeInternal,
			"current Work audience is invalid")
	}
	scope, err := e.backend.Prepare(ctx, work.ChannelID(), audience, 1)
	if err != nil {
		return currentActionPreparation{}, mapTeamworkExecutionError(err)
	}
	if scope.node.PeerID() != localActor || scope.channelID != work.ChannelID() ||
		!sameExecutionProfile(scope.profile, e.profile) {
		return currentActionPreparation{}, NewControlError(CodeActionNotAllowed,
			"local Node is not the frozen Work participant")
	}
	return currentActionPreparation{work: work, audience: audience, scope: scope,
		participant: participant}, nil
}

func executionCurrent(reservation store.ManagedOperationReservation,
	at time.Time,
) (model.CurrentReadReceipt, bool, *ControlError) {
	operation := reservation.Operation
	_, contextBound := operation.ContextHash()
	raw, hasCurrent := reservation.Run.CurrentReadReceipt()
	if !contextBound {
		if hasCurrent || operation.Kind() != model.OperationTeamworkOffer {
			return model.CurrentReadReceipt{}, false, NewControlError(CodeContextInvalid,
				"contextless operation has invalid current authority")
		}
		return model.CurrentReadReceipt{}, false, nil
	}
	if !hasCurrent {
		return model.CurrentReadReceipt{}, false, NewControlError(CodeContextStale,
			"managed current receipt is unavailable")
	}
	current, err := model.ParseCurrentReadReceipt(raw.Bytes())
	if err != nil || current.RunID() != operation.AgentRunID() || current.ProfileID() != operation.ProfileID() ||
		at.Before(current.ReadAt()) {
		return model.CurrentReadReceipt{}, false, NewControlError(CodeContextStale,
			"managed current receipt is stale")
	}
	return current, true, nil
}
