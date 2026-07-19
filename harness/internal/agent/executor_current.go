package agent

import (
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

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
