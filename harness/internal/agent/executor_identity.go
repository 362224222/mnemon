package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func derivedOfferIDs(operation model.OperationID, ordinal uint8) (model.WorkID, model.EventID, error) {
	workText := derivedExecutionID("work", operation, ordinal)
	eventText := derivedExecutionID("event", operation, ordinal)
	workID, workErr := model.ParseWorkID(workText)
	eventID, eventErr := model.ParseEventID(eventText)
	return workID, eventID, errors.Join(workErr, eventErr)
}

func derivedActionEventID(operation model.OperationID) (model.EventID, error) {
	return model.ParseEventID(derivedExecutionID("event", operation, 0))
}

func derivedDeadlineEventID(operation model.OperationID) (model.EventID, error) {
	return model.ParseEventID(derivedExecutionID("deadline-event", operation, 0))
}

func derivedExecutionID(kind string, operation model.OperationID, ordinal uint8) string {
	digest := sha256.Sum256([]byte("mnemon-r5-teamwork-" + kind + "\x00" + operation.String() +
		"\x00" + fmt.Sprintf("%d", ordinal)))
	return kind + "-" + hex.EncodeToString(digest[:])
}
