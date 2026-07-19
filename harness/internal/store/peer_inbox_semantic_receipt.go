package store

import (
	"encoding/json"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const peerInboxSemanticWorkConflictMessage = "Work was cancelled before action commit"

type peerInboxSemanticOperationReceiptExpectation struct {
	settlement     PeerInboxSemanticHandlingSettlement
	operationID    model.OperationID
	settledAt      time.Time
	winningOutcome *model.JSON
	notAfter       time.Time
}

func validatePeerInboxSemanticOperationReceipt(operation model.Operation, result model.JSON,
	finished time.Time, expectation *peerInboxSemanticOperationReceiptExpectation,
) (bool, error) {
	if expectation == nil || expectation.settlement.SourceEventID().IsZero() {
		return false, nil
	}
	expected, err := peerInboxSemanticOperationRejection(expectation.settlement, operation)
	if err != nil {
		return false, err
	}
	if expectation.operationID.IsZero() {
		if result.String() == expected.String() {
			return false, peerInboxSemanticHandlingInvariant(
				"superseded Operation receipt is missing from the AgentRun outcome")
		}
		return false, nil
	}
	if operation.ID() != expectation.operationID {
		return false, nil
	}
	if operation.Status() != model.OperationRejected || !finished.Equal(expectation.settledAt) ||
		result.String() != expected.String() {
		return false, peerInboxSemanticHandlingInvariant("superseded Operation receipt differs")
	}
	return true, nil
}

func peerInboxSemanticHandlingOutcomeReceipt(settlement PeerInboxSemanticHandlingSettlement,
	handling model.Handling, run model.AgentRun, operationID model.OperationID, at time.Time) (model.JSON, error) {
	receipt, err := model.JSONFrom(struct {
		Disposition string           `json:"disposition"`
		HandlingID  model.HandlingID `json:"handling_id"`
		OperationID string           `json:"operation_id,omitempty"`
		RunID       model.RunID      `json:"run_id"`
		Schema      int              `json:"schema_version"`
		SettledAt   string           `json:"settled_at"`
		SourceEvent model.EventID    `json:"source_event_id"`
		Status      string           `json:"status"`
		Work        model.WorkRef    `json:"work_ref"`
	}{string(settlement.Disposition()), handling.ID(), operationID.String(), run.ID(), model.SchemaVersion,
		storeTime(at), settlement.SourceEventID(), "superseded", settlement.WorkRef()})
	if err != nil || len(receipt.Bytes()) > model.MaxContentBytes {
		return model.JSON{}, peerInboxSemanticHandlingInvariant("build bounded AgentRun receipt: %v", err)
	}
	return receipt, nil
}

func peerInboxSemanticHandlingOutcomeOperationID(receipt model.JSON) (model.OperationID, error) {
	var envelope struct {
		OperationID string `json:"operation_id"`
	}
	if err := json.Unmarshal(receipt.Bytes(), &envelope); err != nil {
		return model.OperationID{}, peerInboxSemanticHandlingInvariant("parse AgentRun outcome receipt")
	}
	if envelope.OperationID == "" {
		return model.OperationID{}, nil
	}
	operationID, err := model.ParseOperationID(envelope.OperationID)
	if err != nil {
		return model.OperationID{}, peerInboxSemanticHandlingInvariant("parse superseded Operation identity")
	}
	return operationID, nil
}

func peerInboxSemanticOperationRejection(settlement PeerInboxSemanticHandlingSettlement,
	operation model.Operation,
) (model.JSON, error) {
	code, message := "work_conflict", peerInboxSemanticWorkConflictMessage
	if settlement.Disposition() == "superseded_expired" {
		code, message = "work_expired", workExpiredMessage
	}
	receipt, err := model.NewOperationRejectionReceipt(model.OperationRejectionSpec{
		Code: code, Message: message, OperationID: operation.ID(),
	})
	if err != nil {
		return model.JSON{}, peerInboxSemanticHandlingInvariant(
			"build bounded Operation rejection: %v", err)
	}
	return receipt.JSON(), nil
}
