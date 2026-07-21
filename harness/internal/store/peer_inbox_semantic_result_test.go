package store

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestPeerInboxSemanticCommitResultAccessorsAreDefensive(t *testing.T) {
	imported := parseSemanticResultEventID(t, "event-semantic-result-imported")
	first := parseSemanticResultEventID(t, "event-semantic-result-response-a")
	second := parseSemanticResultEventID(t, "event-semantic-result-response-b")
	receipt := parseSemanticResultEventID(t, "event-semantic-result-receipt")
	decision, err := model.NewJSON([]byte(`{"decision":"accepted"}`))
	if err != nil {
		t.Fatal(err)
	}
	result := PeerInboxSemanticCommitResult{status: model.InboxAccepted,
		importedEvent: imported, responseEvents: []model.EventID{first, second},
		receiptEvent: receipt, hasReceipt: true, decision: decision,
		changed: true}
	responses := result.ResponseEventIDs()
	responses[0] = receipt
	gotReceipt, hasReceipt := result.ReceiptEventID()
	if result.Status() != model.InboxAccepted || result.ImportedEventID() != imported ||
		result.ResponseEventIDs()[0] != first || gotReceipt != receipt || !hasReceipt ||
		result.Decision().String() != decision.String() || !result.Changed() || result.Replayed() {
		t.Fatalf("semantic result accessors = %#v", result)
	}
}

func parseSemanticResultEventID(t *testing.T, value string) model.EventID {
	t.Helper()
	eventID, err := model.ParseEventID(value)
	if err != nil {
		t.Fatal(err)
	}
	return eventID
}
