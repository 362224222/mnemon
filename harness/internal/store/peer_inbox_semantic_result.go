package store

import "github.com/mnemon-dev/mnemon/harness/internal/model"

// CommitPeerInboxSemanticSpec carries one exact semantic fence, an immutable
// Store plan built from its claim outside SQLite, and any local response
// publications assembled from that plan.
type CommitPeerInboxSemanticSpec struct {
	Fence     PeerInboxSemanticFence
	Plan      PeerInboxSemanticPlan
	Scope     LocalAdmissionScope
	Responses []model.SignedPublication
}

// PeerInboxSemanticCommitResult reports the exact durable projection of a
// fresh semantic commit or its validated terminal replay.
type PeerInboxSemanticCommitResult struct {
	status         model.InboxStatus
	diagnostic     string
	importedEvent  model.EventID
	responseEvents []model.EventID
	receiptEvent   model.EventID
	hasReceipt     bool
	decision       model.JSON
	changed        bool
	replayed       bool
}

// Status returns the terminal Inbox status.
func (result PeerInboxSemanticCommitResult) Status() model.InboxStatus { return result.status }

// Diagnostic returns the closed terminal diagnostic, if any.
func (result PeerInboxSemanticCommitResult) Diagnostic() string { return result.diagnostic }

// ImportedEventID returns the locally durable identity of the imported Event.
func (result PeerInboxSemanticCommitResult) ImportedEventID() model.EventID {
	return result.importedEvent
}

// ResponseEventIDs returns a defensive copy of the ordered local response IDs.
func (result PeerInboxSemanticCommitResult) ResponseEventIDs() []model.EventID {
	return append([]model.EventID(nil), result.responseEvents...)
}

// ReceiptEventID returns the terminal local receipt Event, when the decision
// emitted a response.
func (result PeerInboxSemanticCommitResult) ReceiptEventID() (model.EventID, bool) {
	return result.receiptEvent, result.hasReceipt
}

// Decision returns the canonical durable decision projection.
func (result PeerInboxSemanticCommitResult) Decision() model.JSON { return result.decision }

// Changed reports whether this call committed the fresh semantic decision.
func (result PeerInboxSemanticCommitResult) Changed() bool { return result.changed }

// Replayed reports whether this call recovered a validated terminal decision.
func (result PeerInboxSemanticCommitResult) Replayed() bool { return result.replayed }
