package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type peerInboxSemanticHandlingAuthority struct {
	source         model.Event
	work           model.ReviewWork
	profileRuntime model.RuntimeKind
	requireRuntime bool
	handling       model.Handling
}

// settlePeerInboxSemanticHandling closes the exact local Agent obligation
// superseded by a remote home cancellation or expiry. The caller owns tx and
// commits this transition with the imported Event, Work CAS, and Inbox
// decision. In particular, this helper never commits or rolls back tx.
//
// PeerInboxSemanticHandlingSettlement points at the pre-transition Work update Event,
// which may already have advanced to the matching terminal update in this transaction.
func settlePeerInboxSemanticHandling(ctx context.Context, tx *sql.Tx,
	settlement PeerInboxSemanticHandlingSettlement, at time.Time,
) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("%w: nil context or transaction", ErrPeerInboxSemanticInput)
	}
	at, err := canonicalStoreTime(at)
	if err != nil || at.IsZero() {
		return fmt.Errorf("%w: invalid trusted settlement time", ErrPeerInboxSemanticInput)
	}
	authority, err := readPeerInboxSemanticHandlingAuthority(ctx, tx, settlement, true)
	if err != nil {
		return err
	}
	if at.Before(authority.source.AcceptedAt()) || at.Before(authority.work.UpdatedAt()) ||
		at.Before(authority.handling.CreatedAt()) || at.Before(authority.handling.UpdatedAt()) {
		return peerInboxSemanticHandlingInvariant("trusted time precedes durable settlement authority")
	}

	switch authority.handling.Status() {
	case model.HandlingPending:
		if err := validatePeerInboxSemanticUnclaimedHistory(ctx, tx, authority, at); err != nil {
			return err
		}
		if err := updatePeerInboxSemanticPendingHandling(ctx, tx, authority.handling,
			string(settlement.Disposition()), at); err != nil {
			return err
		}
	case model.HandlingClaimed:
		if err := settlePeerInboxSemanticClaimedHandling(ctx, tx, authority,
			settlement, at); err != nil {
			return err
		}
	case model.HandlingCompleted, model.HandlingRejected, model.HandlingDead:
		return validatePeerInboxSemanticTerminalHandling(ctx, tx, authority, settlement)
	default:
		return peerInboxSemanticHandlingInvariant("Handling has an unknown lifecycle")
	}

	authority.handling, err = readAgentHandling(ctx, tx, authority.handling.ID())
	if err != nil {
		return peerInboxSemanticHandlingInvariant("settled Handling cannot be reconstructed: %v", err)
	}
	return validatePeerInboxSemanticTerminalHandling(ctx, tx, authority, settlement)
}

// validatePeerInboxSemanticHandlingSettlement is the read-only half used by
// terminal Peer Inbox replay. It accepts only fully settled targets and never
// turns pending work into terminal work.
func validatePeerInboxSemanticHandlingSettlement(ctx context.Context, tx *sql.Tx,
	settlement PeerInboxSemanticHandlingSettlement,
) error {
	if ctx == nil || tx == nil {
		return fmt.Errorf("%w: nil context or transaction", ErrPeerInboxSemanticInput)
	}
	authority, err := readPeerInboxSemanticHandlingAuthority(ctx, tx, settlement, false)
	if err != nil {
		return err
	}
	if !authority.handling.Status().Terminal() {
		return peerInboxSemanticHandlingInvariant("terminal replay found an unsettled Handling")
	}
	return validatePeerInboxSemanticTerminalHandling(ctx, tx, authority, settlement)
}

func readPeerInboxSemanticHandlingAuthority(ctx context.Context, tx *sql.Tx,
	settlement PeerInboxSemanticHandlingSettlement, requireCurrentProfile bool,
) (peerInboxSemanticHandlingAuthority, error) {
	if settlement.WorkRef().IsZero() || settlement.SourceEventID().IsZero() ||
		!settlement.Disposition().Valid() {
		return peerInboxSemanticHandlingAuthority{}, fmt.Errorf(
			"%w: incomplete or open Handling settlement", ErrPeerInboxSemanticInput)
	}
	source, err := readCurrentSourceEvent(ctx, tx, settlement.SourceEventID())
	if err != nil {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"source Event is unavailable: %v", err)
	}
	if source.Source() != model.EventSourceImported ||
		source.Scope().WorkRef() != settlement.WorkRef() ||
		source.Scope().OriginPeerID() != settlement.WorkRef().HomePeerID() ||
		!peerInboxSemanticActionableSourceType(source.Type()) {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"source Event does not identify an imported actionable Work update")
	}
	work, err := readReviewWork(ctx, tx, settlement.WorkRef())
	if err != nil {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"source Work is unavailable: %v", err)
	}
	node, err := readNode(ctx, tx)
	if err != nil {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"local Node is unavailable: %v", err)
	}
	participants := work.Participants()
	if work.ChannelID() != source.Scope().ChannelID() ||
		participants.InitiatorPeerID() != settlement.WorkRef().HomePeerID() ||
		participants.ReviewerPeerID() != node.PeerID() || source.Audience().Len() != 1 ||
		!source.Audience().Contains(node.PeerID()) ||
		!peerInboxSemanticSettlementMatchesWork(ctx, tx, source, work,
			string(settlement.Disposition())) {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"source Event, Work, participant, or terminal disposition differs")
	}
	var profileRuntime model.RuntimeKind
	if requireCurrentProfile {
		profile, err := readProfile(ctx, tx)
		if err != nil || profile.ID() != model.TeamworkProfileID() {
			return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
				"teamwork-default Profile is unavailable")
		}
		profileRuntime = profile.Runtime()
	}
	handling, err := readExactPeerInboxSemanticHandling(ctx, tx, source.ID())
	if err != nil {
		return peerInboxSemanticHandlingAuthority{}, err
	}
	if handling.Priority() != 0 || !handling.AvailableAt().Equal(source.AcceptedAt()) ||
		!handling.CreatedAt().Equal(source.AcceptedAt()) {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"source Handling immutable projection differs")
	}
	if err := requirePeerInboxSemanticHandlingPins(ctx, tx, handling, source, false); err != nil {
		return peerInboxSemanticHandlingAuthority{}, peerInboxSemanticHandlingInvariant(
			"source Handling Artifact pins differ: %v", err)
	}
	return peerInboxSemanticHandlingAuthority{source: source, work: work,
		profileRuntime: profileRuntime, requireRuntime: requireCurrentProfile,
		handling: handling}, nil
}

func readExactPeerInboxSemanticHandling(ctx context.Context, tx *sql.Tx,
	eventID model.EventID,
) (model.Handling, error) {
	rows, err := tx.QueryContext(ctx, `SELECT handling_id FROM agent_handlings
		WHERE profile_id=? AND event_id=? ORDER BY handling_id LIMIT 2`,
		model.TeamworkProfileID().String(), eventID.String())
	if err != nil {
		return model.Handling{}, peerInboxSemanticHandlingInvariant("query exact Handling: %v", err)
	}
	defer rows.Close()
	ids := make([]model.HandlingID, 0, 2)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return model.Handling{}, peerInboxSemanticHandlingInvariant("scan exact Handling: %v", err)
		}
		id, err := model.ParseHandlingID(text)
		if err != nil {
			return model.Handling{}, peerInboxSemanticHandlingInvariant("invalid Handling identity: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return model.Handling{}, peerInboxSemanticHandlingInvariant("iterate exact Handling: %v", err)
	}
	if len(ids) != 1 {
		return model.Handling{}, peerInboxSemanticHandlingInvariant(
			"source Event resolves to %d Handlings, want exactly one", len(ids))
	}
	wantID, err := peerInboxSemanticHandlingID(eventID)
	if err != nil || ids[0] != wantID {
		return model.Handling{}, peerInboxSemanticHandlingInvariant(
			"source Event Handling identity is not deterministic")
	}
	handling, err := readAgentHandling(ctx, tx, ids[0])
	if err != nil || handling.ProfileID() != model.TeamworkProfileID() ||
		handling.EventID() != eventID {
		return model.Handling{}, peerInboxSemanticHandlingInvariant(
			"exact Handling cannot be reconstructed")
	}
	return handling, nil
}

func peerInboxSemanticSettlementMatchesWork(ctx context.Context, tx *sql.Tx,
	source model.Event, work model.ReviewWork, disposition string,
) bool {
	if work.UpdatedBy() == source.ID() {
		switch source.Type() {
		case model.EventReviewOffered:
			return work.State() == model.WorkOffered
		case model.EventReviewAccepted:
			return work.State() == model.WorkActive
		case model.EventReviewReworkRequested:
			return work.State() == model.WorkRework
		default:
			return false
		}
	}
	wantType, wantState := model.EventReviewCancelled, model.WorkCancelled
	if disposition == "superseded_expired" {
		wantType, wantState = model.EventReviewExpired, model.WorkExpired
	}
	if work.State() != wantState {
		return false
	}
	terminal, err := readCurrentSourceEvent(ctx, tx, work.UpdatedBy())
	if err != nil || terminal.Source() != model.EventSourceImported ||
		terminal.Type() != wantType || terminal.Scope().WorkRef() != work.Ref() ||
		terminal.Scope().OriginPeerID() != work.Ref().HomePeerID() {
		return false
	}
	causes := terminal.CausedBy()
	return len(causes) == 1 && causes[0] == source.Key()
}

func updatePeerInboxSemanticPendingHandling(ctx context.Context, tx *sql.Tx,
	handling model.Handling, disposition string, at time.Time,
) error {
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status='completed',
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition=?,
		outcome_event_id=NULL,last_error=NULL,dead_at=NULL,updated_at=?
		WHERE handling_id=? AND profile_id=? AND event_id=? AND status='pending'
		AND available_at=? AND claim_owner IS NULL AND claim_token_hash IS NULL
		AND lease_until IS NULL AND attempts=? AND recovery_count=?
		AND last_disposition IS ? AND outcome_event_id IS NULL AND last_error IS ?
		AND dead_at IS NULL AND created_at=? AND updated_at=?`, disposition, storeTime(at),
		handling.ID().String(), handling.ProfileID().String(), handling.EventID().String(),
		storeTime(handling.AvailableAt()), handling.Attempts(), handling.RecoveryCount(),
		nullText(handling.LastDisposition()), nullText(handling.LastError()),
		storeTime(handling.CreatedAt()), storeTime(handling.UpdatedAt()))
	if err != nil {
		return peerInboxSemanticHandlingInvariant("complete pending Handling: %v", err)
	}
	if err := requireExactlyOneRow(result, "complete pending semantic Handling fence"); err != nil {
		return peerInboxSemanticHandlingInvariant("pending Handling owner fence: %v", err)
	}
	return nil
}

func settlePeerInboxSemanticClaimedHandling(ctx context.Context, tx *sql.Tx,
	authority peerInboxSemanticHandlingAuthority,
	settlement PeerInboxSemanticHandlingSettlement, at time.Time,
) error {
	handling := authority.handling
	run, err := readExactPeerInboxSemanticHandlingRun(ctx, tx, handling)
	if err != nil {
		return err
	}
	if err := validatePeerInboxSemanticClaimedRun(authority, run, at); err != nil {
		return err
	}
	operations, err := readPeerInboxSemanticHandlingRunOperations(ctx, tx, run)
	if err != nil {
		return err
	}
	if _, read := run.CurrentReadReceipt(); len(operations) != 0 && !read {
		return peerInboxSemanticHandlingInvariant("Operation exists before exact current-read evidence")
	}
	started := make([]model.Operation, 0, 1)
	for _, operation := range operations {
		if err := validatePeerInboxSemanticOperationBinding(operation, run); err != nil {
			return err
		}
		if operation.CreatedAt().After(at) {
			return peerInboxSemanticHandlingInvariant("trusted time precedes an exact Operation")
		}
		if operation.Status() == model.OperationStarted {
			started = append(started, operation)
			continue
		}
		finished, ok := operation.FinishedAt()
		if !ok || finished.After(at) {
			return peerInboxSemanticHandlingInvariant("terminal Operation time exceeds settlement")
		}
	}
	if len(started) > 1 {
		return peerInboxSemanticHandlingInvariant(
			"exact claimed Run has %d started Operations", len(started))
	}
	var supersededOperationID model.OperationID
	for _, operation := range started {
		receipt, err := peerInboxSemanticOperationRejection(settlement, operation)
		if err != nil {
			return err
		}
		if err := rejectPeerInboxSemanticHandlingOperation(ctx, tx, operation, receipt, at); err != nil {
			return err
		}
		supersededOperationID = operation.ID()
	}
	outcome, err := peerInboxSemanticHandlingOutcomeReceipt(settlement, handling, run, supersededOperationID, at)
	if err != nil {
		return err
	}
	if err := finishPeerInboxSemanticHandlingRun(ctx, tx, run, outcome, at); err != nil {
		return err
	}
	if err := completePeerInboxSemanticClaimedHandling(ctx, tx, handling,
		string(settlement.Disposition()), at); err != nil {
		return err
	}
	return nil
}

func validatePeerInboxSemanticClaimedRun(authority peerInboxSemanticHandlingAuthority,
	run model.AgentRun, at time.Time,
) error {
	handling := authority.handling
	handlingID, hasHandling := run.HandlingID()
	runFence, hasFence := run.ClaimFenceHash()
	handlingFence, hasHandlingFence := handling.ClaimTokenHash()
	runLease, hasRunLease := run.LeaseUntil()
	handlingLease, hasHandlingLease := handling.LeaseUntil()
	_, hasOutcome := run.OutcomeReceipt()
	if !hasHandling || handlingID != handling.ID() || run.ProfileID() != handling.ProfileID() ||
		(authority.requireRuntime && run.Runtime() != authority.profileRuntime) ||
		!run.Status().OperationAuthority() ||
		run.HandlingAttempt() != handling.Attempts() ||
		run.HandlingRecovery() != handling.RecoveryCount() || !hasFence ||
		!hasHandlingFence || runFence != handlingFence || !hasRunLease ||
		!hasHandlingLease || !runLease.Equal(handlingLease) || hasOutcome {
		return peerInboxSemanticHandlingInvariant("claimed AgentRun differs from the Handling owner fence")
	}
	if err := validatePeerInboxSemanticHandlingRunCreation(authority, run); err != nil {
		return err
	}
	if at.Before(run.StartedAt()) {
		return peerInboxSemanticHandlingInvariant("trusted time precedes claimed AgentRun")
	}
	if value, ok := run.RuntimeStartedAt(); ok && value.After(at) {
		return peerInboxSemanticHandlingInvariant("trusted time precedes Runtime start")
	}
	if value, ok := run.WakeDeliveredAt(); ok && value.After(at) {
		return peerInboxSemanticHandlingInvariant("trusted time precedes wake delivery")
	}
	if value, ok := run.FinishedAt(); ok && value.After(at) {
		return peerInboxSemanticHandlingInvariant("trusted time precedes Runtime finish")
	}
	if raw, ok := run.CurrentReadReceipt(); ok {
		current, err := model.ParseCurrentReadReceipt(raw.Bytes())
		if err != nil || requireCurrentReceiptBinding(current, run, handling) != nil ||
			current.SourceEvent() != authority.source.Key() ||
			current.ActionWork() != authority.work.Ref() || current.ReadAt().After(at) {
			return peerInboxSemanticHandlingInvariant("current-read evidence exceeds settlement time")
		}
	} else if handling.LastDisposition() != "claimed" {
		return peerInboxSemanticHandlingInvariant("unread claimed Handling disposition differs")
	}
	return nil
}

func rejectPeerInboxSemanticHandlingOperation(ctx context.Context, tx *sql.Tx,
	operation model.Operation, receipt model.JSON, at time.Time,
) error {
	contextHash, _ := operation.ContextHash()
	lease, _ := operation.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected',
		lease_owner=NULL,lease_until=NULL,result_json=?,finished_at=?
		WHERE operation_id=? AND profile_id=? AND agent_run_id=?
		AND client_key_hash=? AND context_hash=? AND kind=? AND request_digest=?
		AND status='started' AND lease_owner=? AND lease_until=? AND result_json IS NULL
		AND finished_at IS NULL AND created_at=?`, receipt.Bytes(), storeTime(at),
		operation.ID().String(), operation.ProfileID().String(), operation.AgentRunID().String(),
		operation.ClientKeyHash().Bytes(), contextHash.Bytes(), string(operation.Kind()),
		operation.RequestDigest().Bytes(), operation.LeaseOwner(), storeTime(lease),
		storeTime(operation.CreatedAt()))
	if err != nil {
		return peerInboxSemanticHandlingInvariant("reject started Operation: %v", err)
	}
	if err := requireExactlyOneRow(result, "reject superseded Operation fence"); err != nil {
		return peerInboxSemanticHandlingInvariant("started Operation owner fence: %v", err)
	}
	return nil
}

func finishPeerInboxSemanticHandlingRun(ctx context.Context, tx *sql.Tx,
	run model.AgentRun, receipt model.JSON, at time.Time,
) error {
	handlingID, _ := run.HandlingID()
	fence, _ := run.ClaimFenceHash()
	lease, _ := run.LeaseUntil()
	finishedAt := any(nil)
	if value, ok := run.FinishedAt(); ok {
		finishedAt = storeTime(value)
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status='outcome_accepted',
		finished_at=COALESCE(finished_at,?),outcome_receipt_json=?,error=NULL
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
		AND runtime_kind=? AND status=? AND started_at=? AND finished_at IS ?
		AND outcome_receipt_json IS NULL`, storeTime(at), receipt.Bytes(), run.ID().String(),
		run.ProfileID().String(), handlingID.String(), run.HandlingAttempt(),
		run.HandlingRecovery(), fence.Bytes(), storeTime(lease), string(run.Runtime()),
		string(run.Status()), storeTime(run.StartedAt()), finishedAt)
	if err != nil {
		return peerInboxSemanticHandlingInvariant("finish claimed AgentRun: %v", err)
	}
	if err := requireExactlyOneRow(result, "finish superseded AgentRun fence"); err != nil {
		return peerInboxSemanticHandlingInvariant("claimed AgentRun owner fence: %v", err)
	}
	return nil
}

func completePeerInboxSemanticClaimedHandling(ctx context.Context, tx *sql.Tx,
	handling model.Handling, disposition string, at time.Time,
) error {
	fence, _ := handling.ClaimTokenHash()
	lease, _ := handling.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status='completed',
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition=?,
		outcome_event_id=NULL,last_error=NULL,dead_at=NULL,updated_at=?
		WHERE handling_id=? AND profile_id=? AND event_id=? AND status='claimed'
		AND available_at=? AND claim_owner=? AND claim_token_hash=? AND lease_until=?
		AND attempts=? AND recovery_count=? AND last_disposition IS ?
		AND outcome_event_id IS NULL AND last_error IS ? AND dead_at IS NULL
		AND created_at=? AND updated_at=?`, disposition, storeTime(at), handling.ID().String(),
		handling.ProfileID().String(), handling.EventID().String(), storeTime(handling.AvailableAt()),
		handling.ClaimOwner(), fence.Bytes(), storeTime(lease), handling.Attempts(),
		handling.RecoveryCount(), nullText(handling.LastDisposition()), nullText(handling.LastError()),
		storeTime(handling.CreatedAt()), storeTime(handling.UpdatedAt()))
	if err != nil {
		return peerInboxSemanticHandlingInvariant("complete claimed Handling: %v", err)
	}
	if err := requireExactlyOneRow(result, "complete claimed semantic Handling fence"); err != nil {
		return peerInboxSemanticHandlingInvariant("claimed Handling owner fence: %v", err)
	}
	return nil
}

func validatePeerInboxSemanticUnclaimedHistory(ctx context.Context, tx *sql.Tx,
	authority peerInboxSemanticHandlingAuthority, at time.Time,
) error {
	handling := authority.handling
	if err := requireNoActivePeerInboxSemanticHandlingRun(ctx, tx, handling, model.RunID{}); err != nil {
		return err
	}
	run, found, err := readOptionalExactPeerInboxSemanticHandlingRun(ctx, tx, handling)
	if err != nil {
		return err
	}
	if handling.Attempts() == 0 {
		if found {
			return peerInboxSemanticHandlingInvariant("zero-attempt pending Handling has an exact Run")
		}
		return nil
	}
	if !found || !run.Status().Terminal() {
		return peerInboxSemanticHandlingInvariant("pending Handling lacks its terminal prior-attempt Run")
	}
	finished, ok := run.FinishedAt()
	if !ok || finished.After(at) || finished.After(handling.UpdatedAt()) {
		return peerInboxSemanticHandlingInvariant("pending Handling prior-attempt Run time differs")
	}
	if err := validatePeerInboxSemanticHistoricalRun(authority, run); err != nil {
		return err
	}
	return validatePeerInboxSemanticTerminalRunOperations(ctx, tx, run,
		&peerInboxSemanticOperationReceiptExpectation{notAfter: handling.UpdatedAt()})
}

func validatePeerInboxSemanticTerminalHandling(ctx context.Context, tx *sql.Tx,
	authority peerInboxSemanticHandlingAuthority,
	settlement PeerInboxSemanticHandlingSettlement,
) error {
	handling := authority.handling
	if !handling.Status().Terminal() {
		return peerInboxSemanticHandlingInvariant("Handling is not terminal")
	}
	if handling.ClaimOwner() != "" {
		return peerInboxSemanticHandlingInvariant("terminal Handling retained claim owner")
	}
	if _, ok := handling.ClaimTokenHash(); ok {
		return peerInboxSemanticHandlingInvariant("terminal Handling retained claim token")
	}
	if _, ok := handling.LeaseUntil(); ok {
		return peerInboxSemanticHandlingInvariant("terminal Handling retained claim lease")
	}
	if err := requireNoActivePeerInboxSemanticHandlingRun(ctx, tx, handling, model.RunID{}); err != nil {
		return err
	}
	if handling.LastDisposition() == string(settlement.Disposition()) {
		return validatePeerInboxSemanticSupersededTerminal(ctx, tx, authority, settlement)
	}
	if validPeerInboxSemanticHandlingDisposition(handling.LastDisposition()) {
		return peerInboxSemanticHandlingInvariant("terminal Handling has another supersede disposition")
	}
	return validatePeerInboxSemanticPriorTerminal(ctx, tx, authority)
}

func validatePeerInboxSemanticSupersededTerminal(ctx context.Context, tx *sql.Tx,
	authority peerInboxSemanticHandlingAuthority,
	settlement PeerInboxSemanticHandlingSettlement,
) error {
	handling := authority.handling
	if handling.Status() != model.HandlingCompleted || handling.LastError() != "" {
		return peerInboxSemanticHandlingInvariant("superseded Handling terminal shape differs")
	}
	if _, ok := handling.OutcomeEventID(); ok {
		return peerInboxSemanticHandlingInvariant("superseded Handling fabricated an outcome Event")
	}
	if _, ok := handling.DeadAt(); ok {
		return peerInboxSemanticHandlingInvariant("superseded Handling retained death evidence")
	}
	run, found, err := readOptionalExactPeerInboxSemanticHandlingRun(ctx, tx, handling)
	if err != nil {
		return err
	}
	if handling.Attempts() == 0 {
		if found {
			return peerInboxSemanticHandlingInvariant("zero-attempt supersede has an exact AgentRun")
		}
		return nil
	}
	if !found || !run.Status().Terminal() {
		return peerInboxSemanticHandlingInvariant("superseded Handling lacks a terminal exact AgentRun")
	}
	if err := validatePeerInboxSemanticHistoricalRun(authority, run); err != nil {
		return err
	}
	finished, ok := run.FinishedAt()
	if !ok || finished.After(handling.UpdatedAt()) {
		return peerInboxSemanticHandlingInvariant("superseded AgentRun finish exceeds Handling settlement")
	}
	outcome, hasOutcome := run.OutcomeReceipt()
	if hasOutcome {
		operationID, err := peerInboxSemanticHandlingOutcomeOperationID(outcome)
		if err != nil {
			return err
		}
		expected, err := peerInboxSemanticHandlingOutcomeReceipt(settlement, handling, run, operationID, handling.UpdatedAt())
		if err != nil {
			return err
		}
		if run.Status() != model.AgentRunOutcomeAccepted || outcome.String() != expected.String() {
			return peerInboxSemanticHandlingInvariant("superseded AgentRun outcome receipt differs")
		}
		return validatePeerInboxSemanticTerminalRunOperations(ctx, tx, run,
			&peerInboxSemanticOperationReceiptExpectation{settlement: settlement,
				operationID: operationID, settledAt: handling.UpdatedAt(), notAfter: handling.UpdatedAt()})
	}
	// A prior attempt keeps its earlier terminal evidence when the remote terminal arrives.
	return validatePeerInboxSemanticTerminalRunOperations(ctx, tx, run,
		&peerInboxSemanticOperationReceiptExpectation{notAfter: handling.UpdatedAt()})
}

func validatePeerInboxSemanticPriorTerminal(ctx context.Context, tx *sql.Tx,
	authority peerInboxSemanticHandlingAuthority,
) error {
	handling := authority.handling
	if handling.Status() != model.HandlingDead && handling.LastError() != "" {
		return peerInboxSemanticHandlingInvariant("non-dead terminal Handling retained an error")
	}
	run, found, err := readOptionalExactPeerInboxSemanticHandlingRun(ctx, tx, handling)
	if err != nil {
		return err
	}
	if handling.Attempts() == 0 {
		if found || handling.Status() != model.HandlingCompleted ||
			handling.LastDisposition() != parentStaleDisposition {
			return peerInboxSemanticHandlingInvariant("zero-attempt terminal Handling is not parent_stale")
		}
		return nil
	}
	if !found || !run.Status().Terminal() {
		return peerInboxSemanticHandlingInvariant("terminal Handling lacks its exact terminal AgentRun")
	}
	if err := validatePeerInboxSemanticHistoricalRun(authority, run); err != nil {
		return err
	}
	finished, ok := run.FinishedAt()
	if !ok || finished.After(handling.UpdatedAt()) {
		return peerInboxSemanticHandlingInvariant("terminal AgentRun finish exceeds Handling update")
	}
	var requireOutcome bool
	switch handling.Status() {
	case model.HandlingCompleted:
		if handling.LastDisposition() != "teamwork_action" && handling.LastDisposition() != "no_action" {
			return peerInboxSemanticHandlingInvariant("completed Handling has an unknown disposition")
		}
		if run.Status() != model.AgentRunOutcomeAccepted {
			return peerInboxSemanticHandlingInvariant("completed Handling Run status differs")
		}
		requireOutcome = true
	case model.HandlingRejected:
		if handling.LastDisposition() != "reject" || run.Status() != model.AgentRunRejected {
			return peerInboxSemanticHandlingInvariant("rejected Handling lifecycle differs")
		}
		requireOutcome = true
	case model.HandlingDead:
		if handling.LastDisposition() != "attempt_budget_exhausted" || handling.LastError() == "" {
			return peerInboxSemanticHandlingInvariant("dead Handling lifecycle differs")
		}
		if run.Status() != model.AgentRunDead && run.Status() != model.AgentRunRequeued &&
			run.Status() != model.AgentRunFailed {
			return peerInboxSemanticHandlingInvariant("dead Handling Run status differs")
		}
	default:
		return peerInboxSemanticHandlingInvariant("unknown prior terminal Handling")
	}
	outcome, hasOutcome := run.OutcomeReceipt()
	if requireOutcome && !hasOutcome {
		return peerInboxSemanticHandlingInvariant("prior terminal AgentRun lacks an outcome receipt")
	}
	if handling.LastDisposition() == "teamwork_action" {
		outcomeEventID, ok := handling.OutcomeEventID()
		if !ok {
			return peerInboxSemanticHandlingInvariant("teamwork action Handling lacks outcome Event")
		}
		outcomeEvent, err := readCurrentSourceEvent(ctx, tx, outcomeEventID)
		causes := outcomeEvent.CausedBy()
		if err != nil || outcomeEvent.Source() != model.EventSourceLocal ||
			outcomeEvent.Scope().WorkRef() != authority.work.Ref() || len(causes) != 1 ||
			causes[0] != authority.source.Key() {
			return peerInboxSemanticHandlingInvariant("teamwork action outcome Event differs")
		}
	} else if _, ok := handling.OutcomeEventID(); ok {
		return peerInboxSemanticHandlingInvariant("non-action terminal Handling has outcome Event")
	}
	var winning *model.JSON
	if hasOutcome {
		winning = &outcome
	}
	return validatePeerInboxSemanticTerminalRunOperations(ctx, tx, run,
		&peerInboxSemanticOperationReceiptExpectation{winningOutcome: winning,
			notAfter: handling.UpdatedAt()})
}

func validatePeerInboxSemanticHistoricalRun(authority peerInboxSemanticHandlingAuthority,
	run model.AgentRun,
) error {
	handlingID, bound := run.HandlingID()
	if !bound || handlingID != authority.handling.ID() ||
		run.ProfileID() != authority.handling.ProfileID() || !run.Runtime().Valid() ||
		run.HandlingAttempt() != authority.handling.Attempts() ||
		run.HandlingRecovery() != authority.handling.RecoveryCount() {
		return peerInboxSemanticHandlingInvariant("historical exact AgentRun binding drifted")
	}
	return validatePeerInboxSemanticHandlingRunCreation(authority, run)
}

func validatePeerInboxSemanticHandlingRunCreation(authority peerInboxSemanticHandlingAuthority,
	run model.AgentRun,
) error {
	kind := ""
	switch run.Launcher() {
	case "external":
		kind = "external_current"
	case "mnemond-wake":
		kind = "mnemond_wake"
	default:
		return peerInboxSemanticHandlingInvariant("exact AgentRun launcher is not a closed Handling launcher")
	}
	wantCause, err := model.JSONFrom(struct {
		Kind       string           `json:"kind"`
		HandlingID model.HandlingID `json:"handling_id"`
		EventID    model.EventID    `json:"event_id"`
	}{kind, authority.handling.ID(), authority.handling.EventID()})
	if err != nil {
		return peerInboxSemanticHandlingInvariant("construct exact AgentRun cause: %v", err)
	}
	if run.Cause().String() != wantCause.String() ||
		run.StartedAt().Before(authority.handling.CreatedAt()) ||
		run.StartedAt().After(authority.handling.UpdatedAt()) {
		return peerInboxSemanticHandlingInvariant("exact AgentRun creation identity drifted")
	}
	return nil
}

func validatePeerInboxSemanticTerminalRunOperations(ctx context.Context, tx *sql.Tx, run model.AgentRun,
	expectation *peerInboxSemanticOperationReceiptExpectation) error {
	operations, err := readPeerInboxSemanticHandlingRunOperations(ctx, tx, run)
	if err != nil {
		return err
	}
	supersededReceipt, winningReceipts := false, 0
	for _, operation := range operations {
		if err := validatePeerInboxSemanticOperationBinding(operation, run); err != nil {
			return err
		}
		if operation.Status() == model.OperationStarted {
			return peerInboxSemanticHandlingInvariant("terminal AgentRun retained a started Operation")
		}
		result, ok := operation.Result()
		finished, finishedOK := operation.FinishedAt()
		if !ok || !finishedOK {
			return peerInboxSemanticHandlingInvariant("terminal Operation lost receipt evidence")
		}
		if expectation != nil && !expectation.notAfter.IsZero() &&
			finished.After(expectation.notAfter) {
			return peerInboxSemanticHandlingInvariant("terminal Operation finish exceeds lifecycle settlement")
		}
		if expectation != nil && expectation.winningOutcome != nil &&
			result.String() == expectation.winningOutcome.String() {
			if operation.Status() != model.OperationCommitted {
				return peerInboxSemanticHandlingInvariant("winning outcome belongs to rejected Operation")
			}
			winningReceipts++
		}
		superseded, err := validatePeerInboxSemanticOperationReceipt(
			operation, result, finished, expectation)
		if err != nil {
			return err
		}
		supersededReceipt = supersededReceipt || superseded
	}
	if supersededReceipt != (expectation != nil && !expectation.operationID.IsZero()) {
		return peerInboxSemanticHandlingInvariant("superseded outcome lacks its rejected Operation")
	}
	if expectation != nil && expectation.winningOutcome != nil && winningReceipts != 1 {
		return peerInboxSemanticHandlingInvariant(
			"AgentRun outcome resolves to %d committed Operations", winningReceipts)
	}
	return nil
}

func readExactPeerInboxSemanticHandlingRun(ctx context.Context, tx *sql.Tx,
	handling model.Handling) (model.AgentRun, error) {
	run, found, err := readOptionalExactPeerInboxSemanticHandlingRun(ctx, tx, handling)
	if err != nil {
		return model.AgentRun{}, err
	}
	if !found {
		return model.AgentRun{}, peerInboxSemanticHandlingInvariant("claimed Handling has no exact AgentRun")
	}
	if err := requireNoActivePeerInboxSemanticHandlingRun(ctx, tx, handling, run.ID()); err != nil {
		return model.AgentRun{}, err
	}
	return run, nil
}

func readOptionalExactPeerInboxSemanticHandlingRun(ctx context.Context, tx *sql.Tx,
	handling model.Handling) (model.AgentRun, bool, error) {
	if handling.Attempts() == 0 {
		var present int
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_runs
			WHERE handling_id=? AND handling_recovery=? AND handling_attempt IS NOT NULL)`,
			handling.ID().String(), handling.RecoveryCount()).Scan(&present); err != nil {
			return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("inspect zero-attempt Runs: %v", err)
		}
		if present == 1 {
			return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant(
				"zero-attempt Handling has a Run in its current recovery generation")
		}
		return model.AgentRun{}, false, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT run_id FROM agent_runs WHERE handling_id=?
		AND handling_recovery=? AND handling_attempt=? ORDER BY run_id LIMIT 2`,
		handling.ID().String(), handling.RecoveryCount(), handling.Attempts())
	if err != nil {
		return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("query exact AgentRun: %v", err)
	}
	defer rows.Close()
	ids := make([]model.RunID, 0, 2)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("scan exact AgentRun: %v", err)
		}
		id, err := model.ParseRunID(text)
		if err != nil {
			return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("invalid exact AgentRun ID: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("iterate exact AgentRun: %v", err)
	}
	if len(ids) > 1 {
		return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("multiple exact AgentRuns")
	}
	if len(ids) == 0 {
		return model.AgentRun{}, false, nil
	}
	run, err := readAgentRun(ctx, tx, ids[0])
	if err != nil {
		return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("reconstruct exact AgentRun: %v", err)
	}
	handlingID, bound := run.HandlingID()
	if !bound || handlingID != handling.ID() || run.ProfileID() != handling.ProfileID() ||
		run.HandlingAttempt() != handling.Attempts() ||
		run.HandlingRecovery() != handling.RecoveryCount() {
		return model.AgentRun{}, false, peerInboxSemanticHandlingInvariant("exact AgentRun binding differs")
	}
	return run, true, nil
}

func requireNoActivePeerInboxSemanticHandlingRun(ctx context.Context, tx *sql.Tx,
	handling model.Handling, allowed model.RunID,
) error {
	rows, err := tx.QueryContext(ctx, `SELECT run_id FROM agent_runs WHERE handling_id=?
		AND status IN ('starting','running','runtime_finished') ORDER BY run_id LIMIT 2`,
		handling.ID().String())
	if err != nil {
		return peerInboxSemanticHandlingInvariant("query active Handling Runs: %v", err)
	}
	defer rows.Close()
	ids := make([]model.RunID, 0, 2)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			return peerInboxSemanticHandlingInvariant("scan active Handling Run: %v", err)
		}
		id, err := model.ParseRunID(text)
		if err != nil {
			return peerInboxSemanticHandlingInvariant("invalid active Handling Run ID: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return peerInboxSemanticHandlingInvariant("iterate active Handling Runs: %v", err)
	}
	if allowed.IsZero() {
		if len(ids) != 0 {
			return peerInboxSemanticHandlingInvariant("unclaimed Handling retains an active AgentRun")
		}
		return nil
	}
	if len(ids) != 1 || ids[0] != allowed {
		return peerInboxSemanticHandlingInvariant("claimed Handling has multiple or drifted active AgentRuns")
	}
	return nil
}

func readPeerInboxSemanticHandlingRunOperations(ctx context.Context, tx *sql.Tx,
	run model.AgentRun) ([]model.Operation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT operation_id FROM operations
		WHERE agent_run_id=? AND profile_id=? ORDER BY operation_id`,
		run.ID().String(), run.ProfileID().String())
	if err != nil {
		return nil, peerInboxSemanticHandlingInvariant("query exact Run Operations: %v", err)
	}
	ids := make([]model.OperationID, 0)
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			_ = rows.Close()
			return nil, peerInboxSemanticHandlingInvariant("scan exact Run Operation: %v", err)
		}
		id, err := model.ParseOperationID(text)
		if err != nil {
			_ = rows.Close()
			return nil, peerInboxSemanticHandlingInvariant("invalid exact Run Operation ID: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, peerInboxSemanticHandlingInvariant("iterate exact Run Operations: %v", err)
	}
	if err := rows.Close(); err != nil {
		return nil, peerInboxSemanticHandlingInvariant("close exact Run Operations: %v", err)
	}
	operations := make([]model.Operation, 0, len(ids))
	for _, id := range ids {
		operation, err := readOperationByID(ctx, tx, id)
		if err != nil {
			return nil, peerInboxSemanticHandlingInvariant("reconstruct exact Run Operation: %v", err)
		}
		operations = append(operations, operation)
	}
	return operations, nil
}

func validatePeerInboxSemanticOperationBinding(operation model.Operation, run model.AgentRun) error {
	contextHash, hasContext := operation.ContextHash()
	fence, hasFence := run.ClaimFenceHash()
	if operation.AgentRunID() != run.ID() || operation.ProfileID() != run.ProfileID() ||
		!hasContext || !hasFence || contextHash != fence {
		return peerInboxSemanticHandlingInvariant("Operation binding drifted from exact AgentRun")
	}
	return nil
}

func validPeerInboxSemanticHandlingDisposition(value string) bool {
	return value == "superseded_cancelled" || value == "superseded_expired"
}

func peerInboxSemanticActionableSourceType(value model.EventType) bool {
	return value == model.EventReviewOffered || value == model.EventReviewAccepted ||
		value == model.EventReviewReworkRequested
}

func peerInboxSemanticHandlingInvariant(format string, arguments ...any) error {
	return fmt.Errorf("%w: Handling settlement: %s", ErrPeerInboxSemanticInvariant,
		fmt.Sprintf(format, arguments...))
}
