package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrManagedResolutionInput     = errors.New("invalid managed resolution input")
	ErrManagedResolutionStale     = errors.New("managed resolution current evidence is stale")
	ErrManagedResolutionInvariant = errors.New("managed resolution durable invariant violated")
)

// ManagedResolutionSpec deliberately carries the reservation returned by
// ReserveManagedOperation rather than caller-selected Handling, Run, attempt,
// Event, or Work authority. CommitManagedResolution ignores the reservation's
// projected Run/Handling values and re-derives all authority from the durable
// Operation and its context hash.
type ManagedResolutionSpec struct {
	Reservation ManagedOperationReservation
	Content     string
	At          time.Time
}

type ManagedResolutionResult struct {
	Operation model.Operation
	Receipt   model.JSON
	Replayed  bool
}

// CommitManagedResolution atomically commits one explicit no-action, retry or
// reject decision. A terminal operation returns its original receipt before
// consulting the now-finished claim, which makes response-loss and restart
// replay independent of expired context files.
func (s *Store) CommitManagedResolution(ctx context.Context,
	spec ManagedResolutionSpec,
) (ManagedResolutionResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return ManagedResolutionResult{}, fmt.Errorf("%w: nil store or context", ErrManagedResolutionInput)
	}
	supplied := spec.Reservation.Operation
	contextHash, hasContext := supplied.ContextHash()
	if supplied.ID().IsZero() || !hasContext || !managedResolutionKind(supplied.Kind()) {
		return ManagedResolutionResult{}, fmt.Errorf("%w: context-bound resolution reservation is required", ErrManagedResolutionInput)
	}
	if err := validateManagedResolutionContent(supplied.Kind(), spec.Content); err != nil {
		return ManagedResolutionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ManagedResolutionResult{}, fmt.Errorf("commit managed resolution: begin: %w", err)
	}
	defer tx.Rollback()

	operation, profile, err := readManagedResolutionBinding(ctx, tx, supplied, contextHash, spec.Content)
	if err != nil {
		return ManagedResolutionResult{}, err
	}
	if operation.Status().Terminal() {
		receipt, ok := operation.Result()
		if !ok {
			return ManagedResolutionResult{}, ErrOperationTerminal
		}
		if operation.Status() != model.OperationCommitted {
			return ManagedResolutionResult{}, ErrOperationTerminal
		}
		if err := validateManagedTerminalResolution(ctx, tx, operation, receipt); err != nil {
			return ManagedResolutionResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return ManagedResolutionResult{}, fmt.Errorf("commit managed resolution: replay read: %w", err)
		}
		return ManagedResolutionResult{Operation: operation, Receipt: receipt, Replayed: true}, nil
	}
	at, err := canonicalStoreTime(spec.At)
	if err != nil || at.IsZero() {
		return ManagedResolutionResult{}, fmt.Errorf("%w: invalid trusted time", ErrManagedResolutionInput)
	}
	if !spec.Reservation.Acquired || supplied.Status() != model.OperationStarted ||
		operation.Status() != model.OperationStarted {
		return ManagedResolutionResult{}, ErrOperationFence
	}
	if _, hasCapture := operation.Capture(); hasCapture {
		return ManagedResolutionResult{}, fmt.Errorf("%w: resolution operation has an Artifact checkpoint", ErrManagedResolutionInvariant)
	}
	suppliedLease, suppliedHasLease := supplied.LeaseUntil()
	durableLease, durableHasLease := operation.LeaseUntil()
	if !suppliedHasLease || !durableHasLease || supplied.LeaseOwner() != operation.LeaseOwner() ||
		!suppliedLease.Equal(durableLease) {
		return ManagedResolutionResult{}, ErrOperationFence
	}
	if err := requireOperationFence(operation, supplied.LeaseOwner(), at); err != nil {
		return ManagedResolutionResult{}, err
	}

	if err := requireActiveManagedProfile(ctx, tx, profile, at); err != nil {
		return ManagedResolutionResult{}, err
	}
	wantOperationID, err := managedOperationID(profile.ID(), operation.ClientKeyHash())
	if err != nil || wantOperationID != operation.ID() {
		return ManagedResolutionResult{}, fmt.Errorf("%w: Operation identity is not server-derived", ErrManagedResolutionInvariant)
	}

	run, handling, err := requireManagedClaimContext(ctx, tx, profile, contextHash, operation.Kind(), at)
	if err != nil {
		return ManagedResolutionResult{}, err
	}
	if run.ID() != operation.AgentRunID() {
		return ManagedResolutionResult{}, fmt.Errorf("%w: Operation belongs to another AgentRun", ErrOperationMismatch)
	}
	current, source, err := requireManagedResolutionCurrent(ctx, tx, run, handling, at)
	if err != nil {
		return ManagedResolutionResult{}, err
	}

	transition, err := managedResolutionTransitionFor(profile, handling, operation.Kind(), at)
	if err != nil {
		return ManagedResolutionResult{}, err
	}
	receipt, err := buildManagedResolutionReceipt(operation, current, source,
		spec.Content, transition, at)
	if err != nil {
		return ManagedResolutionResult{}, err
	}
	if err := updateManagedResolutionHandling(ctx, tx, handling, current, contextHash,
		transition, at); err != nil {
		return ManagedResolutionResult{}, err
	}
	if err := updateManagedResolutionRun(ctx, tx, run, handling, contextHash,
		transition, receipt, at); err != nil {
		return ManagedResolutionResult{}, err
	}
	if err := updateManagedResolutionOperation(ctx, tx, operation, receipt, at); err != nil {
		return ManagedResolutionResult{}, err
	}

	committed, err := operationTerminal(operation, model.OperationCommitted, receipt, at)
	if err != nil {
		return ManagedResolutionResult{}, fmt.Errorf("%w: committed Operation: %v", ErrManagedResolutionInvariant, err)
	}
	if err := verifyManagedResolutionRows(ctx, tx, committed, handling, run, transition, receipt); err != nil {
		return ManagedResolutionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ManagedResolutionResult{}, fmt.Errorf("commit managed resolution: commit: %w", err)
	}
	return ManagedResolutionResult{Operation: committed, Receipt: receipt}, nil
}

func validateManagedTerminalResolution(ctx context.Context, tx *sql.Tx, operation model.Operation,
	receipt model.JSON,
) error {
	run, err := readAgentRun(ctx, tx, operation.AgentRunID())
	if err != nil {
		return fmt.Errorf("%w: terminal resolution AgentRun is unavailable",
			ErrManagedResolutionInvariant)
	}
	handlingID, hasHandling := run.HandlingID()
	contextHash, hasContext := operation.ContextHash()
	if !hasHandling || !hasContext {
		return fmt.Errorf("%w: terminal resolution lost context binding",
			ErrManagedResolutionInvariant)
	}
	fence, hasFence := run.ClaimFenceHash()
	if !hasFence || fence != contextHash {
		return fmt.Errorf("%w: terminal resolution Run fence differs",
			ErrManagedResolutionInvariant)
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if err != nil || handling.ProfileID() != run.ProfileID() {
		return fmt.Errorf("%w: terminal resolution Handling is unavailable",
			ErrManagedResolutionInvariant)
	}
	outcome, hasOutcome := run.OutcomeReceipt()
	if !hasOutcome || outcome.String() != receipt.String() {
		return fmt.Errorf("%w: terminal resolution receipts differ",
			ErrManagedResolutionInvariant)
	}
	if operation.Kind() == model.OperationResolveRetry {
		if err := validateManagedRetryHistory(ctx, tx, operation, run, handling, receipt); err != nil {
			return err
		}
		return nil
	}
	want, err := managedResolutionTerminalShape(operation.Kind())
	if err != nil || handling.Attempts() != run.HandlingAttempt() ||
		handling.RecoveryCount() != run.HandlingRecovery() ||
		handling.Status() != want.handlingStatus ||
		handling.LastDisposition() != want.handlingDisposition || run.Status() != want.runStatus {
		return fmt.Errorf("%w: terminal resolution lifecycle differs",
			ErrManagedResolutionInvariant)
	}
	if _, claimed := handling.ClaimTokenHash(); claimed {
		return fmt.Errorf("%w: terminal resolution retained claim authority",
			ErrManagedResolutionInvariant)
	}
	return nil
}

func validateManagedRetryHistory(ctx context.Context, tx *sql.Tx, operation model.Operation,
	run model.AgentRun, handling model.Handling, receipt model.JSON,
) error {
	var envelope struct {
		Action   model.OperationKind `json:"action"`
		Handling struct {
			Status string `json:"status"`
		} `json:"handling"`
		OperationID string `json:"operation_id"`
		Status      string `json:"status"`
	}
	if err := json.Unmarshal(receipt.Bytes(), &envelope); err != nil ||
		envelope.Action != model.OperationResolveRetry || envelope.OperationID != operation.ID().String() ||
		envelope.Status != "resolved" {
		return fmt.Errorf("%w: terminal retry receipt is invalid", ErrManagedResolutionInvariant)
	}
	want := managedResolutionTransition{}
	switch envelope.Handling.Status {
	case "requeued":
		want = managedResolutionTransition{handlingStatus: model.HandlingPending,
			handlingDisposition: "retry", runStatus: model.AgentRunRequeued}
	case "dead":
		want = managedResolutionTransition{handlingStatus: model.HandlingDead,
			handlingDisposition: "attempt_budget_exhausted", runStatus: model.AgentRunDead}
	default:
		return fmt.Errorf("%w: terminal retry receipt has an unknown lifecycle",
			ErrManagedResolutionInvariant)
	}
	if run.Status() != want.runStatus || handling.RecoveryCount() < run.HandlingRecovery() ||
		(handling.RecoveryCount() == run.HandlingRecovery() &&
			handling.Attempts() < run.HandlingAttempt()) {
		return fmt.Errorf("%w: terminal retry lifecycle regressed",
			ErrManagedResolutionInvariant)
	}
	if handling.RecoveryCount() == run.HandlingRecovery() &&
		handling.Attempts() == run.HandlingAttempt() {
		// Lowering the active attempt budget may monotonically reseal an
		// already-requeued Handling without rewriting the historical Run or
		// its committed retry receipt. That successor state remains valid
		// replay evidence for the original operation.
		budgetResealed := envelope.Handling.Status == "requeued" &&
			handling.Status() == model.HandlingDead &&
			handling.LastDisposition() == "attempt_budget_exhausted" &&
			handling.LastError() == "maximum handling attempts exhausted"
		if budgetResealed {
			if _, claimed := handling.ClaimTokenHash(); claimed {
				return fmt.Errorf("%w: terminal retry retained claim authority",
					ErrManagedResolutionInvariant)
			}
			return nil
		}
		if handling.Status() != want.handlingStatus ||
			handling.LastDisposition() != want.handlingDisposition {
			return fmt.Errorf("%w: terminal retry lifecycle differs",
				ErrManagedResolutionInvariant)
		}
		if _, claimed := handling.ClaimTokenHash(); claimed {
			return fmt.Errorf("%w: terminal retry retained claim authority",
				ErrManagedResolutionInvariant)
		}
		return nil
	}
	if handling.Attempts() == 0 {
		if handling.Status() != model.HandlingPending ||
			handling.LastDisposition() != "setup_recovered" {
			return fmt.Errorf("%w: terminal retry recovery has no setup evidence",
				ErrManagedResolutionInvariant)
		}
		return nil
	}
	var successor int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_runs
		WHERE profile_id=? AND handling_id=? AND handling_recovery=? AND handling_attempt=?)`,
		handling.ProfileID().String(), handling.ID().String(), handling.RecoveryCount(),
		handling.Attempts()).Scan(&successor); err != nil || successor != 1 {
		return fmt.Errorf("%w: terminal retry successor Run is unavailable",
			ErrManagedResolutionInvariant)
	}
	return nil
}

func managedResolutionTerminalShape(kind model.OperationKind) (managedResolutionTransition, error) {
	switch kind {
	case model.OperationResolveNoAction:
		return managedResolutionTransition{handlingStatus: model.HandlingCompleted,
			handlingDisposition: "no_action", runStatus: model.AgentRunOutcomeAccepted}, nil
	case model.OperationResolveReject:
		return managedResolutionTransition{handlingStatus: model.HandlingRejected,
			handlingDisposition: "reject", runStatus: model.AgentRunRejected}, nil
	case model.OperationResolveRetry:
		return managedResolutionTransition{handlingStatus: model.HandlingPending,
			handlingDisposition: "retry", runStatus: model.AgentRunRequeued}, nil
	default:
		return managedResolutionTransition{}, ErrManagedResolutionInput
	}
}

func managedResolutionKind(kind model.OperationKind) bool {
	return kind == model.OperationResolveNoAction || kind == model.OperationResolveRetry ||
		kind == model.OperationResolveReject
}

func validateManagedResolutionContent(kind model.OperationKind, content string) error {
	if kind == model.OperationResolveReject && strings.TrimSpace(content) == "" {
		return fmt.Errorf("%w: reject requires a reason", ErrManagedResolutionInput)
	}
	if !utf8.ValidString(content) || len(content) > model.MaxContentBytes {
		return fmt.Errorf("%w: content must be valid UTF-8 within %d bytes",
			ErrManagedResolutionInput, model.MaxContentBytes)
	}
	for _, character := range content {
		if character == 0 || (character < 0x20 && character != '\n' && character != '\t') {
			return fmt.Errorf("%w: content has a forbidden control character", ErrManagedResolutionInput)
		}
	}
	return nil
}

func sameManagedResolutionOperation(durable, supplied model.Operation) bool {
	durableContext, durableHasContext := durable.ContextHash()
	suppliedContext, suppliedHasContext := supplied.ContextHash()
	return durable.ID() == supplied.ID() && durable.ProfileID() == supplied.ProfileID() &&
		durable.AgentRunID() == supplied.AgentRunID() && durable.ClientKeyHash() == supplied.ClientKeyHash() &&
		durable.Kind() == supplied.Kind() && durable.RequestDigest() == supplied.RequestDigest() &&
		durableHasContext && suppliedHasContext && durableContext == suppliedContext
}

func requireManagedResolutionCurrent(ctx context.Context, tx *sql.Tx, run model.AgentRun,
	handling model.Handling, at time.Time,
) (model.CurrentReadReceipt, model.Event, error) {
	stored, ok := run.CurrentReadReceipt()
	if !ok {
		return model.CurrentReadReceipt{}, model.Event{}, ErrManagedContextStale
	}
	current, err := model.ParseCurrentReadReceipt(stored.Bytes())
	if err != nil {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: current receipt is invalid", ErrManagedResolutionInvariant)
	}
	if err := requireCurrentReceiptBinding(current, run, handling); err != nil {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: current receipt no longer binds the claim", ErrManagedResolutionStale)
	}
	source, err := readCurrentSourceEvent(ctx, tx, handling.EventID())
	if err != nil {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: source Event: %v", ErrManagedResolutionInvariant, err)
	}
	projectedEvent := current.Projection().SourceEvent()
	if projectedEvent.Key() != source.Key() || projectedEvent.Digest() != source.Digest() ||
		projectedEvent.Type() != source.Type() || projectedEvent.WorkRef() != source.Scope().WorkRef() ||
		projectedEvent.Summary() != source.Summary() || projectedEvent.Payload().String() != source.Payload().String() ||
		!projectedEvent.AcceptedAt().Equal(source.AcceptedAt()) ||
		!sameManagedResolutionEventArtifacts(projectedEvent.ArtifactRefs(), source.Artifacts()) {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: source Event differs from current", ErrManagedResolutionStale)
	}
	work, err := readReviewWork(ctx, tx, current.ActionWork())
	if err != nil {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: action Work: %v", ErrManagedResolutionStale, err)
	}
	projectedWork := current.Projection().ActionWork()
	node, err := readNode(ctx, tx)
	if err != nil {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: Node: %v", ErrManagedResolutionInvariant, err)
	}
	role, err := localCurrentRole(node.PeerID(), work)
	if err != nil || work.Ref() != projectedWork.Ref() || work.Version() != projectedWork.Version() ||
		work.Iteration() != projectedWork.Iteration() || work.DeadlineUnixNano() != projectedWork.DeadlineUnixNano() ||
		work.State() != projectedWork.State() || work.StateData().String() != projectedWork.StateData().String() ||
		role != projectedWork.LocalRole() {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: action Work version or projection changed", ErrManagedResolutionStale)
	}
	if at.Before(current.ReadAt()) || at.Before(source.AcceptedAt()) || at.Before(work.UpdatedAt()) {
		return model.CurrentReadReceipt{}, model.Event{},
			fmt.Errorf("%w: trusted time precedes current evidence", ErrManagedResolutionInvariant)
	}
	return current, source, nil
}

func sameManagedResolutionEventArtifacts(current []model.CurrentArtifactRef,
	durable []model.ArtifactRef,
) bool {
	if len(current) != len(durable) {
		return false
	}
	for index := range current {
		if current[index].RootDigest() != durable[index].RootDigest() {
			return false
		}
	}
	return true
}

type managedResolutionTransition struct {
	handlingStatus      model.HandlingStatus
	handlingDisposition string
	runStatus           model.AgentRunStatus
	retryAt             time.Time
	hasRetryAt          bool
	deadAt              time.Time
	hasDeadAt           bool
	lastError           string
}

func managedResolutionTransitionFor(profile model.Profile, handling model.Handling,
	kind model.OperationKind, at time.Time,
) (managedResolutionTransition, error) {
	switch kind {
	case model.OperationResolveNoAction:
		return managedResolutionTransition{handlingStatus: model.HandlingCompleted,
			handlingDisposition: "no_action", runStatus: model.AgentRunOutcomeAccepted}, nil
	case model.OperationResolveReject:
		return managedResolutionTransition{handlingStatus: model.HandlingRejected,
			handlingDisposition: "reject", runStatus: model.AgentRunRejected}, nil
	case model.OperationResolveRetry:
		budget, err := model.ParseHandlingBudget(profile.HandlingBudget())
		if err != nil {
			return managedResolutionTransition{}, fmt.Errorf("%w: Profile retry budget: %v", ErrManagedResolutionInvariant, err)
		}
		if handling.Attempts() >= uint32(budget.Spec().MaxAttempts) {
			return managedResolutionTransition{handlingStatus: model.HandlingDead,
				handlingDisposition: "attempt_budget_exhausted", runStatus: model.AgentRunDead,
				deadAt: at, hasDeadAt: true,
				lastError: "explicit retry exhausted maximum handling attempts"}, nil
		}
		retryAt, err := agentClaimRetryAt(at, handling.Attempts(), budget)
		if err != nil {
			return managedResolutionTransition{}, fmt.Errorf("%w: retry backoff: %v", ErrManagedResolutionInvariant, err)
		}
		return managedResolutionTransition{handlingStatus: model.HandlingPending,
			handlingDisposition: "retry", runStatus: model.AgentRunRequeued,
			retryAt: retryAt, hasRetryAt: true}, nil
	default:
		return managedResolutionTransition{}, fmt.Errorf("%w: unknown resolution kind", ErrManagedResolutionInput)
	}
}

func buildManagedResolutionReceipt(operation model.Operation, current model.CurrentReadReceipt,
	source model.Event, content string, transition managedResolutionTransition,
	at time.Time,
) (model.JSON, error) {
	evidence, err := buildManagedResolutionEvidence(operation, current, source, content, transition, at)
	if err != nil {
		return model.JSON{}, err
	}
	emptyResults := make([]struct{}, 0)
	handlingReceiptStatus := ""
	switch transition.handlingStatus {
	case model.HandlingCompleted:
		handlingReceiptStatus = "completed"
	case model.HandlingPending:
		handlingReceiptStatus = "requeued"
	case model.HandlingRejected:
		handlingReceiptStatus = "rejected"
	case model.HandlingDead:
		handlingReceiptStatus = "dead"
	default:
		return model.JSON{}, fmt.Errorf("%w: unknown resolution receipt kind", ErrManagedResolutionInvariant)
	}
	receipt, err := model.JSONFrom(struct {
		Action   model.OperationKind `json:"action"`
		Handling struct {
			Status string `json:"status"`
		} `json:"handling"`
		OperationID   string     `json:"operation_id"`
		Receipt       string     `json:"receipt"`
		Replayed      bool       `json:"replayed"`
		Results       []struct{} `json:"results"`
		SchemaVersion int        `json:"schema_version"`
		Status        string     `json:"status"`
	}{Action: operation.Kind(), Handling: struct {
		Status string `json:"status"`
	}{handlingReceiptStatus}, OperationID: operation.ID().String(), Receipt: model.Sum(evidence.Bytes()).String(),
		Results: emptyResults, SchemaVersion: model.SchemaVersion, Status: "resolved"})
	if err != nil {
		return model.JSON{}, fmt.Errorf("%w: resolution receipt: %v", ErrManagedResolutionInvariant, err)
	}
	return receipt, nil
}

func buildManagedResolutionEvidence(operation model.Operation, current model.CurrentReadReceipt,
	source model.Event, content string, transition managedResolutionTransition,
	at time.Time,
) (model.JSON, error) {
	var retryAt *string
	if transition.hasRetryAt {
		value := storeTime(transition.retryAt)
		retryAt = &value
	}
	evidence, err := model.JSONFrom(struct {
		ActionWork        model.WorkRef        `json:"action_work"`
		ActionWorkVersion uint64               `json:"action_work_version"`
		Content           string               `json:"content"`
		HandlingStatus    model.HandlingStatus `json:"handling_status"`
		OperationID       string               `json:"operation_id"`
		ProjectionDigest  model.Digest         `json:"projection_digest"`
		RequestDigest     model.Digest         `json:"request_digest"`
		ResolvedAt        string               `json:"resolved_at"`
		RetryAt           *string              `json:"retry_at"`
		SchemaVersion     int                  `json:"schema_version"`
		SourceEvent       model.EventKey       `json:"source_event"`
		SourceEventDigest model.Digest         `json:"source_event_digest"`
	}{current.ActionWork(), current.ActionWorkVersion(), content, transition.handlingStatus,
		operation.ID().String(), current.ProjectionDigest(), operation.RequestDigest(), storeTime(at), retryAt,
		model.SchemaVersion, source.Key(), source.Digest()})
	if err != nil {
		return model.JSON{}, fmt.Errorf("%w: resolution evidence: %v", ErrManagedResolutionInvariant, err)
	}
	return evidence, nil
}

func updateManagedResolutionHandling(ctx context.Context, tx *sql.Tx, handling model.Handling,
	current model.CurrentReadReceipt, contextHash model.Digest, transition managedResolutionTransition,
	at time.Time,
) error {
	lease, _ := handling.LeaseUntil()
	availableAt := handling.AvailableAt()
	if transition.hasRetryAt {
		availableAt = transition.retryAt
	}
	deadAt := any(nil)
	if transition.hasDeadAt {
		deadAt = storeTime(transition.deadAt)
	}
	lastError := any(nil)
	if transition.lastError != "" {
		lastError = transition.lastError
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status=?,available_at=?,
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition=?,
		outcome_event_id=NULL,last_error=?,dead_at=?,updated_at=?
		WHERE handling_id=? AND profile_id=? AND event_id=? AND status='claimed'
		AND available_at=? AND claim_owner=? AND claim_token_hash=? AND lease_until=?
		AND attempts=? AND recovery_count=? AND last_disposition='read' AND outcome_event_id IS NULL
		AND last_error IS NULL AND dead_at IS NULL AND updated_at=?`,
		string(transition.handlingStatus), storeTime(availableAt), transition.handlingDisposition,
		lastError, deadAt, storeTime(at), handling.ID().String(), handling.ProfileID().String(), handling.EventID().String(),
		storeTime(handling.AvailableAt()), handling.ClaimOwner(), contextHash.Bytes(), storeTime(lease),
		handling.Attempts(), handling.RecoveryCount(), storeTime(current.ReadAt()))
	if err != nil {
		return fmt.Errorf("commit managed resolution: update Handling: %w", err)
	}
	if err := requireExactlyOneRow(result, "commit managed resolution: Handling fence"); err != nil {
		return fmt.Errorf("%w: %v", ErrManagedContextStale, err)
	}
	return nil
}

func updateManagedResolutionRun(ctx context.Context, tx *sql.Tx, run model.AgentRun,
	handling model.Handling, contextHash model.Digest,
	transition managedResolutionTransition, receipt model.JSON, at time.Time,
) error {
	lease, _ := run.LeaseUntil()
	if finished, ok := run.FinishedAt(); ok && finished.After(at) {
		return fmt.Errorf("%w: trusted time precedes AgentRun finish", ErrManagedResolutionInvariant)
	}
	currentJSON, _ := run.CurrentReadReceipt()
	result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status=?,finished_at=COALESCE(finished_at,?),
		outcome_receipt_json=?,error=NULL
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND handling_recovery=? AND claim_fence_hash=? AND lease_until=? AND runtime_kind=? AND status=?
		AND current_read_receipt_json=? AND outcome_receipt_json IS NULL
		`, string(transition.runStatus), storeTime(at),
		receipt.Bytes(), run.ID().String(), run.ProfileID().String(), handling.ID().String(),
		run.HandlingAttempt(), run.HandlingRecovery(), contextHash.Bytes(), storeTime(lease), string(run.Runtime()),
		string(run.Status()), currentJSON.Bytes())
	if err != nil {
		return fmt.Errorf("commit managed resolution: update AgentRun: %w", err)
	}
	if err := requireExactlyOneRow(result, "commit managed resolution: AgentRun fence"); err != nil {
		return fmt.Errorf("%w: %v", ErrManagedContextStale, err)
	}
	return nil
}

func updateManagedResolutionOperation(ctx context.Context, tx *sql.Tx, operation model.Operation,
	receipt model.JSON, at time.Time,
) error {
	contextHash, _ := operation.ContextHash()
	lease, _ := operation.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE operations SET status='committed',lease_owner=NULL,
		lease_until=NULL,result_json=?,finished_at=? WHERE operation_id=? AND profile_id=?
		AND agent_run_id=? AND client_key_hash=? AND context_hash=? AND kind=?
		AND request_digest=? AND status='started' AND lease_owner=? AND lease_until=?
		AND capture_json IS NULL AND result_json IS NULL AND finished_at IS NULL`, receipt.Bytes(),
		storeTime(at), operation.ID().String(), operation.ProfileID().String(), operation.AgentRunID().String(),
		operation.ClientKeyHash().Bytes(), contextHash.Bytes(), string(operation.Kind()),
		operation.RequestDigest().Bytes(), operation.LeaseOwner(), storeTime(lease))
	if err != nil {
		return fmt.Errorf("commit managed resolution: update Operation: %w", err)
	}
	if err := requireExactlyOneRow(result, "commit managed resolution: Operation fence"); err != nil {
		return fmt.Errorf("%w: %v", ErrOperationFence, err)
	}
	return nil
}

func verifyManagedResolutionRows(ctx context.Context, tx *sql.Tx, operation model.Operation,
	previousHandling model.Handling, previousRun model.AgentRun, transition managedResolutionTransition,
	receipt model.JSON,
) error {
	durableOperation, err := readOperationByID(ctx, tx, operation.ID())
	if err != nil || durableOperation.Status() != model.OperationCommitted {
		return fmt.Errorf("%w: committed Operation cannot be read", ErrManagedResolutionInvariant)
	}
	durableReceipt, ok := durableOperation.Result()
	if !ok || durableReceipt.String() != receipt.String() {
		return fmt.Errorf("%w: committed Operation receipt differs", ErrManagedResolutionInvariant)
	}
	durableHandling, err := readAgentHandling(ctx, tx, previousHandling.ID())
	if err != nil || durableHandling.Status() != transition.handlingStatus ||
		durableHandling.LastDisposition() != transition.handlingDisposition {
		return fmt.Errorf("%w: resolved Handling cannot be read", ErrManagedResolutionInvariant)
	}
	if _, claimed := durableHandling.ClaimTokenHash(); claimed {
		return fmt.Errorf("%w: resolved Handling retained claim authority", ErrManagedResolutionInvariant)
	}
	if transition.hasRetryAt && !durableHandling.AvailableAt().Equal(transition.retryAt) {
		return fmt.Errorf("%w: retry Handling backoff differs", ErrManagedResolutionInvariant)
	}
	if transition.hasDeadAt {
		deadAt, hasDeadAt := durableHandling.DeadAt()
		if !hasDeadAt || !deadAt.Equal(transition.deadAt) ||
			durableHandling.LastError() != transition.lastError {
			return fmt.Errorf("%w: dead Handling evidence differs", ErrManagedResolutionInvariant)
		}
	}
	durableRun, err := readAgentRun(ctx, tx, previousRun.ID())
	if err != nil || durableRun.Status() != transition.runStatus {
		return fmt.Errorf("%w: resolved AgentRun cannot be read", ErrManagedResolutionInvariant)
	}
	outcome, hasOutcome := durableRun.OutcomeReceipt()
	if !hasOutcome || outcome.String() != receipt.String() {
		return fmt.Errorf("%w: AgentRun resolution receipts differ", ErrManagedResolutionInvariant)
	}
	return nil
}
