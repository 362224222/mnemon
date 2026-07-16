package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrAgentClaimInput     = errors.New("invalid Agent claim input")
	ErrAgentClaimProfile   = errors.New("Agent claim Profile is unavailable")
	ErrAgentClaimAsset     = errors.New("Agent claim asset authority differs")
	ErrAgentClaimInvariant = errors.New("Agent claim durable invariant violated")
)

// AgentClaimStatus is the honest external-current result. Only Actionable
// carries a Handling and AgentRun. Busy means another unexpired claim owns the
// sole T0 concurrency slot; Waiting means obligations exist but are in durable
// backoff.
type AgentClaimStatus string

const (
	AgentClaimNone       AgentClaimStatus = "none"
	AgentClaimBusy       AgentClaimStatus = "busy"
	AgentClaimWaiting    AgentClaimStatus = "waiting"
	AgentClaimActionable AgentClaimStatus = "actionable"
)

func (s AgentClaimStatus) Valid() bool {
	return s == AgentClaimNone || s == AgentClaimBusy ||
		s == AgentClaimWaiting || s == AgentClaimActionable
}

type AgentClaimProbeSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	At                    time.Time
}

type AgentClaimSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	ClaimOwner            string
	ClaimTokenHash        model.Digest
	At                    time.Time
	LeaseUntil            time.Time
}

type AgentClaimResult struct {
	Status   AgentClaimStatus
	Handling model.Handling
	Run      model.AgentRun
}

// ProbeAgentClaim performs a bounded indexed readiness query without claiming
// or exposing Event identity. It does settle an expired lease in the same
// durable way as ClaimAgentCurrent; otherwise an expired final attempt could
// disappear from Hook readiness without ever recording dead evidence.
func (s *Store) ProbeAgentClaim(ctx context.Context, spec AgentClaimProbeSpec) (AgentClaimStatus, error) {
	if s == nil || s.db == nil || ctx == nil {
		return "", fmt.Errorf("%w: nil store or context", ErrAgentClaimInput)
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return "", err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("probe Agent claim: begin: %w", err)
	}
	defer tx.Rollback()

	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return "", err
	}
	if _, err := recoverExpiredAgentClaim(ctx, tx, profile, budget, at); err != nil {
		return "", err
	}
	if err := deadExhaustedPendingHandlings(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at); err != nil {
		return "", err
	}
	status, err := probeAgentClaimStatus(ctx, tx, spec.ProfileID, budget.Spec().MaxAttempts, at)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("probe Agent claim: commit readiness: %w", err)
	}
	return status, nil
}

// ClaimAgentCurrent is the durable external first-current boundary. Lease
// recovery, deterministic selection, owner-fenced Handling mutation and the
// server-owned AgentRun snapshot commit together. It never resumes a claim
// after a response loss: the next caller observes busy until lease expiry.
func (s *Store) ClaimAgentCurrent(ctx context.Context, spec AgentClaimSpec) (AgentClaimResult, error) {
	if s == nil || s.db == nil || ctx == nil {
		return AgentClaimResult{}, fmt.Errorf("%w: nil store or context", ErrAgentClaimInput)
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return AgentClaimResult{}, err
	}
	leaseUntil, err := canonicalClaimTime(spec.LeaseUntil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: lease: %v", ErrAgentClaimInput, err)
	}
	if spec.ClaimTokenHash.IsZero() {
		return AgentClaimResult{}, fmt.Errorf("%w: zero claim token hash", ErrAgentClaimInput)
	}
	if _, err := model.ParseRunID(spec.ClaimOwner); err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: claim owner: %v", ErrAgentClaimInput, err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("claim Agent current: begin: %w", err)
	}
	defer tx.Rollback()

	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.ProfileID, spec.ExpectedAssetRevision)
	if err != nil {
		return AgentClaimResult{}, err
	}
	expectedLease := at.Add(time.Duration(budget.Spec().ClaimLeaseSeconds) * time.Second)
	expectedLease, err = canonicalStoreTime(expectedLease)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("%w: derived lease does not round-trip: %v", ErrAgentClaimInput, err)
	}
	if !leaseUntil.Equal(expectedLease) {
		return AgentClaimResult{}, fmt.Errorf("%w: lease must equal trusted time plus Profile budget", ErrAgentClaimInput)
	}

	busy, err := recoverExpiredAgentClaim(ctx, tx, profile, budget, at)
	if err != nil {
		return AgentClaimResult{}, err
	}
	if busy {
		return AgentClaimResult{Status: AgentClaimBusy}, nil
	}
	if err := deadExhaustedPendingHandlings(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at); err != nil {
		return AgentClaimResult{}, err
	}

	handling, err := selectReadyAgentHandling(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at)
	if errors.Is(err, sql.ErrNoRows) {
		status, probeErr := probeAgentClaimStatus(ctx, tx, profile.ID(), budget.Spec().MaxAttempts, at)
		if probeErr != nil {
			return AgentClaimResult{}, probeErr
		}
		if status == AgentClaimActionable || status == AgentClaimBusy {
			return AgentClaimResult{}, fmt.Errorf("%w: readiness changed inside serialized claim", ErrAgentClaimInvariant)
		}
		if err := tx.Commit(); err != nil {
			return AgentClaimResult{}, fmt.Errorf("claim Agent current: commit recovery: %w", err)
		}
		return AgentClaimResult{Status: status}, nil
	}
	if err != nil {
		return AgentClaimResult{}, err
	}
	if handling.Attempts() >= uint32(budget.Spec().MaxAttempts) {
		return AgentClaimResult{}, fmt.Errorf("%w: selected Handling exhausted its attempt budget", ErrAgentClaimInvariant)
	}
	if at.Before(handling.UpdatedAt()) {
		return AgentClaimResult{}, fmt.Errorf("%w: trusted time precedes selected Handling update", ErrAgentClaimInvariant)
	}

	runID, err := newServerAgentRunID()
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("claim Agent current: allocate Run ID: %w", err)
	}
	nextAttempt := handling.Attempts() + 1
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET
		status='claimed', claim_owner=?, claim_token_hash=?, lease_until=?, attempts=?,
		last_disposition='claimed', last_error=NULL, dead_at=NULL, updated_at=?
		WHERE handling_id=? AND profile_id=? AND status='pending' AND attempts=? AND available_at<=?`,
		spec.ClaimOwner, spec.ClaimTokenHash.Bytes(), storeTime(leaseUntil), nextAttempt,
		storeTime(at), handling.ID().String(), profile.ID().String(), handling.Attempts(), storeTime(at))
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("claim Agent current: claim Handling: %w", err)
	}
	if err := requireExactlyOneRow(result, "claim Agent current: Handling CAS"); err != nil {
		return AgentClaimResult{}, err
	}

	claimed, err := readAgentHandling(ctx, tx, handling.ID())
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("claim Agent current: read claimed Handling: %w", err)
	}
	run, err := newExternalCurrentAgentRun(runID, profile, claimed, spec.ClaimTokenHash, at, leaseUntil)
	if err != nil {
		return AgentClaimResult{}, fmt.Errorf("claim Agent current: build AgentRun: %w", err)
	}
	if err := insertAgentRun(ctx, tx, run); err != nil {
		return AgentClaimResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AgentClaimResult{}, fmt.Errorf("claim Agent current: commit: %w", err)
	}
	return AgentClaimResult{Status: AgentClaimActionable, Handling: claimed, Run: run}, nil
}

func (s *Store) GetAgentRun(ctx context.Context, id model.RunID) (model.AgentRun, error) {
	if s == nil || s.db == nil || ctx == nil || id.IsZero() {
		return model.AgentRun{}, errors.New("get AgentRun: incomplete input")
	}
	run, err := readAgentRun(ctx, s.db, id)
	if err != nil {
		return model.AgentRun{}, fmt.Errorf("get AgentRun: %w", err)
	}
	return run, nil
}

func canonicalClaimTime(value time.Time) (time.Time, error) {
	if value.IsZero() {
		return time.Time{}, fmt.Errorf("%w: zero trusted time", ErrAgentClaimInput)
	}
	canonical, err := canonicalStoreTime(value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrAgentClaimInput, err)
	}
	return canonical, nil
}

func requireAgentClaimAuthority(ctx context.Context, q rowQuerier, id model.ProfileID,
	expectedAsset string,
) (model.Profile, model.HandlingBudget, error) {
	if id != model.TeamworkProfileID() || expectedAsset == "" {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: default Profile and expected asset are required", ErrAgentClaimInput)
	}
	node, err := readNode(ctx, q)
	if err != nil {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: read Node: %v", ErrAgentClaimProfile, err)
	}
	profile, err := readProfile(ctx, q)
	if err != nil {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: read Profile: %v", ErrAgentClaimProfile, err)
	}
	if !profile.Enabled() {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: Teamwork Profile is disabled", ErrAgentClaimProfile)
	}
	if node.ActiveAssetRevision() != expectedAsset || profile.ActiveAssetRevision() != expectedAsset {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: Node=%q Profile=%q expected=%q",
			ErrAgentClaimAsset, node.ActiveAssetRevision(), profile.ActiveAssetRevision(), expectedAsset)
	}
	budget, err := model.ParseHandlingBudget(profile.HandlingBudget())
	if err != nil {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: invalid handling budget: %v", ErrAgentClaimProfile, err)
	}
	if budget.Spec().MaxConcurrency != 1 {
		return model.Profile{}, model.HandlingBudget{}, fmt.Errorf("%w: unsupported concurrency budget", ErrAgentClaimProfile)
	}
	return profile, budget, nil
}

func probeAgentClaimStatus(ctx context.Context, q rowQuerier, profile model.ProfileID,
	maxAttempts int, at time.Time,
) (AgentClaimStatus, error) {
	var claimed, ready, future int
	err := q.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM agent_handlings WHERE profile_id=? AND status='claimed'),
		EXISTS(SELECT 1 FROM agent_handlings WHERE profile_id=? AND status='pending' AND available_at<=? AND attempts<?),
		EXISTS(SELECT 1 FROM agent_handlings WHERE profile_id=? AND status='pending' AND available_at>? AND attempts<?)`,
		profile.String(), profile.String(), storeTime(at), maxAttempts,
		profile.String(), storeTime(at), maxAttempts).
		Scan(&claimed, &ready, &future)
	if err != nil {
		return "", fmt.Errorf("probe Agent claim: query readiness: %w", err)
	}
	switch {
	case claimed == 1:
		return AgentClaimBusy, nil
	case ready == 1:
		return AgentClaimActionable, nil
	case future == 1:
		return AgentClaimWaiting, nil
	default:
		return AgentClaimNone, nil
	}
}

func recoverExpiredAgentClaim(ctx context.Context, tx *sql.Tx, profile model.Profile,
	budget model.HandlingBudget, at time.Time,
) (bool, error) {
	handling, err := scanAgentHandling(tx.QueryRowContext(ctx, handlingSelect+
		" WHERE profile_id=? AND status='claimed'", profile.ID().String()))
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim Agent current: read existing claim: %w", err)
	}
	lease, ok := handling.LeaseUntil()
	if !ok {
		return false, fmt.Errorf("%w: claimed Handling has no lease", ErrAgentClaimInvariant)
	}
	if lease.After(at) {
		if _, err := requireActiveAgentRunForClaim(ctx, tx, profile, handling); err != nil {
			return false, err
		}
		return true, nil
	}
	if at.Before(handling.UpdatedAt()) {
		return false, fmt.Errorf("%w: trusted time precedes claimed Handling update", ErrAgentClaimInvariant)
	}

	dead := handling.Attempts() >= uint32(budget.Spec().MaxAttempts)
	runStatus, disposition, lastError := model.AgentRunRequeued, "lease_expired", "claim lease expired"
	if dead {
		runStatus, disposition, lastError = model.AgentRunDead, "attempt_budget_exhausted", "claim lease expired after maximum attempts"
	}
	runID, err := requireActiveAgentRunForClaim(ctx, tx, profile, handling)
	if err != nil {
		return false, err
	}
	if err := rejectExpiredRunOperations(ctx, tx, runID, profile.ID(), at); err != nil {
		return false, err
	}
	if err := finishExpiredAgentRun(ctx, tx, runID, handling, runStatus, lastError, at); err != nil {
		return false, err
	}

	status, availableAt, deadAt := "pending", "", any(nil)
	if dead {
		status, availableAt, deadAt = "dead", storeTime(handling.AvailableAt()), storeTime(at)
	} else {
		retryAt, err := agentClaimRetryAt(lease, handling.Attempts(), budget)
		if err != nil {
			return false, err
		}
		availableAt = storeTime(retryAt)
	}
	claimHash, _ := handling.ClaimTokenHash()
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status=?, available_at=?, claim_owner=NULL,
		claim_token_hash=NULL, lease_until=NULL, last_disposition=?, last_error=?, dead_at=?, updated_at=?
		WHERE handling_id=? AND profile_id=? AND status='claimed' AND claim_owner=?
		AND claim_token_hash=? AND lease_until=? AND attempts=?`, status, availableAt, disposition, lastError,
		deadAt, storeTime(at), handling.ID().String(), profile.ID().String(), handling.ClaimOwner(),
		claimHash.Bytes(), storeTime(lease), handling.Attempts())
	if err != nil {
		return false, fmt.Errorf("claim Agent current: recover expired Handling: %w", err)
	}
	if err := requireExactlyOneRow(result, "claim Agent current: expired Handling CAS"); err != nil {
		return false, err
	}
	return false, nil
}

func requireActiveAgentRunForClaim(ctx context.Context, tx *sql.Tx, profile model.Profile,
	handling model.Handling,
) (model.RunID, error) {
	fence, hasFence := handling.ClaimTokenHash()
	lease, hasLease := handling.LeaseUntil()
	if !hasFence || !hasLease {
		return model.RunID{}, fmt.Errorf("%w: claimed Handling lacks a complete fence", ErrAgentClaimInvariant)
	}
	var runText string
	err := tx.QueryRowContext(ctx, `SELECT run_id FROM agent_runs WHERE profile_id=? AND handling_id=?
		AND handling_attempt=? AND claim_fence_hash=? AND lease_until=? AND runtime_kind=?
		AND status IN ('starting','running','runtime_finished')`, profile.ID().String(), handling.ID().String(),
		handling.Attempts(), fence.Bytes(), storeTime(lease), string(profile.Runtime())).Scan(&runText)
	if errors.Is(err, sql.ErrNoRows) {
		return model.RunID{}, fmt.Errorf("%w: claimed Handling has no exact active AgentRun", ErrAgentClaimInvariant)
	}
	if err != nil {
		return model.RunID{}, fmt.Errorf("claim Agent current: validate active AgentRun: %w", err)
	}
	runID, err := model.ParseRunID(runText)
	if err != nil {
		return model.RunID{}, fmt.Errorf("%w: invalid active AgentRun ID: %v", ErrAgentClaimInvariant, err)
	}
	run, err := readAgentRun(ctx, tx, runID)
	if err != nil {
		return model.RunID{}, fmt.Errorf("%w: invalid active AgentRun evidence: %v", ErrAgentClaimInvariant, err)
	}
	runHandling, hasHandling := run.HandlingID()
	runFence, hasFence := run.ClaimFenceHash()
	runLease, hasLease := run.LeaseUntil()
	if run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() || !run.Status().OperationAuthority() ||
		!hasHandling || runHandling != handling.ID() || run.HandlingAttempt() != handling.Attempts() ||
		!hasFence || runFence != fence || !hasLease || !runLease.Equal(lease) {
		return model.RunID{}, fmt.Errorf("%w: active AgentRun snapshot differs from Handling", ErrAgentClaimInvariant)
	}
	return runID, nil
}

func rejectExpiredRunOperations(ctx context.Context, tx *sql.Tx, runID model.RunID,
	profile model.ProfileID, at time.Time,
) error {
	var future int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM operations
		WHERE agent_run_id=? AND profile_id=? AND status='started' AND created_at>?)`,
		runID.String(), profile.String(), storeTime(at)).Scan(&future); err != nil {
		return fmt.Errorf("claim Agent current: inspect stale operations: %w", err)
	}
	if future == 1 {
		return fmt.Errorf("%w: trusted time precedes a started operation", ErrAgentClaimInvariant)
	}
	receipt, err := model.JSONFrom(struct {
		Code   string `json:"code"`
		Status string `json:"status"`
	}{"claim_lease_expired", "rejected"})
	if err != nil {
		return fmt.Errorf("claim Agent current: build stale operation receipt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE operations SET status='rejected', lease_owner=NULL,
		lease_until=NULL, result_json=?, finished_at=? WHERE agent_run_id=? AND profile_id=? AND status='started'`,
		receipt.Bytes(), storeTime(at), runID.String(), profile.String()); err != nil {
		return fmt.Errorf("claim Agent current: reject stale operations: %w", err)
	}
	return nil
}

func finishExpiredAgentRun(ctx context.Context, tx *sql.Tx, runID model.RunID, handling model.Handling,
	status model.AgentRunStatus, message string, at time.Time,
) error {
	fence, _ := handling.ClaimTokenHash()
	lease, _ := handling.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status=?, finished_at=COALESCE(finished_at,?),
		error=COALESCE(error,?) WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND claim_fence_hash=? AND lease_until=? AND status IN ('starting','running','runtime_finished')`,
		string(status), storeTime(at), message, runID.String(), handling.ProfileID().String(), handling.ID().String(),
		handling.Attempts(), fence.Bytes(), storeTime(lease))
	if err != nil {
		return fmt.Errorf("claim Agent current: finish expired AgentRun: %w", err)
	}
	if err := requireExactlyOneRow(result, "claim Agent current: expired AgentRun CAS"); err != nil {
		return fmt.Errorf("%w: %v", ErrAgentClaimInvariant, err)
	}
	return nil
}

func agentClaimRetryAt(expiredLease time.Time, attempt uint32, budget model.HandlingBudget) (time.Time, error) {
	seconds := int64(budget.Spec().RetryInitialSeconds)
	maximum := int64(budget.Spec().RetryMaxSeconds)
	for power := uint32(1); power < attempt && seconds < maximum; power++ {
		seconds *= 2
		if seconds > maximum {
			seconds = maximum
		}
	}
	retryAt := expiredLease.Add(time.Duration(seconds) * time.Second)
	canonical, err := canonicalStoreTime(retryAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: derived retry time does not round-trip: %v", ErrAgentClaimInvariant, err)
	}
	return canonical, nil
}

func deadExhaustedPendingHandlings(ctx context.Context, tx *sql.Tx, profile model.ProfileID,
	maxAttempts int, at time.Time,
) error {
	var future int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_handlings
		WHERE profile_id=? AND status='pending' AND attempts>=? AND updated_at>?)`,
		profile.String(), maxAttempts, storeTime(at)).Scan(&future); err != nil {
		return fmt.Errorf("claim Agent current: inspect exhausted pending Handlings: %w", err)
	}
	if future == 1 {
		return fmt.Errorf("%w: trusted time precedes exhausted Handling update", ErrAgentClaimInvariant)
	}
	_, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status='dead',
		last_disposition='attempt_budget_exhausted', last_error='maximum handling attempts exhausted',
		dead_at=?, updated_at=? WHERE profile_id=? AND status='pending' AND attempts>=?`,
		storeTime(at), storeTime(at), profile.String(), maxAttempts)
	if err != nil {
		return fmt.Errorf("claim Agent current: seal exhausted pending Handlings: %w", err)
	}
	return nil
}

func selectReadyAgentHandling(ctx context.Context, tx *sql.Tx, profile model.ProfileID,
	maxAttempts int, at time.Time,
) (model.Handling, error) {
	handling, err := scanAgentHandling(tx.QueryRowContext(ctx, handlingSelect+`
		WHERE profile_id=? AND status='pending' AND available_at<=? AND attempts<?
		ORDER BY priority DESC, available_at ASC, created_at ASC, handling_id ASC LIMIT 1`,
		profile.String(), storeTime(at), maxAttempts))
	if err != nil {
		return model.Handling{}, err
	}
	return handling, nil
}

func newServerAgentRunID() (model.RunID, error) {
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return model.RunID{}, err
	}
	return model.ParseRunID("run-" + base64.RawURLEncoding.EncodeToString(random))
}

func newExternalCurrentAgentRun(id model.RunID, profile model.Profile, handling model.Handling,
	fence model.Digest, at, leaseUntil time.Time,
) (model.AgentRun, error) {
	cause, err := model.JSONFrom(struct {
		Kind       string           `json:"kind"`
		HandlingID model.HandlingID `json:"handling_id"`
		EventID    model.EventID    `json:"event_id"`
	}{"external_current", handling.ID(), handling.EventID()})
	if err != nil {
		return model.AgentRun{}, err
	}
	empty, err := model.NewJSON([]byte(`{}`))
	if err != nil {
		return model.AgentRun{}, err
	}
	handlingID := handling.ID()
	return model.NewAgentRun(model.AgentRunSpec{
		ID: id, ProfileID: profile.ID(), HandlingID: &handlingID, Cause: cause,
		HandlingAttempt: handling.Attempts(), ClaimFenceHash: &fence, LeaseUntil: &leaseUntil,
		Launcher: "external", Runtime: profile.Runtime(), LauncherDiagnostic: empty,
		RuntimeIDs: empty, Status: model.AgentRunRunning, StartedAt: at,
	})
}

func insertAgentRun(ctx context.Context, tx *sql.Tx, run model.AgentRun) error {
	handlingID, handlingAttempt, fence, lease := any(nil), any(nil), any(nil), any(nil)
	attachmentHash, attachmentExpires, attachedAt := any(nil), any(nil), any(nil)
	if id, ok := run.HandlingID(); ok {
		handlingID, handlingAttempt = id.String(), run.HandlingAttempt()
		value, _ := run.ClaimFenceHash()
		until, _ := run.LeaseUntil()
		fence, lease = value.Bytes(), storeTime(until)
	}
	if value, ok := run.AttachmentTokenHash(); ok {
		expires, _ := run.AttachmentExpiresAt()
		attachmentHash, attachmentExpires = value.Bytes(), storeTime(expires)
		if attached, present := run.AttachedAt(); present {
			attachedAt = storeTime(attached)
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO agent_runs(run_id,profile_id,handling_id,cause_json,
		handling_attempt,claim_fence_hash,lease_until,attachment_token_hash,attachment_expires_at,
		attached_at,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, run.ID().String(), run.ProfileID().String(), handlingID,
		run.Cause().Bytes(), handlingAttempt, fence, lease, attachmentHash, attachmentExpires, attachedAt,
		run.Launcher(), string(run.Runtime()),
		run.LauncherDiagnostic().Bytes(), run.RuntimeIDs().Bytes(), string(run.Status()), storeTime(run.StartedAt()))
	if err != nil {
		return fmt.Errorf("claim Agent current: insert AgentRun: %w", err)
	}
	return nil
}

func readAgentRun(ctx context.Context, q rowQuerier, id model.RunID) (model.AgentRun, error) {
	return scanAgentRun(q.QueryRowContext(ctx, agentRunSelect+" WHERE run_id=?", id.String()))
}

const agentRunSelect = `SELECT run_id,profile_id,handling_id,cause_json,handling_attempt,
	claim_fence_hash,lease_until,attachment_token_hash,attachment_expires_at,attached_at,
	launcher,runtime_kind,launcher_diagnostic_json,runtime_ids_json,status,wake_delivered_at,
	started_at,finished_at,current_read_receipt_json,outcome_receipt_json,completion_receipt_json,error
	FROM agent_runs`

func scanAgentRun(row *sql.Row) (model.AgentRun, error) {
	var runText, profileText, launcher, runtimeText, statusText, startedText string
	var handlingText, leaseText, attachmentExpiresText, attachedText, wakeText, finishedText, errorText sql.NullString
	var attempt sql.NullInt64
	var causeBytes, fenceBytes, attachmentHashBytes, diagnosticBytes, runtimeIDsBytes []byte
	var currentBytes, outcomeBytes, completionBytes []byte
	if err := row.Scan(&runText, &profileText, &handlingText, &causeBytes, &attempt, &fenceBytes,
		&leaseText, &attachmentHashBytes, &attachmentExpiresText, &attachedText, &launcher,
		&runtimeText, &diagnosticBytes, &runtimeIDsBytes, &statusText, &wakeText, &startedText,
		&finishedText, &currentBytes, &outcomeBytes, &completionBytes, &errorText); err != nil {
		return model.AgentRun{}, err
	}
	runID, err := model.ParseRunID(runText)
	if err != nil {
		return model.AgentRun{}, err
	}
	profileID, err := model.ParseProfileID(profileText)
	if err != nil {
		return model.AgentRun{}, err
	}
	cause, err := exactCanonicalJSON(causeBytes)
	if err != nil {
		return model.AgentRun{}, fmt.Errorf("AgentRun cause: %w", err)
	}
	diagnostic, err := exactCanonicalJSON(diagnosticBytes)
	if err != nil {
		return model.AgentRun{}, fmt.Errorf("AgentRun launcher diagnostic: %w", err)
	}
	runtimeIDs, err := exactCanonicalJSON(runtimeIDsBytes)
	if err != nil {
		return model.AgentRun{}, fmt.Errorf("AgentRun Runtime IDs: %w", err)
	}
	startedAt, err := parseCanonicalStoreTime(startedText)
	if err != nil {
		return model.AgentRun{}, err
	}
	spec := model.AgentRunSpec{ID: runID, ProfileID: profileID, Cause: cause, Launcher: launcher,
		Runtime: model.RuntimeKind(runtimeText), LauncherDiagnostic: diagnostic, RuntimeIDs: runtimeIDs,
		Status: model.AgentRunStatus(statusText), StartedAt: startedAt, Error: errorText.String}
	if handlingText.Valid {
		handlingID, err := model.ParseHandlingID(handlingText.String)
		if err != nil {
			return model.AgentRun{}, err
		}
		if !attempt.Valid || attempt.Int64 <= 0 || attempt.Int64 > int64(^uint32(0)) {
			return model.AgentRun{}, errors.New("AgentRun has invalid handling attempt")
		}
		fence, err := model.DigestFromBytes(fenceBytes)
		if err != nil {
			return model.AgentRun{}, err
		}
		lease, err := parseOptionalStoreTime(leaseText)
		if err != nil || lease == nil {
			return model.AgentRun{}, errors.New("AgentRun has invalid claim lease")
		}
		spec.HandlingID, spec.HandlingAttempt, spec.ClaimFenceHash, spec.LeaseUntil =
			&handlingID, uint32(attempt.Int64), &fence, lease
	} else if attempt.Valid || len(fenceBytes) != 0 || leaseText.Valid {
		return model.AgentRun{}, errors.New("AgentRun has partial claim snapshot")
	}
	if len(attachmentHashBytes) != 0 || attachmentExpiresText.Valid || attachedText.Valid {
		hash, err := model.DigestFromBytes(attachmentHashBytes)
		if err != nil {
			return model.AgentRun{}, err
		}
		expires, err := parseOptionalStoreTime(attachmentExpiresText)
		if err != nil || expires == nil {
			return model.AgentRun{}, errors.New("AgentRun has invalid attachment expiry")
		}
		attached, err := parseOptionalStoreTime(attachedText)
		if err != nil {
			return model.AgentRun{}, err
		}
		spec.AttachmentTokenHash, spec.AttachmentExpiresAt, spec.AttachedAt = &hash, expires, attached
	}
	if spec.WakeDeliveredAt, err = parseOptionalStoreTime(wakeText); err != nil {
		return model.AgentRun{}, err
	}
	if spec.FinishedAt, err = parseOptionalStoreTime(finishedText); err != nil {
		return model.AgentRun{}, err
	}
	if spec.CurrentReadReceipt, err = optionalExactCanonicalJSON(currentBytes); err != nil {
		return model.AgentRun{}, err
	}
	if spec.OutcomeReceipt, err = optionalExactCanonicalJSON(outcomeBytes); err != nil {
		return model.AgentRun{}, err
	}
	if spec.CompletionReceipt, err = optionalExactCanonicalJSON(completionBytes); err != nil {
		return model.AgentRun{}, err
	}
	return model.NewAgentRun(spec)
}

func exactCanonicalJSON(raw []byte) (model.JSON, error) {
	value, err := model.NewJSON(raw)
	if err != nil {
		return model.JSON{}, err
	}
	if !bytes.Equal(value.Bytes(), raw) {
		return model.JSON{}, errors.New("durable JSON is not canonical")
	}
	return value, nil
}

func optionalExactCanonicalJSON(raw []byte) (*model.JSON, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	value, err := exactCanonicalJSON(raw)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func parseOptionalStoreTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := parseCanonicalStoreTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func requireExactlyOneRow(result sql.Result, operation string) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: rows affected: %w", operation, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: affected %d rows, want 1", operation, count)
	}
	return nil
}
