package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var (
	ErrAgentRuntimeInput     = errors.New("invalid Agent Runtime transition input")
	ErrAgentRuntimeStale     = errors.New("Agent Runtime authority is stale")
	ErrAgentRuntimeInvariant = errors.New("Agent Runtime durable invariant violated")

	ErrAgentDeadRecoveryInput     = errors.New("invalid Agent dead recovery input")
	ErrAgentDeadRecoveryAuthority = errors.New("Agent dead recovery Profile authority differs")
	ErrAgentDeadRecoveryInvariant = errors.New("Agent dead recovery durable invariant violated")
)

// AgentRuntimeTransitionStatus distinguishes a first durable transition from
// response-loss replay and from a semantic outcome that won the transaction
// race. Already-settled never downgrades the winning Handling or AgentRun.
type AgentRuntimeTransitionStatus string

const (
	AgentRuntimeApplied        AgentRuntimeTransitionStatus = "applied"
	AgentRuntimeReplayed       AgentRuntimeTransitionStatus = "replayed"
	AgentRuntimeAlreadySettled AgentRuntimeTransitionStatus = "already_settled"
)

func (s AgentRuntimeTransitionStatus) Valid() bool {
	return s == AgentRuntimeApplied || s == AgentRuntimeReplayed ||
		s == AgentRuntimeAlreadySettled
}

type AgentRuntimeTransitionResult struct {
	Status   AgentRuntimeTransitionStatus
	Run      model.AgentRun
	Handling model.Handling
}

// AgentWakeDeliverySpec carries the adapter-proven Hook receipt plus the exact
// durable authority returned by PreclaimAgentWake. Launch identity is already
// durable and intentionally cannot be replaced with later Thread/Turn data.
type AgentWakeDeliverySpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimFenceHash        model.Digest
	HandlingRecovery      uint32
	WakeReceipt           model.JSON
	At                    time.Time
}

// AgentRuntimeLaunchSpec records the process identity after app-server
// initialization and before any turn can start. This closes the restart
// recovery gap without treating process launch as wake delivery.
type AgentRuntimeLaunchSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimFenceHash        model.Digest
	HandlingRecovery      uint32
	LauncherDiagnostic    model.JSON
	RuntimeIDs            model.JSON
	At                    time.Time
}

type AgentRuntimeFinishSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimFenceHash        model.Digest
	HandlingRecovery      uint32
	CompletionReceipt     model.JSON
	At                    time.Time
}

type AgentRuntimeFailureSpec struct {
	ProfileID             model.ProfileID
	ExpectedAssetRevision string
	RunID                 model.RunID
	ClaimFenceHash        model.Digest
	HandlingRecovery      uint32
	LauncherDiagnostic    model.JSON
	RuntimeIDs            model.JSON
	CompletionReceipt     model.JSON
	Error                 string
	At                    time.Time
}

// AgentDeadRecoverySpec is a setup-only Store boundary. Profile is the exact
// successfully self-checked generation observed by setup; Store re-derives and
// compares active durable authority before recovering any dead Handling.
type AgentDeadRecoverySpec struct {
	Profile model.Profile
	At      time.Time
}

type AgentDeadRecoveryResult struct {
	Recovered uint32
}

type agentRuntimeAuthority struct {
	profile  model.Profile
	budget   model.HandlingBudget
	run      model.AgentRun
	handling model.Handling
}

type agentRuntimeAuthoritySpec struct {
	profileID             model.ProfileID
	expectedAssetRevision string
	runID                 model.RunID
	claimFenceHash        model.Digest
	handlingRecovery      uint32
	at                    time.Time
}

// RecordAgentRuntimeLaunch makes the exact initialized Runtime identity
// durable before turn/start. A response-loss replay may observe later Run
// states, but it can never replace the original launcher evidence.
func (s *Store) RecordAgentRuntimeLaunch(ctx context.Context,
	spec AgentRuntimeLaunchSpec,
) (AgentRuntimeTransitionResult, error) {
	if err := validateAgentRuntimeObject("launcher diagnostic", spec.LauncherDiagnostic); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if err := validateAgentRuntimeObject("Runtime IDs", spec.RuntimeIDs); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if spec.LauncherDiagnostic.String() == `{}` || spec.RuntimeIDs.String() == `{}` {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: launch evidence must be non-empty objects", ErrAgentRuntimeInput)
	}
	authoritySpec := agentRuntimeAuthoritySpec{profileID: spec.ProfileID,
		expectedAssetRevision: spec.ExpectedAssetRevision, runID: spec.RunID,
		claimFenceHash: spec.ClaimFenceHash, handlingRecovery: spec.HandlingRecovery, at: spec.At}
	at, err := validateAgentRuntimeAuthorityInput(s, ctx, authoritySpec)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("record Agent Runtime launch: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readAgentRuntimeAuthority(ctx, tx, authoritySpec, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	authority, err = settleExpiredAgentRuntimeAuthority(ctx, tx, authority, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	diagnosticSet := authority.run.LauncherDiagnostic().String() != `{}`
	runtimeIDsSet := authority.run.RuntimeIDs().String() != `{}`
	if diagnosticSet != runtimeIDsSet {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: launch evidence is partial", ErrAgentRuntimeInvariant)
	}
	runtimeStartedAt, launched := authority.run.RuntimeStartedAt()
	if launched {
		if !diagnosticSet {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: Runtime start has no launch evidence", ErrAgentRuntimeInvariant)
		}
		if authority.run.LauncherDiagnostic().String() != spec.LauncherDiagnostic.String() ||
			authority.run.RuntimeIDs().String() != spec.RuntimeIDs.String() {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: launch replay evidence differs", ErrAgentRuntimeInvariant)
		}
		if authority.run.Status() == model.AgentRunStarting {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: launch evidence retained starting status", ErrAgentRuntimeInvariant)
		}
		if runtimeStartedAt.After(at) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: launch replay time precedes Runtime start", ErrAgentRuntimeInvariant)
		}
		if authority.run.Status() == model.AgentRunRuntimeFinished || authority.run.Status().Terminal() {
			return commitAgentRuntimeResult(tx, authority, AgentRuntimeAlreadySettled,
				"record Agent Runtime launch: terminal replay")
		}
		if authority.run.Status() != model.AgentRunRunning {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: launched Run has invalid active status", ErrAgentRuntimeInvariant)
		}
		if err := requireLiveAgentRuntimeAuthority(authority, spec.ClaimFenceHash, at); err != nil {
			return AgentRuntimeTransitionResult{}, err
		}
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeReplayed,
			"record Agent Runtime launch: active replay")
	}
	if authority.run.Status() != model.AgentRunStarting {
		if !authority.run.Status().Terminal() {
			return commitAgentRuntimeResult(tx, authority, AgentRuntimeAlreadySettled,
				"record Agent Runtime launch: settled")
		}
		finishedAt, hasFinished := authority.run.FinishedAt()
		if !hasFinished || at.After(finishedAt) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: late launch time follows Run finish", ErrAgentRuntimeInvariant)
		}
		if completionAt, completed := authority.run.CompletionAt(); completed && at.After(completionAt) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: late launch time follows Runtime completion", ErrAgentRuntimeInvariant)
		}
		if diagnosticSet && (authority.run.LauncherDiagnostic().String() != spec.LauncherDiagnostic.String() ||
			authority.run.RuntimeIDs().String() != spec.RuntimeIDs.String()) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: late launch changed terminal evidence", ErrAgentRuntimeInvariant)
		}
		if _, completed := authority.run.CompletionReceipt(); completed && !diagnosticSet {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: completed Run lacks exact launch evidence", ErrAgentRuntimeInvariant)
		}
		handlingID, _ := authority.run.HandlingID()
		lease, _ := authority.run.LeaseUntil()
		var result sql.Result
		if diagnosticSet {
			result, err = tx.ExecContext(ctx, `UPDATE agent_runs SET runtime_started_at=?
				WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
				AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
				AND launcher='mnemond-wake' AND runtime_kind=? AND status=? AND finished_at=?
				AND launcher_diagnostic_json=? AND runtime_ids_json=? AND runtime_started_at IS NULL
				AND finished_at>=? AND (completion_at IS NULL OR completion_at>=?)`, storeTime(at),
				authority.run.ID().String(), authority.profile.ID().String(), handlingID.String(),
				authority.run.HandlingAttempt(), authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(),
				storeTime(lease), string(authority.profile.Runtime()), string(authority.run.Status()),
				storeTime(finishedAt), spec.LauncherDiagnostic.Bytes(), spec.RuntimeIDs.Bytes(),
				storeTime(at), storeTime(at))
		} else {
			result, err = tx.ExecContext(ctx, `UPDATE agent_runs SET runtime_started_at=?,
				launcher_diagnostic_json=?,runtime_ids_json=?
				WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
				AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
				AND launcher='mnemond-wake' AND runtime_kind=? AND status=? AND finished_at=?
				AND launcher_diagnostic_json=? AND runtime_ids_json=? AND runtime_started_at IS NULL
				AND completion_receipt_json IS NULL AND finished_at>=?`, storeTime(at),
				spec.LauncherDiagnostic.Bytes(), spec.RuntimeIDs.Bytes(), authority.run.ID().String(),
				authority.profile.ID().String(), handlingID.String(), authority.run.HandlingAttempt(),
				authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(), storeTime(lease),
				string(authority.profile.Runtime()), string(authority.run.Status()), storeTime(finishedAt),
				[]byte(`{}`), []byte(`{}`), storeTime(at))
		}
		if err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("record late Agent Runtime launch: update: %w", err)
		}
		if err := requireExactlyOneRow(result, "record late Agent Runtime launch: AgentRun fence"); err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
		}
		authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
		if err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: late launched AgentRun cannot be read", ErrAgentRuntimeInvariant)
		}
		// The launch evidence is still durable process identity, but a terminal
		// Run no longer grants managed-work authority. In particular, an
		// abandonment or lease settlement may have won while this callback was
		// in flight. Report that winner so the adapter stops before sending any
		// protocol bytes while startup recovery can still observe the process.
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeAlreadySettled,
			"record late Agent Runtime launch: commit")
	}
	if diagnosticSet {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: starting Run contains launch evidence without Runtime start", ErrAgentRuntimeInvariant)
	}
	if err := requireLiveAgentRuntimeAuthority(authority, spec.ClaimFenceHash, at); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status='running',runtime_started_at=?,
		launcher_diagnostic_json=?,runtime_ids_json=? WHERE run_id=? AND profile_id=?
		AND handling_id=? AND handling_attempt=? AND handling_recovery=? AND claim_fence_hash=?
		AND lease_until=? AND launcher='mnemond-wake' AND runtime_kind=? AND status='starting'
		AND launcher_diagnostic_json=? AND runtime_ids_json=? AND finished_at IS NULL
		AND runtime_started_at IS NULL AND completion_at IS NULL AND completion_receipt_json IS NULL`,
		storeTime(at), spec.LauncherDiagnostic.Bytes(), spec.RuntimeIDs.Bytes(),
		authority.run.ID().String(), authority.profile.ID().String(),
		handlingID.String(), authority.run.HandlingAttempt(), authority.run.HandlingRecovery(),
		spec.ClaimFenceHash.Bytes(), storeTime(lease), string(authority.profile.Runtime()),
		[]byte(`{}`), []byte(`{}`))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("record Agent Runtime launch: update: %w", err)
	}
	if err := requireExactlyOneRow(result, "record Agent Runtime launch: AgentRun fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: launched AgentRun cannot be read", ErrAgentRuntimeInvariant)
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied,
		"record Agent Runtime launch: commit")
}

// RecordAgentWakeDelivery records only a cue emitted for an existing
// mnemond-wake preclaim. It can never attribute an external Hook to a later
// Run. An exact replay preserves the first timestamp and evidence bytes.
func (s *Store) RecordAgentWakeDelivery(ctx context.Context,
	spec AgentWakeDeliverySpec,
) (AgentRuntimeTransitionResult, error) {
	if err := validateAgentRuntimeObject("wake receipt", spec.WakeReceipt); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if spec.WakeReceipt.String() == `{}` {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: wake receipt must be a non-empty object", ErrAgentRuntimeInput)
	}
	authoritySpec := agentRuntimeAuthoritySpec{profileID: spec.ProfileID,
		expectedAssetRevision: spec.ExpectedAssetRevision, runID: spec.RunID,
		claimFenceHash: spec.ClaimFenceHash, handlingRecovery: spec.HandlingRecovery, at: spec.At}
	at, err := validateAgentRuntimeAuthorityInput(s, ctx, authoritySpec)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("record Agent wake delivery: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readAgentRuntimeAuthority(ctx, tx, authoritySpec, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	authority, err = settleExpiredAgentRuntimeAuthority(ctx, tx, authority, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if deliveredAt, delivered := authority.run.WakeDeliveredAt(); delivered {
		receipt, hasReceipt := authority.run.WakeReceipt()
		if deliveredAt.After(at) || !hasReceipt || receipt.String() != spec.WakeReceipt.String() {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: wake replay evidence differs",
				ErrAgentRuntimeInvariant)
		}
		if authority.run.Status() == model.AgentRunRuntimeFinished || authority.run.Status().Terminal() {
			return commitAgentRuntimeResult(tx, authority, AgentRuntimeAlreadySettled,
				"record Agent wake delivery: terminal replay")
		}
		if authority.run.Status() != model.AgentRunRunning {
			return AgentRuntimeTransitionResult{}, fmt.Errorf(
				"%w: delivered wake has invalid active status", ErrAgentRuntimeInvariant)
		}
		if err := requireLiveAgentRuntimeAuthority(authority, spec.ClaimFenceHash, at); err != nil {
			return AgentRuntimeTransitionResult{}, err
		}
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeReplayed,
			"record Agent wake delivery: active replay")
	}
	active := authority.run.Status() == model.AgentRunRunning
	diagnosticSet := authority.run.LauncherDiagnostic().String() != `{}`
	runtimeIDsSet := authority.run.RuntimeIDs().String() != `{}`
	if diagnosticSet != runtimeIDsSet {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: wake launch evidence is partial", ErrAgentRuntimeInvariant)
	}
	launcherEvidenceSet := diagnosticSet && runtimeIDsSet
	if !launcherEvidenceSet {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: wake delivery precedes durable Runtime launch", ErrAgentRuntimeInvariant)
	}
	runtimeStartedAt, launched := authority.run.RuntimeStartedAt()
	if !launched || at.Before(runtimeStartedAt) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: wake delivery precedes durable Runtime start", ErrAgentRuntimeInvariant)
	}
	lateTerminalEvidence := authority.run.Status().Terminal()
	if lateTerminalEvidence {
		finishedAt, hasFinished := authority.run.FinishedAt()
		if !hasFinished || at.After(finishedAt) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: late wake time follows Run finish",
				ErrAgentRuntimeInvariant)
		}
		if completionAt, completed := authority.run.CompletionAt(); completed && at.After(completionAt) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: late wake time follows Runtime completion",
				ErrAgentRuntimeInvariant)
		}
	}
	if !active && !lateTerminalEvidence {
		return AgentRuntimeTransitionResult{}, fmt.Errorf(
			"%w: missing wake evidence has invalid Run status", ErrAgentRuntimeInvariant)
	}
	if active {
		if err := requireLiveAgentRuntimeAuthority(authority, spec.ClaimFenceHash, at); err != nil {
			return AgentRuntimeTransitionResult{}, err
		}
	}
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET wake_delivered_at=?,wake_receipt_json=?
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
		AND launcher='mnemond-wake' AND runtime_kind=?
		AND launcher_diagnostic_json=? AND runtime_ids_json=? AND runtime_started_at<=?
		AND (status='running' OR
			(status IN ('outcome_accepted','requeued','rejected','failed','dead')
				AND finished_at>=? AND (completion_at IS NULL OR completion_at>=?)))
		AND wake_delivered_at IS NULL AND wake_receipt_json IS NULL`, storeTime(at),
		spec.WakeReceipt.Bytes(), authority.run.ID().String(),
		authority.profile.ID().String(), handlingID.String(), authority.run.HandlingAttempt(),
		authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(), storeTime(lease),
		string(authority.profile.Runtime()), authority.run.LauncherDiagnostic().Bytes(),
		authority.run.RuntimeIDs().Bytes(), storeTime(at), storeTime(at), storeTime(at))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("record Agent wake delivery: update: %w", err)
	}
	if err := requireExactlyOneRow(result, "record Agent wake delivery: AgentRun fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: delivered AgentRun cannot be read",
			ErrAgentRuntimeInvariant)
	}
	status := AgentRuntimeApplied
	operation := "record Agent wake delivery: active commit"
	if lateTerminalEvidence {
		status = AgentRuntimeAlreadySettled
		operation = "record Agent wake delivery: terminal commit"
	}
	return commitAgentRuntimeResult(tx, authority, status, operation)
}

// FinishAgentRuntime records normal Runtime completion without releasing or
// completing its Handling. A started exact operation may still commit while
// the Handling lease is live; a new operation key is rejected elsewhere.
func (s *Store) FinishAgentRuntime(ctx context.Context,
	spec AgentRuntimeFinishSpec,
) (AgentRuntimeTransitionResult, error) {
	if err := validateAgentRuntimeObject("completion receipt", spec.CompletionReceipt); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	authoritySpec := agentRuntimeAuthoritySpec{profileID: spec.ProfileID,
		expectedAssetRevision: spec.ExpectedAssetRevision, runID: spec.RunID,
		claimFenceHash: spec.ClaimFenceHash, handlingRecovery: spec.HandlingRecovery, at: spec.At}
	at, err := validateAgentRuntimeAuthorityInput(s, ctx, authoritySpec)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("finish Agent Runtime: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readAgentRuntimeAuthority(ctx, tx, authoritySpec, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	authority, err = settleExpiredAgentRuntimeAuthority(ctx, tx, authority, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if receipt, finished := authority.run.CompletionReceipt(); finished {
		completionAt, hasCompletionAt := authority.run.CompletionAt()
		if hasCompletionAt && !completionAt.After(at) && authority.run.Error() == "" &&
			receipt.String() == spec.CompletionReceipt.String() {
			return commitAgentRuntimeResult(tx, authority, AgentRuntimeReplayed,
				"finish Agent Runtime: replay")
		}
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completion replay evidence differs",
			ErrAgentRuntimeInvariant)
	}
	semanticSettled := agentRuntimeSemanticSettled(authority.run)
	leaseSettled := agentRuntimeLeaseSettled(authority.run)
	if authority.run.Status() != model.AgentRunStarting && authority.run.Status() != model.AgentRunRunning &&
		!semanticSettled && !leaseSettled {
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeAlreadySettled,
			"finish Agent Runtime: settled")
	}
	if !semanticSettled && !leaseSettled {
		if err := requireLiveAgentRuntimeAuthority(authority, spec.ClaimFenceHash, at); err != nil {
			return AgentRuntimeTransitionResult{}, err
		}
	}
	wakeDeliveredAt, delivered := authority.run.WakeDeliveredAt()
	if !delivered {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: normal completion precedes wake delivery",
			ErrAgentRuntimeInvariant)
	}
	if wakeDeliveredAt.After(at) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: completion time precedes wake delivery",
			ErrAgentRuntimeInvariant)
	}
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	var result sql.Result
	if semanticSettled {
		result, err = tx.ExecContext(ctx, `UPDATE agent_runs SET completion_at=?,completion_receipt_json=?
			WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
			AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
			AND launcher='mnemond-wake' AND runtime_kind=? AND outcome_receipt_json IS NOT NULL
			AND completion_at IS NULL AND completion_receipt_json IS NULL
			AND wake_delivered_at<=? AND finished_at<=?`,
			storeTime(at), spec.CompletionReceipt.Bytes(), authority.run.ID().String(), authority.profile.ID().String(),
			handlingID.String(), authority.run.HandlingAttempt(), authority.run.HandlingRecovery(),
			spec.ClaimFenceHash.Bytes(), storeTime(lease), string(authority.profile.Runtime()),
			storeTime(at), storeTime(at))
	} else if leaseSettled {
		finishedAt, _ := authority.run.FinishedAt()
		result, err = tx.ExecContext(ctx, `UPDATE agent_runs SET completion_at=?,
			completion_receipt_json=?,error=NULL WHERE run_id=? AND profile_id=? AND handling_id=?
			AND handling_attempt=? AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
			AND launcher='mnemond-wake' AND runtime_kind=? AND status=? AND finished_at=?
			AND wake_delivered_at<=? AND outcome_receipt_json IS NULL AND completion_at IS NULL
			AND completion_receipt_json IS NULL`, storeTime(at), spec.CompletionReceipt.Bytes(),
			authority.run.ID().String(), authority.profile.ID().String(), handlingID.String(),
			authority.run.HandlingAttempt(), authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(),
			storeTime(lease), string(authority.profile.Runtime()), string(authority.run.Status()),
			storeTime(finishedAt), storeTime(at))
	} else {
		result, err = tx.ExecContext(ctx, `UPDATE agent_runs SET status='runtime_finished',finished_at=?,
			completion_at=?,completion_receipt_json=?,error=NULL WHERE run_id=? AND profile_id=? AND handling_id=?
			AND handling_attempt=? AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
			AND launcher='mnemond-wake' AND runtime_kind=? AND status IN ('starting','running')
			AND wake_delivered_at<=? AND finished_at IS NULL AND completion_at IS NULL
			AND completion_receipt_json IS NULL AND outcome_receipt_json IS NULL`, storeTime(at),
			storeTime(at), spec.CompletionReceipt.Bytes(), authority.run.ID().String(), authority.profile.ID().String(),
			handlingID.String(), authority.run.HandlingAttempt(), authority.run.HandlingRecovery(),
			spec.ClaimFenceHash.Bytes(), storeTime(lease), string(authority.profile.Runtime()), storeTime(at))
	}
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("finish Agent Runtime: update: %w", err)
	}
	if err := requireExactlyOneRow(result, "finish Agent Runtime: AgentRun fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: finished AgentRun cannot be read",
			ErrAgentRuntimeInvariant)
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied, "finish Agent Runtime: commit")
}

// FailAgentRuntime atomically rejects any in-flight operation, finishes the
// exact Run, and either requeues or kills the Handling. Backoff is based on the
// durable failure time; response-loss replay never recomputes it.
func (s *Store) FailAgentRuntime(ctx context.Context,
	spec AgentRuntimeFailureSpec,
) (AgentRuntimeTransitionResult, error) {
	if err := validateAgentRuntimeObject("launcher diagnostic", spec.LauncherDiagnostic); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if err := validateAgentRuntimeObject("Runtime IDs", spec.RuntimeIDs); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if err := validateAgentRuntimeObject("completion receipt", spec.CompletionReceipt); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if err := validateAgentRuntimeError(spec.Error); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	authoritySpec := agentRuntimeAuthoritySpec{profileID: spec.ProfileID,
		expectedAssetRevision: spec.ExpectedAssetRevision, runID: spec.RunID,
		claimFenceHash: spec.ClaimFenceHash, handlingRecovery: spec.HandlingRecovery, at: spec.At}
	at, err := validateAgentRuntimeAuthorityInput(s, ctx, authoritySpec)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("fail Agent Runtime: begin: %w", err)
	}
	defer tx.Rollback()
	authority, err := readAgentRuntimeAuthority(ctx, tx, authoritySpec, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	authority, err = settleExpiredAgentRuntimeAuthority(ctx, tx, authority, at)
	if err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if receipt, finished := authority.run.CompletionReceipt(); finished {
		completionAt, hasCompletionAt := authority.run.CompletionAt()
		if !hasCompletionAt || completionAt.After(at) || receipt.String() != spec.CompletionReceipt.String() ||
			authority.run.LauncherDiagnostic().String() != spec.LauncherDiagnostic.String() ||
			authority.run.RuntimeIDs().String() != spec.RuntimeIDs.String() || authority.run.Error() != spec.Error {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: failure replay evidence differs",
				ErrAgentRuntimeInvariant)
		}
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeReplayed,
			"fail Agent Runtime: replay")
	}
	if agentRuntimeSemanticSettled(authority.run) {
		if _, launched := authority.run.RuntimeStartedAt(); launched &&
			(authority.run.LauncherDiagnostic().String() != spec.LauncherDiagnostic.String() ||
				authority.run.RuntimeIDs().String() != spec.RuntimeIDs.String()) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: failure changed launched Runtime evidence",
				ErrAgentRuntimeInvariant)
		}
		handlingID, _ := authority.run.HandlingID()
		lease, _ := authority.run.LeaseUntil()
		result, updateErr := tx.ExecContext(ctx, `UPDATE agent_runs SET launcher_diagnostic_json=?,
			runtime_ids_json=?,completion_at=?,completion_receipt_json=?,error=? WHERE run_id=? AND profile_id=?
			AND handling_id=? AND handling_attempt=? AND handling_recovery=? AND claim_fence_hash=?
			AND lease_until=? AND launcher='mnemond-wake' AND runtime_kind=?
			AND outcome_receipt_json IS NOT NULL AND completion_at IS NULL
			AND completion_receipt_json IS NULL AND finished_at<=?
			AND (wake_delivered_at IS NULL OR wake_delivered_at<=?)`,
			spec.LauncherDiagnostic.Bytes(), spec.RuntimeIDs.Bytes(), storeTime(at),
			spec.CompletionReceipt.Bytes(), spec.Error,
			authority.run.ID().String(), authority.profile.ID().String(), handlingID.String(),
			authority.run.HandlingAttempt(), authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(),
			storeTime(lease), string(authority.profile.Runtime()), storeTime(at), storeTime(at))
		if updateErr != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("fail Agent Runtime: append settled evidence: %w",
				updateErr)
		}
		if err := requireExactlyOneRow(result, "fail Agent Runtime: settled evidence fence"); err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
		}
		authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
		if err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: settled failure Run cannot be read",
				ErrAgentRuntimeInvariant)
		}
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied,
			"fail Agent Runtime: append settled evidence")
	}
	if agentRuntimeLeaseSettled(authority.run) {
		if _, launched := authority.run.RuntimeStartedAt(); launched &&
			(authority.run.LauncherDiagnostic().String() != spec.LauncherDiagnostic.String() ||
				authority.run.RuntimeIDs().String() != spec.RuntimeIDs.String()) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: late failure changed launched Runtime evidence",
				ErrAgentRuntimeInvariant)
		}
		if wakeDeliveredAt, delivered := authority.run.WakeDeliveredAt(); delivered && wakeDeliveredAt.After(at) {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: late failure precedes wake delivery",
				ErrAgentRuntimeInvariant)
		}
		handlingID, _ := authority.run.HandlingID()
		lease, _ := authority.run.LeaseUntil()
		finishedAt, _ := authority.run.FinishedAt()
		result, updateErr := tx.ExecContext(ctx, `UPDATE agent_runs SET launcher_diagnostic_json=?,
			runtime_ids_json=?,completion_at=?,completion_receipt_json=?,error=?
			WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
			AND handling_recovery=? AND claim_fence_hash=? AND lease_until=?
			AND launcher='mnemond-wake' AND runtime_kind=? AND status=? AND finished_at=?
			AND error=? AND outcome_receipt_json IS NULL AND completion_at IS NULL
			AND completion_receipt_json IS NULL AND (wake_delivered_at IS NULL OR wake_delivered_at<=?)`,
			spec.LauncherDiagnostic.Bytes(), spec.RuntimeIDs.Bytes(), storeTime(at),
			spec.CompletionReceipt.Bytes(), spec.Error, authority.run.ID().String(),
			authority.profile.ID().String(), handlingID.String(), authority.run.HandlingAttempt(),
			authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(), storeTime(lease),
			string(authority.profile.Runtime()), string(authority.run.Status()), storeTime(finishedAt),
			authority.run.Error(), storeTime(at))
		if updateErr != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("fail Agent Runtime: append late evidence: %w",
				updateErr)
		}
		if err := requireExactlyOneRow(result, "fail Agent Runtime: late evidence fence"); err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
		}
		authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
		if err != nil {
			return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: late failure Run cannot be read",
				ErrAgentRuntimeInvariant)
		}
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied,
			"fail Agent Runtime: append late evidence")
	}
	if authority.run.Status() != model.AgentRunStarting && authority.run.Status() != model.AgentRunRunning {
		return commitAgentRuntimeResult(tx, authority, AgentRuntimeAlreadySettled,
			"fail Agent Runtime: settled")
	}
	if err := requireLiveAgentRuntimeAuthority(authority, spec.ClaimFenceHash, at); err != nil {
		return AgentRuntimeTransitionResult{}, err
	}
	if _, launched := authority.run.RuntimeStartedAt(); launched &&
		(authority.run.LauncherDiagnostic().String() != spec.LauncherDiagnostic.String() ||
			authority.run.RuntimeIDs().String() != spec.RuntimeIDs.String()) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: failure changed launched Runtime evidence",
			ErrAgentRuntimeInvariant)
	}
	if wakeDeliveredAt, delivered := authority.run.WakeDeliveredAt(); delivered && wakeDeliveredAt.After(at) {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: failure time precedes wake delivery",
			ErrAgentRuntimeInvariant)
	}
	if err := rejectStartedManagedOperations(ctx, tx, managedOperationRejectionSpec{
		RunID: authority.run.ID(), ProfileID: authority.profile.ID(), ContextHash: spec.ClaimFenceHash, Code: "internal",
		Message: "managed Agent Runtime failed before operation completion", At: at,
	}); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: reject failed Runtime operations: %v",
			ErrAgentRuntimeInvariant, err)
	}
	dead := authority.handling.Attempts() >= uint32(authority.budget.Spec().MaxAttempts)
	runStatus := model.AgentRunFailed
	handlingStatus, disposition := model.HandlingPending, "runtime_failed"
	availableAt, deadAt := authority.handling.AvailableAt(), any(nil)
	if dead {
		runStatus, handlingStatus, disposition = model.AgentRunDead, model.HandlingDead,
			"attempt_budget_exhausted"
		deadAt = storeTime(at)
	} else {
		availableAt, err = agentClaimRetryAt(at, authority.handling.Attempts(), authority.budget)
		if err != nil {
			return AgentRuntimeTransitionResult{}, err
		}
	}
	handlingID, _ := authority.run.HandlingID()
	lease, _ := authority.run.LeaseUntil()
	result, err := tx.ExecContext(ctx, `UPDATE agent_runs SET status=?,launcher_diagnostic_json=?,
		runtime_ids_json=?,finished_at=?,completion_at=?,completion_receipt_json=?,error=?
		WHERE run_id=? AND profile_id=? AND handling_id=? AND handling_attempt=?
		AND handling_recovery=? AND claim_fence_hash=? AND lease_until=? AND launcher='mnemond-wake'
		AND runtime_kind=? AND status IN ('starting','running') AND finished_at IS NULL
		AND completion_at IS NULL AND completion_receipt_json IS NULL AND outcome_receipt_json IS NULL
		AND (wake_delivered_at IS NULL OR wake_delivered_at<=?)`, string(runStatus),
		spec.LauncherDiagnostic.Bytes(), spec.RuntimeIDs.Bytes(), storeTime(at), storeTime(at), spec.CompletionReceipt.Bytes(),
		spec.Error, authority.run.ID().String(), authority.profile.ID().String(), handlingID.String(),
		authority.run.HandlingAttempt(), authority.run.HandlingRecovery(), spec.ClaimFenceHash.Bytes(),
		storeTime(lease), string(authority.profile.Runtime()), storeTime(at))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("fail Agent Runtime: finish Run: %w", err)
	}
	if err := requireExactlyOneRow(result, "fail Agent Runtime: AgentRun fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
	}
	claimHash, _ := authority.handling.ClaimTokenHash()
	result, err = tx.ExecContext(ctx, `UPDATE agent_handlings SET status=?,available_at=?,
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,last_disposition=?,outcome_event_id=NULL,
		last_error=?,dead_at=?,updated_at=? WHERE handling_id=? AND profile_id=? AND event_id=?
		AND status='claimed' AND claim_owner=? AND claim_token_hash=? AND lease_until=? AND attempts=?
		AND recovery_count=? AND updated_at<=?`, string(handlingStatus), storeTime(availableAt), disposition,
		spec.Error, deadAt, storeTime(at), authority.handling.ID().String(), authority.profile.ID().String(),
		authority.handling.EventID().String(), authority.handling.ClaimOwner(), claimHash.Bytes(),
		storeTime(lease), authority.handling.Attempts(), authority.handling.RecoveryCount(), storeTime(at))
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("fail Agent Runtime: settle Handling: %w", err)
	}
	if err := requireExactlyOneRow(result, "fail Agent Runtime: Handling fence"); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: %v", ErrAgentRuntimeStale, err)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: failed AgentRun cannot be read",
			ErrAgentRuntimeInvariant)
	}
	authority.handling, err = readAgentHandling(ctx, tx, authority.handling.ID())
	if err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%w: settled Handling cannot be read",
			ErrAgentRuntimeInvariant)
	}
	return commitAgentRuntimeResult(tx, authority, AgentRuntimeApplied, "fail Agent Runtime: commit")
}

// RecoverDeadAgentHandlings starts a new explicit recovery generation after a
// successful setup self-check. Historical Runs remain immutable; attempts are
// reset only on the recovered Handling generation.
func (s *Store) RecoverDeadAgentHandlings(ctx context.Context,
	spec AgentDeadRecoverySpec,
) (AgentDeadRecoveryResult, error) {
	if s == nil || s.db == nil || ctx == nil || spec.Profile.ID() != model.TeamworkProfileID() ||
		!spec.Profile.Enabled() || spec.Profile.ActiveAssetRevision() == "" {
		return AgentDeadRecoveryResult{}, ErrAgentDeadRecoveryInput
	}
	at, err := canonicalClaimTime(spec.At)
	if err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: trusted time: %v", ErrAgentDeadRecoveryInput, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: begin: %w", err)
	}
	defer tx.Rollback()
	profile, _, err := requireAgentClaimAuthority(ctx, tx, spec.Profile.ID(),
		spec.Profile.ActiveAssetRevision())
	if err != nil || !sameProfileIdentity(profile, spec.Profile) ||
		!sameProfileAuthority(profile, spec.Profile) || !profile.UpdatedAt().Equal(spec.Profile.UpdatedAt()) {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: exact setup generation is not active",
			ErrAgentDeadRecoveryAuthority)
	}
	if at.Before(profile.UpdatedAt()) {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: trusted time precedes Profile generation",
			ErrAgentDeadRecoveryInvariant)
	}
	var invalid int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM agent_handlings
		WHERE profile_id=? AND status='dead' AND (updated_at>? OR claim_owner IS NOT NULL
		OR claim_token_hash IS NOT NULL OR lease_until IS NOT NULL OR outcome_event_id IS NOT NULL
		OR dead_at IS NULL OR attempts=0 OR recovery_count>=4294967295))`, profile.ID().String(),
		storeTime(at)).Scan(&invalid); err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: inspect: %w", err)
	}
	if invalid == 1 {
		return AgentDeadRecoveryResult{}, ErrAgentDeadRecoveryInvariant
	}
	var invalidRun int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM agent_handlings AS h
		LEFT JOIN agent_runs AS r ON r.profile_id=h.profile_id AND r.handling_id=h.handling_id
			AND r.handling_recovery=h.recovery_count AND r.handling_attempt=h.attempts
		WHERE h.profile_id=? AND h.status='dead' AND (
			r.run_id IS NULL
			OR NOT (
				r.status='dead'
				OR (h.last_disposition='attempt_budget_exhausted'
					AND h.last_error IS 'maximum handling attempts exhausted'
					AND r.status IN ('requeued','failed'))
			)
			OR (r.launcher='mnemond-wake' AND (
				NOT (r.completion_at IS NOT NULL AND r.completion_receipt_json IS NOT NULL
					AND r.completion_at<=?)
				AND NOT (r.runtime_started_at IS NULL
					AND r.launcher_diagnostic_json=X'7B7D' AND r.runtime_ids_json=X'7B7D'
					AND r.attached_at IS NULL AND r.wake_delivered_at IS NULL
					AND r.wake_receipt_json IS NULL AND r.current_read_receipt_json IS NULL
					AND r.outcome_receipt_json IS NULL AND r.completion_at IS NULL
					AND r.completion_receipt_json IS NULL)
			))
		)
	)`, profile.ID().String(), storeTime(at)).Scan(&invalidRun); err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: inspect terminal Run: %w", err)
	}
	if invalidRun == 1 {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: exact dead Run is not settled before recovery",
			ErrAgentDeadRecoveryInvariant)
	}
	result, err := tx.ExecContext(ctx, `UPDATE agent_handlings SET status='pending',available_at=?,
		claim_owner=NULL,claim_token_hash=NULL,lease_until=NULL,attempts=0,
		last_disposition='setup_recovered',outcome_event_id=NULL,last_error=NULL,
		recovery_count=recovery_count+1,dead_at=NULL,updated_at=?
		WHERE profile_id=? AND status='dead'`, storeTime(at), storeTime(at), profile.ID().String())
	if err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: update: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil || count < 0 || count > int64(^uint32(0)) {
		return AgentDeadRecoveryResult{}, fmt.Errorf("%w: invalid recovered row count",
			ErrAgentDeadRecoveryInvariant)
	}
	if err := tx.Commit(); err != nil {
		return AgentDeadRecoveryResult{}, fmt.Errorf("recover dead Agent Handlings: commit: %w", err)
	}
	return AgentDeadRecoveryResult{Recovered: uint32(count)}, nil
}

func validateAgentRuntimeAuthorityInput(s *Store, ctx context.Context,
	spec agentRuntimeAuthoritySpec,
) (time.Time, error) {
	if s == nil || s.db == nil || ctx == nil || spec.profileID != model.TeamworkProfileID() ||
		spec.expectedAssetRevision == "" || spec.runID.IsZero() || spec.claimFenceHash.IsZero() {
		return time.Time{}, ErrAgentRuntimeInput
	}
	at, err := canonicalClaimTime(spec.at)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: trusted time: %v", ErrAgentRuntimeInput, err)
	}
	return at, nil
}

func readAgentRuntimeAuthority(ctx context.Context, tx *sql.Tx, spec agentRuntimeAuthoritySpec,
	at time.Time,
) (agentRuntimeAuthority, error) {
	profile, budget, err := requireAgentClaimAuthority(ctx, tx, spec.profileID,
		spec.expectedAssetRevision)
	if err != nil {
		return agentRuntimeAuthority{}, err
	}
	node, err := readNode(ctx, tx)
	if err != nil || at.Before(profile.UpdatedAt()) || at.Before(node.UpdatedAt()) {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: trusted time precedes active authority",
			ErrAgentRuntimeInvariant)
	}
	run, err := readAgentRun(ctx, tx, spec.runID)
	if errors.Is(err, sql.ErrNoRows) {
		return agentRuntimeAuthority{}, ErrAgentRuntimeStale
	}
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: read AgentRun: %v", ErrAgentRuntimeInvariant, err)
	}
	handlingID, hasHandling := run.HandlingID()
	fence, hasFence := run.ClaimFenceHash()
	lease, hasLease := run.LeaseUntil()
	attachment, hasAttachment := run.AttachmentTokenHash()
	expiresAt, hasExpiry := run.AttachmentExpiresAt()
	if !hasHandling || !hasFence || !hasLease || !hasAttachment || !hasExpiry ||
		run.ProfileID() != profile.ID() || run.Runtime() != profile.Runtime() ||
		run.Launcher() != "mnemond-wake" || run.HandlingRecovery() != spec.handlingRecovery ||
		!sameCurrentDigest(fence, spec.claimFenceHash) || !sameCurrentDigest(attachment, fence) ||
		!expiresAt.Equal(lease) || at.Before(run.StartedAt()) {
		return agentRuntimeAuthority{}, ErrAgentRuntimeStale
	}
	handling, err := readAgentHandling(ctx, tx, handlingID)
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf("%w: read Handling: %v", ErrAgentRuntimeInvariant, err)
	}
	return agentRuntimeAuthority{profile: profile, budget: budget, run: run, handling: handling}, nil
}

func requireLiveAgentRuntimeAuthority(authority agentRuntimeAuthority, fence model.Digest,
	at time.Time,
) error {
	if err := requireExactCurrentClaim(authority.run, authority.handling, fence, at); err != nil {
		if errors.Is(err, ErrCurrentReadStale) {
			return ErrAgentRuntimeStale
		}
		return fmt.Errorf("%w: %v", ErrAgentRuntimeInvariant, err)
	}
	return nil
}

// settleExpiredAgentRuntimeAuthority makes every managed Runtime evidence
// boundary self-sufficient at the claim lease edge. A Runtime callback or
// completion may be the first Store call after expiry; in that case the same
// transaction first records the lease winner and then lets the caller append
// only the late evidence its transition permits. No second worker, readiness
// probe, or restart is required to make the historical Run consistent.
func settleExpiredAgentRuntimeAuthority(ctx context.Context, tx *sql.Tx,
	authority agentRuntimeAuthority, at time.Time,
) (agentRuntimeAuthority, error) {
	if !authority.run.Status().OperationAuthority() {
		return authority, nil
	}
	lease, hasLease := authority.run.LeaseUntil()
	if !hasLease {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: active managed Run has no claim lease", ErrAgentRuntimeInvariant)
	}
	if lease.After(at) {
		return authority, nil
	}
	fence, hasFence := authority.run.ClaimFenceHash()
	if !hasFence || fence.IsZero() {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: expired managed Run has no claim fence", ErrAgentRuntimeInvariant)
	}
	if err := requireExactActiveOrphan(authority, fence, at); err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: expired managed Run is not the current claim: %v", ErrAgentRuntimeInvariant, err)
	}
	busy, err := recoverExpiredAgentClaim(ctx, tx, authority.profile, authority.budget, at)
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: settle expired claim: %v", ErrAgentRuntimeInvariant, err)
	}
	if busy {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: expired managed claim remained active", ErrAgentRuntimeInvariant)
	}
	authority.run, err = readAgentRun(ctx, tx, authority.run.ID())
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: expired Run cannot be read", ErrAgentRuntimeInvariant)
	}
	authority.handling, err = readAgentHandling(ctx, tx, authority.handling.ID())
	if err != nil {
		return agentRuntimeAuthority{}, fmt.Errorf(
			"%w: expired Handling cannot be read", ErrAgentRuntimeInvariant)
	}
	return authority, nil
}

func agentRuntimeSemanticSettled(run model.AgentRun) bool {
	_, settled := run.OutcomeReceipt()
	return settled
}

func agentRuntimeLeaseSettled(run model.AgentRun) bool {
	if _, settled := run.OutcomeReceipt(); settled {
		return false
	}
	if _, completed := run.CompletionReceipt(); completed {
		return false
	}
	switch run.Status() {
	case model.AgentRunRequeued:
		return run.Error() == "claim lease expired"
	case model.AgentRunDead:
		return run.Error() == "claim lease expired after maximum attempts"
	default:
		return false
	}
}

func commitAgentRuntimeResult(tx *sql.Tx, authority agentRuntimeAuthority,
	status AgentRuntimeTransitionStatus, operation string,
) (AgentRuntimeTransitionResult, error) {
	if err := tx.Commit(); err != nil {
		return AgentRuntimeTransitionResult{}, fmt.Errorf("%s: %w", operation, err)
	}
	return AgentRuntimeTransitionResult{Status: status, Run: authority.run,
		Handling: authority.handling}, nil
}

func validateAgentRuntimeObject(field string, value model.JSON) error {
	if value.IsZero() || len(value.Bytes()) == 0 || value.Bytes()[0] != '{' ||
		len(value.Bytes()) > model.MaxContentBytes {
		return fmt.Errorf("%w: %s must be a canonical object within %d bytes",
			ErrAgentRuntimeInput, field, model.MaxContentBytes)
	}
	return nil
}

func validateAgentRuntimeError(value string) error {
	if value == "" || !utf8.ValidString(value) || len(value) > model.MaxContentBytes {
		return fmt.Errorf("%w: failure error must be non-empty UTF-8 within %d bytes",
			ErrAgentRuntimeInput, model.MaxContentBytes)
	}
	for _, character := range value {
		if character == 0 || (character < 0x20 && character != '\n' && character != '\t') {
			return fmt.Errorf("%w: failure error has a forbidden control character",
				ErrAgentRuntimeInput)
		}
	}
	return nil
}
