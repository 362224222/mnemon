package model

import "time"

// AgentRunStatus is the closed durable lifecycle of one managed Runtime
// attempt. runtime_finished is intentionally still an operation authority:
// the Runtime may have finished after mnemond accepted an operation whose
// durable receipt has not yet reached the client.
type AgentRunStatus string

const (
	AgentRunStarting        AgentRunStatus = "starting"
	AgentRunRunning         AgentRunStatus = "running"
	AgentRunRuntimeFinished AgentRunStatus = "runtime_finished"
	AgentRunOutcomeAccepted AgentRunStatus = "outcome_accepted"
	AgentRunRequeued        AgentRunStatus = "requeued"
	AgentRunRejected        AgentRunStatus = "rejected"
	AgentRunFailed          AgentRunStatus = "failed"
	AgentRunDead            AgentRunStatus = "dead"
)

func (s AgentRunStatus) Valid() bool {
	switch s {
	case AgentRunStarting, AgentRunRunning, AgentRunRuntimeFinished,
		AgentRunOutcomeAccepted, AgentRunRequeued, AgentRunRejected,
		AgentRunFailed, AgentRunDead:
		return true
	default:
		return false
	}
}

func (s AgentRunStatus) OperationAuthority() bool {
	return s == AgentRunStarting || s == AgentRunRunning || s == AgentRunRuntimeFinished
}

func (s AgentRunStatus) Terminal() bool {
	return s == AgentRunOutcomeAccepted || s == AgentRunRequeued ||
		s == AgentRunRejected || s == AgentRunFailed || s == AgentRunDead
}

type AgentRunSpec struct {
	ID                  RunID
	ProfileID           ProfileID
	HandlingID          *HandlingID
	Cause               JSON
	HandlingAttempt     uint32
	HandlingRecovery    uint32
	ClaimFenceHash      *Digest
	LeaseUntil          *time.Time
	AttachmentTokenHash *Digest
	AttachmentExpiresAt *time.Time
	AttachedAt          *time.Time
	Launcher            string
	Runtime             RuntimeKind
	LauncherDiagnostic  JSON
	RuntimeIDs          JSON
	Status              AgentRunStatus
	WakeDeliveredAt     *time.Time
	StartedAt           time.Time
	FinishedAt          *time.Time
	CompletionAt        *time.Time
	CurrentReadReceipt  *JSON
	OutcomeReceipt      *JSON
	CompletionReceipt   *JSON
	Error               string
}

// AgentRun is a validated immutable projection of one durable agent_runs row.
// Store transitions create a fresh value; callers cannot mutate optional
// evidence through pointers retained from AgentRunSpec.
type AgentRun struct {
	spec                  AgentRunSpec
	handlingID            HandlingID
	hasHandling           bool
	claimFenceHash        Digest
	leaseUntil            time.Time
	attachmentTokenHash   Digest
	attachmentExpiresAt   time.Time
	hasAttachment         bool
	attachedAt            time.Time
	hasAttachedAt         bool
	wakeDeliveredAt       time.Time
	hasWakeDeliveredAt    bool
	finishedAt            time.Time
	hasFinishedAt         bool
	completionAt          time.Time
	hasCompletionAt       bool
	currentReadReceipt    JSON
	hasCurrentReadReceipt bool
	outcomeReceipt        JSON
	hasOutcomeReceipt     bool
	completionReceipt     JSON
	hasCompletionReceipt  bool
}

func NewAgentRun(spec AgentRunSpec) (AgentRun, error) {
	if spec.ID.IsZero() || spec.ProfileID != TeamworkProfileID() {
		return AgentRun{}, invalid("AgentRun", "identity and default Profile are required")
	}
	if !spec.Status.Valid() || !spec.Runtime.Valid() {
		return AgentRun{}, invalid("AgentRun", "closed status and Runtime are required")
	}
	if err := validateIdentifier("AgentRun launcher", spec.Launcher); err != nil {
		return AgentRun{}, err
	}
	if err := validateAgentRunObject("AgentRun cause", spec.Cause); err != nil {
		return AgentRun{}, err
	}
	if err := validateAgentRunObject("AgentRun launcher diagnostic", spec.LauncherDiagnostic); err != nil {
		return AgentRun{}, err
	}
	if err := validateAgentRunObject("AgentRun Runtime IDs", spec.RuntimeIDs); err != nil {
		return AgentRun{}, err
	}
	if err := validateText("AgentRun error", spec.Error, MaxContentBytes, true); err != nil {
		return AgentRun{}, err
	}

	startedAt, err := canonicalTime(spec.StartedAt)
	if err != nil {
		return AgentRun{}, err
	}
	result := AgentRun{spec: spec}
	result.spec.StartedAt = startedAt
	result.spec.HandlingID, result.spec.ClaimFenceHash, result.spec.LeaseUntil = nil, nil, nil
	result.spec.AttachmentTokenHash, result.spec.AttachmentExpiresAt, result.spec.AttachedAt = nil, nil, nil
	result.spec.WakeDeliveredAt, result.spec.FinishedAt, result.spec.CompletionAt = nil, nil, nil
	result.spec.CurrentReadReceipt, result.spec.OutcomeReceipt, result.spec.CompletionReceipt = nil, nil, nil

	if spec.HandlingID != nil {
		if spec.HandlingID.IsZero() || spec.HandlingAttempt == 0 ||
			spec.ClaimFenceHash == nil || spec.ClaimFenceHash.IsZero() || spec.LeaseUntil == nil {
			return AgentRun{}, invariant("handling-bound AgentRun requires handling, attempt, fence and lease snapshot")
		}
		leaseUntil, err := canonicalTime(*spec.LeaseUntil)
		if err != nil {
			return AgentRun{}, err
		}
		if !leaseUntil.After(startedAt) {
			return AgentRun{}, invariant("AgentRun claim lease must end after start")
		}
		result.handlingID, result.hasHandling = *spec.HandlingID, true
		result.claimFenceHash, result.leaseUntil = *spec.ClaimFenceHash, leaseUntil
	} else if spec.HandlingAttempt != 0 || spec.HandlingRecovery != 0 ||
		spec.ClaimFenceHash != nil || spec.LeaseUntil != nil {
		return AgentRun{}, invariant("operation-scoped AgentRun cannot carry a handling claim snapshot")
	}

	if spec.AttachmentTokenHash != nil || spec.AttachmentExpiresAt != nil || spec.AttachedAt != nil {
		if spec.AttachmentTokenHash == nil || spec.AttachmentTokenHash.IsZero() || spec.AttachmentExpiresAt == nil {
			return AgentRun{}, invariant("AgentRun attachment requires token hash and expiry")
		}
		expiresAt, err := canonicalTime(*spec.AttachmentExpiresAt)
		if err != nil {
			return AgentRun{}, err
		}
		if !expiresAt.After(startedAt) {
			return AgentRun{}, invariant("AgentRun attachment must expire after start")
		}
		result.attachmentTokenHash, result.attachmentExpiresAt, result.hasAttachment =
			*spec.AttachmentTokenHash, expiresAt, true
		if spec.AttachedAt != nil {
			attachedAt, err := canonicalTime(*spec.AttachedAt)
			if err != nil {
				return AgentRun{}, err
			}
			if attachedAt.Before(startedAt) || attachedAt.After(expiresAt) {
				return AgentRun{}, invariant("AgentRun attachment time is outside its lifetime")
			}
			result.attachedAt, result.hasAttachedAt = attachedAt, true
		}
	}

	if spec.WakeDeliveredAt != nil {
		value, err := canonicalAgentRunEvidenceTime("wake delivery", *spec.WakeDeliveredAt, startedAt)
		if err != nil {
			return AgentRun{}, err
		}
		result.wakeDeliveredAt, result.hasWakeDeliveredAt = value, true
	}
	if spec.FinishedAt != nil {
		value, err := canonicalAgentRunEvidenceTime("finish", *spec.FinishedAt, startedAt)
		if err != nil {
			return AgentRun{}, err
		}
		result.finishedAt, result.hasFinishedAt = value, true
	}
	if result.hasWakeDeliveredAt && result.hasFinishedAt && result.finishedAt.Before(result.wakeDeliveredAt) {
		return AgentRun{}, invariant("AgentRun finish precedes wake delivery")
	}
	if spec.CompletionAt != nil {
		value, err := canonicalAgentRunEvidenceTime("Runtime completion", *spec.CompletionAt, startedAt)
		if err != nil {
			return AgentRun{}, err
		}
		if result.hasWakeDeliveredAt && value.Before(result.wakeDeliveredAt) {
			return AgentRun{}, invariant("AgentRun Runtime completion precedes wake delivery")
		}
		if result.hasFinishedAt && value.Before(result.finishedAt) {
			return AgentRun{}, invariant("AgentRun Runtime completion precedes finish")
		}
		result.completionAt, result.hasCompletionAt = value, true
	}
	if spec.Status == AgentRunStarting || spec.Status == AgentRunRunning {
		if result.hasFinishedAt {
			return AgentRun{}, invariant("active AgentRun cannot have a finish time")
		}
	} else if !result.hasFinishedAt {
		return AgentRun{}, invariant("finished AgentRun status requires a finish time")
	}

	var receiptErr error
	result.currentReadReceipt, result.hasCurrentReadReceipt, receiptErr = optionalAgentRunObject(
		"AgentRun current-read receipt", spec.CurrentReadReceipt)
	if receiptErr != nil {
		return AgentRun{}, receiptErr
	}
	result.outcomeReceipt, result.hasOutcomeReceipt, receiptErr = optionalAgentRunObject(
		"AgentRun outcome receipt", spec.OutcomeReceipt)
	if receiptErr != nil {
		return AgentRun{}, receiptErr
	}
	result.completionReceipt, result.hasCompletionReceipt, receiptErr = optionalAgentRunObject(
		"AgentRun completion receipt", spec.CompletionReceipt)
	if receiptErr != nil {
		return AgentRun{}, receiptErr
	}
	if result.hasCompletionAt != result.hasCompletionReceipt {
		return AgentRun{}, invariant("AgentRun Runtime completion time and receipt must be recorded together")
	}
	if (spec.Status == AgentRunStarting || spec.Status == AgentRunRunning) && result.hasCompletionAt {
		return AgentRun{}, invariant("active AgentRun cannot have Runtime completion evidence")
	}
	if spec.Status == AgentRunRuntimeFinished &&
		(!result.hasCompletionAt || !result.completionAt.Equal(result.finishedAt)) {
		return AgentRun{}, invariant("runtime_finished AgentRun requires completion at its finish time")
	}
	return result, nil
}

func validateAgentRunObject(field string, value JSON) error {
	if value.IsZero() || value.String()[0] != '{' {
		return invalid(field, "must be a canonical JSON object")
	}
	return nil
}

func optionalAgentRunObject(field string, value *JSON) (JSON, bool, error) {
	if value == nil {
		return JSON{}, false, nil
	}
	if err := validateAgentRunObject(field, *value); err != nil {
		return JSON{}, false, err
	}
	return *value, true, nil
}

func canonicalAgentRunEvidenceTime(field string, value, startedAt time.Time) (time.Time, error) {
	canonical, err := canonicalTime(value)
	if err != nil {
		return time.Time{}, err
	}
	if canonical.Before(startedAt) {
		return time.Time{}, invariant("AgentRun " + field + " precedes start")
	}
	return canonical, nil
}

func (r AgentRun) ID() RunID                           { return r.spec.ID }
func (r AgentRun) ProfileID() ProfileID                { return r.spec.ProfileID }
func (r AgentRun) Cause() JSON                         { return r.spec.Cause }
func (r AgentRun) HandlingID() (HandlingID, bool)      { return r.handlingID, r.hasHandling }
func (r AgentRun) HandlingAttempt() uint32             { return r.spec.HandlingAttempt }
func (r AgentRun) HandlingRecovery() uint32            { return r.spec.HandlingRecovery }
func (r AgentRun) ClaimFenceHash() (Digest, bool)      { return r.claimFenceHash, r.hasHandling }
func (r AgentRun) LeaseUntil() (time.Time, bool)       { return r.leaseUntil, r.hasHandling }
func (r AgentRun) AttachmentTokenHash() (Digest, bool) { return r.attachmentTokenHash, r.hasAttachment }
func (r AgentRun) AttachmentExpiresAt() (time.Time, bool) {
	return r.attachmentExpiresAt, r.hasAttachment
}
func (r AgentRun) AttachedAt() (time.Time, bool)      { return r.attachedAt, r.hasAttachedAt }
func (r AgentRun) Launcher() string                   { return r.spec.Launcher }
func (r AgentRun) Runtime() RuntimeKind               { return r.spec.Runtime }
func (r AgentRun) LauncherDiagnostic() JSON           { return r.spec.LauncherDiagnostic }
func (r AgentRun) RuntimeIDs() JSON                   { return r.spec.RuntimeIDs }
func (r AgentRun) Status() AgentRunStatus             { return r.spec.Status }
func (r AgentRun) WakeDeliveredAt() (time.Time, bool) { return r.wakeDeliveredAt, r.hasWakeDeliveredAt }
func (r AgentRun) StartedAt() time.Time               { return r.spec.StartedAt }
func (r AgentRun) FinishedAt() (time.Time, bool)      { return r.finishedAt, r.hasFinishedAt }
func (r AgentRun) CompletionAt() (time.Time, bool)    { return r.completionAt, r.hasCompletionAt }
func (r AgentRun) CurrentReadReceipt() (JSON, bool) {
	return r.currentReadReceipt, r.hasCurrentReadReceipt
}
func (r AgentRun) OutcomeReceipt() (JSON, bool) { return r.outcomeReceipt, r.hasOutcomeReceipt }
func (r AgentRun) CompletionReceipt() (JSON, bool) {
	return r.completionReceipt, r.hasCompletionReceipt
}
func (r AgentRun) Error() string { return r.spec.Error }
