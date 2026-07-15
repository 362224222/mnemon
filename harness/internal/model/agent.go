package model

import "time"

type OperationKind string

const (
	OperationTeamworkOffer   OperationKind = "teamwork.offer"
	OperationTeamworkAccept  OperationKind = "teamwork.accept"
	OperationTeamworkDecline OperationKind = "teamwork.decline"
	OperationTeamworkDeliver OperationKind = "teamwork.deliver"
	OperationTeamworkRework  OperationKind = "teamwork.rework"
	OperationTeamworkClose   OperationKind = "teamwork.close"
	OperationTeamworkCancel  OperationKind = "teamwork.cancel"
	OperationResolveNoAction OperationKind = "agent.resolve.no-action"
	OperationResolveRetry    OperationKind = "agent.resolve.retry"
	OperationResolveReject   OperationKind = "agent.resolve.reject"
)

func (k OperationKind) Valid() bool {
	switch k {
	case OperationTeamworkOffer, OperationTeamworkAccept, OperationTeamworkDecline,
		OperationTeamworkDeliver, OperationTeamworkRework, OperationTeamworkClose,
		OperationTeamworkCancel, OperationResolveNoAction, OperationResolveRetry,
		OperationResolveReject:
		return true
	default:
		return false
	}
}

type OperationStatus string

const (
	OperationStarted   OperationStatus = "started"
	OperationCommitted OperationStatus = "committed"
	OperationRejected  OperationStatus = "rejected"
)

func (s OperationStatus) Valid() bool {
	return s == OperationStarted || s == OperationCommitted || s == OperationRejected
}

func (s OperationStatus) Terminal() bool { return s == OperationCommitted || s == OperationRejected }

type OperationSpec struct {
	ID            OperationID
	ProfileID     ProfileID
	AgentRunID    RunID
	ClientKeyHash Digest
	ContextHash   *Digest
	Kind          OperationKind
	RequestDigest Digest
	Status        OperationStatus
	LeaseOwner    string
	LeaseUntil    *time.Time
	Capture       *JSON
	Result        *JSON
	CreatedAt     time.Time
	FinishedAt    *time.Time
}

type Operation struct {
	spec          OperationSpec
	contextHash   Digest
	hasContext    bool
	leaseUntil    time.Time
	hasLease      bool
	capture       JSON
	hasCapture    bool
	result        JSON
	hasResult     bool
	finishedAt    time.Time
	hasFinishedAt bool
}

func NewOperation(spec OperationSpec) (Operation, error) {
	if spec.ID.IsZero() || spec.ProfileID != TeamworkProfileID() || spec.AgentRunID.IsZero() ||
		spec.ClientKeyHash.IsZero() || spec.RequestDigest.IsZero() {
		return Operation{}, invalid("operation", "identity, AgentRun, default Profile and request digests are required")
	}
	if !spec.Kind.Valid() || !spec.Status.Valid() {
		return Operation{}, invalid("operation", "unknown kind or status")
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Operation{}, err
	}
	result := Operation{spec: spec}
	result.spec.CreatedAt = createdAt
	result.spec.ContextHash, result.spec.LeaseUntil = nil, nil
	result.spec.Capture, result.spec.Result, result.spec.FinishedAt = nil, nil, nil
	if spec.ContextHash != nil {
		if spec.ContextHash.IsZero() {
			return Operation{}, invalid("context hash", "must not be zero")
		}
		result.contextHash, result.hasContext = *spec.ContextHash, true
	} else if spec.Kind != OperationTeamworkOffer {
		return Operation{}, invariant("only teamwork.offer may be contextless initiate")
	}
	if spec.Capture != nil {
		if spec.Capture.IsZero() || spec.Capture.raw[0] != '{' {
			return Operation{}, invalid("capture checkpoint", "must be a canonical JSON object")
		}
		result.capture, result.hasCapture = *spec.Capture, true
	}
	if spec.Status == OperationStarted {
		if spec.LeaseOwner == "" || spec.LeaseUntil == nil || spec.Result != nil || spec.FinishedAt != nil {
			return Operation{}, invariant("started operation requires only an active lease")
		}
		if err := validateIdentifier("operation lease owner", spec.LeaseOwner); err != nil {
			return Operation{}, err
		}
		leaseUntil, err := canonicalTime(*spec.LeaseUntil)
		if err != nil {
			return Operation{}, err
		}
		if !leaseUntil.After(createdAt) {
			return Operation{}, invariant("operation lease must end after creation")
		}
		result.leaseUntil, result.hasLease = leaseUntil, true
	} else {
		if spec.LeaseOwner != "" || spec.LeaseUntil != nil || spec.Result == nil || spec.FinishedAt == nil {
			return Operation{}, invariant("terminal operation requires result/finish and no lease")
		}
		if spec.Result.IsZero() || spec.Result.raw[0] != '{' {
			return Operation{}, invalid("operation result", "must be a canonical JSON object")
		}
		finishedAt, err := canonicalTime(*spec.FinishedAt)
		if err != nil {
			return Operation{}, err
		}
		if finishedAt.Before(createdAt) {
			return Operation{}, invariant("operation finish precedes creation")
		}
		result.result, result.hasResult = *spec.Result, true
		result.finishedAt, result.hasFinishedAt = finishedAt, true
	}
	return result, nil
}

func (o Operation) ID() OperationID               { return o.spec.ID }
func (o Operation) ProfileID() ProfileID          { return o.spec.ProfileID }
func (o Operation) AgentRunID() RunID             { return o.spec.AgentRunID }
func (o Operation) ClientKeyHash() Digest         { return o.spec.ClientKeyHash }
func (o Operation) ContextHash() (Digest, bool)   { return o.contextHash, o.hasContext }
func (o Operation) Kind() OperationKind           { return o.spec.Kind }
func (o Operation) RequestDigest() Digest         { return o.spec.RequestDigest }
func (o Operation) Status() OperationStatus       { return o.spec.Status }
func (o Operation) LeaseOwner() string            { return o.spec.LeaseOwner }
func (o Operation) LeaseUntil() (time.Time, bool) { return o.leaseUntil, o.hasLease }
func (o Operation) Capture() (JSON, bool)         { return o.capture, o.hasCapture }
func (o Operation) Result() (JSON, bool)          { return o.result, o.hasResult }
func (o Operation) CreatedAt() time.Time          { return o.spec.CreatedAt }
func (o Operation) FinishedAt() (time.Time, bool) { return o.finishedAt, o.hasFinishedAt }

type HandlingStatus string

const (
	HandlingPending   HandlingStatus = "pending"
	HandlingClaimed   HandlingStatus = "claimed"
	HandlingCompleted HandlingStatus = "completed"
	HandlingRejected  HandlingStatus = "rejected"
	HandlingDead      HandlingStatus = "dead"
)

func (s HandlingStatus) Valid() bool {
	return s == HandlingPending || s == HandlingClaimed || s == HandlingCompleted || s == HandlingRejected || s == HandlingDead
}

func (s HandlingStatus) Terminal() bool {
	return s == HandlingCompleted || s == HandlingRejected || s == HandlingDead
}

type HandlingSpec struct {
	ID              HandlingID
	ProfileID       ProfileID
	EventID         EventID
	Status          HandlingStatus
	Priority        int
	AvailableAt     time.Time
	ClaimOwner      string
	ClaimTokenHash  *Digest
	LeaseUntil      *time.Time
	Attempts        uint32
	LastDisposition string
	OutcomeEventID  *EventID
	LastError       string
	RecoveryCount   uint32
	DeadAt          *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Handling struct {
	spec           HandlingSpec
	claimTokenHash Digest
	hasClaim       bool
	leaseUntil     time.Time
	outcomeEventID EventID
	hasOutcome     bool
	deadAt         time.Time
	hasDeadAt      bool
}

func NewHandling(spec HandlingSpec) (Handling, error) {
	if spec.ID.IsZero() || spec.ProfileID != TeamworkProfileID() || spec.EventID.IsZero() || !spec.Status.Valid() {
		return Handling{}, invalid("handling", "identity, default Profile and closed status are required")
	}
	if err := validateText("last disposition", spec.LastDisposition, MaxIdentifierBytes, true); err != nil {
		return Handling{}, err
	}
	if err := validateText("last error", spec.LastError, MaxContentBytes, true); err != nil {
		return Handling{}, err
	}
	availableAt, err := canonicalTime(spec.AvailableAt)
	if err != nil {
		return Handling{}, err
	}
	createdAt, err := canonicalTime(spec.CreatedAt)
	if err != nil {
		return Handling{}, err
	}
	updatedAt, err := canonicalTime(spec.UpdatedAt)
	if err != nil {
		return Handling{}, err
	}
	if updatedAt.Before(createdAt) {
		return Handling{}, invariant("handling update precedes creation")
	}
	result := Handling{spec: spec}
	result.spec.AvailableAt, result.spec.CreatedAt, result.spec.UpdatedAt = availableAt, createdAt, updatedAt
	result.spec.ClaimTokenHash, result.spec.LeaseUntil = nil, nil
	result.spec.OutcomeEventID, result.spec.DeadAt = nil, nil
	if spec.Status == HandlingClaimed {
		if spec.ClaimOwner == "" || spec.ClaimTokenHash == nil || spec.ClaimTokenHash.IsZero() || spec.LeaseUntil == nil || spec.Attempts == 0 {
			return Handling{}, invariant("claimed handling requires owner, token, lease and positive attempt")
		}
		if err := validateIdentifier("claim owner", spec.ClaimOwner); err != nil {
			return Handling{}, err
		}
		leaseUntil, err := canonicalTime(*spec.LeaseUntil)
		if err != nil {
			return Handling{}, err
		}
		if !leaseUntil.After(updatedAt) {
			return Handling{}, invariant("handling claim lease must end after its update time")
		}
		result.claimTokenHash, result.hasClaim = *spec.ClaimTokenHash, true
		result.leaseUntil = leaseUntil
	} else if spec.ClaimOwner != "" || spec.ClaimTokenHash != nil || spec.LeaseUntil != nil {
		return Handling{}, invariant("unclaimed handling cannot retain claim authority")
	}
	if spec.OutcomeEventID != nil {
		if spec.OutcomeEventID.IsZero() {
			return Handling{}, invalid("outcome Event ID", "must not be zero")
		}
		if spec.Status != HandlingCompleted {
			return Handling{}, invariant("only completed handling may reference an outcome Event")
		}
		result.outcomeEventID, result.hasOutcome = *spec.OutcomeEventID, true
	}
	if spec.Status == HandlingDead {
		if spec.DeadAt == nil || spec.Attempts == 0 {
			return Handling{}, invariant("dead handling requires death time and a prior attempt")
		}
		deadAt, err := canonicalTime(*spec.DeadAt)
		if err != nil {
			return Handling{}, err
		}
		if deadAt.Before(createdAt) || updatedAt.Before(deadAt) {
			return Handling{}, invariant("handling death time must fall between creation and update")
		}
		result.deadAt, result.hasDeadAt = deadAt, true
	} else if spec.DeadAt != nil {
		return Handling{}, invariant("non-dead handling cannot carry dead_at")
	}
	return result, nil
}

func (h Handling) ID() HandlingID                  { return h.spec.ID }
func (h Handling) ProfileID() ProfileID            { return h.spec.ProfileID }
func (h Handling) EventID() EventID                { return h.spec.EventID }
func (h Handling) Status() HandlingStatus          { return h.spec.Status }
func (h Handling) Priority() int                   { return h.spec.Priority }
func (h Handling) AvailableAt() time.Time          { return h.spec.AvailableAt }
func (h Handling) ClaimOwner() string              { return h.spec.ClaimOwner }
func (h Handling) ClaimTokenHash() (Digest, bool)  { return h.claimTokenHash, h.hasClaim }
func (h Handling) LeaseUntil() (time.Time, bool)   { return h.leaseUntil, h.hasClaim }
func (h Handling) Attempts() uint32                { return h.spec.Attempts }
func (h Handling) LastDisposition() string         { return h.spec.LastDisposition }
func (h Handling) OutcomeEventID() (EventID, bool) { return h.outcomeEventID, h.hasOutcome }
func (h Handling) LastError() string               { return h.spec.LastError }
func (h Handling) RecoveryCount() uint32           { return h.spec.RecoveryCount }
func (h Handling) DeadAt() (time.Time, bool)       { return h.deadAt, h.hasDeadAt }
func (h Handling) CreatedAt() time.Time            { return h.spec.CreatedAt }
func (h Handling) UpdatedAt() time.Time            { return h.spec.UpdatedAt }
