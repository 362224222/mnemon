package store

import (
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// PeerInboxSemanticDisposition is the closed terminal result accepted by the
// Store. A transient Teamwork retry is deliberately absent: the controller
// must release the claim through RetryPeerInboxSemantic instead of committing
// a terminal decision.
type PeerInboxSemanticDisposition string

const (
	PeerInboxSemanticApply       PeerInboxSemanticDisposition = "apply"
	PeerInboxSemanticReject      PeerInboxSemanticDisposition = "reject"
	PeerInboxSemanticConflict    PeerInboxSemanticDisposition = "conflict"
	PeerInboxSemanticReceiptOnly PeerInboxSemanticDisposition = "receipt_only"
)

func (disposition PeerInboxSemanticDisposition) Valid() bool {
	switch disposition {
	case PeerInboxSemanticApply, PeerInboxSemanticReject,
		PeerInboxSemanticConflict, PeerInboxSemanticReceiptOnly:
		return true
	default:
		return false
	}
}

func (disposition PeerInboxSemanticDisposition) inboxStatus() model.InboxStatus {
	switch disposition {
	case PeerInboxSemanticApply, PeerInboxSemanticReceiptOnly:
		return model.InboxAccepted
	case PeerInboxSemanticReject:
		return model.InboxRejected
	case PeerInboxSemanticConflict:
		return model.InboxConflicted
	default:
		return ""
	}
}

// PeerInboxSemanticEffectSource identifies the immutable Event that supplies
// authority for a planned Work or Handling effect.
type PeerInboxSemanticEffectSource string

const (
	PeerInboxSemanticFromImportedEvent PeerInboxSemanticEffectSource = "imported_event"
	PeerInboxSemanticFromLocalResponse PeerInboxSemanticEffectSource = "local_response"
)

func (source PeerInboxSemanticEffectSource) Valid() bool {
	return source == PeerInboxSemanticFromImportedEvent ||
		source == PeerInboxSemanticFromLocalResponse
}

type PeerInboxSemanticWorkIntentSpec struct {
	Source             PeerInboxSemanticEffectSource
	ResponseOrdinal    uint8
	WorkRef            model.WorkRef
	ChannelID          model.ChannelID
	Participants       model.ParticipantSnapshot
	ExpectedVersion    uint64
	ExpectedState      model.WorkState
	ExpectedIteration  uint8
	NextVersion        uint64
	NextState          model.WorkState
	NextIteration      uint8
	DeadlineUnixNano   int64
	StateData          model.JSON
	ObservedAtUnixNano int64
}

type PeerInboxSemanticWorkIntent struct {
	spec PeerInboxSemanticWorkIntentSpec
}

func (intent PeerInboxSemanticWorkIntent) Source() PeerInboxSemanticEffectSource {
	return intent.spec.Source
}
func (intent PeerInboxSemanticWorkIntent) ResponseOrdinal() uint8 {
	return intent.spec.ResponseOrdinal
}
func (intent PeerInboxSemanticWorkIntent) WorkRef() model.WorkRef { return intent.spec.WorkRef }
func (intent PeerInboxSemanticWorkIntent) ChannelID() model.ChannelID {
	return intent.spec.ChannelID
}
func (intent PeerInboxSemanticWorkIntent) Participants() model.ParticipantSnapshot {
	return intent.spec.Participants
}
func (intent PeerInboxSemanticWorkIntent) ExpectedVersion() uint64 {
	return intent.spec.ExpectedVersion
}
func (intent PeerInboxSemanticWorkIntent) ExpectedState() model.WorkState {
	return intent.spec.ExpectedState
}
func (intent PeerInboxSemanticWorkIntent) ExpectedIteration() uint8 {
	return intent.spec.ExpectedIteration
}
func (intent PeerInboxSemanticWorkIntent) NextVersion() uint64 { return intent.spec.NextVersion }
func (intent PeerInboxSemanticWorkIntent) NextState() model.WorkState {
	return intent.spec.NextState
}
func (intent PeerInboxSemanticWorkIntent) NextIteration() uint8 {
	return intent.spec.NextIteration
}
func (intent PeerInboxSemanticWorkIntent) DeadlineUnixNano() int64 {
	return intent.spec.DeadlineUnixNano
}
func (intent PeerInboxSemanticWorkIntent) StateData() model.JSON { return intent.spec.StateData }
func (intent PeerInboxSemanticWorkIntent) ObservedAtUnixNano() int64 {
	return intent.spec.ObservedAtUnixNano
}
func (intent PeerInboxSemanticWorkIntent) IsCreation() bool {
	return intent.spec.ExpectedVersion == 0
}

type PeerInboxSemanticHandlingIntentSpec struct {
	Source          PeerInboxSemanticEffectSource
	ResponseOrdinal uint8
	WorkRef         model.WorkRef
	LocalRole       model.WorkRole
	SourceEventType model.EventType
}

type PeerInboxSemanticHandlingIntent struct {
	spec PeerInboxSemanticHandlingIntentSpec
}

func (intent PeerInboxSemanticHandlingIntent) Source() PeerInboxSemanticEffectSource {
	return intent.spec.Source
}
func (intent PeerInboxSemanticHandlingIntent) ResponseOrdinal() uint8 {
	return intent.spec.ResponseOrdinal
}
func (intent PeerInboxSemanticHandlingIntent) WorkRef() model.WorkRef { return intent.spec.WorkRef }
func (intent PeerInboxSemanticHandlingIntent) LocalRole() model.WorkRole {
	return intent.spec.LocalRole
}
func (intent PeerInboxSemanticHandlingIntent) SourceEventType() model.EventType {
	return intent.spec.SourceEventType
}

type PeerInboxSemanticHandlingSettlementSpec struct {
	WorkRef       model.WorkRef
	SourceEventID model.EventID
	Disposition   PeerInboxSemanticSettlementDisposition
}

type PeerInboxSemanticSettlementDisposition string

const (
	PeerInboxSemanticSupersededCancelled PeerInboxSemanticSettlementDisposition = "superseded_cancelled"
	PeerInboxSemanticSupersededExpired   PeerInboxSemanticSettlementDisposition = "superseded_expired"
)

func (disposition PeerInboxSemanticSettlementDisposition) Valid() bool {
	return disposition == PeerInboxSemanticSupersededCancelled ||
		disposition == PeerInboxSemanticSupersededExpired
}

type PeerInboxSemanticHandlingSettlement struct {
	spec PeerInboxSemanticHandlingSettlementSpec
}

func (settlement PeerInboxSemanticHandlingSettlement) WorkRef() model.WorkRef {
	return settlement.spec.WorkRef
}
func (settlement PeerInboxSemanticHandlingSettlement) SourceEventID() model.EventID {
	return settlement.spec.SourceEventID
}
func (settlement PeerInboxSemanticHandlingSettlement) Disposition() PeerInboxSemanticSettlementDisposition {
	return settlement.spec.Disposition
}

type PeerInboxSemanticResponseIntentSpec struct {
	EventType model.EventType
	Payload   model.JSON
	Cause     model.EventKey
}

type PeerInboxSemanticResponseIntent struct {
	spec PeerInboxSemanticResponseIntentSpec
}

func (intent PeerInboxSemanticResponseIntent) EventType() model.EventType {
	return intent.spec.EventType
}
func (intent PeerInboxSemanticResponseIntent) Payload() model.JSON { return intent.spec.Payload }
func (intent PeerInboxSemanticResponseIntent) Cause() model.EventKey {
	return intent.spec.Cause
}

type PeerInboxSemanticPlanSpec struct {
	Disposition PeerInboxSemanticDisposition
	Diagnostic  string
	Work        *PeerInboxSemanticWorkIntentSpec
	Handling    *PeerInboxSemanticHandlingIntentSpec
	Settlement  *PeerInboxSemanticHandlingSettlementSpec
	Responses   []PeerInboxSemanticResponseIntentSpec
}

// PeerInboxSemanticPlan is an immutable Store-owned materialization plan. It
// is bound to the exact claimed attempt and snapshot, but not to lease_until,
// so a renewed fence for the same attempt may commit it.
type PeerInboxSemanticPlan struct {
	inboxID        model.InboxID
	attempt        uint32
	snapshotDigest model.Digest
	decisionAt     time.Time
	disposition    PeerInboxSemanticDisposition
	diagnostic     string
	work           PeerInboxSemanticWorkIntent
	hasWork        bool
	handling       PeerInboxSemanticHandlingIntent
	hasHandling    bool
	settlement     PeerInboxSemanticHandlingSettlement
	hasSettlement  bool
	responses      []PeerInboxSemanticResponseIntent
}

// NewPeerInboxSemanticPlan validates and freezes one policy result outside a
// Store transaction. CommitPeerInboxSemantic independently revalidates every
// effect against the live durable snapshot before applying it.
func NewPeerInboxSemanticPlan(claim PeerInboxSemanticClaim, decisionAt time.Time,
	spec PeerInboxSemanticPlanSpec,
) (PeerInboxSemanticPlan, error) {
	at, err := canonicalStoreTime(decisionAt)
	if err != nil || at.IsZero() || claim.InboxID().IsZero() || claim.Fence().attempt == 0 ||
		claim.SnapshotDigest().IsZero() || claim.ImportedEvent().ID().IsZero() {
		return PeerInboxSemanticPlan{}, fmt.Errorf("%w: semantic plan authority or time",
			ErrPeerInboxSemanticInput)
	}
	plan := PeerInboxSemanticPlan{inboxID: claim.InboxID(), attempt: claim.Fence().attempt,
		snapshotDigest: claim.SnapshotDigest(), decisionAt: at,
		disposition: spec.Disposition, diagnostic: spec.Diagnostic}
	if spec.Work != nil {
		plan.work, plan.hasWork = PeerInboxSemanticWorkIntent{spec: *spec.Work}, true
	}
	if spec.Handling != nil {
		plan.handling, plan.hasHandling = PeerInboxSemanticHandlingIntent{spec: *spec.Handling}, true
	}
	if spec.Settlement != nil {
		plan.settlement = PeerInboxSemanticHandlingSettlement{spec: *spec.Settlement}
		plan.hasSettlement = true
	}
	plan.responses = make([]PeerInboxSemanticResponseIntent, len(spec.Responses))
	for index, response := range spec.Responses {
		plan.responses[index] = PeerInboxSemanticResponseIntent{spec: response}
	}
	if err := validatePeerInboxSemanticFrozenPlan(claim, plan); err != nil {
		return PeerInboxSemanticPlan{}, err
	}
	return plan, nil
}

func (plan PeerInboxSemanticPlan) Disposition() PeerInboxSemanticDisposition {
	return plan.disposition
}
func (plan PeerInboxSemanticPlan) InboxStatus() model.InboxStatus {
	return plan.disposition.inboxStatus()
}
func (plan PeerInboxSemanticPlan) Diagnostic() string { return plan.diagnostic }
func (plan PeerInboxSemanticPlan) DecisionAt() time.Time {
	return plan.decisionAt
}
func (plan PeerInboxSemanticPlan) Work() (PeerInboxSemanticWorkIntent, bool) {
	return plan.work, plan.hasWork
}
func (plan PeerInboxSemanticPlan) Handling() (PeerInboxSemanticHandlingIntent, bool) {
	return plan.handling, plan.hasHandling
}
func (plan PeerInboxSemanticPlan) Settlement() (PeerInboxSemanticHandlingSettlement, bool) {
	return plan.settlement, plan.hasSettlement
}
func (plan PeerInboxSemanticPlan) Responses() []PeerInboxSemanticResponseIntent {
	return append([]PeerInboxSemanticResponseIntent(nil), plan.responses...)
}

func validatePeerInboxSemanticFrozenPlan(claim PeerInboxSemanticClaim,
	plan PeerInboxSemanticPlan,
) error {
	if err := validatePeerInboxSemanticTerminalShape(plan); err != nil {
		return err
	}
	if err := validatePeerInboxSemanticResponseIntents(plan.responses); err != nil {
		return err
	}
	if err := validatePeerInboxSemanticEffectIntents(plan); err != nil {
		return err
	}
	snapshot := peerInboxSemanticSnapshot{importedEvent: claim.ImportedEvent(),
		digest: claim.SnapshotDigest()}
	if current, ok := claim.CurrentWork(); ok {
		snapshot.currentWork, snapshot.hasCurrentWork = current, true
	}
	if err := validatePeerInboxSemanticWorkPredecessor(snapshot, plan); err != nil {
		return fmt.Errorf("%w: %v", ErrPeerInboxSemanticInput, err)
	}
	return nil
}

func validatePeerInboxSemanticTerminalShape(plan PeerInboxSemanticPlan) error {
	accepted := plan.InboxStatus() == model.InboxAccepted
	if !plan.disposition.Valid() || len(plan.responses) > 2 ||
		accepted != (plan.diagnostic == "") ||
		!accepted && !validPublicationDiagnostic(plan.diagnostic) {
		return fmt.Errorf("%w: invalid terminal semantic plan", ErrPeerInboxSemanticInput)
	}
	return nil
}

func validatePeerInboxSemanticResponseIntents(
	responses []PeerInboxSemanticResponseIntent,
) error {
	for _, response := range responses {
		if !response.EventType().ControllerAdmitted() || response.Payload().IsZero() ||
			response.Cause().IsZero() {
			return fmt.Errorf("%w: invalid semantic response intent", ErrPeerInboxSemanticInput)
		}
	}
	return nil
}

func validatePeerInboxSemanticEffectIntents(plan PeerInboxSemanticPlan) error {
	responseCount := len(plan.responses)
	validWork := !plan.hasWork || validPeerInboxSemanticWorkIntent(plan.work, responseCount)
	validHandling := !plan.hasHandling ||
		validPeerInboxSemanticHandlingIntent(plan.handling, responseCount)
	validSettlement := !plan.hasSettlement ||
		!plan.settlement.WorkRef().IsZero() && !plan.settlement.SourceEventID().IsZero() &&
			plan.settlement.Disposition().Valid()
	if !validWork || !validHandling || !validSettlement {
		return fmt.Errorf("%w: invalid semantic effect intent", ErrPeerInboxSemanticInput)
	}
	return nil
}

func validPeerInboxSemanticWorkIntent(intent PeerInboxSemanticWorkIntent, responseCount int) bool {
	participants := intent.Participants()
	return validPeerInboxSemanticEffectSource(intent.Source(), intent.ResponseOrdinal(), responseCount) &&
		!intent.WorkRef().IsZero() && !intent.ChannelID().IsZero() &&
		participants.ChannelID() == intent.ChannelID() && participants.RosterRevision() != 0 &&
		!participants.InitiatorPeerID().IsZero() && !participants.ReviewerPeerID().IsZero() &&
		intent.NextVersion() != 0 && intent.NextState().Valid() && intent.NextIteration() >= 1 &&
		intent.NextIteration() <= 2 && intent.DeadlineUnixNano() > 0 &&
		!intent.StateData().IsZero() && intent.ObservedAtUnixNano() > 0
}

func validPeerInboxSemanticHandlingIntent(intent PeerInboxSemanticHandlingIntent,
	responseCount int,
) bool {
	return validPeerInboxSemanticEffectSource(intent.Source(), intent.ResponseOrdinal(), responseCount) &&
		!intent.WorkRef().IsZero() && intent.LocalRole().Valid() &&
		intent.SourceEventType().Valid()
}

func validPeerInboxSemanticEffectSource(source PeerInboxSemanticEffectSource,
	ordinal uint8, responseCount int,
) bool {
	return source == PeerInboxSemanticFromImportedEvent && ordinal == 0 ||
		source == PeerInboxSemanticFromLocalResponse && int(ordinal) < responseCount
}
