package teamwork

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

var ErrInvalidImport = errors.New("invalid imported Teamwork Event")

// ImportDisposition is the closed semantic result of planning one durable
// Peer Inbox row. It deliberately says nothing about transport dissemination:
// an imported publication is never a new local Gossip source.
type ImportDisposition string

const (
	ImportApply       ImportDisposition = "apply"
	ImportRetry       ImportDisposition = "retry"
	ImportReject      ImportDisposition = "reject"
	ImportConflict    ImportDisposition = "conflict"
	ImportReceiptOnly ImportDisposition = "receipt_only"
)

func (d ImportDisposition) Valid() bool {
	switch d {
	case ImportApply, ImportRetry, ImportReject, ImportConflict, ImportReceiptOnly:
		return true
	default:
		return false
	}
}

// ImportEventFact is a narrow, immutable projection of an Event already
// proven durable by the Store. Constructing it from the exact Event prevents a
// caller from supplying only a convenient type or version while omitting its
// identity, scope, audience, source, payload, or Artifact closure.
type ImportEventFact struct {
	key        model.EventKey
	channelID  model.ChannelID
	workRef    model.WorkRef
	origin     model.PeerID
	source     model.EventSource
	eventType  model.EventType
	audience   []model.PeerID
	payload    importedPayload
	artifacts  []model.ArtifactRef
	acceptedAt time.Time
}

func NewImportEventFact(event model.Event) (ImportEventFact, error) {
	if event.ID().IsZero() || event.Scope().ChannelID().IsZero() || !event.Source().Valid() || !event.Type().Valid() {
		return ImportEventFact{}, fmt.Errorf("%w: causal Event is incomplete", ErrInvalidImport)
	}
	payload, err := parseImportedPayload(event)
	if err != nil {
		return ImportEventFact{}, fmt.Errorf("%w: causal Event payload: %v", ErrInvalidImport, err)
	}
	return ImportEventFact{
		key:        event.Key(),
		channelID:  event.Scope().ChannelID(),
		workRef:    event.Scope().WorkRef(),
		origin:     event.Scope().OriginPeerID(),
		source:     event.Source(),
		eventType:  event.Type(),
		audience:   event.Audience().Peers(),
		payload:    payload,
		artifacts:  event.Artifacts(),
		acceptedAt: event.AcceptedAt(),
	}, nil
}

func (f ImportEventFact) Key() model.EventKey        { return f.key }
func (f ImportEventFact) ChannelID() model.ChannelID { return f.channelID }
func (f ImportEventFact) WorkRef() model.WorkRef     { return f.workRef }
func (f ImportEventFact) OriginPeerID() model.PeerID { return f.origin }
func (f ImportEventFact) Source() model.EventSource  { return f.source }
func (f ImportEventFact) EventType() model.EventType { return f.eventType }
func (f ImportEventFact) Audience() []model.PeerID   { return append([]model.PeerID(nil), f.audience...) }
func (f ImportEventFact) Artifacts() []model.ArtifactRef {
	return append([]model.ArtifactRef(nil), f.artifacts...)
}
func (f ImportEventFact) AcceptedAt() time.Time { return f.acceptedAt }

// ImportPlanSpec contains only trusted controller inputs. Current is nil when
// no local Work exists. Facts must have been built from exact durable Events;
// the planner still rebinds every used fact to the imported Event and Work.
type ImportPlanSpec struct {
	LocalPeerID model.PeerID
	Event       model.Event
	Current     *model.ReviewWork
	Facts       []ImportEventFact
	Now         time.Time
}

type ImportWorkSource string

const (
	ImportWorkFromEvent    ImportWorkSource = "imported_event"
	ImportWorkFromResponse ImportWorkSource = "local_response"
)

func (s ImportWorkSource) Valid() bool {
	return s == ImportWorkFromEvent || s == ImportWorkFromResponse
}

// ImportWorkIntent describes either mirror creation or one exact Work CAS. A
// response-backed mutation names its response ordinal instead of an Event ID;
// Event allocation and signing remain controller/Store responsibilities.
type ImportWorkIntent struct {
	source             ImportWorkSource
	responseOrdinal    uint8
	workRef            model.WorkRef
	channelID          model.ChannelID
	participants       model.ParticipantSnapshot
	expectedVersion    uint64
	expectedState      model.WorkState
	expectedIteration  uint8
	nextVersion        uint64
	nextState          model.WorkState
	nextIteration      uint8
	deadlineUnixNano   int64
	stateData          model.JSON
	observedAtUnixNano int64
	transition         TransitionIntent
	hasTransition      bool
}

func (i ImportWorkIntent) Source() ImportWorkSource                { return i.source }
func (i ImportWorkIntent) ResponseOrdinal() uint8                  { return i.responseOrdinal }
func (i ImportWorkIntent) WorkRef() model.WorkRef                  { return i.workRef }
func (i ImportWorkIntent) ChannelID() model.ChannelID              { return i.channelID }
func (i ImportWorkIntent) Participants() model.ParticipantSnapshot { return i.participants }
func (i ImportWorkIntent) ExpectedVersion() uint64                 { return i.expectedVersion }
func (i ImportWorkIntent) ExpectedState() model.WorkState          { return i.expectedState }
func (i ImportWorkIntent) ExpectedIteration() uint8                { return i.expectedIteration }
func (i ImportWorkIntent) NextVersion() uint64                     { return i.nextVersion }
func (i ImportWorkIntent) NextState() model.WorkState              { return i.nextState }
func (i ImportWorkIntent) NextIteration() uint8                    { return i.nextIteration }
func (i ImportWorkIntent) DeadlineUnixNano() int64                 { return i.deadlineUnixNano }
func (i ImportWorkIntent) StateData() model.JSON                   { return i.stateData }
func (i ImportWorkIntent) ObservedAtUnixNano() int64               { return i.observedAtUnixNano }
func (i ImportWorkIntent) IsCreation() bool                        { return i.expectedVersion == 0 }
func (i ImportWorkIntent) Transition() (TransitionIntent, bool)    { return i.transition, i.hasTransition }

type ImportHandlingSource string

const (
	ImportHandlingFromEvent    ImportHandlingSource = "imported_event"
	ImportHandlingFromResponse ImportHandlingSource = "local_response"
)

func (s ImportHandlingSource) Valid() bool {
	return s == ImportHandlingFromEvent || s == ImportHandlingFromResponse
}

// ImportHandlingIntent identifies the accepted Event that should create one
// local pending handling. It never chooses a Profile, HandlingID, priority, or
// timestamp; those remain Store-owned policy.
type ImportHandlingIntent struct {
	source          ImportHandlingSource
	responseOrdinal uint8
	workRef         model.WorkRef
	localRole       model.WorkRole
	sourceEventType model.EventType
}

func (i ImportHandlingIntent) Source() ImportHandlingSource     { return i.source }
func (i ImportHandlingIntent) ResponseOrdinal() uint8           { return i.responseOrdinal }
func (i ImportHandlingIntent) WorkRef() model.WorkRef           { return i.workRef }
func (i ImportHandlingIntent) LocalRole() model.WorkRole        { return i.localRole }
func (i ImportHandlingIntent) SourceEventType() model.EventType { return i.sourceEventType }

// ImportHandlingSettlement fences the currently actionable Event when a
// remote home terminal Event supersedes it. The Store resolves the Event to a
// Handling and applies its own owner fence.
type ImportHandlingSettlement struct {
	workRef     model.WorkRef
	sourceEvent model.EventID
	disposition string
}

func (i ImportHandlingSettlement) WorkRef() model.WorkRef       { return i.workRef }
func (i ImportHandlingSettlement) SourceEventID() model.EventID { return i.sourceEvent }
func (i ImportHandlingSettlement) Disposition() string          { return i.disposition }

// LocalResponseIntent is a closed controller Event draft. Identity, sequences,
// actor, audience, accepted time, Artifact closure, signature, publication,
// and receipt identity are intentionally absent. The Store derives and checks
// a delivered response's exact referenced closure from Cause.
type LocalResponseIntent struct {
	eventType model.EventType
	payload   model.JSON
	cause     model.EventKey
}

func (i LocalResponseIntent) EventType() model.EventType { return i.eventType }
func (i LocalResponseIntent) Payload() model.JSON        { return i.payload }
func (i LocalResponseIntent) Cause() model.EventKey      { return i.cause }

type ImportPlan struct {
	disposition ImportDisposition
	work        ImportWorkIntent
	hasWork     bool
	handling    ImportHandlingIntent
	hasHandling bool
	settlement  ImportHandlingSettlement
	hasSettle   bool
	responses   []LocalResponseIntent
	inboxStatus model.InboxStatus
	diagnostic  string
}

func (p ImportPlan) Disposition() ImportDisposition { return p.disposition }
func (p ImportPlan) Work() (ImportWorkIntent, bool) { return p.work, p.hasWork }
func (p ImportPlan) Handling() (ImportHandlingIntent, bool) {
	return p.handling, p.hasHandling
}
func (p ImportPlan) Settlement() (ImportHandlingSettlement, bool) {
	return p.settlement, p.hasSettle
}
func (p ImportPlan) Responses() []LocalResponseIntent {
	return append([]LocalResponseIntent(nil), p.responses...)
}
func (p ImportPlan) InboxStatus() model.InboxStatus { return p.inboxStatus }
func (p ImportPlan) Diagnostic() string             { return p.diagnostic }

type importPlanner struct {
	local   model.PeerID
	event   model.Event
	payload importedPayload
	current *model.ReviewWork
	facts   map[model.EventKey]ImportEventFact
	now     time.Time
}

// PlanImportedEvent is deterministic and performs no I/O. It plans the
// semantic half of Peer Inbox apply after publication and Artifact validation.
func PlanImportedEvent(spec ImportPlanSpec) (ImportPlan, error) {
	if spec.LocalPeerID.IsZero() || spec.Event.ID().IsZero() || spec.Now.IsZero() {
		return ImportPlan{}, fmt.Errorf("%w: local Peer, imported Event, and trusted time are required", ErrInvalidImport)
	}
	now := spec.Now.Round(0).UTC()
	if now.UnixNano() <= 0 || !time.Unix(0, now.UnixNano()).UTC().Equal(now) {
		return ImportPlan{}, fmt.Errorf("%w: trusted time must fit positive Unix nanoseconds", ErrInvalidImport)
	}
	if spec.Event.Source() != model.EventSourceImported {
		return ImportPlan{}, fmt.Errorf("%w: Event source must be imported", ErrInvalidImport)
	}
	payload, err := parseImportedPayload(spec.Event)
	if err != nil {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "invalid_payload"), nil
	}
	if spec.Event.Scope().OriginPeerID() == spec.LocalPeerID || spec.Event.Audience().Len() != 1 ||
		!spec.Event.Audience().Contains(spec.LocalPeerID) {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "invalid_audience"), nil
	}
	facts := make(map[model.EventKey]ImportEventFact, len(spec.Facts))
	for _, fact := range spec.Facts {
		if fact.key.IsZero() || fact.channelID.IsZero() || fact.workRef.IsZero() || !fact.source.Valid() ||
			!fact.eventType.Valid() || fact.payload.workVersion == 0 {
			return ImportPlan{}, fmt.Errorf("%w: zero or malformed durable Event fact", ErrInvalidImport)
		}
		if _, exists := facts[fact.key]; exists {
			return ImportPlan{}, fmt.Errorf("%w: duplicate durable Event fact", ErrInvalidImport)
		}
		facts[fact.key] = fact
	}
	var current *model.ReviewWork
	if spec.Current != nil {
		copy := *spec.Current
		current = &copy
	}
	planner := importPlanner{spec.LocalPeerID, spec.Event, payload, current, facts, now}
	if spec.Event.Type() == model.EventReviewOutcome {
		return planner.planOutcome(), nil
	}
	if spec.Event.Type().ParticipantInput() {
		return planner.planParticipantInput(), nil
	}
	if spec.Event.Type().HomeAuthoritative() {
		return planner.planHomeEvent(), nil
	}
	return terminalImportPlan(ImportConflict, model.InboxConflicted, "unknown_event_type"), nil
}

func (p importPlanner) planHomeEvent() ImportPlan {
	event := p.event
	work := event.Scope().WorkRef()
	if event.Scope().OriginPeerID() != work.HomePeerID() || p.local == work.HomePeerID() {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "invalid_home_authority")
	}
	if event.Type() == model.EventReviewOffered {
		if p.current != nil {
			if validateWorkScope(*p.current, event) != "" ||
				p.current.Participants().ReviewerPeerID() != p.local {
				return terminalImportPlan(ImportConflict, model.InboxConflicted, "participant_conflict")
			}
			return p.conflictedImportedEvent("duplicate_work_identity")
		}
		return p.planOffered()
	}
	if p.current == nil {
		return terminalImportPlan(ImportRetry, model.InboxRetry, "missing_work")
	}
	if diagnostic := validateWorkScope(*p.current, event); diagnostic != "" ||
		p.current.Participants().ReviewerPeerID() != p.local {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "participant_conflict")
	}
	if !homeEventArtifactsValid(event) {
		return p.conflictedImportedEvent("artifact_conflict")
	}
	if event.Type() == model.EventReviewExpired && p.payload.deadlineUnixNano != p.current.DeadlineUnixNano() {
		return p.conflictedImportedEvent("deadline_conflict")
	}
	if p.payload.workVersion > p.current.Version() {
		return terminalImportPlan(ImportRetry, model.InboxRetry, "local_work_behind")
	}
	if p.payload.workVersion == p.current.Version() && p.payload.iteration != p.current.Iteration() {
		return p.conflictedImportedEvent("iteration_conflict")
	}

	switch event.Type() {
	case model.EventReviewAcceptRejected:
		return p.planHomeAcceptRejected()
	case model.EventReviewAccepted, model.EventReviewDelivered, model.EventReviewReworkRequested,
		model.EventReviewClosed, model.EventReviewDeclined, model.EventReviewCancelled,
		model.EventReviewExpired:
		if p.payload.workVersion < p.current.Version() {
			return p.staleHomeEvent()
		}
		return p.planHomeTransition()
	default:
		return p.conflictedImportedEvent("invalid_home_event")
	}
}

func (p importPlanner) planOffered() ImportPlan {
	event := p.event
	if p.payload.workVersion != 1 || p.payload.iteration != 1 || p.payload.deadlineUnixNano <= 0 ||
		len(event.CausedBy()) > 1 {
		return p.conflictedImportedEvent("invalid_offer")
	}
	duration := p.payload.deadlineUnixNano - event.AcceptedAt().UnixNano()
	if duration < int64(model.MinimumReviewDeadline) || duration > int64(model.MaximumReviewDeadline) {
		return p.conflictedImportedEvent("invalid_offer_deadline")
	}
	participants, err := model.NewParticipantSnapshot(event.Scope().ChannelID(),
		event.Scope().PublicationRoster().Revision(), event.Scope().WorkRef().HomePeerID(), p.local)
	if err != nil {
		return p.conflictedImportedEvent("participant_conflict")
	}
	work := ImportWorkIntent{
		source: ImportWorkFromEvent, workRef: event.Scope().WorkRef(), channelID: event.Scope().ChannelID(),
		participants: participants, nextVersion: 1, nextState: model.WorkOffered, nextIteration: 1,
		deadlineUnixNano: p.payload.deadlineUnixNano, stateData: event.Payload(),
		observedAtUnixNano: event.AcceptedAt().UnixNano(),
	}
	handling := importedHandling(event, model.WorkRoleReviewer)
	response := outcomeResponse(event, p.payload, "accepted", "applied")
	return appliedPlan(work, &handling, nil, []LocalResponseIntent{response})
}

func (p importPlanner) planHomeAcceptRejected() ImportPlan {
	fact, disposition, diagnostic := p.requireSingleCause()
	if disposition != "" {
		return p.causalFailure(disposition, diagnostic)
	}
	if p.payload.workVersion != 1 || p.payload.iteration != 1 ||
		!factMatchesPeerRequest(fact, p.event, p.local, model.EventReviewAcceptRequested) ||
		fact.payload.workVersion != p.payload.workVersion || fact.payload.iteration != p.payload.iteration ||
		len(p.event.Artifacts()) != 0 {
		return p.conflictedImportedEvent("cause_conflict")
	}
	response := outcomeResponse(p.event, p.payload, "accepted", "applied")
	return appliedPlan(ImportWorkIntent{}, nil, nil, []LocalResponseIntent{response})
}

func (p importPlanner) planHomeTransition() ImportPlan {
	current := *p.current
	event := p.event
	if p.payload.workVersion != current.Version() || p.payload.iteration != current.Iteration() {
		return p.conflictedImportedEvent("work_version_conflict")
	}

	var fact ImportEventFact
	var disposition ImportDisposition
	var diagnostic string
	switch event.Type() {
	case model.EventReviewAccepted:
		fact, disposition, diagnostic = p.requireSingleCause()
		if disposition == "" && (!factMatchesPeerRequest(fact, event, p.local, model.EventReviewAcceptRequested) ||
			fact.payload.workVersion != current.Version() || fact.payload.iteration != current.Iteration()) {
			disposition, diagnostic = ImportConflict, "cause_conflict"
		}
	case model.EventReviewDelivered:
		fact, disposition, diagnostic = p.requireSingleCause()
		if disposition == "" && (!factMatchesPeerRequest(fact, event, p.local, model.EventReviewDeliveryReady) ||
			fact.payload.workVersion != current.Version() || fact.payload.iteration != current.Iteration()) {
			disposition, diagnostic = ImportConflict, "cause_conflict"
		} else if disposition == "" && !exactDeliveredArtifactClosure(fact.artifacts, event.Artifacts()) {
			disposition, diagnostic = ImportConflict, "artifact_conflict"
		}
	case model.EventReviewDeclined:
		fact, disposition, diagnostic = p.requireSingleCause()
		if disposition == "" && (!factMatchesPeerRequest(fact, event, p.local, model.EventReviewDeclineRequested) ||
			fact.payload.workVersion != current.Version() || fact.payload.iteration != current.Iteration()) {
			disposition, diagnostic = ImportConflict, "cause_conflict"
		}
	case model.EventReviewReworkRequested, model.EventReviewClosed,
		model.EventReviewCancelled, model.EventReviewExpired:
		fact, disposition, diagnostic = p.requireCurrentCause()
	}
	if disposition != "" {
		return p.causalFailure(disposition, diagnostic)
	}
	transition, err := PlanHomeTransition(HomeTransitionSpec{
		Work: current, ActorPeerID: current.Ref().HomePeerID(), ExpectedVersion: current.Version(),
		EventType: event.Type(), NowUnixNano: event.AcceptedAt().UnixNano(),
	})
	if err != nil || transition.AuthoritativeEventType() != event.Type() {
		return p.conflictedImportedEvent(transitionDiagnostic(err))
	}
	work := transitionWorkIntent(ImportWorkFromEvent, 0, current, transition, event.Payload())
	var handling *ImportHandlingIntent
	if event.Type() == model.EventReviewAccepted || event.Type() == model.EventReviewReworkRequested {
		value := importedHandling(event, model.WorkRoleReviewer)
		handling = &value
	}
	var settlement *ImportHandlingSettlement
	settlesActionableHandling := event.Type() == model.EventReviewExpired ||
		(event.Type() == model.EventReviewCancelled &&
			(current.State() == model.WorkOffered || current.State() == model.WorkActive ||
				current.State() == model.WorkRework))
	if settlesActionableHandling {
		disposition := "superseded_cancelled"
		if event.Type() == model.EventReviewExpired {
			disposition = "superseded_expired"
		}
		value := ImportHandlingSettlement{current.Ref(), current.UpdatedBy(), disposition}
		settlement = &value
	}
	response := outcomeResponse(event, p.payload, "accepted", "applied")
	return appliedPlan(work, handling, settlement, []LocalResponseIntent{response})
}

func (p importPlanner) staleHomeEvent() ImportPlan {
	fact, disposition, diagnostic := p.requireSingleCause()
	if disposition != "" {
		return p.causalFailure(disposition, diagnostic)
	}
	valid := false
	switch p.event.Type() {
	case model.EventReviewAccepted:
		valid = factMatchesPeerRequest(fact, p.event, p.local, model.EventReviewAcceptRequested) &&
			fact.payload.workVersion == p.payload.workVersion && fact.payload.iteration == p.payload.iteration
	case model.EventReviewDelivered:
		valid = factMatchesPeerRequest(fact, p.event, p.local, model.EventReviewDeliveryReady) &&
			fact.payload.workVersion == p.payload.workVersion && fact.payload.iteration == p.payload.iteration
		if valid && !exactDeliveredArtifactClosure(fact.artifacts, p.event.Artifacts()) {
			return p.conflictedImportedEvent("artifact_conflict")
		}
	case model.EventReviewDeclined:
		valid = factMatchesPeerRequest(fact, p.event, p.local, model.EventReviewDeclineRequested) &&
			fact.payload.workVersion == p.payload.workVersion && fact.payload.iteration == p.payload.iteration
	case model.EventReviewReworkRequested, model.EventReviewClosed,
		model.EventReviewCancelled, model.EventReviewExpired:
		valid = historicalHomeCause(fact, p.event, p.local, p.payload)
	}
	if !valid {
		return p.conflictedImportedEvent("predecessor_conflict")
	}
	return p.rejectedImportedEvent("stale_home_event")
}

func (p importPlanner) planParticipantInput() ImportPlan {
	if p.current == nil {
		return terminalImportPlan(ImportRetry, model.InboxRetry, "missing_work")
	}
	current := *p.current
	event := p.event
	if diagnostic := validateWorkScope(current, event); diagnostic != "" || p.local != current.Ref().HomePeerID() ||
		event.Scope().OriginPeerID() != current.Participants().ReviewerPeerID() {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "participant_conflict")
	}
	if !participantArtifactsValid(event) {
		return p.conflictedParticipantRequest("artifact_conflict")
	}
	fact, disposition, diagnostic := p.requireSingleCause()
	if disposition != "" {
		return p.causalFailure(disposition, diagnostic)
	}
	if p.payload.workVersion > current.Version() {
		return terminalImportPlan(ImportRetry, model.InboxRetry, "local_work_behind")
	}
	if p.payload.workVersion == current.Version() && p.payload.iteration != current.Iteration() {
		return p.conflictedParticipantRequest("iteration_conflict")
	}
	if !factMatchesCurrentUpdate(fact, current) {
		if p.payload.workVersion < current.Version() {
			if historicalParticipantCause(fact, event, current, p.payload) {
				return p.staleParticipantRequest()
			}
			return p.conflictedParticipantRequest("predecessor_conflict")
		}
		return p.conflictedParticipantRequest("predecessor_conflict")
	}
	if fact.source != model.EventSourceLocal || len(fact.audience) != 1 ||
		fact.audience[0] != event.Scope().OriginPeerID() || !fact.acceptedAt.Equal(current.UpdatedAt()) {
		return p.conflictedParticipantRequest("predecessor_conflict")
	}
	if p.payload.workVersion < current.Version() {
		if historicalParticipantCause(fact, event, current, p.payload) {
			return p.staleParticipantRequest()
		}
		return p.conflictedParticipantRequest("predecessor_conflict")
	}
	if p.now.Before(current.UpdatedAt()) {
		return p.conflictedParticipantRequest("clock_regressed")
	}

	wanted, stateAllowed := participantResponse(event.Type(), current.State())
	if !stateAllowed {
		if current.State().Terminal() {
			return p.staleParticipantRequest()
		}
		return p.conflictedParticipantRequest("state_conflict")
	}
	transition, err := PlanHomeTransition(HomeTransitionSpec{
		Work: current, ActorPeerID: p.local, ExpectedVersion: current.Version(), EventType: wanted,
		NowUnixNano: p.now.UnixNano(),
	})
	if err != nil {
		return p.conflictedParticipantRequest(transitionDiagnostic(err))
	}
	if transition.DeadlineWon() {
		return p.deadlineParticipantRequest(current, transition)
	}
	response := decisionResponse(wanted, event, p.payload)
	work := transitionWorkIntent(ImportWorkFromResponse, 0, current, transition, response.Payload())
	var handling *ImportHandlingIntent
	if wanted == model.EventReviewDelivered {
		value := responseHandling(current.Ref(), 0, model.WorkRoleInitiator, model.EventReviewDelivered)
		handling = &value
	}
	return appliedPlan(work, handling, nil, []LocalResponseIntent{response})
}

func (p importPlanner) deadlineParticipantRequest(current model.ReviewWork, transition TransitionIntent) ImportPlan {
	expiry := expiryResponse(p.event, current, transition)
	rejection := rejectedRequestResponse(p.event, p.payload, "work_expired")
	work := transitionWorkIntent(ImportWorkFromResponse, 0, current, transition, expiry.Payload())
	plan := appliedPlan(work, nil, nil, []LocalResponseIntent{expiry, rejection})
	plan.disposition = ImportReject
	plan.inboxStatus = model.InboxRejected
	plan.diagnostic = "work_expired"
	return plan
}

func (p importPlanner) staleParticipantRequest() ImportPlan {
	response := rejectedRequestResponse(p.event, p.payload, "stale_request")
	plan := terminalImportPlan(ImportReject, model.InboxRejected, "stale_request")
	return p.withReceiptResponse(plan, response)
}

func (p importPlanner) conflictedParticipantRequest(diagnostic string) ImportPlan {
	return p.conflictedImportedEvent(diagnostic)
}

func (p importPlanner) conflictedImportedEvent(diagnostic string) ImportPlan {
	response := outcomeResponse(p.event, p.payload, "conflicted", diagnostic)
	plan := terminalImportPlan(ImportConflict, model.InboxConflicted, diagnostic)
	return p.withReceiptResponse(plan, response)
}

func (p importPlanner) rejectedImportedEvent(diagnostic string) ImportPlan {
	response := outcomeResponse(p.event, p.payload, "rejected", diagnostic)
	plan := terminalImportPlan(ImportReject, model.InboxRejected, diagnostic)
	return p.withReceiptResponse(plan, response)
}

// A local semantic response must itself pass local Event admission. Invalid
// first offers have no Work against which a receipt can bind, and tuples such
// as v4/i1 or v6/i2 describe result Works rather than a source Event in the
// closed T0 state machine. Keep the Inbox decision as durable evidence, but do
// not plan a response that the Store can never commit.
func (p importPlanner) withReceiptResponse(plan ImportPlan, response LocalResponseIntent) ImportPlan {
	if p.current != nil && receiptTupleAtOrBeforeCurrent(*p.current,
		p.payload.workVersion, p.payload.iteration) {
		plan.responses = []LocalResponseIntent{response}
	}
	return plan
}

func (p importPlanner) causalFailure(disposition ImportDisposition, diagnostic string) ImportPlan {
	switch disposition {
	case ImportConflict:
		return p.conflictedImportedEvent(diagnostic)
	case ImportReject:
		return p.rejectedImportedEvent(diagnostic)
	default:
		return terminalImportPlan(disposition, statusForDisposition(disposition), diagnostic)
	}
}

func (p importPlanner) planOutcome() ImportPlan {
	if p.current == nil {
		return terminalImportPlan(ImportRetry, model.InboxRetry, "missing_work")
	}
	current := *p.current
	if validateWorkScope(current, p.event) != "" {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "participant_conflict")
	}
	remote := current.Participants().ReviewerPeerID()
	if p.local == remote {
		remote = current.Ref().HomePeerID()
	} else if p.local != current.Ref().HomePeerID() {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "participant_conflict")
	}
	if p.event.Scope().OriginPeerID() != remote || len(p.event.Artifacts()) != 0 {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "participant_conflict")
	}
	fact, disposition, diagnostic := p.requireSingleCause()
	if disposition != "" {
		return terminalImportPlan(disposition, statusForDisposition(disposition), diagnostic)
	}
	if fact.source != model.EventSourceLocal || fact.origin != p.local || fact.channelID != current.ChannelID() ||
		fact.workRef != current.Ref() || fact.eventType == model.EventReviewOutcome ||
		len(fact.audience) != 1 || fact.audience[0] != remote ||
		fact.payload.workVersion != p.payload.workVersion || fact.payload.iteration != p.payload.iteration ||
		p.payload.decisionRef != fact.key.EventID().String() ||
		!receiptTupleAtOrBeforeCurrent(current, fact.payload.workVersion, fact.payload.iteration) {
		return terminalImportPlan(ImportConflict, model.InboxConflicted, "receipt_cause_conflict")
	}
	return terminalImportPlan(ImportReceiptOnly, model.InboxAccepted, "")
}

func receiptTupleAtOrBeforeCurrent(current model.ReviewWork, version uint64, iteration uint8) bool {
	valid := iteration == 1 && version >= 1 && version <= 3
	valid = valid || iteration == 2 && version >= 4 && version <= 5
	if !valid || version > current.Version() {
		return false
	}
	return version < current.Version() || iteration == current.Iteration()
}

func (p importPlanner) requireSingleCause() (ImportEventFact, ImportDisposition, string) {
	causes := p.event.CausedBy()
	if len(causes) != 1 {
		return ImportEventFact{}, ImportConflict, "cause_conflict"
	}
	fact, ok := p.facts[causes[0]]
	if !ok {
		return ImportEventFact{}, ImportRetry, "missing_cause"
	}
	if fact.key != causes[0] {
		return ImportEventFact{}, ImportConflict, "cause_conflict"
	}
	return fact, "", ""
}

func (p importPlanner) requireCurrentCause() (ImportEventFact, ImportDisposition, string) {
	fact, disposition, diagnostic := p.requireSingleCause()
	if disposition != "" {
		return ImportEventFact{}, disposition, diagnostic
	}
	if !factMatchesCurrentUpdate(fact, *p.current) || fact.source != model.EventSourceImported ||
		len(fact.audience) != 1 || fact.audience[0] != p.local ||
		!fact.acceptedAt.Equal(p.current.UpdatedAt()) {
		return ImportEventFact{}, ImportConflict, "predecessor_conflict"
	}
	return fact, "", ""
}

func factMatchesPeerRequest(fact ImportEventFact, response model.Event, local model.PeerID,
	want model.EventType,
) bool {
	return fact.source == model.EventSourceLocal && fact.origin == local && fact.eventType == want &&
		fact.channelID == response.Scope().ChannelID() && fact.workRef == response.Scope().WorkRef() &&
		len(fact.audience) == 1 && fact.audience[0] == response.Scope().OriginPeerID()
}

func factMatchesCurrentUpdate(fact ImportEventFact, current model.ReviewWork) bool {
	return fact.key.EventID() == current.UpdatedBy() && fact.channelID == current.ChannelID() &&
		fact.workRef == current.Ref() && fact.origin == current.Ref().HomePeerID() &&
		fact.eventType == eventTypeForImportedState(current.State())
}

func historicalParticipantCause(fact ImportEventFact, request model.Event,
	current model.ReviewWork, payload importedPayload,
) bool {
	version, iteration, ok := factEstablishedWork(fact)
	if !ok {
		return false
	}
	allowed := (request.Type() == model.EventReviewAcceptRequested ||
		request.Type() == model.EventReviewDeclineRequested) && fact.eventType == model.EventReviewOffered
	allowed = allowed || request.Type() == model.EventReviewDeliveryReady &&
		(fact.eventType == model.EventReviewAccepted || fact.eventType == model.EventReviewReworkRequested)
	return allowed && fact.source == model.EventSourceLocal && fact.origin == current.Ref().HomePeerID() &&
		fact.channelID == request.Scope().ChannelID() && fact.workRef == request.Scope().WorkRef() &&
		len(fact.audience) == 1 && fact.audience[0] == request.Scope().OriginPeerID() &&
		version == payload.workVersion && iteration == payload.iteration
}

func historicalHomeCause(fact ImportEventFact, event model.Event, local model.PeerID,
	payload importedPayload,
) bool {
	version, iteration, ok := factEstablishedWork(fact)
	if !ok || fact.source != model.EventSourceImported || fact.origin != event.Scope().OriginPeerID() ||
		fact.channelID != event.Scope().ChannelID() || fact.workRef != event.Scope().WorkRef() ||
		len(fact.audience) != 1 || fact.audience[0] != local ||
		version != payload.workVersion || iteration != payload.iteration {
		return false
	}
	switch event.Type() {
	case model.EventReviewReworkRequested, model.EventReviewClosed:
		return fact.eventType == model.EventReviewDelivered
	case model.EventReviewCancelled:
		return fact.eventType == model.EventReviewOffered || fact.eventType == model.EventReviewAccepted ||
			fact.eventType == model.EventReviewDelivered || fact.eventType == model.EventReviewReworkRequested
	case model.EventReviewExpired:
		return fact.eventType == model.EventReviewOffered || fact.eventType == model.EventReviewAccepted ||
			fact.eventType == model.EventReviewReworkRequested
	default:
		return false
	}
}

// factEstablishedWork derives the version represented by a home mutation
// Event. Home Event payloads bind their predecessor, except offered which
// creates version one. Receipt-only Events never establish a Work version.
func factEstablishedWork(fact ImportEventFact) (uint64, uint8, bool) {
	if fact.eventType == model.EventReviewOffered {
		if fact.payload.workVersion != 1 || fact.payload.iteration != 1 {
			return 0, 0, false
		}
		return 1, 1, true
	}
	if fact.payload.workVersion >= model.MaxSQLiteInteger {
		return 0, 0, false
	}
	predecessor := model.WorkState("")
	switch fact.eventType {
	case model.EventReviewAccepted:
		if fact.payload.workVersion != 1 || fact.payload.iteration != 1 {
			return 0, 0, false
		}
		predecessor = model.WorkOffered
	case model.EventReviewDelivered:
		if fact.payload.workVersion == 2 && fact.payload.iteration == 1 {
			predecessor = model.WorkActive
		} else if fact.payload.workVersion == 4 && fact.payload.iteration == 2 {
			predecessor = model.WorkRework
		} else {
			return 0, 0, false
		}
	case model.EventReviewReworkRequested:
		if fact.payload.workVersion != 3 || fact.payload.iteration != 1 {
			return 0, 0, false
		}
		predecessor = model.WorkDelivered
	default:
		return 0, 0, false
	}
	_, nextIteration, ok := model.NextReviewWorkState(predecessor, fact.payload.iteration, fact.eventType)
	if !ok {
		return 0, 0, false
	}
	return fact.payload.workVersion + 1, nextIteration, true
}

func validateWorkScope(current model.ReviewWork, event model.Event) string {
	if current.Ref() != event.Scope().WorkRef() || current.ChannelID() != event.Scope().ChannelID() ||
		current.Participants().ChannelID() != current.ChannelID() ||
		current.Participants().InitiatorPeerID() != current.Ref().HomePeerID() ||
		current.Version() == 0 || current.Version() > model.MaxSQLiteInteger {
		return "work_scope_conflict"
	}
	return ""
}

func eventTypeForImportedState(state model.WorkState) model.EventType {
	switch state {
	case model.WorkOffered:
		return model.EventReviewOffered
	case model.WorkActive:
		return model.EventReviewAccepted
	case model.WorkDelivered:
		return model.EventReviewDelivered
	case model.WorkRework:
		return model.EventReviewReworkRequested
	case model.WorkClosed:
		return model.EventReviewClosed
	case model.WorkDeclined:
		return model.EventReviewDeclined
	case model.WorkExpired:
		return model.EventReviewExpired
	case model.WorkCancelled:
		return model.EventReviewCancelled
	default:
		return ""
	}
}

func participantResponse(eventType model.EventType, state model.WorkState) (model.EventType, bool) {
	switch eventType {
	case model.EventReviewAcceptRequested:
		return model.EventReviewAccepted, state == model.WorkOffered
	case model.EventReviewDeclineRequested:
		return model.EventReviewDeclined, state == model.WorkOffered
	case model.EventReviewDeliveryReady:
		return model.EventReviewDelivered, state == model.WorkActive || state == model.WorkRework
	default:
		return "", false
	}
}

func homeEventArtifactsValid(event model.Event) bool {
	switch event.Type() {
	case model.EventReviewDelivered, model.EventReviewReworkRequested:
		return true
	default:
		return len(event.Artifacts()) == 0
	}
}

func participantArtifactsValid(event model.Event) bool {
	if event.Type() == model.EventReviewDeliveryReady {
		return true
	}
	return len(event.Artifacts()) == 0
}

func exactDeliveredArtifactClosure(source, delivered []model.ArtifactRef) bool {
	if len(source) != len(delivered) {
		return false
	}
	for index := range source {
		if source[index].RootDigest() != delivered[index].RootDigest() ||
			delivered[index].Role() != model.ArtifactReferenced {
			return false
		}
	}
	return true
}

func transitionWorkIntent(source ImportWorkSource, ordinal uint8, current model.ReviewWork,
	transition TransitionIntent, stateData model.JSON,
) ImportWorkIntent {
	return ImportWorkIntent{
		source: source, responseOrdinal: ordinal, workRef: current.Ref(), channelID: current.ChannelID(),
		participants: current.Participants(), expectedVersion: transition.ExpectedVersion(),
		expectedState: transition.ExpectedState(), expectedIteration: transition.ExpectedIteration(),
		nextVersion: transition.NextVersion(), nextState: transition.NextState(),
		nextIteration: transition.NextIteration(), deadlineUnixNano: transition.DeadlineUnixNano(),
		stateData: stateData, observedAtUnixNano: transition.ObservedAtUnixNano(),
		transition: transition, hasTransition: true,
	}
}

func importedHandling(event model.Event, role model.WorkRole) ImportHandlingIntent {
	return ImportHandlingIntent{ImportHandlingFromEvent, 0, event.Scope().WorkRef(), role, event.Type()}
}

func responseHandling(work model.WorkRef, ordinal uint8, role model.WorkRole,
	eventType model.EventType,
) ImportHandlingIntent {
	return ImportHandlingIntent{ImportHandlingFromResponse, ordinal, work, role, eventType}
}

func decisionResponse(eventType model.EventType, source model.Event,
	payload importedPayload,
) LocalResponseIntent {
	value, _ := model.JSONFrom(versionResponsePayload{payload.workVersion, payload.iteration})
	return LocalResponseIntent{eventType, value, source.Key()}
}

func rejectedRequestResponse(source model.Event, payload importedPayload, diagnostic string) LocalResponseIntent {
	if source.Type() == model.EventReviewAcceptRequested &&
		payload.workVersion == 1 && payload.iteration == 1 {
		value, _ := model.JSONFrom(diagnosticResponsePayload{diagnostic, payload.workVersion, payload.iteration})
		return LocalResponseIntent{model.EventReviewAcceptRejected, value, source.Key()}
	}
	return outcomeResponse(source, payload, "rejected", diagnostic)
}

func expiryResponse(source model.Event, current model.ReviewWork,
	transition TransitionIntent,
) LocalResponseIntent {
	value, _ := model.JSONFrom(expiryResponsePayload{
		time.Unix(0, current.DeadlineUnixNano()).UTC().Format(time.RFC3339Nano),
		current.Version(), current.Iteration(),
	})
	causes := source.CausedBy()
	return LocalResponseIntent{model.EventReviewExpired, value, causes[0]}
}

func outcomeResponse(source model.Event, payload importedPayload, status, diagnostic string) LocalResponseIntent {
	value, _ := model.JSONFrom(outcomeResponsePayload{
		status, diagnostic, source.ID().String(), payload.workVersion, payload.iteration,
	})
	return LocalResponseIntent{model.EventReviewOutcome, value, source.Key()}
}

func appliedPlan(work ImportWorkIntent, handling *ImportHandlingIntent,
	settlement *ImportHandlingSettlement, responses []LocalResponseIntent,
) ImportPlan {
	plan := terminalImportPlan(ImportApply, model.InboxAccepted, "")
	if work.source.Valid() {
		plan.work, plan.hasWork = work, true
	}
	if handling != nil {
		plan.handling, plan.hasHandling = *handling, true
	}
	if settlement != nil {
		plan.settlement, plan.hasSettle = *settlement, true
	}
	plan.responses = append([]LocalResponseIntent(nil), responses...)
	return plan
}

func terminalImportPlan(disposition ImportDisposition, status model.InboxStatus,
	diagnostic string,
) ImportPlan {
	return ImportPlan{disposition: disposition, inboxStatus: status, diagnostic: diagnostic}
}

func statusForDisposition(disposition ImportDisposition) model.InboxStatus {
	switch disposition {
	case ImportRetry:
		return model.InboxRetry
	case ImportReject:
		return model.InboxRejected
	case ImportConflict:
		return model.InboxConflicted
	case ImportApply, ImportReceiptOnly:
		return model.InboxAccepted
	default:
		return model.InboxConflicted
	}
}

func transitionDiagnostic(err error) string {
	switch {
	case errors.Is(err, ErrWorkVersionExhausted):
		return "work_version_exhausted"
	case errors.Is(err, ErrTerminalWork):
		return "terminal_work"
	case errors.Is(err, ErrTransitionNotAllowed):
		return "state_conflict"
	default:
		return "transition_conflict"
	}
}

type importedPayload struct {
	workVersion      uint64
	iteration        uint8
	deadlineUnixNano int64
	content          string
	note             string
	diagnosticCode   string
	status           string
	decisionRef      string
}

type versionResponsePayload struct {
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

type diagnosticResponsePayload struct {
	DiagnosticCode string `json:"diagnostic_code"`
	WorkVersion    uint64 `json:"work_version"`
	Iteration      uint8  `json:"iteration"`
}

type expiryResponsePayload struct {
	Deadline    string `json:"deadline"`
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

type outcomeResponsePayload struct {
	Status         string `json:"status"`
	DiagnosticCode string `json:"diagnostic_code"`
	DecisionRef    string `json:"decision_ref"`
	WorkVersion    uint64 `json:"work_version"`
	Iteration      uint8  `json:"iteration"`
}

type importedVersionPayload struct {
	WorkVersion uint64 `json:"work_version"`
	Iteration   uint8  `json:"iteration"`
}

func parseImportedPayload(event model.Event) (importedPayload, error) {
	result := importedPayload{}
	var deadline string
	switch event.Type() {
	case model.EventReviewOffered:
		value := struct {
			Content  string `json:"content"`
			Deadline string `json:"deadline"`
			importedVersionPayload
		}{}
		if err := decodeImportedPayload(event, &value); err != nil || validateImportedText(value.Content, true) != nil {
			return importedPayload{}, payloadError(err)
		}
		result.content, deadline = value.Content, value.Deadline
		result.workVersion, result.iteration = value.WorkVersion, value.Iteration
	case model.EventReviewAcceptRequested, model.EventReviewClosed:
		value := struct {
			Note string `json:"note"`
			importedVersionPayload
		}{}
		if err := decodeImportedPayload(event, &value); err != nil || validateImportedText(value.Note, false) != nil {
			return importedPayload{}, payloadError(err)
		}
		result.note, result.workVersion, result.iteration = value.Note, value.WorkVersion, value.Iteration
	case model.EventReviewDeclineRequested, model.EventReviewDeliveryReady,
		model.EventReviewReworkRequested, model.EventReviewCancelled:
		value := struct {
			Content string `json:"content"`
			importedVersionPayload
		}{}
		if err := decodeImportedPayload(event, &value); err != nil || validateImportedText(value.Content, true) != nil {
			return importedPayload{}, payloadError(err)
		}
		result.content, result.workVersion, result.iteration = value.Content, value.WorkVersion, value.Iteration
	case model.EventReviewAccepted, model.EventReviewDelivered, model.EventReviewDeclined:
		value := importedVersionPayload{}
		if err := decodeImportedPayload(event, &value); err != nil {
			return importedPayload{}, payloadError(err)
		}
		result.workVersion, result.iteration = value.WorkVersion, value.Iteration
	case model.EventReviewAcceptRejected:
		value := struct {
			DiagnosticCode string `json:"diagnostic_code"`
			importedVersionPayload
		}{}
		if err := decodeImportedPayload(event, &value); err != nil || validateImportedToken(value.DiagnosticCode) != nil {
			return importedPayload{}, payloadError(err)
		}
		result.diagnosticCode, result.workVersion, result.iteration = value.DiagnosticCode, value.WorkVersion, value.Iteration
	case model.EventReviewExpired:
		value := struct {
			Deadline string `json:"deadline"`
			importedVersionPayload
		}{}
		if err := decodeImportedPayload(event, &value); err != nil {
			return importedPayload{}, payloadError(err)
		}
		deadline, result.workVersion, result.iteration = value.Deadline, value.WorkVersion, value.Iteration
	case model.EventReviewOutcome:
		value := struct {
			Status         string `json:"status"`
			DiagnosticCode string `json:"diagnostic_code"`
			DecisionRef    string `json:"decision_ref"`
			importedVersionPayload
		}{}
		if err := decodeImportedPayload(event, &value); err != nil ||
			(value.Status != "accepted" && value.Status != "rejected" && value.Status != "conflicted") ||
			validateImportedToken(value.DiagnosticCode) != nil || validateImportedToken(value.DecisionRef) != nil {
			return importedPayload{}, payloadError(err)
		}
		result.status, result.diagnosticCode, result.decisionRef = value.Status, value.DiagnosticCode, value.DecisionRef
		result.workVersion, result.iteration = value.WorkVersion, value.Iteration
	default:
		return importedPayload{}, errors.New("unknown closed Event type")
	}
	if result.workVersion == 0 || result.workVersion > model.MaxSQLiteInteger ||
		result.iteration < 1 || result.iteration > 2 {
		return importedPayload{}, errors.New("Work version or iteration is outside durable bounds")
	}
	if deadline != "" {
		parsed, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil || parsed.UnixNano() <= 0 || parsed.Location() != time.UTC ||
			parsed.UTC().Format(time.RFC3339Nano) != deadline {
			return importedPayload{}, errors.New("deadline is not canonical UTC RFC3339Nano")
		}
		result.deadlineUnixNano = parsed.UnixNano()
	}
	return result, nil
}

func decodeImportedPayload(event model.Event, destination any) error {
	raw := event.Payload().Bytes()
	canonical, err := model.NewJSON(raw)
	if err != nil || !bytes.Equal(canonical.Bytes(), raw) {
		return errors.New("payload is not canonical JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON value")
	}
	return nil
}

func validateImportedText(value string, required bool) error {
	if !utf8.ValidString(value) || (required && value == "") || len(value) > model.MaxContentBytes {
		return errors.New("invalid bounded UTF-8 content")
	}
	for _, character := range value {
		if character == 0 || (character < 0x20 && character != '\n' && character != '\t') {
			return errors.New("content contains a forbidden control character")
		}
	}
	return nil
}

func validateImportedToken(value string) error {
	if !utf8.ValidString(value) || value == "" || len(value) > model.MaxIdentifierBytes {
		return errors.New("invalid token")
	}
	for _, character := range value {
		if character <= 0x20 || character == 0x7f {
			return errors.New("invalid token")
		}
	}
	return nil
}

func payloadError(err error) error {
	if err == nil {
		return errors.New("payload violates its closed Event schema")
	}
	return err
}
