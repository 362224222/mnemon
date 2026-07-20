package model

import "strings"

const (
	TeamworkActionCount       = 7
	MaxTeamworkActionContexts = 8
)

// TeamworkActionContext is the closed structural vocabulary used by managed
// Action assets. Durable code derives these contexts from trusted Work/Event
// facts; an empty context set never implies the contextless initiation value.
type TeamworkActionContext string

const (
	TeamworkActionContextNone                    TeamworkActionContext = "none"
	TeamworkActionContextReviewerOffered         TeamworkActionContext = "reviewer_offered"
	TeamworkActionContextReviewerActive          TeamworkActionContext = "reviewer_active"
	TeamworkActionContextReviewerRework          TeamworkActionContext = "reviewer_rework"
	TeamworkActionContextParentResume            TeamworkActionContext = "parent_resume"
	TeamworkActionContextHomeDelivered           TeamworkActionContext = "home_delivered"
	TeamworkActionContextHomeDeliveredIteration1 TeamworkActionContext = "home_delivered_iteration_1"
	TeamworkActionContextHomeNonterminal         TeamworkActionContext = "home_nonterminal"
)

type teamworkActionMechanic struct {
	operation  OperationKind
	eventType  EventType
	contexts   [MaxTeamworkActionContexts]TeamworkActionContext
	contextLen uint8
	maxResults uint8
}

func teamworkActionMechanics() [TeamworkActionCount]teamworkActionMechanic {
	return [TeamworkActionCount]teamworkActionMechanic{
		{operation: OperationTeamworkOffer, eventType: EventReviewOffered,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextNone, TeamworkActionContextReviewerActive,
				TeamworkActionContextReviewerRework,
			}, contextLen: 3, maxResults: MaxChildWorks},
		{operation: OperationTeamworkAccept, eventType: EventReviewAcceptRequested,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextReviewerOffered,
			}, contextLen: 1, maxResults: 1},
		{operation: OperationTeamworkDecline, eventType: EventReviewDeclineRequested,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextReviewerOffered,
			}, contextLen: 1, maxResults: 1},
		{operation: OperationTeamworkDeliver, eventType: EventReviewDeliveryReady,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextReviewerActive, TeamworkActionContextReviewerRework,
				TeamworkActionContextParentResume,
			}, contextLen: 3, maxResults: 1},
		{operation: OperationTeamworkRework, eventType: EventReviewReworkRequested,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextHomeDeliveredIteration1,
			}, contextLen: 1, maxResults: 1},
		{operation: OperationTeamworkClose, eventType: EventReviewClosed,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextHomeDelivered,
			}, contextLen: 1, maxResults: 1},
		{operation: OperationTeamworkCancel, eventType: EventReviewCancelled,
			contexts: [MaxTeamworkActionContexts]TeamworkActionContext{
				TeamworkActionContextHomeNonterminal,
			}, contextLen: 1, maxResults: 1},
	}
}

func (context TeamworkActionContext) Valid() bool {
	switch context {
	case TeamworkActionContextNone, TeamworkActionContextReviewerOffered,
		TeamworkActionContextReviewerActive, TeamworkActionContextReviewerRework,
		TeamworkActionContextParentResume, TeamworkActionContextHomeDelivered,
		TeamworkActionContextHomeDeliveredIteration1, TeamworkActionContextHomeNonterminal:
		return true
	default:
		return false
	}
}

// TeamworkActionPolicyEntrySpec is projected from one canonical asset
// descriptor joined with the Agent-owned typed Event mechanic. The model
// validates that binding without deriving a second mutable transport schema.
type TeamworkActionPolicyEntrySpec struct {
	Ordinal         uint8
	OperationKind   OperationKind
	EventType       EventType
	AllowedContexts []TeamworkActionContext
	MaxResults      uint8
}

type TeamworkActionPolicySpec struct {
	AssetRevision Digest
	Entries       []TeamworkActionPolicyEntrySpec
}

// TeamworkActionPolicyEntry is one immutable runtime projection. It carries no
// callbacks and grants no mutation authority.
type TeamworkActionPolicyEntry struct {
	ordinal    uint8
	operation  OperationKind
	eventType  EventType
	contexts   [MaxTeamworkActionContexts]TeamworkActionContext
	contextLen uint8
	maxResults uint8
}

func (entry TeamworkActionPolicyEntry) Ordinal() uint8               { return entry.ordinal }
func (entry TeamworkActionPolicyEntry) OperationKind() OperationKind { return entry.operation }
func (entry TeamworkActionPolicyEntry) EventType() EventType         { return entry.eventType }
func (entry TeamworkActionPolicyEntry) MaxResults() uint8            { return entry.maxResults }
func (entry TeamworkActionPolicyEntry) AllowedContexts() []TeamworkActionContext {
	return append([]TeamworkActionContext(nil), entry.contexts[:entry.contextLen]...)
}

func (entry TeamworkActionPolicyEntry) AllowsContext(context TeamworkActionContext) bool {
	for _, allowed := range entry.contexts[:entry.contextLen] {
		if allowed == context {
			return true
		}
	}
	return false
}

// TeamworkActionPolicy is a revision-bound, deterministic and immutable
// projection shared by Agent execution and lower Store validation. The zero
// value is inert and every lookup fails closed.
type TeamworkActionPolicy struct {
	assetRevision Digest
	entries       [TeamworkActionCount]TeamworkActionPolicyEntry
	ready         bool
}

func NewTeamworkActionPolicy(spec TeamworkActionPolicySpec) (TeamworkActionPolicy, error) {
	if spec.AssetRevision.IsZero() || len(spec.Entries) != TeamworkActionCount ||
		!validTeamworkActionMechanics() {
		return TeamworkActionPolicy{}, invalid("Teamwork Action policy",
			"asset revision and complete Action entries are required")
	}
	result := TeamworkActionPolicy{assetRevision: spec.AssetRevision}
	for index, candidate := range spec.Entries {
		entry, err := newTeamworkActionPolicyEntry(candidate, uint8(index), result.entries[:index])
		if err != nil {
			return TeamworkActionPolicy{}, err
		}
		result.entries[index] = entry
	}
	if !validTeamworkActionPolicyShape(result.entries) {
		return TeamworkActionPolicy{}, invalid("Teamwork Action policy",
			"contextless initiation and batch result shape must have one shared owner")
	}
	result.ready = true
	return result, nil
}

func newTeamworkActionPolicyEntry(spec TeamworkActionPolicyEntrySpec, ordinal uint8,
	prior []TeamworkActionPolicyEntry,
) (TeamworkActionPolicyEntry, error) {
	mechanic, mechanicOK := teamworkActionMechanicFor(spec.OperationKind)
	if spec.Ordinal != ordinal || !mechanicOK || spec.EventType != mechanic.eventType ||
		len(spec.AllowedContexts) == 0 || len(spec.AllowedContexts) > MaxTeamworkActionContexts ||
		spec.MaxResults == 0 || spec.MaxResults > mechanic.maxResults {
		return TeamworkActionPolicyEntry{}, invalid("Teamwork Action policy entry",
			"identity, Event, context or result shape is invalid")
	}
	for _, existing := range prior {
		if existing.operation == spec.OperationKind {
			return TeamworkActionPolicyEntry{}, invalid("Teamwork Action policy entry",
				"operation identities must be unique")
		}
		if existing.eventType == spec.EventType {
			return TeamworkActionPolicyEntry{}, invalid("Teamwork Action policy entry",
				"Event identities must be unique")
		}
	}
	entry := TeamworkActionPolicyEntry{ordinal: spec.Ordinal, operation: spec.OperationKind,
		eventType: spec.EventType, contextLen: uint8(len(spec.AllowedContexts)),
		maxResults: spec.MaxResults}
	for index, context := range spec.AllowedContexts {
		if !context.Valid() {
			return TeamworkActionPolicyEntry{}, invalid("Teamwork Action policy entry",
				"contains an unknown context")
		}
		for priorIndex := 0; priorIndex < index; priorIndex++ {
			if entry.contexts[priorIndex] == context {
				return TeamworkActionPolicyEntry{}, invalid("Teamwork Action policy entry",
					"contains a duplicate context")
			}
		}
		entry.contexts[index] = context
	}
	return entry, nil
}

func teamworkActionMechanicFor(operation OperationKind) (teamworkActionMechanic, bool) {
	if !operation.Valid() {
		return teamworkActionMechanic{}, false
	}
	for _, mechanic := range teamworkActionMechanics() {
		if mechanic.operation == operation {
			return mechanic, true
		}
	}
	return teamworkActionMechanic{}, false
}

func validTeamworkActionMechanics() bool {
	operations := make(map[OperationKind]struct{}, TeamworkActionCount)
	events := make(map[EventType]struct{}, TeamworkActionCount)
	batchOperation, contextlessOperation := OperationKind(""), OperationKind("")
	for _, mechanic := range teamworkActionMechanics() {
		if !validTeamworkActionMechanic(mechanic) {
			return false
		}
		if _, duplicate := operations[mechanic.operation]; duplicate {
			return false
		}
		if _, duplicate := events[mechanic.eventType]; duplicate {
			return false
		}
		operations[mechanic.operation], events[mechanic.eventType] = struct{}{}, struct{}{}
		if mechanic.maxResults == MaxChildWorks {
			if batchOperation != "" {
				return false
			}
			batchOperation = mechanic.operation
		}
		if mechanicAllowsContext(mechanic, TeamworkActionContextNone) {
			if contextlessOperation != "" {
				return false
			}
			contextlessOperation = mechanic.operation
		}
	}
	return len(operations) == TeamworkActionCount && len(events) == TeamworkActionCount &&
		batchOperation == OperationTeamworkOffer && contextlessOperation == OperationTeamworkOffer
}

func validTeamworkActionMechanic(mechanic teamworkActionMechanic) bool {
	if !mechanic.operation.Valid() || !strings.HasPrefix(string(mechanic.operation), "teamwork.") ||
		!mechanic.eventType.AgentAdmitted() || mechanic.contextLen == 0 ||
		mechanic.contextLen > MaxTeamworkActionContexts ||
		(mechanic.maxResults != 1 && mechanic.maxResults != MaxChildWorks) {
		return false
	}
	for index, context := range mechanic.contexts[:mechanic.contextLen] {
		if !context.Valid() {
			return false
		}
		for prior := 0; prior < index; prior++ {
			if mechanic.contexts[prior] == context {
				return false
			}
		}
	}
	for _, context := range mechanic.contexts[mechanic.contextLen:] {
		if context != "" {
			return false
		}
	}
	return true
}

func mechanicAllowsContext(mechanic teamworkActionMechanic, context TeamworkActionContext) bool {
	for _, candidate := range mechanic.contexts[:mechanic.contextLen] {
		if candidate == context {
			return true
		}
	}
	return false
}

func validTeamworkActionPolicyShape(entries [TeamworkActionCount]TeamworkActionPolicyEntry) bool {
	batchOwner, contextlessOwner := -1, -1
	for index, entry := range entries {
		if entry.maxResults > 1 {
			if batchOwner != -1 {
				return false
			}
			batchOwner = index
		}
		if entry.AllowsContext(TeamworkActionContextNone) {
			if contextlessOwner != -1 {
				return false
			}
			contextlessOwner = index
		}
	}
	return batchOwner >= 0 && batchOwner == contextlessOwner &&
		entries[batchOwner].operation == OperationTeamworkOffer
}

func (policy TeamworkActionPolicy) AssetRevision() Digest { return policy.assetRevision }

func (policy TeamworkActionPolicy) Entries() []TeamworkActionPolicyEntry {
	if !policy.ready {
		return nil
	}
	return append([]TeamworkActionPolicyEntry(nil), policy.entries[:]...)
}

func (policy TeamworkActionPolicy) Operation(kind OperationKind) (TeamworkActionPolicyEntry, bool) {
	if !policy.ready {
		return TeamworkActionPolicyEntry{}, false
	}
	for _, entry := range policy.entries {
		if entry.operation == kind {
			return entry, true
		}
	}
	return TeamworkActionPolicyEntry{}, false
}

// ActionsForContexts returns the union in canonical asset ordinal order.
// Empty input is a valid empty managed context set and never enables `none`.
func (policy TeamworkActionPolicy) ActionsForContexts(
	contexts []TeamworkActionContext,
) ([]OperationKind, error) {
	if !policy.ready {
		return nil, invariant("Teamwork Action policy is unavailable")
	}
	for index, context := range contexts {
		if !context.Valid() {
			return nil, invalid("Teamwork Action contexts", "contain an unknown value")
		}
		if context == TeamworkActionContextNone && len(contexts) != 1 {
			return nil, invalid("Teamwork Action contexts",
				"contextless initiation cannot be combined with managed context")
		}
		for prior := 0; prior < index; prior++ {
			if contexts[prior] == context {
				return nil, invalid("Teamwork Action contexts", "contain a duplicate value")
			}
		}
	}
	result := make([]OperationKind, 0, TeamworkActionCount)
	for _, entry := range policy.entries {
		for _, context := range contexts {
			if entry.AllowsContext(context) {
				result = append(result, entry.OperationKind())
				break
			}
		}
	}
	return result, nil
}
