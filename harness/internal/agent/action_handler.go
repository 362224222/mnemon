package agent

import (
	"errors"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

// ActionHandler is one immutable join between a canonical Action descriptor
// and the fixed Go mechanics needed to admit its typed Event. Policy remains
// owned by the descriptor; the handler contributes no second schema.
type ActionHandler struct {
	descriptor    teamwork.ActionDescriptor
	assetRevision model.Digest
	policyEntry   model.TeamworkActionPolicyEntry
	mechanic      actionMechanic
	ready         bool
}

func (handler ActionHandler) Name() string { return handler.descriptor.Name() }
func (handler ActionHandler) OperationKind() model.OperationKind {
	return handler.descriptor.OperationKind()
}
func (handler ActionHandler) EventType() model.EventType { return handler.policyEntry.EventType() }
func (handler ActionHandler) Descriptor() teamwork.ActionDescriptor {
	return handler.descriptor
}

// ActionHandlers is the closed immutable runtime projection of exactly one
// whole managed asset revision. Its zero value is inert.
type ActionHandlers struct {
	actions [teamwork.TeamworkActionCount]ActionHandler
	policy  model.TeamworkActionPolicy
	ready   bool
}

func NewActionHandlers(policy ActionPolicy) (ActionHandlers, error) {
	revision, descriptors := policy.AssetRevision(), policy.Actions()
	if revision.IsZero() || len(descriptors) != teamwork.TeamworkActionCount {
		return ActionHandlers{}, errors.New("compose Agent Action handlers: Action policy is unavailable")
	}
	entries := make([]model.TeamworkActionPolicyEntrySpec, len(descriptors))
	for index, descriptor := range descriptors {
		_, mechanicOK := actionMechanicFor(descriptor.OperationKind())
		if !mechanicOK || descriptor.Ordinal() != uint8(index) ||
			descriptor.Receipt().Action() != descriptor.OperationKind() {
			return ActionHandlers{}, errors.New("compose Agent Action handlers: typed mechanics are incomplete")
		}
		entries[index] = model.TeamworkActionPolicyEntrySpec{Ordinal: descriptor.Ordinal(),
			OperationKind: descriptor.OperationKind(), AllowedContexts: descriptor.AllowedContexts(),
			MaxResults: descriptor.Receipt().MaxResults()}
	}
	runtimePolicy, err := model.NewTeamworkActionPolicy(model.TeamworkActionPolicySpec{
		AssetRevision: revision, Entries: entries})
	if err != nil {
		return ActionHandlers{}, errors.New("compose Agent Action handlers: runtime policy is incomplete")
	}
	result := ActionHandlers{policy: runtimePolicy}
	for index, descriptor := range descriptors {
		entry, entryOK := runtimePolicy.Operation(descriptor.OperationKind())
		mechanic, mechanicOK := actionMechanicFor(descriptor.OperationKind())
		if !entryOK || !mechanicOK || entry.Ordinal() != uint8(index) ||
			!mechanic.compatible(descriptor, entry) {
			return ActionHandlers{}, errors.New("compose Agent Action handlers: typed mechanics are incomplete")
		}
		result.actions[index] = ActionHandler{descriptor: descriptor,
			assetRevision: revision, policyEntry: entry, mechanic: mechanic, ready: true}
	}
	result.ready = true
	return result, nil
}

func (handlers ActionHandlers) AssetRevision() model.Digest { return handlers.policy.AssetRevision() }
func (handlers ActionHandlers) RuntimePolicy() model.TeamworkActionPolicy {
	return handlers.policy
}
func (handlers ActionHandlers) Actions() []ActionHandler {
	if !handlers.ready {
		return nil
	}
	return append([]ActionHandler(nil), handlers.actions[:]...)
}
func (handlers ActionHandlers) Action(name string) (ActionHandler, bool) {
	if !handlers.ready {
		return ActionHandler{}, false
	}
	for _, handler := range handlers.actions {
		if handler.Name() == name {
			return handler, true
		}
	}
	return ActionHandler{}, false
}
func (handlers ActionHandlers) Operation(kind model.OperationKind) (ActionHandler, bool) {
	if !handlers.ready {
		return ActionHandler{}, false
	}
	for _, handler := range handlers.actions {
		if handler.OperationKind() == kind {
			return handler, true
		}
	}
	return ActionHandler{}, false
}

func (handler ActionHandler) candidate(content string,
	deadline time.Duration,
) (event.AgentCandidate, error) {
	if !handler.ready || handler.mechanic.candidate == nil {
		return nil, errors.New("Teamwork action has no typed Event mechanic")
	}
	return handler.mechanic.candidate(content, deadline)
}

func (handler ActionHandler) currentAuthority(work model.ReviewWork) (
	participant bool, localActor, target model.PeerID, valid bool,
) {
	eventType := handler.EventType()
	home, reviewer := work.Ref().HomePeerID(), work.Participants().ReviewerPeerID()
	if eventType.ParticipantInput() {
		return true, reviewer, home, teamwork.ActionResultStateAllowed(eventType, work.State())
	}
	return false, home, reviewer, eventType.AgentAdmitted() && eventType.HomeAuthoritative()
}

type actionCandidateFactory func(string, time.Duration) (event.AgentCandidate, error)

type actionMechanic struct {
	candidate       actionCandidateFactory
	contentRequired bool
	selection       bool
}

func actionMechanicFor(kind model.OperationKind) (actionMechanic, bool) {
	mechanic := actionMechanic{}
	switch kind {
	case model.OperationTeamworkOffer:
		mechanic = actionMechanic{candidate: func(content string, deadline time.Duration) (event.AgentCandidate, error) {
			return event.NewOfferCandidate(content, deadline)
		}, contentRequired: true, selection: true}
	case model.OperationTeamworkAccept:
		mechanic = actionMechanic{candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
			return event.NewAcceptCandidate(content)
		}}
	case model.OperationTeamworkDecline:
		mechanic = actionMechanic{candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
			return event.NewDeclineCandidate(content)
		}, contentRequired: true}
	case model.OperationTeamworkDeliver:
		mechanic = actionMechanic{candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
			return event.NewDeliverCandidate(content)
		}, contentRequired: true}
	case model.OperationTeamworkRework:
		mechanic = actionMechanic{candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
			return event.NewReworkCandidate(content)
		}, contentRequired: true}
	case model.OperationTeamworkClose:
		mechanic = actionMechanic{candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
			return event.NewCloseCandidate(content)
		}}
	case model.OperationTeamworkCancel:
		mechanic = actionMechanic{candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
			return event.NewCancelCandidate(content)
		}, contentRequired: true}
	default:
		return actionMechanic{}, false
	}
	return mechanic, true
}

func (mechanic actionMechanic) typedEvent() (model.EventType, bool) {
	if mechanic.candidate == nil {
		return "", false
	}
	probe, err := mechanic.candidate("typed Action mechanic probe", 0)
	if err != nil || probe == nil || !probe.EventType().AgentAdmitted() {
		return "", false
	}
	return probe.EventType(), true
}

func (mechanic actionMechanic) compatible(descriptor teamwork.ActionDescriptor,
	entry model.TeamworkActionPolicyEntry,
) bool {
	eventType, eventOK := mechanic.typedEvent()
	return eventOK && eventType == entry.EventType() &&
		mechanic.executionShapeCompatible(descriptor, entry) &&
		actionContextsCompatible(entry) &&
		artifactShapeCompatible(descriptor.Artifacts(), entry.EventType())
}

func (mechanic actionMechanic) executionShapeCompatible(descriptor teamwork.ActionDescriptor,
	entry model.TeamworkActionPolicyEntry,
) bool {
	_, hasDeadline := descriptor.Deadline()
	_, hasSelectors := descriptor.Selectors()
	receipt := descriptor.Receipt()
	wantHandling := teamwork.ReceiptHandlingCompleted
	if mechanic.selection {
		wantHandling = teamwork.ReceiptHandlingContextDependent
	}
	multiResultReady := entry.MaxResults() == 1 || mechanic.selection
	contextlessReady := !entry.AllowsContext(model.TeamworkActionContextNone) || mechanic.selection
	return multiResultReady && contextlessReady &&
		entry.OperationKind() == descriptor.OperationKind() &&
		(!mechanic.contentRequired || descriptor.Content().Required()) &&
		hasDeadline == mechanic.selection && hasSelectors == mechanic.selection &&
		receipt.Handling() == wantHandling && receipt.MaxResults() == entry.MaxResults()
}

func actionContextsCompatible(entry model.TeamworkActionPolicyEntry) bool {
	for _, context := range entry.AllowedContexts() {
		if !teamwork.ActionContextSupportsEvent(context, entry.EventType()) {
			return false
		}
	}
	return true
}

func artifactShapeCompatible(policy teamwork.ActionArtifactPolicy, eventType model.EventType) bool {
	return !policy.Allowed() ||
		(eventType.AllowsArtifacts() &&
			policy.MaxEntries() == artifact.MaxEntries &&
			policy.MaxPathBytes() == artifact.MaxLogicalPathBytes &&
			policy.MaxRoots() <= artifact.MaxRoots &&
			policy.MaxTotalBytes() == artifact.MaxTotalBytes)
}
