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
	mechanic      actionMechanic
	ready         bool
}

func (handler ActionHandler) Name() string { return handler.descriptor.Name() }
func (handler ActionHandler) OperationKind() model.OperationKind {
	return handler.descriptor.OperationKind()
}
func (handler ActionHandler) EventType() model.EventType { return handler.mechanic.eventType }
func (handler ActionHandler) Descriptor() teamwork.ActionDescriptor {
	return handler.descriptor
}

// ActionHandlers is the closed immutable runtime projection of exactly one
// whole managed asset revision. Its zero value is inert.
type ActionHandlers struct {
	actions       [teamwork.TeamworkActionCount]ActionHandler
	assetRevision model.Digest
	ready         bool
}

func NewActionHandlers(policy ActionPolicy) (ActionHandlers, error) {
	revision, descriptors := policy.AssetRevision(), policy.Actions()
	if revision.IsZero() || len(descriptors) != teamwork.TeamworkActionCount {
		return ActionHandlers{}, errors.New("compose Agent Action handlers: Action policy is unavailable")
	}
	result := ActionHandlers{assetRevision: revision}
	for index, descriptor := range descriptors {
		mechanic, ok := actionMechanicFor(descriptor.OperationKind())
		if !ok || descriptor.Ordinal() != uint8(index) ||
			descriptor.Receipt().Action() != descriptor.OperationKind() ||
			!mechanic.compatible(descriptor) {
			return ActionHandlers{}, errors.New("compose Agent Action handlers: typed mechanics are incomplete")
		}
		for prior := 0; prior < index; prior++ {
			if result.actions[prior].EventType() == mechanic.eventType {
				return ActionHandlers{}, errors.New("compose Agent Action handlers: typed Event mechanics are duplicated")
			}
		}
		result.actions[index] = ActionHandler{descriptor: descriptor,
			assetRevision: revision, mechanic: mechanic, ready: true}
	}
	result.ready = true
	return result, nil
}

func (handlers ActionHandlers) AssetRevision() model.Digest { return handlers.assetRevision }
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

type actionActor uint8

const (
	actionActorOffer actionActor = iota + 1
	actionActorParticipant
	actionActorHome
)

type actionCandidateFactory func(string, time.Duration) (event.AgentCandidate, error)
type committedStateValidator func(model.WorkState) bool

type actionMechanic struct {
	eventType       model.EventType
	candidate       actionCandidateFactory
	actor           actionActor
	contexts        []teamwork.ActionContext
	contentRequired bool
	selection       bool
	batch           bool
	committedState  committedStateValidator
}

func actionMechanicFor(kind model.OperationKind) (actionMechanic, bool) {
	mechanic := actionMechanic{}
	switch kind {
	case model.OperationTeamworkOffer:
		mechanic = actionMechanic{eventType: model.EventReviewOffered,
			candidate: func(content string, deadline time.Duration) (event.AgentCandidate, error) {
				return event.NewOfferCandidate(content, deadline)
			}, actor: actionActorOffer,
			contexts: []teamwork.ActionContext{teamwork.ActionContextNone,
				teamwork.ActionContextReviewerActive, teamwork.ActionContextReviewerRework},
			contentRequired: true, selection: true, batch: true}
	case model.OperationTeamworkAccept:
		mechanic = actionMechanic{eventType: model.EventReviewAcceptRequested,
			candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
				return event.NewAcceptCandidate(content)
			}, actor: actionActorParticipant,
			contexts:       []teamwork.ActionContext{teamwork.ActionContextReviewerOffered},
			committedState: func(state model.WorkState) bool { return state == model.WorkOffered }}
	case model.OperationTeamworkDecline:
		mechanic = actionMechanic{eventType: model.EventReviewDeclineRequested,
			candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
				return event.NewDeclineCandidate(content)
			}, actor: actionActorParticipant,
			contexts:        []teamwork.ActionContext{teamwork.ActionContextReviewerOffered},
			contentRequired: true,
			committedState:  func(state model.WorkState) bool { return state == model.WorkOffered }}
	case model.OperationTeamworkDeliver:
		mechanic = actionMechanic{eventType: model.EventReviewDeliveryReady,
			candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
				return event.NewDeliverCandidate(content)
			}, actor: actionActorParticipant,
			contexts: []teamwork.ActionContext{teamwork.ActionContextReviewerActive,
				teamwork.ActionContextReviewerRework, teamwork.ActionContextParentResume},
			contentRequired: true,
			committedState: func(state model.WorkState) bool {
				return state == model.WorkActive || state == model.WorkRework
			}}
	case model.OperationTeamworkRework:
		mechanic = actionMechanic{eventType: model.EventReviewReworkRequested,
			candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
				return event.NewReworkCandidate(content)
			}, actor: actionActorHome,
			contexts:        []teamwork.ActionContext{teamwork.ActionContextHomeDeliveredIteration1},
			contentRequired: true,
			committedState:  func(state model.WorkState) bool { return state == model.WorkRework }}
	case model.OperationTeamworkClose:
		mechanic = actionMechanic{eventType: model.EventReviewClosed,
			candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
				return event.NewCloseCandidate(content)
			}, actor: actionActorHome,
			contexts:       []teamwork.ActionContext{teamwork.ActionContextHomeDelivered},
			committedState: func(state model.WorkState) bool { return state == model.WorkClosed }}
	case model.OperationTeamworkCancel:
		mechanic = actionMechanic{eventType: model.EventReviewCancelled,
			candidate: func(content string, _ time.Duration) (event.AgentCandidate, error) {
				return event.NewCancelCandidate(content)
			}, actor: actionActorHome,
			contexts:        []teamwork.ActionContext{teamwork.ActionContextHomeNonterminal},
			contentRequired: true,
			committedState:  func(state model.WorkState) bool { return state == model.WorkCancelled }}
	default:
		return actionMechanic{}, false
	}
	return mechanic, true
}

func (mechanic actionMechanic) compatible(descriptor teamwork.ActionDescriptor) bool {
	_, hasDeadline := descriptor.Deadline()
	_, hasSelectors := descriptor.Selectors()
	receipt := descriptor.Receipt()
	wantResults := uint8(1)
	wantHandling := teamwork.ReceiptHandlingCompleted
	if mechanic.batch {
		wantResults = model.MaxChildWorks
		wantHandling = teamwork.ReceiptHandlingContextDependent
	}
	stateMechanicReady := mechanic.actor == actionActorOffer || mechanic.committedState != nil
	artifactPolicy := descriptor.Artifacts()
	artifactMechanicReady := !artifactPolicy.Allowed() ||
		(mechanic.eventType.AllowsArtifacts() &&
			artifactPolicy.MaxEntries() == artifact.MaxEntries &&
			artifactPolicy.MaxPathBytes() == artifact.MaxLogicalPathBytes &&
			artifactPolicy.MaxRoots() <= artifact.MaxRoots &&
			artifactPolicy.MaxTotalBytes() == artifact.MaxTotalBytes)
	return mechanic.candidate != nil && mechanic.eventType.Valid() && mechanic.actor != 0 && stateMechanicReady &&
		artifactMechanicReady && sameActionContexts(mechanic.contexts, descriptor.AllowedContexts()) &&
		(!mechanic.contentRequired || descriptor.Content().Required()) &&
		hasDeadline == mechanic.selection && hasSelectors == mechanic.selection &&
		receipt.Handling() == wantHandling && receipt.MaxResults() == wantResults
}

func sameActionContexts(left, right []teamwork.ActionContext) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[teamwork.ActionContext]struct{}, len(left))
	for _, context := range left {
		seen[context] = struct{}{}
	}
	for _, context := range right {
		if _, ok := seen[context]; !ok {
			return false
		}
	}
	return true
}
