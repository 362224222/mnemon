package teamwork

import "github.com/mnemon-dev/mnemon/harness/internal/model"

// ActionContextSupportsEvent verifies that an asset-owned context is a safe
// subset of the typed Teamwork state machine. It does not choose contexts for
// an Action; it only prevents a reviewed asset from advertising an Event where
// its trusted context facts cannot satisfy that Event's actor/state mechanics.
func ActionContextSupportsEvent(context ActionContext, eventType model.EventType) bool {
	if !context.Valid() || !eventType.AgentAdmitted() {
		return false
	}
	switch context {
	case ActionContextNone:
		return eventType == model.EventReviewOffered
	case ActionContextReviewerOffered:
		return participantRequestAllowed(eventType, model.WorkOffered)
	case ActionContextReviewerActive:
		return reviewerActionAllowed(eventType, model.WorkActive)
	case ActionContextReviewerRework:
		return reviewerActionAllowed(eventType, model.WorkRework)
	case ActionContextParentResume:
		return reviewerActionAllowed(eventType, model.WorkActive) &&
			reviewerActionAllowed(eventType, model.WorkRework)
	case ActionContextHomeDelivered:
		return homeActionAllowed(eventType, model.WorkDelivered, 1) &&
			homeActionAllowed(eventType, model.WorkDelivered, 2)
	case ActionContextHomeDeliveredIteration1:
		return homeActionAllowed(eventType, model.WorkDelivered, 1)
	case ActionContextHomeNonterminal:
		return homeNonterminalActionAllowed(eventType)
	default:
		return false
	}
}

// ActionResultStateAllowed validates the Work state carried by a committed
// Action receipt from the same typed transition authority. Participant request
// receipts retain their source state; home decisions carry their successor.
func ActionResultStateAllowed(eventType model.EventType, state model.WorkState) bool {
	if eventType == model.EventReviewOffered {
		return state == model.WorkOffered
	}
	if _, participant := eventType.ParticipantResponse(); participant {
		return participantRequestAllowed(eventType, state)
	}
	if !eventType.AgentAdmitted() || !eventType.HomeAuthoritative() {
		return false
	}
	for _, fact := range nonterminalWorkFacts() {
		next, _, allowed := model.NextReviewWorkState(fact.state, fact.iteration, eventType)
		if allowed && next == state {
			return true
		}
	}
	return false
}

func participantRequestAllowed(eventType model.EventType, state model.WorkState) bool {
	_, allowed := participantResponse(eventType, state)
	return allowed
}

func reviewerActionAllowed(eventType model.EventType, state model.WorkState) bool {
	if eventType == model.EventReviewOffered {
		return state == model.WorkActive || state == model.WorkRework
	}
	return participantRequestAllowed(eventType, state)
}

func homeActionAllowed(eventType model.EventType, state model.WorkState, iteration uint8) bool {
	_, _, allowed := model.NextReviewWorkState(state, iteration, eventType)
	return allowed
}

func homeNonterminalActionAllowed(eventType model.EventType) bool {
	for _, fact := range nonterminalWorkFacts() {
		if !homeActionAllowed(eventType, fact.state, fact.iteration) {
			return false
		}
	}
	return true
}

func nonterminalWorkFacts() [5]struct {
	state     model.WorkState
	iteration uint8
} {
	return [5]struct {
		state     model.WorkState
		iteration uint8
	}{
		{model.WorkOffered, 1},
		{model.WorkActive, 1},
		{model.WorkDelivered, 1},
		{model.WorkDelivered, 2},
		{model.WorkRework, 2},
	}
}
