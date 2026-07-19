package teamwork

import (
	"reflect"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestActionContextSupportsTypedEventMechanics(t *testing.T) {
	t.Parallel()
	contexts := []ActionContext{ActionContextNone, ActionContextReviewerOffered,
		ActionContextReviewerActive, ActionContextReviewerRework, ActionContextParentResume,
		ActionContextHomeDelivered, ActionContextHomeDeliveredIteration1, ActionContextHomeNonterminal}
	tests := []struct {
		event model.EventType
		want  []ActionContext
	}{
		{model.EventReviewOffered, []ActionContext{ActionContextNone,
			ActionContextReviewerActive, ActionContextReviewerRework, ActionContextParentResume}},
		{model.EventReviewAcceptRequested, []ActionContext{ActionContextReviewerOffered}},
		{model.EventReviewDeclineRequested, []ActionContext{ActionContextReviewerOffered}},
		{model.EventReviewDeliveryReady, []ActionContext{ActionContextReviewerActive,
			ActionContextReviewerRework, ActionContextParentResume}},
		{model.EventReviewReworkRequested, []ActionContext{ActionContextHomeDeliveredIteration1}},
		{model.EventReviewClosed, []ActionContext{ActionContextHomeDelivered,
			ActionContextHomeDeliveredIteration1}},
		{model.EventReviewCancelled, []ActionContext{ActionContextHomeDelivered,
			ActionContextHomeDeliveredIteration1, ActionContextHomeNonterminal}},
	}
	for _, test := range tests {
		got := make([]ActionContext, 0, len(contexts))
		for _, context := range contexts {
			if ActionContextSupportsEvent(context, test.event) {
				got = append(got, context)
			}
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("contexts for %s = %v, want %v", test.event, got, test.want)
		}
		parent := ActionContextSupportsEvent(ActionContextParentResume, test.event)
		active := ActionContextSupportsEvent(ActionContextReviewerActive, test.event)
		rework := ActionContextSupportsEvent(ActionContextReviewerRework, test.event)
		if parent != (active && rework) {
			t.Fatalf("parent-resume capability for %s is not the reviewer-state intersection", test.event)
		}
	}
	if ActionContextSupportsEvent(ActionContext("unknown"), model.EventReviewOffered) ||
		ActionContextSupportsEvent(ActionContextNone, model.EventReviewAccepted) {
		t.Fatal("unknown context or controller Event gained Action support")
	}
}

func TestActionResultStateUsesTypedTransitionMechanics(t *testing.T) {
	t.Parallel()
	states := []model.WorkState{model.WorkOffered, model.WorkActive, model.WorkDelivered,
		model.WorkRework, model.WorkClosed, model.WorkDeclined, model.WorkExpired, model.WorkCancelled}
	tests := []struct {
		event model.EventType
		want  []model.WorkState
	}{
		{model.EventReviewOffered, []model.WorkState{model.WorkOffered}},
		{model.EventReviewAcceptRequested, []model.WorkState{model.WorkOffered}},
		{model.EventReviewDeclineRequested, []model.WorkState{model.WorkOffered}},
		{model.EventReviewDeliveryReady, []model.WorkState{model.WorkActive, model.WorkRework}},
		{model.EventReviewReworkRequested, []model.WorkState{model.WorkRework}},
		{model.EventReviewClosed, []model.WorkState{model.WorkClosed}},
		{model.EventReviewCancelled, []model.WorkState{model.WorkCancelled}},
	}
	for _, test := range tests {
		got := make([]model.WorkState, 0, len(states))
		for _, state := range states {
			if ActionResultStateAllowed(test.event, state) {
				got = append(got, state)
			}
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("result states for %s = %v, want %v", test.event, got, test.want)
		}
	}
	if ActionResultStateAllowed(model.EventReviewAccepted, model.WorkActive) ||
		ActionResultStateAllowed("unknown", model.WorkActive) {
		t.Fatal("controller or unknown Event gained Agent receipt state")
	}
}
