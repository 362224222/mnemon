package agent

import (
	"bytes"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestActionHandlersJoinEveryAssetToTypedMechanics(t *testing.T) {
	t.Parallel()
	handlers := testActionHandlers(t)
	wantEvents := map[model.OperationKind]model.EventType{
		model.OperationTeamworkOffer: model.EventReviewOffered, model.OperationTeamworkAccept: model.EventReviewAcceptRequested,
		model.OperationTeamworkDecline: model.EventReviewDeclineRequested, model.OperationTeamworkDeliver: model.EventReviewDeliveryReady,
		model.OperationTeamworkRework: model.EventReviewReworkRequested, model.OperationTeamworkClose: model.EventReviewClosed,
		model.OperationTeamworkCancel: model.EventReviewCancelled,
	}
	if len(handlers.Actions()) != teamwork.TeamworkActionCount {
		t.Fatalf("Actions() count = %d", len(handlers.Actions()))
	}
	for index, handler := range handlers.Actions() {
		byName, nameOK := handlers.Action(handler.Name())
		byOperation, operationOK := handlers.Operation(handler.OperationKind())
		if !nameOK || !operationOK || handler.Descriptor().Ordinal() != uint8(index) ||
			byName.OperationKind() != handler.OperationKind() || byOperation.Name() != handler.Name() ||
			handler.EventType() != wantEvents[handler.OperationKind()] {
			t.Fatalf("handler %d = %#v", index, handler)
		}
	}
	returned := handlers.Actions()
	returned[0] = ActionHandler{}
	if handlers.Actions()[0].Name() == "" {
		t.Fatal("Actions returned caller-owned storage")
	}
	var zero ActionHandlers
	if !zero.AssetRevision().IsZero() || zero.Actions() != nil {
		t.Fatalf("zero handlers = %#v", zero)
	}
	if _, ok := zero.Action("offer"); ok {
		t.Fatal("zero handlers resolved an Action")
	}
	if handlers, err := NewActionHandlers(ActionPolicy{}); err == nil || !handlers.AssetRevision().IsZero() {
		t.Fatalf("NewActionHandlers(zero) = (%#v, %v)", handlers, err)
	}
}

func TestActionHandlersRejectPolicyUnsupportedByTypedMechanics(t *testing.T) {
	t.Parallel()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		path    string
		oldText string
		newText string
	}{
		{name: "candidate content contract", path: "actions/teamwork/offer.json",
			oldText: `"required":true`, newText: `"required":false`},
		{name: "batch receipt contract", path: "actions/teamwork/offer.json",
			oldText: `"max_results":7`, newText: `"max_results":6`},
		{name: "capture entry contract", path: "actions/teamwork/offer.json",
			oldText: `"max_entries":4096`, newText: `"max_entries":4095`},
		{name: "capture path contract", path: "actions/teamwork/offer.json",
			oldText: `"max_path_bytes":512`, newText: `"max_path_bytes":511`},
		{name: "managed context contract", path: "actions/teamwork/accept.json",
			oldText: `"allowed_context":["reviewer_offered"]`,
			newText: `"allowed_context":["home_delivered"]`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := newActionPolicyProviderStub(t, bundle)
			provider.raw[test.path] = bytes.Replace(provider.raw[test.path],
				[]byte(test.oldText), []byte(test.newText), 1)
			policy, policyErr := NewActionPolicy(provider)
			if policyErr != nil {
				t.Fatalf("generic ActionPolicy rejected fixture before typed join: %v", policyErr)
			}
			handlers, handlerErr := NewActionHandlers(policy)
			if handlerErr == nil || !handlers.AssetRevision().IsZero() {
				t.Fatalf("NewActionHandlers() = (%#v, %v)", handlers, handlerErr)
			}
		})
	}
}

func TestActionHandlersExposeExhaustiveExecutionMechanics(t *testing.T) {
	t.Parallel()
	handlers := testActionHandlers(t)
	states := []model.WorkState{model.WorkOffered, model.WorkActive, model.WorkDelivered,
		model.WorkRework, model.WorkClosed, model.WorkDeclined, model.WorkExpired, model.WorkCancelled}
	tests := []struct {
		name   string
		actor  actionActor
		batch  bool
		states []model.WorkState
	}{
		{name: "offer", actor: actionActorOffer, batch: true},
		{name: "accept", actor: actionActorParticipant, states: []model.WorkState{model.WorkOffered}},
		{name: "decline", actor: actionActorParticipant, states: []model.WorkState{model.WorkOffered}},
		{name: "deliver", actor: actionActorParticipant, states: []model.WorkState{model.WorkActive, model.WorkRework}},
		{name: "rework", actor: actionActorHome, states: []model.WorkState{model.WorkRework}},
		{name: "close", actor: actionActorHome, states: []model.WorkState{model.WorkClosed}},
		{name: "cancel", actor: actionActorHome, states: []model.WorkState{model.WorkCancelled}},
	}
	for _, test := range tests {
		handler, ok := handlers.Action(test.name)
		if !ok || handler.mechanic.actor != test.actor || handler.mechanic.batch != test.batch {
			t.Fatalf("%s execution mechanic = %#v", test.name, handler.mechanic)
		}
		if test.actor == actionActorOffer {
			if handler.mechanic.committedState != nil {
				t.Fatalf("offer unexpectedly has current-state receipt mechanics")
			}
			continue
		}
		want := make(map[model.WorkState]bool, len(test.states))
		for _, state := range test.states {
			want[state] = true
		}
		for _, state := range states {
			if handler.mechanic.committedState(state) != want[state] {
				t.Fatalf("%s committed state %s = %t, want %t", test.name, state,
					handler.mechanic.committedState(state), want[state])
			}
		}
	}
}

func testActionHandlers(t testing.TB) ActionHandlers {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewActionPolicy(bundle)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewActionHandlers(policy)
	if err != nil {
		t.Fatal(err)
	}
	return handlers
}
