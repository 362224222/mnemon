package agent

import (
	"bytes"
	"reflect"
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
		candidate, candidateErr := handler.candidate("typed mechanic parity", 0)
		if !nameOK || !operationOK || handler.Descriptor().Ordinal() != uint8(index) ||
			byName.OperationKind() != handler.OperationKind() || byOperation.Name() != handler.Name() ||
			handler.EventType() != wantEvents[handler.OperationKind()] || candidateErr != nil ||
			candidate.EventType() != handler.EventType() {
			t.Fatalf("handler %d = %#v", index, handler)
		}
	}
	assertActionHandlerRuntimePolicy(t, handlers)
	returned := handlers.Actions()
	returned[0] = ActionHandler{}
	if handlers.Actions()[0].Name() == "" {
		t.Fatal("Actions returned caller-owned storage")
	}
	var zero ActionHandlers
	if !zero.AssetRevision().IsZero() || zero.Actions() != nil || zero.RuntimePolicy().Entries() != nil {
		t.Fatalf("zero handlers = %#v", zero)
	}
	if _, ok := zero.Action("offer"); ok {
		t.Fatal("zero handlers resolved an Action")
	}
	if handlers, err := NewActionHandlers(ActionPolicy{}); err == nil || !handlers.AssetRevision().IsZero() {
		t.Fatalf("NewActionHandlers(zero) = (%#v, %v)", handlers, err)
	}
}

func assertActionHandlerRuntimePolicy(t testing.TB, handlers ActionHandlers) {
	t.Helper()
	policy := handlers.RuntimePolicy()
	if policy.AssetRevision() != handlers.AssetRevision() ||
		len(policy.Entries()) != teamwork.TeamworkActionCount {
		t.Fatalf("runtime policy = (%s, %d entries)", policy.AssetRevision(), len(policy.Entries()))
	}
	for index, handler := range handlers.Actions() {
		entry, ok := policy.Operation(handler.OperationKind())
		if !ok || entry.Ordinal() != uint8(index) || entry.EventType() != handler.EventType() ||
			entry.MaxResults() != handler.Descriptor().Receipt().MaxResults() ||
			!reflect.DeepEqual(entry.AllowedContexts(), handler.Descriptor().AllowedContexts()) {
			t.Fatalf("runtime policy entry %d = %#v, present=%t", index, entry, ok)
		}
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
		{name: "capture entry contract", path: "actions/teamwork/offer.json",
			oldText: `"max_entries":4096`, newText: `"max_entries":4095`},
		{name: "capture path contract", path: "actions/teamwork/offer.json",
			oldText: `"max_path_bytes":512`, newText: `"max_path_bytes":511`},
		{name: "Event Artifact capability", path: "actions/teamwork/accept.json",
			oldText: `"artifacts":{"allowed":false,"max_entries":0,"max_path_bytes":0,"max_roots":0,"max_total_bytes":0}`,
			newText: `"artifacts":{"allowed":true,"max_entries":4096,"max_path_bytes":512,"max_roots":16,"max_total_bytes":268435456}`},
		{name: "contextless capability ceiling", path: "actions/teamwork/accept.json",
			oldText: `"allowed_context":["reviewer_offered"]`,
			newText: `"allowed_context":["none"]`},
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

func TestActionHandlersProjectAssetOwnedContexts(t *testing.T) {
	t.Parallel()
	contextHandlers := testActionHandlersWithAssetReplacement(t, "actions/teamwork/accept.json",
		`"allowed_context":["reviewer_offered"]`, `"allowed_context":["home_delivered"]`)
	actions, err := contextHandlers.RuntimePolicy().ActionsForContexts(
		[]model.TeamworkActionContext{model.TeamworkActionContextHomeDelivered})
	if err != nil || !reflect.DeepEqual(actions,
		[]model.OperationKind{model.OperationTeamworkAccept, model.OperationTeamworkClose}) {
		t.Fatalf("home-delivered ActionsForContexts() = (%v, %v)", actions, err)
	}
}

func TestActionHandlersAllowAssetsToNarrowArtifactCapability(t *testing.T) {
	t.Parallel()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		oldText     string
		newText     string
		wantAllowed bool
		wantRoots   uint8
	}{
		{name: "lower root bound", oldText: `"max_roots":16`, newText: `"max_roots":1`,
			wantAllowed: true, wantRoots: 1},
		{name: "forbid artifacts",
			oldText: `"artifacts":{"allowed":true,"max_entries":4096,"max_path_bytes":512,"max_roots":16,"max_total_bytes":268435456}`,
			newText: `"artifacts":{"allowed":false,"max_entries":0,"max_path_bytes":0,"max_roots":0,"max_total_bytes":0}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider := newActionPolicyProviderStub(t, bundle)
			path := "actions/teamwork/offer.json"
			provider.raw[path] = bytes.Replace(provider.raw[path],
				[]byte(test.oldText), []byte(test.newText), 1)
			policy, policyErr := NewActionPolicy(provider)
			if policyErr != nil {
				t.Fatal(policyErr)
			}
			handlers, handlerErr := NewActionHandlers(policy)
			handler, ok := handlers.Action("offer")
			if handlerErr != nil || !ok || handler.Descriptor().Artifacts().Allowed() != test.wantAllowed ||
				handler.Descriptor().Artifacts().MaxRoots() != test.wantRoots {
				t.Fatalf("narrowed offer handler = (%#v, %v)", handler, handlerErr)
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
		name    string
		actor   actionActor
		selects bool
		states  []model.WorkState
	}{
		{name: "offer", actor: actionActorOffer, selects: true},
		{name: "accept", actor: actionActorParticipant, states: []model.WorkState{model.WorkOffered}},
		{name: "decline", actor: actionActorParticipant, states: []model.WorkState{model.WorkOffered}},
		{name: "deliver", actor: actionActorParticipant, states: []model.WorkState{model.WorkActive, model.WorkRework}},
		{name: "rework", actor: actionActorHome, states: []model.WorkState{model.WorkRework}},
		{name: "close", actor: actionActorHome, states: []model.WorkState{model.WorkClosed}},
		{name: "cancel", actor: actionActorHome, states: []model.WorkState{model.WorkCancelled}},
	}
	for _, test := range tests {
		handler, ok := handlers.Action(test.name)
		if !ok || handler.mechanic.actor != test.actor || handler.mechanic.selection != test.selects {
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

func testActionHandlersWithAssetReplacement(t testing.TB, path, oldText, newText string) ActionHandlers {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	provider := newActionPolicyProviderStub(t, bundle)
	provider.raw[path] = bytes.Replace(provider.raw[path], []byte(oldText), []byte(newText), 1)
	policy, err := NewActionPolicy(provider)
	if err != nil {
		t.Fatal(err)
	}
	handlers, err := NewActionHandlers(policy)
	if err != nil {
		t.Fatal(err)
	}
	return handlers
}
