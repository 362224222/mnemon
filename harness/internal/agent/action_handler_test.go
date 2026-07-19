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
		candidate, candidateErr := handler.candidate("typed parity probe", 0)
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

func TestActionHandlerRuntimePolicyMatchesCanonicalContextMatrix(t *testing.T) {
	t.Parallel()
	policy := testActionHandlers(t).RuntimePolicy()
	tests := []struct {
		context model.TeamworkActionContext
		want    []model.OperationKind
	}{
		{model.TeamworkActionContextNone, []model.OperationKind{model.OperationTeamworkOffer}},
		{model.TeamworkActionContextReviewerOffered,
			[]model.OperationKind{model.OperationTeamworkAccept, model.OperationTeamworkDecline}},
		{model.TeamworkActionContextReviewerActive,
			[]model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver}},
		{model.TeamworkActionContextReviewerRework,
			[]model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver}},
		{model.TeamworkActionContextParentResume, []model.OperationKind{model.OperationTeamworkDeliver}},
		{model.TeamworkActionContextHomeDelivered, []model.OperationKind{model.OperationTeamworkClose}},
		{model.TeamworkActionContextHomeDeliveredIteration1,
			[]model.OperationKind{model.OperationTeamworkRework}},
		{model.TeamworkActionContextHomeNonterminal, []model.OperationKind{model.OperationTeamworkCancel}},
	}
	for _, test := range tests {
		got, err := policy.ActionsForContexts([]model.TeamworkActionContext{test.context})
		if err != nil || !reflect.DeepEqual(got, test.want) {
			t.Fatalf("canonical context %s = (%v, %v), want %v", test.context, got, err, test.want)
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
		{name: "managed context contract", path: "actions/teamwork/accept.json",
			oldText: `"allowed_context":["reviewer_offered"]`,
			newText: `"allowed_context":["home_delivered"]`},
		{name: "participant state context", path: "actions/teamwork/accept.json",
			oldText: `"allowed_context":["reviewer_offered"]`,
			newText: `"allowed_context":["reviewer_active"]`},
		{name: "multi-result capability ceiling", path: "actions/teamwork/accept.json",
			oldText: `"max_results":1`, newText: `"max_results":2`},
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

func TestActionHandlersProjectAssetOwnedSafeContextsAndResultBounds(t *testing.T) {
	t.Parallel()
	offerHandlers := testActionHandlersWithAssetReplacement(t, "actions/teamwork/offer.json",
		`"max_results":7`, `"max_results":6`)
	offer, ok := offerHandlers.Action("offer")
	if !ok || offer.policyEntry.MaxResults() != 6 {
		t.Fatalf("narrowed offer = %#v, present=%t", offer, ok)
	}

	contextHandlers := testActionHandlersWithAssetReplacement(t, "actions/teamwork/offer.json",
		`"allowed_context":["none","reviewer_active","reviewer_rework"]`,
		`"allowed_context":["none","reviewer_active","reviewer_rework","parent_resume"]`)
	actions, err := contextHandlers.RuntimePolicy().ActionsForContexts(
		[]model.TeamworkActionContext{model.TeamworkActionContextParentResume})
	if err != nil || !reflect.DeepEqual(actions,
		[]model.OperationKind{model.OperationTeamworkOffer, model.OperationTeamworkDeliver}) {
		t.Fatalf("parent-resume ActionsForContexts() = (%v, %v)", actions, err)
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
	for _, handler := range handlers.Actions() {
		wantSelection := handler.EventType() == model.EventReviewOffered
		if handler.mechanic.candidate == nil || handler.mechanic.selection != wantSelection {
			t.Fatalf("%s execution mechanic = %#v", handler.Name(), handler.mechanic)
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
