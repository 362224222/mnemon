package teamwork

import (
	"bytes"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type actionCatalogExpectation struct {
	name              string
	operation         model.OperationKind
	contexts          []ActionContext
	contentRequired   bool
	artifactsAllowed  bool
	selectors         bool
	receiptHandling   ReceiptHandling
	receiptMaxResults uint8
}

func TestActionCatalogProjectsExactCanonicalAssets(t *testing.T) {
	revision, sources, rawByPath := realActionCatalogSources(t)
	slices.Reverse(sources)
	catalog, err := ParseActionCatalog(revision, sources)
	if err != nil {
		t.Fatal(err)
	}
	if catalog.AssetRevision() != revision {
		t.Fatalf("AssetRevision() = %s, want %s", catalog.AssetRevision(), revision)
	}

	want := []actionCatalogExpectation{
		{name: "accept", operation: model.OperationTeamworkAccept,
			contexts: []ActionContext{ActionContextReviewerOffered}, receiptHandling: ReceiptHandlingCompleted, receiptMaxResults: 1},
		{name: "cancel", operation: model.OperationTeamworkCancel,
			contexts: []ActionContext{ActionContextHomeNonterminal}, contentRequired: true,
			receiptHandling: ReceiptHandlingCompleted, receiptMaxResults: 1},
		{name: "close", operation: model.OperationTeamworkClose,
			contexts: []ActionContext{ActionContextHomeDelivered}, receiptHandling: ReceiptHandlingCompleted, receiptMaxResults: 1},
		{name: "decline", operation: model.OperationTeamworkDecline,
			contexts: []ActionContext{ActionContextReviewerOffered}, contentRequired: true,
			receiptHandling: ReceiptHandlingCompleted, receiptMaxResults: 1},
		{name: "deliver", operation: model.OperationTeamworkDeliver,
			contexts:        []ActionContext{ActionContextReviewerActive, ActionContextReviewerRework, ActionContextParentResume},
			contentRequired: true, artifactsAllowed: true,
			receiptHandling: ReceiptHandlingCompleted, receiptMaxResults: 1},
		{name: "offer", operation: model.OperationTeamworkOffer,
			contexts:        []ActionContext{ActionContextNone, ActionContextReviewerActive, ActionContextReviewerRework},
			contentRequired: true, artifactsAllowed: true, selectors: true,
			receiptHandling: ReceiptHandlingContextDependent, receiptMaxResults: model.MaxChildWorks},
		{name: "rework", operation: model.OperationTeamworkRework,
			contexts: []ActionContext{ActionContextHomeDeliveredIteration1}, contentRequired: true, artifactsAllowed: true,
			receiptHandling: ReceiptHandlingCompleted, receiptMaxResults: 1},
	}
	actions := catalog.Actions()
	if len(actions) != len(want) {
		t.Fatalf("Actions() count = %d, want %d", len(actions), len(want))
	}
	for index, expected := range want {
		assertCanonicalActionDescriptor(t, catalog, actions[index], expected, rawByPath)
	}
	if _, ok := catalog.Action("unknown"); ok {
		t.Fatal("unknown action resolved")
	}
	if _, ok := catalog.Operation(model.OperationResolveRetry); ok {
		t.Fatal("non-Teamwork operation resolved")
	}
}

func assertCanonicalActionDescriptor(t *testing.T, catalog ActionCatalog, got ActionDescriptor,
	want actionCatalogExpectation, rawByPath map[string][]byte,
) {
	t.Helper()
	assertCanonicalActionIdentity(t, got, want, rawByPath)
	assertCanonicalContentReceipt(t, got, want)
	assertCanonicalArtifactPolicy(t, want.name, got.Artifacts(), want.artifactsAllowed)
	assertCanonicalSelectionPolicy(t, want.name, got, want.selectors)
	byName, nameOK := catalog.Action(want.name)
	byOperation, operationOK := catalog.Operation(want.operation)
	if !nameOK || !operationOK || byName.Name() != want.name || byOperation.Name() != want.name {
		t.Fatalf("descriptor %s lookup mismatch", want.name)
	}
}

func assertCanonicalActionIdentity(t *testing.T, got ActionDescriptor,
	want actionCatalogExpectation, rawByPath map[string][]byte,
) {
	t.Helper()
	wantPath := "actions/teamwork/" + want.name + ".json"
	if got.Name() != want.name || got.SourcePath() != wantPath || got.SchemaVersion() != ActionSchemaVersion ||
		got.OperationKind() != want.operation || !bytes.Equal(got.SourceBytes(), rawByPath[wantPath]) ||
		!reflect.DeepEqual(got.AllowedContexts(), want.contexts) {
		t.Fatalf("descriptor %s identity/source/context mismatch: %#v", want.name, got)
	}
	for _, context := range want.contexts {
		if !got.AllowsContext(context) {
			t.Fatalf("descriptor %s rejects source context %q", want.name, context)
		}
	}
	if got.AllowsContext(ActionContext("unsupported")) {
		t.Fatalf("descriptor %s accepts unsupported context", want.name)
	}
}

func assertCanonicalContentReceipt(t *testing.T, got ActionDescriptor, want actionCatalogExpectation) {
	t.Helper()
	content := got.Content()
	if content.MaxBytes() != model.MaxContentBytes || content.Required() != want.contentRequired ||
		content.Source() != ContentFileOrStdin {
		t.Fatalf("descriptor %s content = %#v", want.name, content)
	}
	receipt := got.Receipt()
	if receipt.Action() != want.operation || receipt.Handling() != want.receiptHandling ||
		receipt.MaxResults() != want.receiptMaxResults || receipt.Status() != ReceiptStatusAccepted {
		t.Fatalf("descriptor %s receipt = %#v", want.name, receipt)
	}
}

func assertCanonicalArtifactPolicy(t *testing.T, name string, policy ActionArtifactPolicy, allowed bool) {
	t.Helper()
	if policy.Allowed() != allowed {
		t.Fatalf("descriptor %s Artifact allowed = %t, want %t", name, policy.Allowed(), allowed)
	}
	if allowed {
		if policy.MaxEntries() != 4096 || policy.MaxPathBytes() != model.MaxIdentifierBytes ||
			policy.MaxRoots() != model.MaxArtifactRefs || policy.MaxTotalBytes() != 256<<20 {
			t.Fatalf("descriptor %s Artifact bounds = %#v", name, policy)
		}
		return
	}
	if policy.MaxEntries() != 0 || policy.MaxPathBytes() != 0 || policy.MaxRoots() != 0 || policy.MaxTotalBytes() != 0 {
		t.Fatalf("descriptor %s forbidden Artifact bounds = %#v", name, policy)
	}
}

func assertCanonicalSelectionPolicy(t *testing.T, name string, descriptor ActionDescriptor, present bool) {
	t.Helper()
	deadline, hasDeadline := descriptor.Deadline()
	selectors, hasSelectors := descriptor.Selectors()
	if hasDeadline != present || hasSelectors != present {
		t.Fatalf("descriptor %s selection presence = (%t, %t), want %t", name, hasDeadline, hasSelectors, present)
	}
	if !present {
		return
	}
	if deadline.Default() != DefaultOfferDeadline || deadline.Minimum() != MinimumOfferDeadline ||
		deadline.Maximum() != MaximumOfferDeadline || selectors.Channel() != SelectorOptionalWhenUnambiguous ||
		!reflect.DeepEqual(selectors.Participants(),
			[]ParticipantSelector{ParticipantEffectiveAlias, ParticipantAuto, ParticipantTeam}) {
		t.Fatalf("descriptor %s deadline/selectors = (%#v, %#v)", name, deadline, selectors)
	}
}

func TestActionCatalogIsDeterministicImmutableAndZeroSafe(t *testing.T) {
	revision, sources, _ := realActionCatalogSources(t)
	originalSource := sources[0].Bytes()
	callerRaw := sources[0].raw
	callerRaw[0] ^= 0xff
	if bytes.Equal(sources[0].Bytes(), originalSource) {
		t.Fatal("test did not mutate its package-private source copy")
	}
	sources[0] = NewActionSource(sources[0].Path(), originalSource)

	first, err := ParseActionCatalog(revision, sources)
	if err != nil {
		t.Fatal(err)
	}
	rotated := append(append([]ActionSource(nil), sources[3:]...), sources[:3]...)
	second, err := ParseActionCatalog(revision, rotated)
	if err != nil || !reflect.DeepEqual(first.Actions(), second.Actions()) {
		t.Fatalf("source order changed catalog: %v", err)
	}

	actions := first.Actions()
	actions[0] = ActionDescriptor{}
	fresh := first.Actions()[0]
	raw := fresh.SourceBytes()
	raw[0] ^= 0xff
	contexts := fresh.AllowedContexts()
	contexts[0] = ActionContext("tampered")
	offer, _ := first.Action("offer")
	selectors, _ := offer.Selectors()
	participants := selectors.Participants()
	participants[0] = ParticipantSelector("tampered")
	if first.Actions()[0].Name() == "" || bytes.Equal(raw, first.Actions()[0].SourceBytes()) ||
		first.Actions()[0].AllowedContexts()[0] == ActionContext("tampered") {
		t.Fatal("catalog returned mutable descriptor state")
	}
	selectors, _ = offer.Selectors()
	if selectors.Participants()[0] == ParticipantSelector("tampered") {
		t.Fatal("selector participants were mutable")
	}

	var zero ActionCatalog
	if !zero.AssetRevision().IsZero() || zero.Actions() != nil {
		t.Fatalf("zero catalog = %#v", zero)
	}
	if _, ok := zero.Action("offer"); ok {
		t.Fatal("zero catalog resolved an action")
	}
	if _, ok := zero.Operation(model.OperationTeamworkOffer); ok {
		t.Fatal("zero catalog resolved an operation")
	}
	var descriptor ActionDescriptor
	if descriptor.SourceBytes() != nil || descriptor.AllowedContexts() != nil || descriptor.AllowsContext(ActionContextNone) {
		t.Fatalf("zero descriptor is not inert: %#v", descriptor)
	}
	if _, ok := descriptor.Deadline(); ok {
		t.Fatal("zero descriptor exposes deadline")
	}
	if _, ok := descriptor.Selectors(); ok {
		t.Fatal("zero descriptor exposes selectors")
	}
}

func TestActionSourceDefensivelyCopiesBytes(t *testing.T) {
	raw := []byte("source")
	source := NewActionSource("path", raw)
	raw[0] = 'X'
	got := source.Bytes()
	got[0] = 'Y'
	if string(source.Bytes()) != "source" || source.Path() != "path" {
		t.Fatalf("ActionSource changed through caller alias: %q", source.Bytes())
	}
}

func realActionCatalogSources(t testing.TB) (model.Digest, []ActionSource, map[string][]byte) {
	t.Helper()
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	revision, err := model.ParseDigest(bundle.Manifest().AssetRevision)
	if err != nil {
		t.Fatal(err)
	}
	sources := make([]ActionSource, 0, TeamworkActionCount)
	rawByPath := make(map[string][]byte, TeamworkActionCount)
	for _, record := range bundle.Manifest().Files {
		if !strings.HasPrefix(record.Path, "actions/teamwork/") {
			continue
		}
		raw, readErr := bundle.Read(record.Path)
		if readErr != nil {
			t.Fatal(readErr)
		}
		sources = append(sources, NewActionSource(record.Path, raw))
		rawByPath[record.Path] = append([]byte(nil), raw...)
	}
	if len(sources) != TeamworkActionCount {
		t.Fatalf("canonical Teamwork source count = %d, want %d", len(sources), TeamworkActionCount)
	}
	return revision, sources, rawByPath
}
