package teamwork

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestActionCatalogRejectsMalformedOrInconsistentSources(t *testing.T) {
	revision, baseline, _ := realActionCatalogSources(t)
	tests := []struct {
		name     string
		revision func() model.Digest
		mutate   func(*testing.T, []ActionSource) []ActionSource
	}{
		{name: "zero revision", revision: func() model.Digest { return model.Digest{} }},
		{name: "missing", mutate: func(_ *testing.T, sources []ActionSource) []ActionSource { return sources[:6] }},
		{name: "extra", mutate: func(_ *testing.T, sources []ActionSource) []ActionSource { return append(sources, sources[0]) }},
		{name: "duplicate path", mutate: func(_ *testing.T, sources []ActionSource) []ActionSource { sources[1] = sources[0]; return sources }},
		{name: "unknown path", mutate: mutateActionPath("accept", "actions/teamwork/unknown.json")},
		{name: "empty source", mutate: replaceActionRaw("accept", nil)},
		{name: "oversize source", mutate: replaceActionRaw("accept", bytes.Repeat([]byte("x"), maxActionSourceBytes+1))},
		{name: "malformed JSON", mutate: replaceActionRaw("accept", []byte("{\n"))},
		{name: "unknown field", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("}\n"), []byte(",\"unknown\":true}\n"), 1)
		})},
		{name: "duplicate field", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`{"action":"accept",`), []byte(`{"action":"accept","action":"accept",`), 1)
		})},
		{name: "trailing value", mutate: transformActionRaw("accept", func(raw []byte) []byte { return append(raw, []byte("{}")...) })},
		{name: "second trailing LF", mutate: transformActionRaw("accept", func(raw []byte) []byte { return append(raw, '\n') })},
		{name: "CRLF", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return append(bytes.TrimSuffix(raw, []byte("\n")), '\r', '\n')
		})},
		{name: "noncanonical whitespace", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("}\n"), []byte(" }\n"), 1)
		})},
		{name: "missing top-level field", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`,"selectors":null`), nil, 1)
		})},
		{name: "missing ordinal", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`,"ordinal":1`), nil, 1)
		})},
		{name: "noncanonical ordinal position", mutate: transformActionRaw("accept", func(raw []byte) []byte {
			return bytes.Replace(raw, []byte(`"deadline":null,"ordinal":1`), []byte(`"ordinal":1,"deadline":null`), 1)
		})},
		{name: "duplicate ordinal", mutate: mutateActionWire("accept", func(w *actionWire) { w.Ordinal = 0 })},
		{name: "out of range ordinal", mutate: mutateActionWire("accept", func(w *actionWire) { w.Ordinal = TeamworkActionCount })},
		{name: "schema version", mutate: mutateActionWire("accept", func(w *actionWire) { w.SchemaVersion++ })},
		{name: "path action mismatch", mutate: mutateActionWire("accept", func(w *actionWire) { w.Action = "cancel" })},
		{name: "unknown operation", mutate: func(t *testing.T, sources []ActionSource) []ActionSource {
			sources = mutateActionWire("accept", func(w *actionWire) { w.Action, w.Receipt.Action = "invent", "teamwork.invent" })(t, sources)
			return mutateActionPath("accept", "actions/teamwork/invent.json")(t, sources)
		}},
		{name: "unknown context", mutate: mutateActionWire("accept", func(w *actionWire) { w.AllowedContext = []string{"unknown"} })},
		{name: "duplicate context", mutate: mutateActionWire("accept", func(w *actionWire) {
			w.AllowedContext = []string{"reviewer_offered", "reviewer_offered"}
		})},
		{name: "zero content bound", mutate: mutateActionWire("accept", func(w *actionWire) { w.Content.MaxBytes = 0 })},
		{name: "content bound exceeds maximum", mutate: mutateActionWire("accept", func(w *actionWire) { w.Content.MaxBytes++ })},
		{name: "unknown content source", mutate: mutateActionWire("accept", func(w *actionWire) { w.Content.Source = "argv" })},
		{name: "forbidden Artifact has bound", mutate: mutateActionWire("accept", func(w *actionWire) { w.Artifacts.MaxRoots = 1 })},
		{name: "allowed Artifact misses bound", mutate: mutateActionWire("deliver", func(w *actionWire) { w.Artifacts.MaxEntries = 0 })},
		{name: "Artifact entries exceed maximum", mutate: mutateActionWire("deliver", func(w *actionWire) { w.Artifacts.MaxEntries++ })},
		{name: "Artifact path exceeds maximum", mutate: mutateActionWire("deliver", func(w *actionWire) { w.Artifacts.MaxPathBytes++ })},
		{name: "Artifact roots exceed maximum", mutate: mutateActionWire("deliver", func(w *actionWire) { w.Artifacts.MaxRoots++ })},
		{name: "Artifact bytes exceed maximum", mutate: mutateActionWire("deliver", func(w *actionWire) { w.Artifacts.MaxTotalBytes++ })},
		{name: "deadline below minimum", mutate: mutateActionWire("offer", func(w *actionWire) { w.Deadline.Minimum = "4m" })},
		{name: "deadline above maximum", mutate: mutateActionWire("offer", func(w *actionWire) { w.Deadline.Maximum = "169h" })},
		{name: "deadline order", mutate: mutateActionWire("offer", func(w *actionWire) { w.Deadline.Default = "4m" })},
		{name: "malformed deadline", mutate: mutateActionWire("offer", func(w *actionWire) { w.Deadline.Default = "soon" })},
		{name: "unknown selector channel", mutate: mutateActionWire("offer", func(w *actionWire) { w.Selectors.Channel = "any" })},
		{name: "unknown participant selector", mutate: mutateActionWire("offer", func(w *actionWire) { w.Selectors.Participant = "peer" })},
		{name: "empty participant selector", mutate: mutateActionWire("offer", func(w *actionWire) { w.Selectors.Participant = "" })},
		{name: "receipt action mismatch", mutate: mutateActionWire("accept", func(w *actionWire) { w.Receipt.Action = "teamwork.close" })},
		{name: "receipt status", mutate: mutateActionWire("accept", func(w *actionWire) { w.Receipt.Status = "rejected" })},
		{name: "receipt handling", mutate: mutateActionWire("accept", func(w *actionWire) { w.Receipt.Handling = "pending" })},
		{name: "receipt zero results", mutate: mutateActionWire("accept", func(w *actionWire) { w.Receipt.MaxResults = 0 })},
		{name: "receipt multiple results", mutate: mutateActionWire("accept", func(w *actionWire) { w.Receipt.MaxResults = 2 })},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sources := cloneActionSources(baseline)
			if test.mutate != nil {
				sources = test.mutate(t, sources)
			}
			gotRevision := revision
			if test.revision != nil {
				gotRevision = test.revision()
			}
			catalog, err := ParseActionCatalog(gotRevision, sources)
			if !errors.Is(err, ErrInvalidActionCatalog) {
				t.Fatalf("ParseActionCatalog() error = %v, want %v", err, ErrInvalidActionCatalog)
			}
			if catalog.Actions() != nil || !catalog.AssetRevision().IsZero() {
				t.Fatalf("failed parse returned authority: %#v", catalog)
			}
		})
	}
}

func TestActionCatalogPolicyComesFromCanonicalSource(t *testing.T) {
	revision, sources, _ := realActionCatalogSources(t)
	sources = mutateActionWire("accept", func(w *actionWire) {
		w.AllowedContext = []string{"parent_resume", "reviewer_active", "home_delivered", "none"}
		w.Content.Required, w.Content.MaxBytes = true, 4096
		w.Artifacts = actionArtifactWire{Allowed: true, MaxEntries: 128, MaxPathBytes: 128,
			MaxRoots: 2, MaxTotalBytes: 1 << 20}
		w.Selectors = &actionSelectorWire{Channel: SelectorOptionalWhenUnambiguous,
			Participant: ParticipantEffectiveAlias}
	})(t, sources)
	sources = mutateActionWire("offer", func(w *actionWire) {
		w.Deadline = &actionDeadlineWire{Minimum: "10m", Default: "12h", Maximum: "48h"}
		w.Selectors = nil
	})(t, sources)
	closeIndex := actionSourceIndex(t, sources, "close")
	sources[closeIndex] = NewActionSource(sources[closeIndex].Path(),
		bytes.TrimSuffix(sources[closeIndex].Bytes(), []byte("\n")))
	catalog, err := ParseActionCatalog(revision, sources)
	if err != nil {
		t.Fatal(err)
	}
	accept, _ := catalog.Action("accept")
	if !reflect.DeepEqual(accept.AllowedContexts(), []ActionContext{ActionContextParentResume, ActionContextReviewerActive,
		ActionContextHomeDelivered, ActionContextNone}) ||
		!accept.Content().Required() || accept.Content().MaxBytes() != 4096 || !accept.Artifacts().Allowed() ||
		accept.Artifacts().MaxEntries() != 128 || accept.Receipt().MaxResults() != 1 {
		t.Fatalf("accept descriptor did not project reviewed source: %#v", accept)
	}
	acceptSelectors, hasAcceptSelectors := accept.Selectors()
	if !hasAcceptSelectors || acceptSelectors.Participant() != ParticipantEffectiveAlias {
		t.Fatalf("accept selectors did not project independently: %#v", acceptSelectors)
	}
	offer, _ := catalog.Action("offer")
	deadline, ok := offer.Deadline()
	if !ok || deadline.Minimum().String() != "10m0s" || deadline.Default().String() != "12h0m0s" ||
		deadline.Maximum().String() != "48h0m0s" {
		t.Fatalf("offer deadline did not project reviewed source: %#v", deadline)
	}
	if _, hasSelectors := offer.Selectors(); hasSelectors {
		t.Fatal("offer selectors were reconstructed instead of projected")
	}
	closeAction, _ := catalog.Action("close")
	if bytes.HasSuffix(closeAction.SourceBytes(), []byte("\n")) {
		t.Fatal("canonical zero-LF source was not preserved exactly")
	}
}

func FuzzParseActionCatalog(f *testing.F) {
	revision, sources, _ := realActionCatalogSources(f)
	offerIndex := actionSourceIndex(f, sources, "offer")
	f.Add(sources[offerIndex].Bytes())
	f.Add([]byte(`{"action":"offer"}`))
	f.Add([]byte("{}\n{}"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		candidate := cloneActionSources(sources)
		candidate[offerIndex] = NewActionSource(candidate[offerIndex].Path(), raw)
		catalog, err := ParseActionCatalog(revision, candidate)
		if err != nil {
			return
		}
		action, ok := catalog.Action("offer")
		if !ok || catalog.AssetRevision() != revision || len(catalog.Actions()) != TeamworkActionCount ||
			!bytes.Equal(action.SourceBytes(), raw) {
			t.Fatal("successful parse did not preserve bounded authority")
		}
	})
}

func cloneActionSources(sources []ActionSource) []ActionSource {
	result := make([]ActionSource, len(sources))
	for index, source := range sources {
		result[index] = NewActionSource(source.Path(), source.Bytes())
	}
	return result
}

func actionSourceIndex(t testing.TB, sources []ActionSource, action string) int {
	t.Helper()
	suffix := "/" + action + ".json"
	for index, source := range sources {
		if strings.HasSuffix(source.Path(), suffix) {
			return index
		}
	}
	t.Fatalf("missing action source %q", action)
	return -1
}

func mutateActionWire(action string, mutate func(*actionWire)) func(*testing.T, []ActionSource) []ActionSource {
	return func(t *testing.T, sources []ActionSource) []ActionSource {
		index := actionSourceIndex(t, sources, action)
		var wire actionWire
		if err := json.Unmarshal(sources[index].raw, &wire); err != nil {
			t.Fatal(err)
		}
		mutate(&wire)
		raw, err := json.Marshal(wire)
		if err != nil {
			t.Fatal(err)
		}
		sources[index] = NewActionSource(sources[index].Path(), append(raw, '\n'))
		return sources
	}
}

func mutateActionPath(action, path string) func(*testing.T, []ActionSource) []ActionSource {
	return func(t *testing.T, sources []ActionSource) []ActionSource {
		index := actionSourceIndex(t, sources, action)
		sources[index] = NewActionSource(path, sources[index].Bytes())
		return sources
	}
}

func replaceActionRaw(action string, raw []byte) func(*testing.T, []ActionSource) []ActionSource {
	return func(t *testing.T, sources []ActionSource) []ActionSource {
		index := actionSourceIndex(t, sources, action)
		sources[index] = NewActionSource(sources[index].Path(), raw)
		return sources
	}
}

func transformActionRaw(action string, transform func([]byte) []byte) func(*testing.T, []ActionSource) []ActionSource {
	return func(t *testing.T, sources []ActionSource) []ActionSource {
		index := actionSourceIndex(t, sources, action)
		sources[index] = NewActionSource(sources[index].Path(), transform(sources[index].Bytes()))
		return sources
	}
}
