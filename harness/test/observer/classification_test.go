package observer

import (
	"slices"
	"sort"
	"strings"
	"testing"
)

type factClassification struct {
	source string
	truth  string
}

var factClassifications = map[string]factClassification{
	"runtime.turn.started":     {source: "runtime", truth: "observation"},
	"runtime.hook.cue":         {source: "runtime", truth: "observation"},
	"runtime.view.received":    {source: "runtime", truth: "derived_projection"},
	"runtime.intent.submitted": {source: "runtime", truth: "observation"},
	"runtime.turn.ended":       {source: "runtime", truth: "observation"},
	"runtime.turn.timed_out":   {source: "runtime", truth: "observation"},
	"system.node.restarted":    {source: "runner", truth: "observation"},
	"r7.receipt.accepted":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.receipt.rejected":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.receipt.replayed":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.event.accepted":        {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.handling.created":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.handling.advanced":     {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.handling.resolved":     {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.reference.published":   {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.reference.superseded":  {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.reference.retracted":   {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.delivery.pending":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.delivery.readmitted":   {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.delivery.settled":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.delivery.expired":      {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.artifact.captured":     {source: "r7_authority", truth: "accepted_local_fact"},
	"r7.artifact.read":         {source: "runtime", truth: "observation"},
	"r7.artifact.verified":     {source: "r7_authority", truth: "accepted_local_fact"},
	"r8.selection.seeded":      {source: "r8_selector", truth: "local_preference"},
	"r8.round.frozen":          {source: "r8_selector", truth: "local_preference"},
	"r8.vote.observed":         {source: "r8_selector", truth: "observation"},
	"r8.round.settled":         {source: "r8_selector", truth: "local_preference"},
	"r8.observation.produced":  {source: "r8_selector", truth: "local_preference"},
	"test.gate.checked":        {source: "oracle", truth: "assertion"},
}

func validFactClassification(fact factRecord) bool {
	expected, exists := factClassifications[fact.Kind]
	if !exists || fact.Source.Class != expected.source || fact.Truth != expected.truth {
		return false
	}
	return !strings.HasPrefix(fact.Kind, "r8.") || fact.Refs.Selection != ""
}

func knownFactKinds() []string {
	result := make([]string, 0, len(factClassifications))
	for kind := range factClassifications {
		result = append(result, kind)
	}
	sort.Strings(result)
	return result
}

func factClassificationRows() []string {
	result := make([]string, 0, len(factClassifications))
	for kind, classification := range factClassifications {
		result = append(result, kind+"|"+classification.source+"|"+classification.truth)
	}
	sort.Strings(result)
	return result
}

func TestFactClassificationMatchesSchemaAndBrowser(t *testing.T) {
	html := string(readFile(t, "index.html"))
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	factVariant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "fact")

	assertSameStrings(t, "schema fact classifications",
		schemaFactClassificationRows(t, factVariant), factClassificationRows())
	assertSameStrings(t, "browser fact classifications",
		javascriptStringArray(t, html, "const FACT_CLASSIFICATION_ROWS"), factClassificationRows())
}

func TestFactClassificationFailsClosed(t *testing.T) {
	for kind, expected := range factClassifications {
		fact := factRecord{Kind: kind, Source: sourceWire{Class: expected.source}, Truth: expected.truth}
		if strings.HasPrefix(kind, "r8.") {
			fact.Refs.Selection = "sha256:" + strings.Repeat("a", 64)
		}
		if !validFactClassification(fact) {
			t.Fatalf("valid classification for %q was rejected", kind)
		}
		fact.Source.Class = "runner"
		if fact.Source.Class == expected.source {
			fact.Source.Class = "runtime"
		}
		if validFactClassification(fact) {
			t.Fatalf("wrong source for %q was accepted", kind)
		}
		fact.Source.Class = expected.source
		fact.Truth = "assertion"
		if fact.Truth == expected.truth {
			fact.Truth = "observation"
		}
		if validFactClassification(fact) {
			t.Fatalf("wrong truth for %q was accepted", kind)
		}
	}
	r8 := factRecord{Kind: "r8.selection.seeded", Source: sourceWire{Class: "r8_selector"}, Truth: "local_preference"}
	if validFactClassification(r8) {
		t.Fatal("R8 fact without SelectionID was accepted")
	}
}

func schemaFactClassificationRows(t *testing.T, factVariant map[string]any) []string {
	t.Helper()
	constraints := arrayField(t, factVariant, "allOf")
	if len(constraints) != 1 {
		t.Fatalf("fact classification constraint count = %d, want 1", len(constraints))
	}
	constraint, ok := constraints[0].(map[string]any)
	if !ok {
		t.Fatal("fact classification constraint is not an object")
	}
	branches := arrayField(t, constraint, "oneOf")
	var rows []string
	for _, value := range branches {
		branch, ok := value.(map[string]any)
		if !ok {
			t.Fatal("fact classification branch is not an object")
		}
		properties := objectField(t, branch, "properties")
		kinds := stringArrayField(t, objectField(t, properties, "kind"), "enum")
		source := objectField(t, objectField(t, properties, "source"), "properties")
		sourceClass, _ := objectField(t, source, "class")["const"].(string)
		truth, _ := objectField(t, properties, "truth")["const"].(string)
		if sourceClass == "" || truth == "" {
			t.Fatal("fact classification branch has no source or truth constant")
		}
		for _, kind := range kinds {
			rows = append(rows, kind+"|"+sourceClass+"|"+truth)
		}
	}
	if len(rows) != len(factClassifications) {
		t.Fatalf("schema classification count = %d, want %d", len(rows), len(factClassifications))
	}
	if !slices.Equal(knownFactKinds(), sortedKindsFromRows(rows)) {
		t.Fatal("schema classifications do not cover every known kind exactly")
	}
	return rows
}

func sortedKindsFromRows(rows []string) []string {
	result := make([]string, 0, len(rows))
	for _, row := range rows {
		kind, _, _ := strings.Cut(row, "|")
		result = append(result, kind)
	}
	sort.Strings(result)
	return result
}
