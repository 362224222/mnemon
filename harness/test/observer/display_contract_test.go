package observer

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestObserverKeepsScenarioSemanticsInTraceData(t *testing.T) {
	html := string(readFile(t, "index.html"))
	for _, forbidden := range []string{
		"domain-ops", "payment", "ledger", "gateway", "blackboard", "contract-net",
	} {
		if strings.Contains(strings.ToLower(html), forbidden) {
			t.Fatalf("observer hard-codes scenario vocabulary %q", forbidden)
		}
	}
	for _, required := range []string{
		`laneKey(fact.source.node, fact.agent)`,
		`standaloneRuntimeIDs.has(fact.id)`,
		`collaborationPriority(right) - collaborationPriority(left)`,
		`fact.facts.semantic_kind`,
		`fact.facts.action`,
		`fact.facts.attempt_count`,
		`fact.facts.code`,
		`fact.facts.targets`,
		`fact.facts.outcome`,
		`fact.facts.state`,
		`record.causes.length !== 0`,
		`record.facts.attempt_count < 1`,
		`INTENT_DENIAL_CODES.includes(record.facts.code)`,
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("observer is missing generic evidence projection %q", required)
		}
	}
}

func TestTraceSchemaRequiresMinimumDisplayEvidence(t *testing.T) {
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	factVariant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "fact")
	outer := arrayField(t, factVariant, "allOf")
	if len(outer) != 1 {
		t.Fatalf("fact constraint count = %d, want one combined constraint", len(outer))
	}
	combined, ok := outer[0].(map[string]any)
	if !ok {
		t.Fatal("combined fact constraint is not an object")
	}
	conditions := arrayField(t, combined, "allOf")
	actual := make(map[string][]string, len(conditions))
	for _, value := range conditions {
		condition, ok := value.(map[string]any)
		if !ok {
			t.Fatal("display-evidence condition is not an object")
		}
		ifProperties := objectField(t, objectField(t, condition, "if"), "properties")
		kind, _ := objectField(t, ifProperties, "kind")["const"].(string)
		if kind == "" {
			t.Fatal("display-evidence condition has no kind")
		}
		thenProperties := objectField(t, objectField(t, condition, "then"), "properties")
		for _, objectName := range []string{"facts", "refs"} {
			object, present := thenProperties[objectName].(map[string]any)
			if !present {
				continue
			}
			for _, field := range stringArrayField(t, object, "required") {
				actual[kind] = append(actual[kind], objectName+"."+field)
			}
		}
		slices.Sort(actual[kind])
	}
	expected := map[string][]string{
		"runtime.domain.operation": {"facts.action", "facts.attempt_count", "facts.batched_unattributed_count", "facts.invalid_result_count", "facts.success_count", "facts.tool_error_count"},
		"runtime.intent.denied":    {"facts.action", "facts.code", "facts.count"},
		"r7.event.accepted":        {"facts.consequence", "facts.semantic_kind", "refs.event", "refs.event_digest"},
		"r7.handling.resolved":     {"facts.outcome", "facts.state", "refs.handling"},
		"test.attention.wave":      {"facts.active_claims", "facts.episode", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used", "facts.unseen_open"},
		"test.attention.exhausted": {"facts.active_claims", "facts.episode", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used", "facts.unseen_open"},
		"r8.selection.seeded":      {"facts.phase", "facts.preference_after"},
		"r8.round.frozen":          {"facts.alpha", "facts.margin_before", "facts.preference_before", "facts.round", "facts.sample_size"},
		"r8.vote.observed":         {"facts.authenticated", "facts.round", "facts.votes_a", "facts.votes_b"},
		"r8.round.settled":         {"facts.margin_after", "facts.margin_before", "facts.phase", "facts.preference_after", "facts.preference_before", "facts.recolored", "facts.round"},
		"r8.observation.produced":  {"facts.margin_after", "facts.phase", "facts.preference_after", "facts.result", "facts.round"},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("display-evidence schema = %#v, want %#v", actual, expected)
	}
}

func TestRuntimeIntentDenialVocabularyMatchesBrowserAndSchema(t *testing.T) {
	html := string(readFile(t, "index.html"))
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	factVariant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "fact")
	outer := arrayField(t, factVariant, "allOf")
	combined, ok := outer[0].(map[string]any)
	if !ok {
		t.Fatal("combined fact constraint is not an object")
	}
	conditions := arrayField(t, combined, "allOf")
	var schemaCodes []string
	for _, value := range conditions {
		condition, ok := value.(map[string]any)
		if !ok {
			t.Fatal("display-evidence condition is not an object")
		}
		ifProperties := objectField(t, objectField(t, condition, "if"), "properties")
		if objectField(t, ifProperties, "kind")["const"] != "runtime.intent.denied" {
			continue
		}
		thenProperties := objectField(t, objectField(t, condition, "then"), "properties")
		facts := objectField(t, thenProperties, "facts")
		fields := objectField(t, facts, "properties")
		schemaCodes = stringArrayField(t, objectField(t, fields, "code"), "enum")
	}
	if len(schemaCodes) == 0 {
		t.Fatal("runtime.intent.denied schema has no closed code vocabulary")
	}
	assertSameStrings(t, "browser Intent denial codes",
		javascriptStringArray(t, html, "const INTENT_DENIAL_CODES"), schemaCodes)
}
