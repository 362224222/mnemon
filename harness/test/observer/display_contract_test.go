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
		`fact.facts.targets`,
		`fact.facts.outcome`,
		`fact.facts.state`,
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
		"r7.event.accepted":       {"facts.consequence", "facts.semantic_kind", "refs.event", "refs.event_digest"},
		"r7.handling.resolved":    {"facts.outcome", "facts.state", "refs.handling"},
		"r8.selection.seeded":     {"facts.phase", "facts.preference_after"},
		"r8.round.frozen":         {"facts.alpha", "facts.margin_before", "facts.preference_before", "facts.round", "facts.sample_size"},
		"r8.vote.observed":        {"facts.authenticated", "facts.round", "facts.votes_a", "facts.votes_b"},
		"r8.round.settled":        {"facts.margin_after", "facts.margin_before", "facts.phase", "facts.preference_after", "facts.preference_before", "facts.recolored", "facts.round"},
		"r8.observation.produced": {"facts.margin_after", "facts.phase", "facts.preference_after", "facts.result", "facts.round"},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("display-evidence schema = %#v, want %#v", actual, expected)
	}
}
