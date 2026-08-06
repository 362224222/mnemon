package observer

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestTraceVersionMatchesSchemaAndBrowser(t *testing.T) {
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	for _, record := range []string{"run", "fact", "result"} {
		variant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), record)
		version, ok := objectField(t, variant, "properties")["version"].(map[string]any)
		if !ok || version["const"] != float64(traceVersion) {
			t.Fatalf("%s schema version = %#v, want %d", record, version["const"], traceVersion)
		}
	}
	html := string(readFile(t, "index.html"))
	want := fmt.Sprintf("const TRACE_VERSION = %d;", traceVersion)
	if !strings.Contains(html, want) {
		t.Fatalf("browser trace version does not contain %q", want)
	}
}

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
		"runtime.view.received":    {"facts.action", "facts.has_current", "facts.open_total", "facts.related_projected", "facts.related_total", "facts.truncated"},
		"runtime.intent.denied":    {"facts.action", "facts.code", "facts.count"},
		"r7.event.accepted":        {"facts.consequence", "facts.semantic_kind", "refs.event", "refs.event_digest"},
		"r7.handling.resolved":     {"facts.outcome", "facts.state", "refs.handling"},
		"test.attention.wave":      {"facts.episode", "facts.occupied_claims", "facts.open_unclaimed", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used"},
		"test.attention.outcome":   {"facts.episode", "facts.goal_digest", "facts.goal_satisfied", "facts.occupied_claims", "facts.open_unclaimed", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used"},
		"test.attention.exhausted": {"facts.episode", "facts.goal_digest", "facts.goal_satisfied", "facts.occupied_claims", "facts.open_unclaimed", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used"},
		"test.attention.quiescent": {"facts.episode", "facts.goal_digest", "facts.goal_satisfied", "facts.occupied_claims", "facts.open_unclaimed", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used"},
		"test.attention.occupied":  {"facts.episode", "facts.occupied_claims", "facts.open_unclaimed", "facts.role", "facts.round", "facts.turn_limit", "facts.turns_used"},
		"test.gate.checked":        {"facts.gate_id", "facts.status"},
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

func TestFinalAttentionSemanticsMatchSchema(t *testing.T) {
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	factsDefinition := objectField(t, objectField(t, root, "$defs"), "facts")
	paired := objectField(t, factsDefinition, "dependentRequired")
	if !slices.Equal(stringArrayField(t, paired, "goal_digest"), []string{"goal_satisfied"}) ||
		!slices.Equal(stringArrayField(t, paired, "goal_satisfied"), []string{"goal_digest"}) {
		t.Fatalf("goal metadata pairing = %#v", paired)
	}
	factVariant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "fact")
	combined := arrayField(t, factVariant, "allOf")[0].(map[string]any)
	conditions := arrayField(t, combined, "allOf")
	want := map[string]map[string]any{
		"test.attention.outcome":   {"goal_satisfied": true, "occupied_claims": float64(0)},
		"test.attention.exhausted": {"goal_satisfied": false, "occupied_claims": float64(0)},
		"test.attention.quiescent": {"goal_satisfied": false, "occupied_claims": float64(0), "open_unclaimed": float64(0)},
	}
	got := collectAttentionConstants(t, conditions, want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("attention schema semantics = %#v, want %#v", got, want)
	}
	occupiedForbiddenFields := collectOccupiedForbiddenFields(t, conditions)
	slices.Sort(occupiedForbiddenFields)
	if !slices.Equal(occupiedForbiddenFields, []string{"goal_digest", "goal_satisfied"}) {
		t.Fatal("attention schema permits occupied evidence to depend on an external goal")
	}
}

func TestFinalAttentionSemanticsMatchBrowser(t *testing.T) {
	html := string(readFile(t, "index.html"))
	for _, required := range []string{
		`record.kind === "test.attention.outcome"`,
		`!record.facts.goal_satisfied || record.facts.occupied_claims !== 0`,
		`record.kind === "test.attention.exhausted"`,
		`record.facts.goal_satisfied || record.facts.occupied_claims !== 0`,
		`record.kind === "test.attention.quiescent"`,
		`record.facts.open_unclaimed !== 0`,
		`record.kind === "test.attention.occupied"`,
		`record.facts.goal_digest !== undefined`,
		`must precede and omit external goal evidence`,
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("browser attention validator is missing %q", required)
		}
	}
}

func collectAttentionConstants(t *testing.T, conditions []any,
	want map[string]map[string]any,
) map[string]map[string]any {
	t.Helper()
	result := make(map[string]map[string]any, len(want))
	for _, value := range conditions {
		condition, ok := value.(map[string]any)
		if !ok {
			t.Fatal("attention condition is not an object")
		}
		kind := schemaConditionKind(t, condition)
		if _, tracked := want[kind]; !tracked {
			continue
		}
		facts := schemaConditionFacts(t, condition)
		properties := objectField(t, facts, "properties")
		result[kind] = make(map[string]any, len(properties))
		for field, value := range properties {
			definition, ok := value.(map[string]any)
			if !ok {
				t.Fatalf("%s.%s constraint is not an object", kind, field)
			}
			result[kind][field] = definition["const"]
		}
	}
	return result
}

func collectOccupiedForbiddenFields(t *testing.T, conditions []any) []string {
	t.Helper()
	for _, value := range conditions {
		condition, ok := value.(map[string]any)
		if !ok {
			t.Fatal("attention condition is not an object")
		}
		if schemaConditionKind(t, condition) != "test.attention.occupied" {
			continue
		}
		var result []string
		facts := schemaConditionFacts(t, condition)
		for _, raw := range arrayField(t, objectField(t, facts, "not"), "anyOf") {
			forbidden, ok := raw.(map[string]any)
			if !ok {
				t.Fatal("occupied forbidden-goal condition is not an object")
			}
			result = append(result, stringArrayField(t, forbidden, "required")...)
		}
		return result
	}
	t.Fatal("occupied attention condition is absent")
	return nil
}

func schemaConditionKind(t *testing.T, condition map[string]any) string {
	t.Helper()
	kind, _ := objectField(t,
		objectField(t, objectField(t, condition, "if"), "properties"), "kind")["const"].(string)
	return kind
}

func schemaConditionFacts(t *testing.T, condition map[string]any) map[string]any {
	t.Helper()
	return objectField(t,
		objectField(t, objectField(t, condition, "then"), "properties"), "facts")
}

func TestGateSettlementSemanticsMatchSchemaAndBrowser(t *testing.T) {
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	gate := objectField(t, objectField(t, root, "$defs"), "gate")
	gateConditions := arrayField(t, gate, "allOf")
	if len(gateConditions) != 2 {
		t.Fatalf("gate settlement condition count = %d, want 2", len(gateConditions))
	}
	gateJSON := fmt.Sprintf("%v", gateConditions)
	for _, required := range []string{"pass", "fail", "unknown", "minItems:1", "maxItems:0"} {
		if !strings.Contains(gateJSON, required) {
			t.Fatalf("gate schema is missing settlement constraint %q", required)
		}
	}
	result := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "result")
	resultConditions := arrayField(t, result, "allOf")
	if len(resultConditions) != 2 {
		t.Fatalf("result settlement condition count = %d, want 2", len(resultConditions))
	}
	resultJSON := fmt.Sprintf("%v", resultConditions)
	for _, required := range []string{
		"passed", "pass", "not_applicable", "contains", "minContains:1", "failed", "fail",
	} {
		if !strings.Contains(resultJSON, required) {
			t.Fatalf("result schema is missing settlement constraint %q", required)
		}
	}

	html := string(readFile(t, "index.html"))
	for _, required := range []string{
		`gateIDs.has(gate.id)`,
		`gate.evidence.length === 0`,
		`gate.status === "unknown" && gate.evidence.length !== 0`,
		`assertion.id !== gate.id || assertion.status !== gate.status`,
		`if (!hasPass) fail`,
		`record.gates.some(gate => gate.status === "fail" || gate.status === "unknown")`,
		`record.status === "failed" && !hasFailure`,
	} {
		if !strings.Contains(html, required) {
			t.Fatalf("browser gate settlement validator is missing %q", required)
		}
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
