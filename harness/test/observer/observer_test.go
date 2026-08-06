package observer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var (
	dangerousKeys = []string{
		"api_key", "args", "artifact_bytes", "attachment_credential", "chain_of_thought",
		"command", "credential", "environment", "message", "operation_key", "payload",
		"private_key", "prompt", "reasoning", "signature", "tool_result", "transcript",
	}
)

type envelope struct {
	Schema  string `json:"schema"`
	Version int    `json:"version"`
	Record  string `json:"record"`
}

type runRecord struct {
	Schema          string        `json:"schema"`
	Version         int           `json:"version"`
	Record          string        `json:"record"`
	RunID           string        `json:"run_id"`
	Scenario        scenarioWire  `json:"scenario"`
	Redaction       string        `json:"redaction"`
	StartedAt       string        `json:"started_at"`
	CandidateDigest string        `json:"candidate_digest,omitempty"`
	Participants    []participant `json:"participants"`
}

type scenarioWire struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

type participant struct {
	Node    string `json:"node"`
	Agent   string `json:"agent,omitempty"`
	Runtime string `json:"runtime,omitempty"`
	Model   string `json:"model,omitempty"`
}

type factRecord struct {
	Schema     string     `json:"schema"`
	Version    int        `json:"version"`
	Record     string     `json:"record"`
	Sequence   int        `json:"seq"`
	ID         string     `json:"id"`
	CapturedAt string     `json:"captured_at"`
	Source     sourceWire `json:"source"`
	Agent      string     `json:"agent,omitempty"`
	Turn       string     `json:"turn,omitempty"`
	Kind       string     `json:"kind"`
	Truth      string     `json:"truth"`
	Causes     []string   `json:"causes"`
	Refs       refsWire   `json:"refs"`
	Facts      factsWire  `json:"facts"`
}

type sourceWire struct {
	Class string `json:"class"`
	Node  string `json:"node"`
}

type refsWire struct {
	Artifact      string `json:"artifact,omitempty"`
	Correlation   string `json:"correlation,omitempty"`
	Delivery      string `json:"delivery,omitempty"`
	Event         string `json:"event,omitempty"`
	EventDigest   string `json:"event_digest,omitempty"`
	Handling      string `json:"handling,omitempty"`
	Principal     string `json:"principal,omitempty"`
	ReferenceHead string `json:"reference_head,omitempty"`
	Selection     string `json:"selection,omitempty"`
}

type factsWire struct {
	Action           string   `json:"action,omitempty"`
	Alpha            *int     `json:"alpha,omitempty"`
	ArtifactCount    *int     `json:"artifact_count,omitempty"`
	AttemptCount     *int     `json:"attempt_count,omitempty"`
	Authenticated    *bool    `json:"authenticated,omitempty"`
	BypassedHook     *bool    `json:"bypassed_hook,omitempty"`
	ByteSize         *int64   `json:"byte_size,omitempty"`
	Code             string   `json:"code,omitempty"`
	Count            *int     `json:"count,omitempty"`
	Consequence      string   `json:"consequence,omitempty"`
	DurationMillis   *int64   `json:"duration_ms,omitempty"`
	GateID           string   `json:"gate_id,omitempty"`
	HookCue          *bool    `json:"hook_cue,omitempty"`
	InvalidVotes     *int     `json:"invalid_votes,omitempty"`
	MarginAfter      *int     `json:"margin_after,omitempty"`
	MarginBefore     *int     `json:"margin_before,omitempty"`
	NoVote           *bool    `json:"no_vote,omitempty"`
	NoVotes          *int     `json:"no_votes,omitempty"`
	Outcome          string   `json:"outcome,omitempty"`
	PayloadBytes     *int     `json:"payload_bytes,omitempty"`
	Phase            string   `json:"phase,omitempty"`
	PreferenceAfter  string   `json:"preference_after,omitempty"`
	PreferenceBefore string   `json:"preference_before,omitempty"`
	Recolored        *bool    `json:"recolored,omitempty"`
	Replayed         *bool    `json:"replayed,omitempty"`
	Result           string   `json:"result,omitempty"`
	Round            *int     `json:"round,omitempty"`
	SampleSize       *int     `json:"sample_size,omitempty"`
	SemanticKind     string   `json:"semantic_kind,omitempty"`
	State            string   `json:"state,omitempty"`
	Status           string   `json:"status,omitempty"`
	SuccessCount     *int     `json:"success_count,omitempty"`
	TargetCount      *int     `json:"target_count,omitempty"`
	Targets          []string `json:"targets,omitempty"`
	TimedOut         *bool    `json:"timed_out,omitempty"`
	ViewNonempty     *bool    `json:"view_nonempty,omitempty"`
	VotesA           *int     `json:"votes_a,omitempty"`
	VotesB           *int     `json:"votes_b,omitempty"`
}

type resultRecord struct {
	Schema      string     `json:"schema"`
	Version     int        `json:"version"`
	Record      string     `json:"record"`
	Status      string     `json:"status"`
	FinishedAt  string     `json:"finished_at"`
	RecordCount int        `json:"record_count"`
	TraceDigest string     `json:"trace_digest"`
	Gates       []gateWire `json:"gates"`
}

type gateWire struct {
	ID       string   `json:"id"`
	Status   string   `json:"status"`
	Evidence []string `json:"evidence"`
}

type parsedTrace struct {
	Run    runRecord
	Facts  []factRecord
	Result resultRecord
}

func TestObserverIsSingleFileLocalOnlyAndMarkupSafe(t *testing.T) {
	raw := readFile(t, "index.html")
	text := string(raw)
	for _, forbidden := range []string{
		"http://", "https://", "<script src=", "<link ", "fetch(", "WebSocket(",
		"EventSource(", "XMLHttpRequest", ".innerHTML", ".outerHTML",
		"insertAdjacentHTML", "eval(", "new Function(", "connectReference(", "referenceOwners",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("observer HTML contains forbidden surface %q", forbidden)
		}
	}
	for _, required := range []string{
		`connect-src 'none'`, `id="traceFiles"`, `id="dropZone"`, `new FileReader()`,
		`readAsArrayBuffer`, `crypto.subtle.digest`, `TextDecoder("utf-8", { fatal: true })`,
		`id="summary"`, `id="agents"`, `id="causality"`, `id="collaboration"`,
		`id="selection"`, `.textContent`, "LOCAL PREFERENCE ONLY",
		"not agreement, consensus, finality, truth", "contains unknown field",
		"does not refer to an earlier fact", "trace_digest does not cover",
		"invalid kind/source/truth classification", "explicit backward causes",
		"no preference evidence is merged across selections", "collaborationComponents",
		"validateKindEvidence", "isStandaloneRuntimeComponent", "collaborationPriority",
		"semantic_kind", "terminal Handling outcome", "Agent evidence lane",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("observer HTML is missing %q", required)
		}
	}
}

func TestObserverFilePickerAcceptsDocumentedTraceExtension(t *testing.T) {
	html := string(readFile(t, "index.html"))
	const picker = `<input id="traceFiles" type="file" multiple accept=".trace,.jsonl,.ndjson,application/x-ndjson">`
	if count := strings.Count(html, picker); count != 1 {
		t.Fatalf("observer trace file picker count = %d, want exactly one documented picker", count)
	}
}

func TestTraceSchemaMatchesGoFactDefinitions(t *testing.T) {
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	definitions := objectField(t, root, "$defs")
	factsDefinition := objectField(t, definitions, "facts")
	schemaFields := objectKeys(objectField(t, factsDefinition, "properties"))
	goFields := jsonFieldNames(reflect.TypeOf(factsWire{}))
	assertSameStrings(t, "facts properties", schemaFields, goFields)
	sourceDefinition := objectField(t, definitions, "source")
	sourceProperties := objectField(t, sourceDefinition, "properties")
	assertSameStrings(t, "source class enum", stringArrayField(t, objectField(t, sourceProperties, "class"), "enum"), sourceClasses)

	factVariant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "fact")
	factProperties := objectField(t, factVariant, "properties")
	kindDefinition := objectField(t, factProperties, "kind")
	schemaKinds := stringArrayField(t, kindDefinition, "enum")
	assertSameStrings(t, "fact kind enum", schemaKinds, knownFactKinds())
	truthDefinition := objectField(t, factProperties, "truth")
	assertSameStrings(t, "truth class enum", stringArrayField(t, truthDefinition, "enum"), truthClasses)
}

func TestBrowserValidatorMatchesSchemaAndGoDefinitions(t *testing.T) {
	html := string(readFile(t, "index.html"))
	root := decodeJSONObject(t, readFile(t, "trace-schema.json"), "schema root")
	definitions := objectField(t, root, "$defs")
	schemaFields := objectKeys(objectField(t, objectField(t, definitions, "facts"), "properties"))
	factVariant := schemaRecordVariant(t, arrayField(t, root, "oneOf"), "fact")
	factProperties := objectField(t, factVariant, "properties")
	schemaKinds := stringArrayField(t, objectField(t, factProperties, "kind"), "enum")

	assertSameStrings(t, "browser facts properties", javascriptStringArray(t, html, "const FACT_FIELDS"), schemaFields)
	assertSameStrings(t, "browser fact kinds", sortedKindsFromRows(
		javascriptStringArray(t, html, "const FACT_CLASSIFICATION_ROWS")), schemaKinds)
	assertSameStrings(t, "browser source classes", javascriptStringArray(t, html, "const SOURCE_CLASSES"), sourceClasses)
	assertSameStrings(t, "browser truth classes", javascriptStringArray(t, html, "const TRUTH_CLASSES"), truthClasses)
}

func TestTraceSchemaIsClosedAndMetadataOnly(t *testing.T) {
	raw := readFile(t, "trace-schema.json")
	if !json.Valid(raw) {
		t.Fatal("trace-schema.json is not valid JSON")
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	oneOf, ok := root["oneOf"].([]any)
	if !ok || len(oneOf) != 3 {
		t.Fatalf("schema variants = %#v, want run/fact/result", root["oneOf"])
	}
	assertClosedObjects(t, root, "root")
	assertNoDangerousKeys(t, root)
	for _, required := range []string{"mnemon.test.trace", "accepted_local_fact", "local_preference", "additionalProperties"} {
		if !bytes.Contains(raw, []byte(required)) {
			t.Fatalf("trace schema is missing %q", required)
		}
	}
}

func TestObserverFixturesAreStrictRedactedRenderInputs(t *testing.T) {
	paths, err := filepath.Glob("fixtures/*.trace")
	if err != nil || len(paths) != 2 {
		t.Fatalf("fixture paths = %v, %v", paths, err)
	}
	var kinds []string
	for _, path := range paths {
		trace := parseTrace(t, path)
		for _, fact := range trace.Facts {
			kinds = append(kinds, fact.Kind)
		}
	}
	for _, required := range []string{
		"runtime.turn.started", "r7.event.accepted", "r7.delivery.readmitted",
		"r7.handling.resolved", "r7.reference.published", "r8.selection.seeded",
		"r8.round.settled", "r8.observation.produced", "test.gate.checked",
	} {
		if !slices.Contains(kinds, required) {
			t.Fatalf("fixtures do not exercise %q", required)
		}
	}
}

func TestTraceDecoderRejectsDangerousOrUnknownFields(t *testing.T) {
	for _, field := range dangerousKeys {
		raw := []byte(fmt.Sprintf(`{"schema":"%s","version":1,"record":"fact","%s":"secret"}`,
			traceSchema, field))
		var value factRecord
		if err := decodeStrict(raw, &value); err == nil {
			t.Fatalf("strict fact decoder accepted forbidden/unknown field %q", field)
		}
	}
	nested := []struct {
		name        string
		raw         string
		destination any
	}{
		{"facts", `{"facts":{"reason":"not-in-schema"}}`, &factRecord{}},
		{"source", `{"source":{"unexpected":true}}`, &factRecord{}},
		{"refs", `{"refs":{"unexpected":true}}`, &factRecord{}},
		{"participant", `{"participants":[{"node":"node-a","unexpected":true}]}`, &runRecord{}},
		{"gate", `{"gates":[{"id":"gate-a","status":"pass","evidence":[],"unexpected":true}]}`, &resultRecord{}},
	}
	for _, test := range nested {
		if err := decodeStrict([]byte(test.raw), test.destination); err == nil {
			t.Fatalf("strict decoder accepted unknown nested %s field", test.name)
		}
	}
}

func parseTrace(t *testing.T, path string) parsedTrace {
	t.Helper()
	raw := readFile(t, path)
	assertNoDangerousJSONKeys(t, raw)
	if len(raw) == 0 || raw[len(raw)-1] != '\n' || bytes.Contains(raw, []byte("\n\n")) {
		t.Fatalf("%s must be canonical non-empty JSONL with one final newline", path)
	}
	lines := bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n"))
	if len(lines) < 2 || len(lines) > maxTraceFacts+2 {
		t.Fatalf("%s line count = %d", path, len(lines))
	}
	for index, line := range lines {
		if len(line) == 0 || len(line) > maxTraceLine || !json.Valid(line) {
			t.Fatalf("%s line %d is empty, oversized, or invalid", path, index+1)
		}
	}
	var trace parsedTrace
	if err := decodeStrict(lines[0], &trace.Run); err != nil {
		t.Fatalf("%s run header: %v", path, err)
	}
	validateRun(t, trace.Run)
	seen := make(map[string]struct{}, len(lines)-2)
	for index, line := range lines[1 : len(lines)-1] {
		var env envelope
		if err := json.Unmarshal(line, &env); err != nil || env.Record != "fact" {
			t.Fatalf("%s line %d is not a fact", path, index+2)
		}
		var fact factRecord
		if err := decodeStrict(line, &fact); err != nil {
			t.Fatalf("%s fact %d: %v", path, index+1, err)
		}
		validateFact(t, fact, index+1, seen)
		seen[fact.ID] = struct{}{}
		trace.Facts = append(trace.Facts, fact)
	}
	if err := decodeStrict(lines[len(lines)-1], &trace.Result); err != nil {
		t.Fatalf("%s result: %v", path, err)
	}
	validateResult(t, trace.Result, trace.Facts, seen)
	prefixLength := bytes.LastIndex(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n")) + 1
	digest := sha256.Sum256(raw[:prefixLength])
	want := "sha256:" + hex.EncodeToString(digest[:])
	if trace.Result.TraceDigest != want {
		t.Fatalf("%s trace digest = %q, want %q", path, trace.Result.TraceDigest, want)
	}
	return trace
}

func validateRun(t *testing.T, run runRecord) {
	t.Helper()
	if run.Schema != traceSchema || run.Version != traceVersion || run.Record != "run" ||
		run.Redaction != "metadata" || !validToken(run.RunID) || !validToken(run.Scenario.ID) ||
		!digestPattern.MatchString(run.Scenario.Digest) || !validTime(run.StartedAt) ||
		len(run.Participants) > 32 {
		t.Fatalf("invalid run header: %#v", run)
	}
	if run.CandidateDigest != "" && !digestPattern.MatchString(run.CandidateDigest) {
		t.Fatalf("invalid candidate digest %q", run.CandidateDigest)
	}
	for _, participant := range run.Participants {
		if !validToken(participant.Node) {
			t.Fatalf("invalid participant node %q", participant.Node)
		}
		for _, value := range []string{participant.Agent, participant.Runtime, participant.Model} {
			if value != "" && !validToken(value) {
				t.Fatalf("invalid participant token %q", value)
			}
		}
	}
}

func validateFact(t *testing.T, fact factRecord, sequence int, seen map[string]struct{}) {
	t.Helper()
	validateFactIdentity(t, fact, sequence)
	validateFactTokens(t, fact, sequence)
	validateFactCauses(t, fact, sequence, seen)
	validateFactReferences(t, fact, sequence)
	validateFactClassification(t, fact)
	validateClosedFacts(t, sequence, fact.Facts)
	validateRequiredFactEvidence(t, fact)
}

func validateRequiredFactEvidence(t *testing.T, fact factRecord) {
	t.Helper()
	if err := validateKindEvidence(factEvidenceInput(fact), fact.Sequence); err != nil {
		t.Fatal(err)
	}
}

func factEvidenceInput(fact factRecord) Fact {
	return Fact{
		Kind:   fact.Kind,
		Causes: slices.Clone(fact.Causes),
		References: References{
			Event: fact.Refs.Event, EventDigest: fact.Refs.EventDigest,
			Handling: fact.Refs.Handling,
		},
		Fields: FactFields{
			SemanticKind: fact.Facts.SemanticKind, Consequence: fact.Facts.Consequence,
			Outcome: fact.Facts.Outcome, State: fact.Facts.State, Phase: fact.Facts.Phase,
			Action: fact.Facts.Action, Code: fact.Facts.Code, Count: fact.Facts.Count,
			AttemptCount: fact.Facts.AttemptCount, SuccessCount: fact.Facts.SuccessCount,
			PreferenceBefore: fact.Facts.PreferenceBefore, PreferenceAfter: fact.Facts.PreferenceAfter,
			Result: fact.Facts.Result, Round: fact.Facts.Round, SampleSize: fact.Facts.SampleSize,
			Alpha: fact.Facts.Alpha, VotesA: fact.Facts.VotesA, VotesB: fact.Facts.VotesB,
			MarginBefore: fact.Facts.MarginBefore, MarginAfter: fact.Facts.MarginAfter,
			Authenticated: fact.Facts.Authenticated, Recolored: fact.Facts.Recolored,
		},
	}
}

func validateFactIdentity(t *testing.T, fact factRecord, sequence int) {
	t.Helper()
	checks := []struct {
		name  string
		valid bool
	}{
		{"schema", fact.Schema == traceSchema},
		{"version", fact.Version == traceVersion},
		{"record", fact.Record == "fact"},
		{"sequence", fact.Sequence == sequence},
		{"id", tracePattern.MatchString(fact.ID)},
		{"captured_at", validTime(fact.CapturedAt)},
		{"source.class", slices.Contains(sourceClasses, fact.Source.Class)},
		{"source.node", validToken(fact.Source.Node)},
		{"kind", slices.Contains(knownFactKinds(), fact.Kind)},
		{"truth", slices.Contains(truthClasses, fact.Truth)},
	}
	for _, check := range checks {
		if !check.valid {
			t.Fatalf("fact %d has invalid %s", sequence, check.name)
		}
	}
}

func validateFactTokens(t *testing.T, fact factRecord, sequence int) {
	t.Helper()
	values := append([]string{fact.Agent, fact.Turn, fact.Facts.Code,
		fact.Facts.GateID, fact.Facts.SemanticKind}, fact.Facts.Targets...)
	for _, value := range values {
		if value != "" && !validToken(value) {
			t.Fatalf("fact %d has invalid token %q", sequence, value)
		}
	}
}

func validateFactCauses(t *testing.T, fact factRecord, sequence int, seen map[string]struct{}) {
	t.Helper()
	if _, duplicate := seen[fact.ID]; duplicate {
		t.Fatalf("fact %d repeats id %q", sequence, fact.ID)
	}
	if len(fact.Causes) > 16 {
		t.Fatalf("fact %d has %d causes, max 16", sequence, len(fact.Causes))
	}
	unique := make(map[string]struct{}, len(fact.Causes))
	for _, cause := range fact.Causes {
		if !tracePattern.MatchString(cause) {
			t.Fatalf("fact %d has invalid cause %q", sequence, cause)
		}
		if _, duplicate := unique[cause]; duplicate {
			t.Fatalf("fact %d repeats cause %q", sequence, cause)
		}
		if _, present := seen[cause]; !present {
			t.Fatalf("fact %d cause %q is not an earlier fact", sequence, cause)
		}
		unique[cause] = struct{}{}
	}
}

func validateFactReferences(t *testing.T, fact factRecord, sequence int) {
	t.Helper()
	for _, digest := range []string{fact.Refs.Artifact, fact.Refs.EventDigest, fact.Refs.Selection} {
		if digest != "" && !digestPattern.MatchString(digest) {
			t.Fatalf("fact %d has invalid digest %q", sequence, digest)
		}
	}
	for _, value := range []string{fact.Refs.Correlation, fact.Refs.Delivery, fact.Refs.Event,
		fact.Refs.Handling, fact.Refs.Principal, fact.Refs.ReferenceHead} {
		if value != "" && !validToken(value) {
			t.Fatalf("fact %d has invalid reference %q", sequence, value)
		}
	}
}

func validateFactClassification(t *testing.T, fact factRecord) {
	t.Helper()
	if !validFactClassification(fact) {
		t.Fatalf("fact %q has invalid source/truth classification or required scope", fact.Kind)
	}
}

func validateClosedFacts(t *testing.T, sequence int, facts factsWire) {
	t.Helper()
	checkEnum := func(name, value string, allowed []string) {
		if value != "" && !slices.Contains(allowed, value) {
			t.Fatalf("fact %d has invalid %s %q", sequence, name, value)
		}
	}
	checkEnum("action", facts.Action, []string{"current", "submit", "capture", "read", "probe", "mutation", "other"})
	checkEnum("consequence", facts.Consequence, []string{
		"handling.create", "handling.advance", "handling.resolve.completed",
		"handling.resolve.declined", "handling.resolve.unresolved", "reference.publish",
		"reference.supersede", "reference.retract",
	})
	checkEnum("outcome", facts.Outcome, []string{"accepted", "rejected", "replayed", "completed", "declined", "unresolved"})
	checkEnum("phase", facts.Phase, []string{"awaiting_seed", "active", "observed"})
	checkEnum("preference_after", facts.PreferenceAfter, []string{"A", "B"})
	checkEnum("preference_before", facts.PreferenceBefore, []string{"A", "B"})
	checkEnum("result", facts.Result, []string{"threshold_reached", "inconclusive"})
	checkEnum("state", facts.State, []string{"open", "active", "pending", "settled", "expired", "retracted", "terminal"})
	checkEnum("status", facts.Status, []string{"pass", "fail", "incomplete", "unknown", "not_applicable"})
	validateOptionalInt(t, sequence, "alpha", facts.Alpha, 1, 64)
	validateOptionalInt(t, sequence, "artifact_count", facts.ArtifactCount, 0, 64)
	validateOptionalInt(t, sequence, "attempt_count", facts.AttemptCount, 0, 256)
	validateOptionalInt(t, sequence, "count", facts.Count, 1, 256)
	validateOptionalInt(t, sequence, "invalid_votes", facts.InvalidVotes, 0, 128)
	validateOptionalInt(t, sequence, "margin_after", facts.MarginAfter, -1024, 1024)
	validateOptionalInt(t, sequence, "margin_before", facts.MarginBefore, -1024, 1024)
	validateOptionalInt(t, sequence, "no_votes", facts.NoVotes, 0, 64)
	validateOptionalInt(t, sequence, "payload_bytes", facts.PayloadBytes, 0, 32<<10)
	validateOptionalInt(t, sequence, "round", facts.Round, 0, 1024)
	validateOptionalInt(t, sequence, "sample_size", facts.SampleSize, 0, 64)
	validateOptionalInt(t, sequence, "success_count", facts.SuccessCount, 0, 256)
	validateOptionalInt(t, sequence, "target_count", facts.TargetCount, 0, 16)
	validateOptionalInt(t, sequence, "votes_a", facts.VotesA, 0, 64)
	validateOptionalInt(t, sequence, "votes_b", facts.VotesB, 0, 64)
	validateOptionalInt64(t, sequence, "byte_size", facts.ByteSize, 0, 16<<20)
	validateOptionalInt64(t, sequence, "duration_ms", facts.DurationMillis, 0, 3600000)
	if len(facts.Targets) > 16 {
		t.Fatalf("fact %d targets exceed 16", sequence)
	}
	if facts.TargetCount != nil && *facts.TargetCount != len(facts.Targets) {
		t.Fatalf("fact %d target count %d does not match %d targets",
			sequence, *facts.TargetCount, len(facts.Targets))
	}
	if (facts.AttemptCount == nil) != (facts.SuccessCount == nil) ||
		(facts.AttemptCount != nil && *facts.SuccessCount > *facts.AttemptCount) {
		t.Fatalf("fact %d has inconsistent operation counts", sequence)
	}
}

func validateOptionalInt(t *testing.T, sequence int, name string, value *int, minimum, maximum int) {
	t.Helper()
	if value != nil && (*value < minimum || *value > maximum) {
		t.Fatalf("fact %d has out-of-bound %s %d", sequence, name, *value)
	}
}

func validateOptionalInt64(t *testing.T, sequence int, name string, value *int64, minimum, maximum int64) {
	t.Helper()
	if value != nil && (*value < minimum || *value > maximum) {
		t.Fatalf("fact %d has out-of-bound %s %d", sequence, name, *value)
	}
}

func validateResult(t *testing.T, result resultRecord, facts []factRecord, seen map[string]struct{}) {
	t.Helper()
	if result.Schema != traceSchema || result.Version != traceVersion || result.Record != "result" ||
		!slices.Contains([]string{"passed", "failed", "incomplete"}, result.Status) ||
		!validTime(result.FinishedAt) || result.RecordCount != len(facts) ||
		!digestPattern.MatchString(result.TraceDigest) || len(result.Gates) > 64 {
		t.Fatalf("invalid result: %#v", result)
	}
	for _, gate := range result.Gates {
		if !validToken(gate.ID) || !slices.Contains([]string{"pass", "fail", "unknown", "not_applicable"}, gate.Status) {
			t.Fatalf("invalid gate %#v", gate)
		}
		if len(gate.Evidence) > 32 {
			t.Fatalf("gate %q has %d evidence references, max 32", gate.ID, len(gate.Evidence))
		}
		unique := make(map[string]struct{}, len(gate.Evidence))
		for _, evidence := range gate.Evidence {
			if _, duplicate := unique[evidence]; duplicate {
				t.Fatalf("gate %q repeats evidence %q", gate.ID, evidence)
			}
			if _, present := seen[evidence]; !present {
				t.Fatalf("gate %q cites missing evidence %q", gate.ID, evidence)
			}
			unique[evidence] = struct{}{}
		}
	}
}

func decodeStrict(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func validToken(value string) bool { return tokenPattern.MatchString(value) }

func validTime(value string) bool {
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func decodeJSONObject(t *testing.T, raw []byte, label string) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	return value
}

func objectField(t *testing.T, object map[string]any, name string) map[string]any {
	t.Helper()
	value, ok := object[name].(map[string]any)
	if !ok {
		t.Fatalf("schema field %q is not an object", name)
	}
	return value
}

func arrayField(t *testing.T, object map[string]any, name string) []any {
	t.Helper()
	value, ok := object[name].([]any)
	if !ok {
		t.Fatalf("schema field %q is not an array", name)
	}
	return value
}

func schemaRecordVariant(t *testing.T, variants []any, record string) map[string]any {
	t.Helper()
	for _, value := range variants {
		variant, ok := value.(map[string]any)
		if !ok {
			t.Fatal("schema variant is not an object")
		}
		properties := objectField(t, variant, "properties")
		recordDefinition := objectField(t, properties, "record")
		if constant, _ := recordDefinition["const"].(string); constant == record {
			return variant
		}
	}
	t.Fatalf("schema has no %q record variant", record)
	return nil
}

func stringArrayField(t *testing.T, object map[string]any, name string) []string {
	t.Helper()
	values := arrayField(t, object, name)
	result := make([]string, 0, len(values))
	for _, value := range values {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("schema field %q contains a non-string value", name)
		}
		result = append(result, text)
	}
	return result
}

func objectKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	return keys
}

func jsonFieldNames(typ reflect.Type) []string {
	fields := make([]string, 0, typ.NumField())
	for index := 0; index < typ.NumField(); index++ {
		name, _, _ := strings.Cut(typ.Field(index).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			fields = append(fields, name)
		}
	}
	return fields
}

func javascriptStringArray(t *testing.T, source, marker string) []string {
	t.Helper()
	markerIndex := strings.Index(source, marker)
	if markerIndex < 0 {
		t.Fatalf("browser validator is missing %q", marker)
	}
	tail := source[markerIndex+len(marker):]
	start := strings.Index(tail, "[")
	if start < 0 {
		t.Fatalf("browser validator %q has no array", marker)
	}
	end := strings.Index(tail[start:], "]")
	if end < 0 {
		t.Fatalf("browser validator %q has no array terminator", marker)
	}
	var values []string
	if err := json.Unmarshal([]byte(tail[start:start+end+1]), &values); err != nil {
		t.Fatalf("browser validator %q array: %v", marker, err)
	}
	return values
}

func assertSameStrings(t *testing.T, label string, actual, expected []string) {
	t.Helper()
	actual = sortedUniqueStrings(t, label+" schema", actual)
	expected = sortedUniqueStrings(t, label+" Go", expected)
	if !slices.Equal(actual, expected) {
		t.Fatalf("%s = %v, want %v", label, actual, expected)
	}
}

func sortedUniqueStrings(t *testing.T, label string, values []string) []string {
	t.Helper()
	values = slices.Clone(values)
	slices.Sort(values)
	for index := 1; index < len(values); index++ {
		if values[index] == values[index-1] {
			t.Fatalf("%s repeats %q", label, values[index])
		}
	}
	return values
}

func assertNoDangerousJSONKeys(t *testing.T, raw []byte) {
	t.Helper()
	var values []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var value map[string]any
		if err := json.Unmarshal(line, &value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	for _, value := range values {
		assertNoDangerousKeys(t, value)
	}
}

func assertNoDangerousKeys(t *testing.T, value any) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if slices.Contains(dangerousKeys, strings.ToLower(key)) {
				t.Fatalf("forbidden trace field %q", key)
			}
			assertNoDangerousKeys(t, child)
		}
	case []any:
		for _, child := range typed {
			assertNoDangerousKeys(t, child)
		}
	}
}

func assertClosedObjects(t *testing.T, value any, path string) {
	t.Helper()
	switch typed := value.(type) {
	case map[string]any:
		if _, hasProperties := typed["properties"]; hasProperties && typed["type"] == "object" {
			closed, present := typed["additionalProperties"]
			if !present || closed != false {
				t.Fatalf("schema object %s is not closed", path)
			}
		}
		for key, child := range typed {
			assertClosedObjects(t, child, path+"/"+key)
		}
	case []any:
		for index, child := range typed {
			assertClosedObjects(t, child, fmt.Sprintf("%s/%d", path, index))
		}
	}
}
