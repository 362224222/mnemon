package observer

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTraceWriterProducesDeterministicStrictTrace(t *testing.T) {
	first := writeTestTrace(t)
	second := writeTestTrace(t)
	if !bytes.Equal(first, second) {
		t.Fatal("same trace inputs produced different bytes")
	}
	for _, forbidden := range [][]byte{
		[]byte(`"participants":null`), []byte(`"causes":null`),
		[]byte(`"gates":null`), []byte(`"evidence":null`),
	} {
		if bytes.Contains(first, forbidden) {
			t.Fatalf("writer emitted schema-invalid null array %s", forbidden)
		}
	}

	path := filepath.Join(t.TempDir(), "writer.trace")
	if err := os.WriteFile(path, first, 0o600); err != nil {
		t.Fatal(err)
	}
	trace := parseTrace(t, path)
	if len(trace.Facts) != 2 || trace.Facts[0].Sequence != 1 || trace.Facts[1].Sequence != 2 {
		t.Fatalf("writer fact sequence = %#v", trace.Facts)
	}
	if trace.Facts[1].Causes[0] != trace.Facts[0].ID {
		t.Fatalf("writer did not preserve explicit cause: %#v", trace.Facts[1].Causes)
	}
}

func TestTraceWriterNormalizesEmptyArrays(t *testing.T) {
	var output bytes.Buffer
	writer, err := NewWriter(&output, Run{
		ID: "empty-trace", Scenario: Scenario{
			ID: "empty", Digest: "sha256:" + strings.Repeat("b", 64),
		}, StartedAt: testTime(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Finish(Result{Status: ResultIncomplete, FinishedAt: testTime(2)}); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, required := range []string{`"participants":[]`, `"gates":[]`} {
		if !strings.Contains(text, required) {
			t.Fatalf("empty trace is missing %s: %s", required, text)
		}
	}
}

func TestTraceWriterRejectsInvalidCausalityWithoutConsumingSequence(t *testing.T) {
	var output bytes.Buffer
	writer := newTestWriter(t, &output)
	root := testFact("trace:root", "runtime.turn.started", SourceRuntime, TruthObservation)
	if sequence, err := writer.Append(root); err != nil || sequence != 1 {
		t.Fatalf("append root = %d, %v", sequence, err)
	}
	written := output.Len()

	invalid := testFact("trace:invalid", "runtime.turn.ended", SourceRuntime, TruthObservation)
	invalid.Causes = []string{"trace:not-yet-written"}
	if _, err := writer.Append(invalid); err == nil || output.Len() != written {
		t.Fatalf("dangling cause append error = %v, bytes = %d want %d", err, output.Len(), written)
	}
	invalid.Causes = []string{root.ID, root.ID}
	if _, err := writer.Append(invalid); err == nil || output.Len() != written {
		t.Fatalf("duplicate cause append error = %v, bytes = %d want %d", err, output.Len(), written)
	}
	invalid.ID = root.ID
	invalid.Causes = nil
	if _, err := writer.Append(invalid); err == nil || output.Len() != written {
		t.Fatalf("duplicate id append error = %v, bytes = %d want %d", err, output.Len(), written)
	}

	valid := testFact("trace:second", "runtime.turn.ended", SourceRuntime, TruthObservation)
	valid.Causes = []string{root.ID}
	if sequence, err := writer.Append(valid); err != nil || sequence != 2 {
		t.Fatalf("append after rejected attempts = %d, %v; want sequence 2", sequence, err)
	}
}

func TestTraceWriterFailsClosedOnClassificationAndGateEvidence(t *testing.T) {
	var output bytes.Buffer
	writer := newTestWriter(t, &output)

	wrongBoundary := testFact("trace:wrong-boundary", "r7.event.accepted", SourceRuntime, TruthObservation)
	if _, err := writer.Append(wrongBoundary); err == nil {
		t.Fatal("writer accepted an R7 authority fact from the runtime boundary")
	}
	r8WithoutSelection := testFact(
		"trace:r8-no-selection", "r8.selection.seeded", SourceR8Selector, TruthLocalPreference)
	if _, err := writer.Append(r8WithoutSelection); err == nil {
		t.Fatal("writer accepted R8 evidence without a SelectionID")
	}

	fact := testFact("trace:fact", "runtime.turn.started", SourceRuntime, TruthObservation)
	if _, err := writer.Append(fact); err != nil {
		t.Fatal(err)
	}
	written := output.Len()
	invalidResult := Result{
		Status: ResultPassed, FinishedAt: testTime(4),
		Gates: []Gate{{ID: "gate-a", Status: GatePass, Evidence: []string{"trace:missing"}}},
	}
	if err := writer.Finish(invalidResult); err == nil || output.Len() != written {
		t.Fatalf("missing gate evidence finish error = %v, bytes = %d want %d", err, output.Len(), written)
	}
	invalidResult.Gates[0].Evidence = []string{fact.ID}
	if err := writer.Finish(invalidResult); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Append(testFact(
		"trace:after-footer", "runtime.turn.ended", SourceRuntime, TruthObservation)); err == nil {
		t.Fatal("writer accepted a fact after the result footer")
	}
}

func TestTraceWriterRejectsInconsistentTargetMetadata(t *testing.T) {
	var output bytes.Buffer
	writer := newTestWriter(t, &output)
	fact := testFact("trace:targets", "r7.event.accepted",
		SourceR7Authority, TruthAcceptedLocalFact)
	count := 2
	fact.Fields.TargetCount = &count
	fact.Fields.Targets = []string{"peer-a"}
	if _, err := writer.Append(fact); err == nil {
		t.Fatal("writer accepted a target count that did not match its bounded aliases")
	}
}

func TestTraceWriterRejectsKindsWithoutMinimumDisplayEvidence(t *testing.T) {
	tests := []struct {
		name string
		fact Fact
	}{
		{"accepted Event", requiredEvidenceFact("trace:event", "r7.event.accepted")},
		{"resolved Handling", requiredEvidenceFact("trace:resolved", "r7.handling.resolved")},
		{"selection seed", requiredEvidenceFact("trace:seed", "r8.selection.seeded")},
		{"frozen round", requiredEvidenceFact("trace:frozen", "r8.round.frozen")},
		{"vote", requiredEvidenceFact("trace:vote", "r8.vote.observed")},
		{"settled round", requiredEvidenceFact("trace:settled", "r8.round.settled")},
		{"preference observation", requiredEvidenceFact("trace:observation", "r8.observation.produced")},
	}
	tests[0].fact.Fields.SemanticKind = ""
	tests[1].fact.Fields.Outcome = ""
	tests[2].fact.Fields.PreferenceAfter = ""
	tests[3].fact.Fields.Alpha = nil
	tests[4].fact.Fields.Authenticated = nil
	tests[5].fact.Fields.Recolored = nil
	tests[6].fact.Fields.Result = ""

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			writer := newTestWriter(t, &output)
			written := output.Len()
			if _, err := writer.Append(test.fact); err == nil || output.Len() != written {
				t.Fatalf("append missing display evidence error = %v, bytes = %d want %d",
					err, output.Len(), written)
			}
		})
	}
}

func TestKindEvidenceRulesMatchClosedDisplayContract(t *testing.T) {
	expected := []string{
		"r7.event.accepted", "r7.handling.resolved", "r8.selection.seeded",
		"r8.round.frozen", "r8.vote.observed", "r8.round.settled",
		"r8.observation.produced",
	}
	if len(kindEvidenceRules) != len(expected) {
		t.Fatalf("kind evidence rules = %d, want %d", len(kindEvidenceRules), len(expected))
	}
	for _, kind := range expected {
		rule, exists := kindEvidenceRules[kind]
		if !exists || rule.label == "" || rule.valid == nil {
			t.Fatalf("kind evidence rule %q is missing or incomplete", kind)
		}
	}
}

func TestTraceWriterMakesOutputFailureTerminal(t *testing.T) {
	destination := &failAfterFirstWrite{}
	writer := newTestWriter(t, destination)
	fact := testFact("trace:first", "runtime.turn.started", SourceRuntime, TruthObservation)
	if _, err := writer.Append(fact); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("append error = %v, want injected output failure", err)
	}
	if _, err := writer.Append(fact); !errors.Is(err, errInjectedWrite) {
		t.Fatalf("append after output failure error = %v, want same terminal failure", err)
	}
}

func writeTestTrace(t *testing.T) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := newTestWriter(t, &output)
	turn := testFact("trace:turn", "runtime.turn.started", SourceRuntime, TruthObservation)
	if _, err := writer.Append(turn); err != nil {
		t.Fatal(err)
	}
	gateFact := testFact("trace:gate", "test.gate.checked", SourceOracle, TruthAssertion)
	gateFact.Causes = []string{turn.ID}
	gateFact.Fields = FactFields{GateID: "scenario.outcome", Status: "pass"}
	if _, err := writer.Append(gateFact); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finish(Result{
		Status: ResultPassed, FinishedAt: testTime(3),
		Gates: []Gate{{ID: "scenario.outcome", Status: GatePass, Evidence: []string{gateFact.ID}}},
	}); err != nil {
		t.Fatal(err)
	}
	return bytes.Clone(output.Bytes())
}

func newTestWriter(t *testing.T, destination interface{ Write([]byte) (int, error) }) *Writer {
	t.Helper()
	writer, err := NewWriter(destination, Run{
		ID: "writer-test",
		Scenario: Scenario{
			ID: "protocol-neutral", Digest: "sha256:" + strings.Repeat("a", 64),
		},
		StartedAt: testTime(1),
		Participants: []Participant{{
			Node: "node-a", Agent: "agent-a", Runtime: "pi", Model: "deepseek-v4-flash",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return writer
}

func testFact(id, kind string, source SourceClass, truth TruthClass) Fact {
	return Fact{
		ID: id, CapturedAt: testTime(2), Source: Source{Class: source, Node: "node-a"},
		Agent: "agent-a", Turn: "turn-a", Kind: kind, Truth: truth,
	}
}

func requiredEvidenceFact(id, kind string) Fact {
	integer := 1
	zero := 0
	boolean := true
	source, truth := SourceR7Authority, TruthAcceptedLocalFact
	if strings.HasPrefix(kind, "r8.") {
		source, truth = SourceR8Selector, TruthLocalPreference
		if kind == "r8.vote.observed" {
			truth = TruthObservation
		}
	}
	fact := testFact(id, kind, source, truth)
	fact.References = References{Event: "event:one", EventDigest: "sha256:" + strings.Repeat("1", 64),
		Handling: "handling:one", Selection: "sha256:" + strings.Repeat("2", 64)}
	fact.Fields = FactFields{SemanticKind: "work.result", Consequence: "handling.resolve.completed",
		Outcome: "completed", State: "terminal", PreferenceBefore: "A", PreferenceAfter: "B",
		Phase: "observed", Result: "threshold_reached", Round: &integer, SampleSize: &integer,
		Alpha: &integer, VotesA: &zero, VotesB: &integer, MarginBefore: &zero,
		MarginAfter: &integer, Authenticated: &boolean, Recolored: &boolean}
	return fact
}

func testTime(second int) time.Time {
	return time.Date(2026, 8, 4, 10, 0, second, 123456789, time.FixedZone("test", 8*60*60))
}

var errInjectedWrite = errors.New("injected write failure")

type failAfterFirstWrite struct {
	writes int
}

func (writer *failAfterFirstWrite) Write(data []byte) (int, error) {
	writer.writes++
	if writer.writes > 1 {
		return 0, errInjectedWrite
	}
	return len(data), nil
}
