package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/test/observer"
)

func TestRuntimeProjectionAddsNoCausalEdges(t *testing.T) {
	started := time.Date(2026, 8, 4, 1, 0, 0, 0, time.UTC)
	var output bytes.Buffer
	writer, err := observer.NewWriter(&output, observer.Run{
		ID: "runtime-edge-test",
		Scenario: observer.Scenario{ID: "runtime-edge-test",
			Digest: agency.Sum([]byte("runtime-edge-test")).String()},
		StartedAt:    started,
		Participants: []observer.Participant{{Node: "lead", Agent: "lead", Runtime: "pi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	turn := turnSummary{Role: "lead", Turn: "initial-lead", HookCues: 1,
		CapturedAt:    started.Add(time.Minute).Format(time.RFC3339Nano),
		DelegateCalls: 1, CurrentReads: 1, SubmitAttempts: 1, IntentSubmits: 1,
		AgentEnd: true}
	if err := appendRuntimeFacts(writer, []turnSummary{turn}); err != nil {
		t.Fatal(err)
	}
	foundDelegate := false
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n")[1:] {
		var record struct {
			Record string   `json:"record"`
			Kind   string   `json:"kind"`
			Causes []string `json:"causes"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Record == "fact" && len(record.Causes) != 0 {
			t.Fatalf("runtime observation inferred causes: %v", record.Causes)
		}
		foundDelegate = foundDelegate || record.Kind == "runtime.delegate.invoked"
	}
	if !foundDelegate {
		t.Fatal("runtime projection omitted the bounded delegate invocation observation")
	}
}

func TestAppendEventFactsProjectsExpectedReferenceHead(t *testing.T) {
	started := time.Date(2026, 8, 4, 1, 0, 0, 0, time.UTC)
	var output bytes.Buffer
	writer, err := observer.NewWriter(&output, observer.Run{
		ID: "expected-reference-test",
		Scenario: observer.Scenario{ID: "expected-reference-test",
			Digest: agency.Sum([]byte("expected-reference-test")).String()},
		StartedAt: started,
	})
	if err != nil {
		t.Fatal(err)
	}
	referenceDigest := agency.Sum([]byte("reference head")).String()
	nodes := []nodeEvidence{{Role: "lead", Events: []eventEvidence{
		{Node: "lead", ID: "event:reference", Digest: referenceDigest,
			AcceptedAt: started.Add(time.Second), OriginSequence: 1,
			SourcePrincipal: "principal:lead", SemanticKind: "knowledge.keep",
			Consequence: "reference.publish"},
		{Node: "lead", ID: "event:supersede", Digest: agency.Sum([]byte("supersede")).String(),
			AcceptedAt: started.Add(2 * time.Second), OriginSequence: 2,
			SourcePrincipal: "principal:lead", SemanticKind: "knowledge.refine",
			Consequence: "reference.supersede", ReferenceHead: "event:reference",
			ReferenceDigest: referenceDigest},
	}}}
	if _, _, err := appendEventFacts(writer, nodes); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"reference_head":"event:reference"`) {
		t.Fatal("accepted supersede Fact omitted its exact expected Reference head")
	}
}

func TestValidateReportBoundsRuntimePrivateDelegation(t *testing.T) {
	report := validReport()
	report.Turns[0].DelegateCalls = 1
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport() rejected one bounded Runtime-private delegate: %v", err)
	}
	report.Turns[0].DelegateCalls = 2
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted more than one Runtime-private delegate in a turn")
	}
}

func TestValidateFailureReportBoundsRuntimePrivateDelegation(t *testing.T) {
	report := validFailureReport()
	report.Turns[0].DelegateCalls = 1
	if err := validateFailureReport(report); err != nil {
		t.Fatalf("validateFailureReport() rejected one bounded Runtime-private delegate: %v", err)
	}
	report.Turns[0].DelegateCalls = 2
	if err := validateFailureReport(report); err == nil {
		t.Fatal("validateFailureReport() accepted more than one Runtime-private delegate")
	}
}

func TestReportValidationAccountsRuntimeSubmitInvocationFailures(t *testing.T) {
	report := validReport()
	report.Turns[0].SubmitAttempts = 1
	report.Turns[0].SubmitInvocationFailures = 1
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport() rejected one bounded Runtime invocation failure: %v", err)
	}
	report.Turns[0].SubmitInvocationFailures = 0
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted an unaccounted submit attempt")
	}

	failure := validFailureReport()
	failure.Turns[0].SubmitAttempts = 1
	failure.Turns[0].SubmitInvocationFailures = 1
	if err := validateFailureReport(failure); err != nil {
		t.Fatalf("validateFailureReport() rejected one bounded Runtime invocation failure: %v", err)
	}
}
