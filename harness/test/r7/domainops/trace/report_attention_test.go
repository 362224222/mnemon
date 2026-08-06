package main

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestValidateReportBindsFirstAttentionTurnsAndBarrier(t *testing.T) {
	report := validReport()
	settlement := &report.Protocol.FirstAttention[0]
	wave := firstAttentionWave{Wave: 1}
	for _, role := range domainRoles {
		node := firstAttentionNode{Role: role}
		if role == "data" {
			node.UnseenOpen = 2
		}
		wave.Nodes = append(wave.Nodes, node)
	}
	settlement.Waves = []firstAttentionWave{wave}
	settlement.TurnsUsed = 1
	report.Turns = append(report.Turns, turnSummary{Role: "data",
		Turn:       "episode-1-attention-debt-1-data",
		CapturedAt: "2026-08-04T01:00:51Z", HookCues: 1, AgentEnd: true})
	barrier := deliveryQuiescenceSummary{Phase: "episode-1-attention-debt-1",
		Status: "quiescent", Attempts: 1}
	for _, role := range domainRoles {
		barrier.Nodes = append(barrier.Nodes, deliveryNodeOccupancySummary{Role: role})
	}
	report.Protocol.DeliveryQuiescence = append(report.Protocol.DeliveryQuiescence, barrier)
	if err := validateReport(report); err != nil {
		t.Fatalf("validateReport() rejected first-attention evidence: %v", err)
	}

	report.Turns = report.Turns[:len(report.Turns)-1]
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted an attention wave without its target turn")
	}
	report = validReport()
	report.Protocol.FirstAttention[0].Final[0].UnseenOpen = 1
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted unpaid final attention debt")
	}
}

func TestFailureReportAndTraceRetainBoundedAttentionExhaustion(t *testing.T) {
	report := attentionExhaustionFailureReport()
	if err := validateFailureReport(report); err != nil {
		t.Fatalf("validateFailureReport() rejected bounded exhaustion: %v", err)
	}

	var output bytes.Buffer
	scenario := scenarioEvidence{Digest: agency.Sum([]byte("attention scenario")).String()}
	if err := writeFailureTrace(&output, report, scenario, nil); err != nil {
		t.Fatalf("writeFailureTrace() error = %v", err)
	}
	waveFacts, exhaustedFacts, gateEvidence := inspectAttentionTrace(t, output.String())
	if waveFacts != 15 || exhaustedFacts != len(domainRoles) || gateEvidence != 1+len(domainRoles) {
		t.Fatalf("attention trace facts = waves %d exhausted %d gate evidence %d",
			waveFacts, exhaustedFacts, gateEvidence)
	}

	report.FirstAttention = nil
	if err := validateFailureReport(report); err == nil {
		t.Fatal("validateFailureReport() accepted budget exhaustion without a final snapshot")
	}
	report = attentionExhaustionFailureReport()
	report.Failure.Code = "scenario.episode-2.attention-budget-exhausted"
	if err := validateFailureReport(report); err == nil {
		t.Fatal("validateFailureReport() accepted a mismatched attention episode")
	}
}

func attentionExhaustionFailureReport() failureReport {
	report := validFailureReport()
	report.Failure.Code = "scenario.episode-1.attention-budget-exhausted"
	settlement := &firstAttentionSettlement{Episode: "episode-1",
		Status: "budget_exhausted", TurnLimit: firstAttentionTurnLimit, TurnsUsed: 15}
	for wave := 1; wave <= 3; wave++ {
		settlement.Waves = append(settlement.Waves, attentionWave(wave))
		for _, role := range domainRoles {
			report.Turns = append(report.Turns, turnSummary{Role: role,
				Turn:       "episode-1-attention-debt-" + strconv.Itoa(wave) + "-" + role,
				CapturedAt: "2026-08-04T01:00:30Z", HookCues: 1, AgentEnd: true})
		}
	}
	settlement.Final = finalAttentionDebt()
	report.FirstAttention = settlement
	return report
}

func attentionWave(wave int) firstAttentionWave {
	value := firstAttentionWave{Wave: wave}
	for _, role := range domainRoles {
		value.Nodes = append(value.Nodes, firstAttentionNode{Role: role, UnseenOpen: 1})
	}
	return value
}

func finalAttentionDebt() []firstAttentionNode {
	nodes := make([]firstAttentionNode, 0, len(domainRoles))
	for _, role := range domainRoles {
		unseen := 0
		if role == "lead" || role == "payment" {
			unseen = 1
		}
		nodes = append(nodes, firstAttentionNode{Role: role, UnseenOpen: unseen})
	}
	return nodes
}

type attentionTraceRecord struct {
	Record string `json:"record"`
	Kind   string `json:"kind"`
	Facts  struct {
		Episode      string `json:"episode"`
		Role         string `json:"role"`
		UnseenOpen   *int   `json:"unseen_open"`
		ActiveClaims *int   `json:"active_claims"`
		TurnLimit    *int   `json:"turn_limit"`
		TurnsUsed    *int   `json:"turns_used"`
	} `json:"facts"`
	Gates []struct {
		ID       string   `json:"id"`
		Evidence []string `json:"evidence"`
	} `json:"gates"`
}

func inspectAttentionTrace(t *testing.T, output string) (int, int, int) {
	t.Helper()
	waveFacts, exhaustedFacts, gateEvidence := 0, 0, 0
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		var record attentionTraceRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		wave, exhausted := classifyAttentionTraceFact(t, record)
		waveFacts += wave
		exhaustedFacts += exhausted
		for _, gate := range record.Gates {
			if gate.ID == "scenario.run" {
				gateEvidence = len(gate.Evidence)
			}
		}
	}
	return waveFacts, exhaustedFacts, gateEvidence
}

func classifyAttentionTraceFact(t *testing.T, record attentionTraceRecord) (int, int) {
	t.Helper()
	if record.Kind != "test.attention.wave" && record.Kind != "test.attention.exhausted" {
		return 0, 0
	}
	if record.Facts.Episode != "episode-1" || record.Facts.Role == "" ||
		record.Facts.UnseenOpen == nil || record.Facts.ActiveClaims == nil ||
		record.Facts.TurnLimit == nil || record.Facts.TurnsUsed == nil {
		t.Fatal("attention Fact omitted closed numeric evidence")
	}
	if record.Kind == "test.attention.wave" {
		return 1, 0
	}
	return 0, 1
}
