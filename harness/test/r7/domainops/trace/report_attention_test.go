package main

import "testing"

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
