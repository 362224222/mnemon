package main

import "testing"

func TestValidateReportRejectsBrokenServiceReceiptCorrespondence(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*liveReport)
	}{
		{"wrong retained capture", func(report *liveReport) {
			report.World.Recovery.Receipts[0].CaptureID++
		}},
		{"hidden extra charge", func(report *liveReport) {
			report.World.StabilityCharges.Result = append(report.World.StabilityCharges.Result,
				report.World.StabilityCharges.Result[0])
		}},
		{"duplicate sequence", func(report *liveReport) {
			report.World.RecoveryCharges.Result[1].Sequence =
				report.World.RecoveryCharges.Result[0].Sequence
		}},
		{"missing void reason", func(report *liveReport) {
			for index := range report.World.IncidentCharges.Result {
				if report.World.IncidentCharges.Result[index].State == "voided" {
					report.World.IncidentCharges.Result[index].VoidReason = ""
					return
				}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := validReport()
			test.mutate(&report)
			if err := validateReport(report); err == nil {
				t.Fatal("validateReport() accepted broken service-receipt evidence")
			}
		})
	}
}

func TestValidateReportRejectsNonQuiescentDeliveryBarrier(t *testing.T) {
	report := validReport()
	report.Protocol.DeliveryQuiescence[0].PendingDeliveryRecords = 1
	report.Protocol.DeliveryQuiescence[0].Nodes[0].PendingOutbox = 1
	if err := validateReport(report); err == nil {
		t.Fatal("validateReport() accepted a non-quiescent delivery barrier")
	}
}
