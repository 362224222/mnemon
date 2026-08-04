package main

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/test/observer"
)

type failureReport struct {
	Schema  string    `json:"schema"`
	Version int       `json:"version"`
	Status  string    `json:"status"`
	Model   string    `json:"model"`
	Run     runReport `json:"run"`
	Failure struct {
		Code       string `json:"code"`
		ObservedAt string `json:"observed_at"`
	} `json:"failure"`
	Turns                      []turnSummary `json:"turns"`
	RawProviderStreamsRetained bool          `json:"raw_provider_streams_retained"`
}

func loadFailureReport(path string) (failureReport, error) {
	var report failureReport
	if err := readBoundedJSON(path, maxReportBytes, &report); err != nil {
		return failureReport{}, err
	}
	if err := validateFailureReport(report); err != nil {
		return failureReport{}, err
	}
	return report, nil
}

func validateFailureReport(report failureReport) error {
	if report.Schema != "mnemon.r7.domain-ops.failure-report" || report.Version != 1 ||
		report.Status != "failed" || report.RawProviderStreamsRetained ||
		report.Model == "" || report.Failure.Code == "" {
		return errors.New("sanitized failure report has invalid identity or status")
	}
	for label, value := range map[string]string{"model": report.Model,
		"run ID": report.Run.ID, "failure code": report.Failure.Code} {
		if _, err := agency.NewOpaqueHandle(value); err != nil {
			return fmt.Errorf("sanitized failure report %s: %w", label, err)
		}
	}
	if _, err := agency.ParseDigest(report.Run.CandidateDigest); err != nil {
		return fmt.Errorf("sanitized failure report candidate: %w", err)
	}
	startedAt, err := parseReportTime("started_at", report.Run.StartedAt)
	if err != nil {
		return err
	}
	finishedAt, err := parseReportTime("finished_at", report.Run.FinishedAt)
	if err != nil || finishedAt.Before(startedAt) {
		return errors.New("sanitized failure report has invalid run interval")
	}
	observedAt, err := parseReportTime("failure observed_at", report.Failure.ObservedAt)
	if err != nil || observedAt.Before(startedAt) || observedAt.After(finishedAt) {
		return errors.New("sanitized failure report has invalid failure time")
	}
	return validateCompletedTurnSubset(report.Turns)
}

func validateCompletedTurnSubset(turns []turnSummary) error {
	if len(turns) > 1+8*len(domainRoles) {
		return errors.New("sanitized failure report contains too many completed turns")
	}
	seen := make(map[string]struct{}, len(turns))
	for _, turn := range turns {
		if !slices.Contains(domainRoles, turn.Role) || turn.Turn == "" {
			return errors.New("sanitized failure report contains an unknown role or turn")
		}
		if _, duplicate := seen[turn.Turn]; duplicate {
			return errors.New("sanitized failure report repeats a turn")
		}
		seen[turn.Turn] = struct{}{}
		values := []int{turn.HookCues, turn.BashCalls, turn.CurrentReads, turn.SubmitAttempts,
			turn.IntentSubmits,
			turn.AcceptedReceipts, turn.RejectedReceipts, turn.SubmitDenials,
			turn.PostAcceptDenials,
			turn.PrivateBindingProbes}
		if _, err := parseReportTime("turn captured_at", turn.CapturedAt); err != nil {
			return err
		}
		if !turn.AgentEnd || turn.HookCues < 1 || slices.ContainsFunc(values,
			func(value int) bool { return value < 0 || value > 256 }) ||
			turn.PrivateBindingProbes != 0 || turn.CurrentReads > turn.BashCalls ||
			turn.AcceptedReceipts > 1 ||
			turn.PostAcceptDenials > turn.SubmitDenials ||
			(turn.PostAcceptDenials > 0 && turn.AcceptedReceipts != 1) ||
			turn.IntentSubmits != turn.AcceptedReceipts+turn.RejectedReceipts ||
			turn.SubmitAttempts != turn.IntentSubmits+turn.SubmitDenials {
			return errors.New("sanitized failure report contains an invalid completed turn")
		}
	}
	return nil
}

func writeFailureTrace(destination io.Writer, report failureReport,
	scenario scenarioEvidence, nodes []nodeEvidence,
) error {
	startedAt, _ := parseReportTime("started_at", report.Run.StartedAt)
	finishedAt, _ := parseReportTime("finished_at", report.Run.FinishedAt)
	observedAt, _ := parseReportTime("failure observed_at", report.Failure.ObservedAt)
	participants := make([]observer.Participant, 0, len(domainRoles))
	for _, role := range domainRoles {
		participants = append(participants, observer.Participant{Node: role, Agent: role,
			Runtime: "pi", Model: report.Model})
	}
	writer, err := observer.NewWriter(destination, observer.Run{ID: report.Run.ID,
		Scenario:  observer.Scenario{ID: scenarioID, Digest: scenario.Digest},
		StartedAt: startedAt, CandidateDigest: report.Run.CandidateDigest,
		Participants: participants})
	if err != nil {
		return err
	}
	if err := appendRuntimeFacts(writer, report.Turns); err != nil {
		return err
	}
	if _, err := appendArtifactFacts(writer, nodes); err != nil {
		return err
	}
	eventFacts, ordered, err := appendEventFacts(writer, nodes)
	if err != nil {
		return err
	}
	if err := appendDomainEffectFacts(writer, nodes, eventFacts, ordered); err != nil {
		return err
	}
	if _, err := appendReceiptFacts(writer, nodes, eventFacts); err != nil {
		return err
	}
	if _, err := appendDeliveryFacts(writer, nodes, eventFacts); err != nil {
		return err
	}
	return finishFailedTrace(writer, report.Failure.Code, observedAt, finishedAt)
}

func finishFailedTrace(writer *observer.Writer, code string, observedAt, finishedAt time.Time) error {
	failureFact := hashedFactID("failed-run", code)
	if _, err := writer.Append(observer.Fact{ID: failureFact, CapturedAt: observedAt,
		Source: observer.Source{Class: observer.SourceOracle, Node: "runner"},
		Kind:   "test.gate.checked", Truth: observer.TruthAssertion,
		Fields: observer.FactFields{GateID: "scenario.run", Status: "fail", Code: code}}); err != nil {
		return err
	}
	gates := []observer.Gate{{ID: "scenario.run", Status: observer.GateFail,
		Evidence: []string{failureFact}}}
	for _, gate := range []string{"scenario.recovery", "scenario.service-receipts",
		"r7.operation-receipts", "r7.peer-accepted-effect", "r7.delivery-quiescence",
		"scenario.isolation"} {
		gates = append(gates, observer.Gate{ID: gate, Status: observer.GateUnknown})
	}
	gates = append(gates, observer.Gate{ID: "r8.applicability",
		Status: observer.GateNotApplicable})
	return writer.Finish(observer.Result{Status: observer.ResultFailed,
		FinishedAt: finishedAt, Gates: gates})
}
