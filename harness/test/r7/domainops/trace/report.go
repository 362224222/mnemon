package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

const maxReportBytes = 2 << 20

var domainRoles = []string{"data", "edge", "lead", "payment", "platform"}

type liveReport struct {
	Schema    string    `json:"schema"`
	Version   int       `json:"version"`
	Status    string    `json:"status"`
	Model     string    `json:"model"`
	Rounds    int       `json:"rounds"`
	Run       runReport `json:"run"`
	Isolation struct {
		Passed bool `json:"passed"`
	} `json:"isolation"`
	World struct {
		Baseline         loadSummary        `json:"baseline"`
		Recovery         loadSummary        `json:"recovery"`
		Stability        loadSummary        `json:"stability"`
		IncidentAfter    domainResult       `json:"incident_after"`
		IncidentCharges  domainChargeResult `json:"incident_charges"`
		RecoveryCharges  domainChargeResult `json:"recovery_charges"`
		StabilityCharges domainChargeResult `json:"stability_charges"`
	} `json:"world"`
	Protocol struct {
		AcceptedPeerEffects int                         `json:"accepted_peer_effects"`
		ByReceiver          []peerEffectSummary         `json:"by_receiver"`
		DeliveryQuiescence  []deliveryQuiescenceSummary `json:"delivery_quiescence"`
	} `json:"protocol"`
	Turns                      []turnSummary `json:"turns"`
	RawProviderStreamsRetained bool          `json:"raw_provider_streams_retained"`
}

type runReport struct {
	ID              string `json:"id"`
	StartedAt       string `json:"started_at"`
	FinishedAt      string `json:"finished_at"`
	CandidateDigest string `json:"candidate_digest"`
}

type turnSummary struct {
	Role                 string `json:"role"`
	Turn                 string `json:"turn"`
	CapturedAt           string `json:"captured_at"`
	HookCues             int    `json:"hook_cues"`
	BashCalls            int    `json:"bash_calls"`
	CurrentReads         int    `json:"current_reads"`
	SubmitAttempts       int    `json:"submit_attempts"`
	IntentSubmits        int    `json:"intent_submits"`
	AcceptedReceipts     int    `json:"accepted_receipts"`
	RejectedReceipts     int    `json:"rejected_receipts"`
	PrivateBindingProbes int    `json:"private_binding_probes"`
	AgentEnd             bool   `json:"agent_end"`
}

type peerEffectSummary struct {
	Role                string `json:"role"`
	AcceptedPeerEffects int    `json:"accepted_peer_effects"`
}

type deliveryQuiescenceSummary struct {
	Phase                  string                         `json:"phase"`
	Status                 string                         `json:"status"`
	Attempts               int                            `json:"attempts"`
	ElapsedSeconds         int                            `json:"elapsed_seconds"`
	PendingDeliveryRecords int                            `json:"pending_delivery_records"`
	Nodes                  []deliveryNodeOccupancySummary `json:"nodes"`
}

type deliveryNodeOccupancySummary struct {
	Role          string `json:"role"`
	PendingOutbox int    `json:"pending_outbox"`
	StagedInbox   int    `json:"staged_inbox"`
}

type loadSummary struct {
	Prefix       string           `json:"prefix"`
	Sent         int              `json:"sent"`
	Accepted     int              `json:"accepted"`
	Failed       int              `json:"failed"`
	Receipts     []serviceReceipt `json:"receipts"`
	ElapsedMS    int64            `json:"elapsed_ms"`
	Observed     monitorStatus    `json:"observed"`
	ObservedAtMS int64            `json:"observed_at_ms"`
}

type serviceReceipt struct {
	BusinessID string `json:"business_id"`
	CaptureID  int64  `json:"capture_id"`
}

type monitorStatus struct {
	Gateway gatewayStatus `json:"gateway"`
	Ledger  ledgerStatus  `json:"ledger"`
}

type gatewayStatus struct {
	Route     string `json:"route"`
	Requests  int64  `json:"requests"`
	Succeeded int64  `json:"succeeded"`
	Failed    int64  `json:"failed"`
}

type ledgerStatus struct {
	Charges             int `json:"charges"`
	ActiveCharges       int `json:"active_charges"`
	VoidedCharges       int `json:"voided_charges"`
	UniqueBusinesses    int `json:"unique_businesses"`
	DuplicateBusinesses int `json:"duplicate_businesses"`
}

type domainResult struct {
	Role   string       `json:"role"`
	Result ledgerStatus `json:"result"`
}

type domainChargeResult struct {
	Role   string         `json:"role"`
	Result []chargeRecord `json:"result"`
}

type chargeRecord struct {
	Sequence   int64  `json:"sequence"`
	BusinessID string `json:"business_id"`
	AttemptKey string `json:"attempt_key"`
	State      string `json:"state"`
	VoidReason string `json:"void_reason,omitempty"`
}

func loadReport(path string) (liveReport, error) {
	var report liveReport
	if err := readBoundedJSON(path, maxReportBytes, &report); err != nil {
		return liveReport{}, err
	}
	if err := validateReport(report); err != nil {
		return liveReport{}, err
	}
	return report, nil
}

func validateReport(report liveReport) error {
	if report.Schema != "mnemon.r7.domain-ops.live-report" || report.Version != 1 ||
		report.Status != "passed" || report.Model == "" || report.Rounds < 1 || report.Rounds > 8 ||
		report.RawProviderStreamsRetained || !report.Isolation.Passed {
		return errors.New("sanitized live report has invalid identity or terminal status")
	}
	if _, err := agency.NewOpaqueHandle(report.Model); err != nil {
		return fmt.Errorf("sanitized live report model: %w", err)
	}
	if _, err := agency.ParseDigest(report.Run.CandidateDigest); err != nil {
		return fmt.Errorf("sanitized live report candidate: %w", err)
	}
	startedAt, err := parseReportTime("started_at", report.Run.StartedAt)
	if err != nil {
		return err
	}
	finishedAt, err := parseReportTime("finished_at", report.Run.FinishedAt)
	if err != nil || finishedAt.Before(startedAt) {
		return errors.New("sanitized live report has invalid run interval")
	}
	if _, err := agency.NewOpaqueHandle(report.Run.ID); err != nil {
		return fmt.Errorf("sanitized live report run ID: %w", err)
	}
	if err := validateWorld(report); err != nil {
		return err
	}
	if err := validateProtocolSummary(report.Protocol.AcceptedPeerEffects,
		report.Protocol.ByReceiver); err != nil {
		return err
	}
	if err := validateDeliveryQuiescence(report.Rounds,
		report.Protocol.DeliveryQuiescence); err != nil {
		return err
	}
	return validateTurns(report.Rounds, report.Turns)
}

func validateWorld(report liveReport) error {
	baseline := report.World.Baseline
	if baseline.Sent != 4 || baseline.Accepted != 4 || baseline.Failed != 0 ||
		len(baseline.Receipts) != 4 || baseline.Observed.Ledger != (ledgerStatus{
		Charges: 8, ActiveCharges: 8, UniqueBusinesses: 4, DuplicateBusinesses: 4,
	}) {
		return errors.New("sanitized live report does not prove the initial incident")
	}
	if !validFreshSummary(report.World.Recovery, 6) ||
		!validFreshSummary(report.World.Stability, 6) {
		return errors.New("sanitized live report does not prove recovery and stability")
	}
	incident := report.World.IncidentAfter
	if incident.Role != "data" || incident.Result != (ledgerStatus{
		Charges: 8, ActiveCharges: 4, VoidedCharges: 4,
		UniqueBusinesses: 4, DuplicateBusinesses: 0,
	}) {
		return errors.New("sanitized live report does not prove historical reconciliation")
	}
	checks := []struct {
		summary loadSummary
		charges domainChargeResult
		copies  int
		voided  int
	}{
		{report.World.Baseline, report.World.IncidentCharges, 2, 1},
		{report.World.Recovery, report.World.RecoveryCharges, 1, 0},
		{report.World.Stability, report.World.StabilityCharges, 1, 0},
	}
	for _, check := range checks {
		if err := validateServiceReceiptEvidence(check.summary, check.charges,
			check.copies, check.voided); err != nil {
			return err
		}
	}
	return nil
}

func validFreshSummary(summary loadSummary, count int) bool {
	if summary.Sent != count || summary.Accepted != count || summary.Failed != 0 ||
		len(summary.Receipts) != count || summary.Observed.Ledger != (ledgerStatus{
		Charges: count, ActiveCharges: count, UniqueBusinesses: count,
	}) {
		return false
	}
	seen := make(map[string]struct{}, count)
	for _, receipt := range summary.Receipts {
		if receipt.BusinessID == "" || receipt.CaptureID <= 0 {
			return false
		}
		if _, duplicate := seen[receipt.BusinessID]; duplicate {
			return false
		}
		seen[receipt.BusinessID] = struct{}{}
	}
	return true
}

func validateProtocolSummary(total int, values []peerEffectSummary) error {
	if total < 1 || len(values) != len(domainRoles) {
		return errors.New("sanitized live report has no complete peer-effect summary")
	}
	byRole := make(map[string]int, len(values))
	sum := 0
	for _, value := range values {
		if value.AcceptedPeerEffects < 0 {
			return errors.New("sanitized live report has a negative peer-effect count")
		}
		if _, duplicate := byRole[value.Role]; duplicate {
			return errors.New("sanitized live report repeats a peer-effect receiver")
		}
		byRole[value.Role] = value.AcceptedPeerEffects
		sum += value.AcceptedPeerEffects
	}
	if sum != total {
		return errors.New("sanitized live report peer-effect total is inconsistent")
	}
	for _, role := range domainRoles {
		if _, exists := byRole[role]; !exists {
			return errors.New("sanitized live report omits a peer-effect receiver")
		}
	}
	return nil
}

func validateDeliveryQuiescence(rounds int, values []deliveryQuiescenceSummary) error {
	want := make(map[string]struct{}, rounds+1)
	want["initial-lead"] = struct{}{}
	for round := 1; round <= rounds; round++ {
		want[fmt.Sprintf("round-%d", round)] = struct{}{}
	}
	if len(values) != len(want) {
		return errors.New("sanitized live report has an incomplete delivery barrier summary")
	}
	for _, value := range values {
		if _, exists := want[value.Phase]; !exists || value.Status != "quiescent" ||
			value.Attempts < 1 || value.Attempts > 256 || value.ElapsedSeconds < 0 ||
			value.ElapsedSeconds > 30 || value.PendingDeliveryRecords != 0 ||
			len(value.Nodes) != len(domainRoles) {
			return errors.New("sanitized live report contains an invalid delivery barrier")
		}
		delete(want, value.Phase)
		seen := make(map[string]struct{}, len(value.Nodes))
		for _, node := range value.Nodes {
			if !slices.Contains(domainRoles, node.Role) || node.PendingOutbox != 0 ||
				node.StagedInbox != 0 {
				return errors.New("sanitized live report contains non-quiescent peer occupancy")
			}
			if _, duplicate := seen[node.Role]; duplicate {
				return errors.New("sanitized live report repeats a delivery barrier node")
			}
			seen[node.Role] = struct{}{}
		}
	}
	return nil
}

func validateTurns(rounds int, turns []turnSummary) error {
	expected := map[string]string{"initial-lead": "lead"}
	for round := 1; round <= rounds; round++ {
		for _, role := range domainRoles {
			expected[fmt.Sprintf("round-%d-%s", round, role)] = role
		}
	}
	if len(turns) != len(expected) {
		return fmt.Errorf("sanitized live report has %d turns, want %d", len(turns), len(expected))
	}
	for _, turn := range turns {
		role, exists := expected[turn.Turn]
		if !exists || role != turn.Role {
			return errors.New("sanitized live report contains an unknown role/turn pair")
		}
		delete(expected, turn.Turn)
		values := []int{turn.HookCues, turn.BashCalls, turn.CurrentReads, turn.SubmitAttempts,
			turn.IntentSubmits,
			turn.AcceptedReceipts, turn.RejectedReceipts, turn.PrivateBindingProbes}
		if _, err := parseReportTime("turn captured_at", turn.CapturedAt); err != nil {
			return err
		}
		if !turn.AgentEnd || turn.HookCues < 1 || slices.ContainsFunc(values,
			func(value int) bool { return value < 0 || value > 256 }) {
			return errors.New("sanitized live report contains an invalid bounded turn")
		}
		if turn.PrivateBindingProbes != 0 || turn.CurrentReads > turn.BashCalls ||
			turn.SubmitAttempts > 1 || turn.SubmitAttempts > turn.BashCalls ||
			turn.IntentSubmits != turn.SubmitAttempts ||
			turn.IntentSubmits != turn.AcceptedReceipts+turn.RejectedReceipts {
			return errors.New("sanitized live report contains inconsistent successful CLI observations")
		}
	}
	if len(expected) != 0 {
		return errors.New("sanitized live report omits an expected attention turn")
	}
	return nil
}

func parseReportTime(label, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || value != parsed.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, fmt.Errorf("sanitized live report %s is not canonical UTC RFC3339Nano", label)
	}
	return parsed.UTC(), nil
}

func readBoundedJSON(path string, maximum int64, destination any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open sanitized report: %w", err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return fmt.Errorf("read sanitized report: %w", err)
	}
	if int64(len(raw)) > maximum {
		return fmt.Errorf("sanitized report exceeds %d bytes", maximum)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode sanitized report: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("sanitized report has trailing data")
	}
	return nil
}
