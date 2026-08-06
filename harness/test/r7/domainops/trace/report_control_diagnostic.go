package main

import (
	"errors"
	"slices"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

type controlDiagnostic struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func validControlDiagnostic(diagnostic controlDiagnostic) bool {
	if diagnostic.Message == "" || len(diagnostic.Message) > agency.MaxDiagnosticBytes {
		return false
	}
	switch diagnostic.Code {
	case "invalid_argument", "content_required", "content_too_large", "artifact_invalid",
		"artifact_too_large", "authentication_failed", "context_required", "context_stale",
		"asset_revision_mismatch", "action_not_allowed", "operation_mismatch",
		"operation_pending", "mnemond_unavailable", "internal":
		return true
	default:
		return false
	}
}

func validateTurnSummary(turn turnSummary) error {
	values := []int{turn.HookCues, turn.BashCalls, turn.DelegateCalls, turn.CurrentReads,
		turn.SubmitAttempts, turn.IntentSubmits, turn.AcceptedReceipts, turn.RejectedReceipts,
		turn.SubmitDenials, turn.SubmitInvocationFailures, turn.PostAcceptDenials,
		turn.PrivateBindingProbes}
	if _, err := parseReportTime("turn captured_at", turn.CapturedAt); err != nil {
		return err
	}
	if !turn.AgentEnd || turn.HookCues < 1 || slices.ContainsFunc(values,
		func(value int) bool { return value < 0 || value > 256 }) {
		return errors.New("sanitized live report contains an invalid bounded turn")
	}
	if turn.PrivateBindingProbes != 0 || turn.DelegateCalls > 1 ||
		turn.CurrentReads > turn.BashCalls || turn.AcceptedReceipts > 1 ||
		turn.PostAcceptDenials > turn.SubmitDenials ||
		(turn.PostAcceptDenials > 0 && turn.AcceptedReceipts != 1) ||
		turn.IntentSubmits != turn.AcceptedReceipts+turn.RejectedReceipts ||
		turn.SubmitAttempts != turn.IntentSubmits+turn.SubmitDenials+turn.SubmitInvocationFailures {
		return errors.New("sanitized live report contains inconsistent successful CLI observations")
	}
	if len(turn.SubmitControlDiagnostics) > 32 {
		return errors.New("sanitized live report contains unbounded control diagnostics")
	}
	seenDiagnostics := make(map[controlDiagnostic]struct{}, len(turn.SubmitControlDiagnostics))
	for _, diagnostic := range turn.SubmitControlDiagnostics {
		if !validControlDiagnostic(diagnostic) {
			return errors.New("sanitized live report contains an invalid control diagnostic")
		}
		if _, exists := seenDiagnostics[diagnostic]; exists {
			return errors.New("sanitized live report repeats a control diagnostic")
		}
		seenDiagnostics[diagnostic] = struct{}{}
	}
	return nil
}
