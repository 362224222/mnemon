package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/spf13/cobra"
)

const controlShortObserveMaxAttempts = 4

var (
	controlTeamworkSignalID          string
	controlTeamworkSignalScope       string
	controlTeamworkSignalUrgency     string
	controlTeamworkSignalTTL         string
	controlTeamworkSignalStatement   string
	controlTeamworkSignalWhy         string
	controlTeamworkSignalNeeded      []string
	controlTeamworkSignalEvidence    []string
	controlTeamworkSignalContextRefs []string

	controlTeamworkAssignID          string
	controlTeamworkAssignSignalRef   string
	controlTeamworkAssignAssignee    string
	controlTeamworkAssignScope       string
	controlTeamworkAssignTTL         string
	controlTeamworkAssignReportOn    []string
	controlTeamworkAssignWork        string
	controlTeamworkAssignFeedback    string
	controlTeamworkAssignRationale   string
	controlTeamworkAssignEvidence    []string
	controlTeamworkAssignContextRefs []string

	controlTeamworkProgressAssignmentRef string
	controlTeamworkProgressScope         string
	controlTeamworkProgressFeedbackKind  string
	controlTeamworkProgressSummary       string
	controlTeamworkProgressBlocker       string
	controlTeamworkProgressResult        string
	controlTeamworkProgressChanged       []string
	controlTeamworkProgressSuggestedNext string
	controlTeamworkProgressEvidence      []string
	controlTeamworkProgressArtifacts     []string

	controlProfileAvailability   string
	controlProfileFreshness      string
	controlProfileTTL            string
	controlProfileFocus          string
	controlProfileAdvantages     []string
	controlProfileConstraints    []string
	controlProfileSummary        string
	controlProfileActiveScopes   []string
	controlProfileRecentEvidence []string
)

var controlTeamworkCmd = &cobra.Command{
	Use:   "teamwork",
	Short: "Emit short R2 teamwork event drafts through the channel",
}

var controlTeamworkSignalCmd = &cobra.Command{
	Use:   "signal",
	Short: "Emit a teamwork_signal event without hand-writing nested JSON",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireShortFields(map[string]string{
			"--scope":        controlTeamworkSignalScope,
			"--statement":    controlTeamworkSignalStatement,
			"--why-teamwork": controlTeamworkSignalWhy,
			"--ttl":          controlTeamworkSignalTTL,
		}); err != nil {
			return err
		}
		evidence := cleanStrings(controlTeamworkSignalEvidence)
		if len(evidence) == 0 {
			return fmt.Errorf("teamwork signal requires at least one --evidence")
		}
		rule := map[string]any{
			"scope": controlTeamworkSignalScope,
			"ttl":   controlTeamworkSignalTTL,
		}
		putString(rule, "signal_id", controlTeamworkSignalID)
		putString(rule, "urgency", controlTeamworkSignalUrgency)
		narrative := map[string]any{
			"statement":      controlTeamworkSignalStatement,
			"why_teamwork":   controlTeamworkSignalWhy,
			"needed_context": cleanStrings(controlTeamworkSignalNeeded),
		}
		refs := map[string]any{
			"evidence_refs": evidence,
			"context_refs":  cleanStrings(controlTeamworkSignalContextRefs),
		}
		return controlShortObserve(cmd, "teamwork_signal.write_candidate.observed", "teamwork-signal", eventmodel.BuildPayload(rule, narrative, refs))
	},
}

var controlTeamworkAssignCmd = &cobra.Command{
	Use:   "assign",
	Short: "Emit an assignment event without hand-writing nested JSON",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireShortFields(map[string]string{
			"--assignee": controlTeamworkAssignAssignee,
			"--scope":    controlTeamworkAssignScope,
			"--work":     controlTeamworkAssignWork,
			"--feedback": controlTeamworkAssignFeedback,
			"--ttl":      controlTeamworkAssignTTL,
		}); err != nil {
			return err
		}
		evidence := cleanStrings(controlTeamworkAssignEvidence)
		if len(evidence) == 0 {
			return fmt.Errorf("teamwork assign requires at least one --evidence")
		}
		rule := map[string]any{
			"assignee": controlTeamworkAssignAssignee,
			"scope":    controlTeamworkAssignScope,
			"ttl":      controlTeamworkAssignTTL,
		}
		assignmentID := strings.TrimSpace(controlTeamworkAssignID)
		if assignmentID == "" {
			assignmentID = defaultShortAssignmentID(controlTeamworkAssignScope, controlTeamworkAssignAssignee, controlTeamworkAssignReportOn, controlTeamworkAssignWork)
		}
		putString(rule, "assignment_id", assignmentID)
		putString(rule, "signal_ref", controlTeamworkAssignSignalRef)
		putStrings(rule, "report_on", controlTeamworkAssignReportOn)
		narrative := map[string]any{
			"expected_work":     controlTeamworkAssignWork,
			"expected_feedback": controlTeamworkAssignFeedback,
		}
		putString(narrative, "rationale", controlTeamworkAssignRationale)
		refs := map[string]any{
			"evidence_refs": evidence,
			"context_refs":  cleanStrings(controlTeamworkAssignContextRefs),
		}
		return controlShortObserve(cmd, "assignment.write_candidate.observed", "assignment", eventmodel.BuildPayload(rule, narrative, refs))
	},
}

var controlTeamworkProgressCmd = &cobra.Command{
	Use:   "progress",
	Short: "Emit a progress_digest event without hand-writing nested JSON",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireShortFields(map[string]string{
			"--feedback-kind": controlTeamworkProgressFeedbackKind,
			"--summary":       controlTeamworkProgressSummary,
		}); err != nil {
			return err
		}
		rule := map[string]any{
			"feedback_kind": controlTeamworkProgressFeedbackKind,
		}
		putString(rule, "assignment_ref", controlTeamworkProgressAssignmentRef)
		putString(rule, "scope", controlTeamworkProgressScope)
		narrative := map[string]any{
			"summary": controlTeamworkProgressSummary,
		}
		putString(narrative, "blocker", controlTeamworkProgressBlocker)
		putString(narrative, "result", controlTeamworkProgressResult)
		putStrings(narrative, "changed_context", controlTeamworkProgressChanged)
		putString(narrative, "suggested_next", controlTeamworkProgressSuggestedNext)
		refs := map[string]any{}
		putStrings(refs, "evidence_refs", controlTeamworkProgressEvidence)
		putStrings(refs, "artifact_refs", controlTeamworkProgressArtifacts)
		return controlShortObserve(cmd, "progress_digest.write_candidate.observed", "progress-digest", eventmodel.BuildPayload(rule, narrative, refs))
	},
}

var controlProfileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Emit short R2 profile event drafts through the channel",
}

var controlProfileUpdateCmd = &cobra.Command{
	Use:   "update",
	Short: "Emit an agent_profile event without hand-writing nested JSON",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := requireShortFields(map[string]string{
			"--principal":    controlPrincipal,
			"--availability": controlProfileAvailability,
			"--ttl":          controlProfileTTL,
			"--focus":        controlProfileFocus,
			"--summary":      controlProfileSummary,
		}); err != nil {
			return err
		}
		advantages := cleanStrings(controlProfileAdvantages)
		if len(advantages) == 0 {
			return fmt.Errorf("profile update requires at least one --advantage")
		}
		rule := map[string]any{
			"actor":        controlPrincipal,
			"availability": controlProfileAvailability,
			"ttl":          controlProfileTTL,
		}
		putString(rule, "freshness", controlProfileFreshness)
		narrative := map[string]any{
			"focus":              controlProfileFocus,
			"context_advantages": advantages,
			"summary":            controlProfileSummary,
		}
		putStrings(narrative, "constraints", controlProfileConstraints)
		refs := map[string]any{}
		putStrings(refs, "active_scopes", controlProfileActiveScopes)
		putStrings(refs, "recent_evidence", controlProfileRecentEvidence)
		return controlShortObserve(cmd, "agent_profile.write_candidate.observed", "agent-profile", eventmodel.BuildPayload(rule, narrative, refs))
	},
}

func controlShortObserveCommands() []*cobra.Command {
	return []*cobra.Command{
		controlTeamworkSignalCmd,
		controlTeamworkAssignCmd,
		controlTeamworkProgressCmd,
		controlProfileUpdateCmd,
	}
}

func controlShortObserve(cmd *cobra.Command, eventType, fallbackIDPrefix string, payload map[string]any) error {
	client, err := controlClient()
	if err != nil {
		return err
	}
	return withControlShortObserveLock(func() error {
		return controlShortObserveLocked(cmd, client, eventType, fallbackIDPrefix, payload)
	})
}

func controlShortObserveLocked(cmd *cobra.Command, client *access.Client, eventType, fallbackIDPrefix string, payload map[string]any) error {
	externalID := shortExternalID(fallbackIDPrefix)
	for attempt := 0; attempt < controlShortObserveMaxAttempts; attempt++ {
		attemptExternalID := retryExternalID(externalID, attempt)
		rec, err := client.IngestObserve(contract.ActorID(controlPrincipal), contract.ObservationEnvelope{
			ExternalID: attemptExternalID,
			Event:      contract.Event{Type: eventType, Payload: payload},
		})
		if err != nil {
			return fmt.Errorf("channel observe failed (service unreachable or rejected): %w", err)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "observed seq=%d dup=%v ticked=%v\n", rec.Seq, rec.Dup, rec.Ticked)
		if rec.ProcessingError == "" {
			return nil
		}
		fmt.Fprintf(cmd.OutOrStdout(), "processing error: %s\n", rec.ProcessingError)
		if !retryableShortObserveProcessingError(rec.ProcessingError) || attempt == controlShortObserveMaxAttempts-1 {
			return fmt.Errorf("channel observe processing failed: %s", rec.ProcessingError)
		}
		nextExternalID := retryExternalID(externalID, attempt+1)
		fmt.Fprintf(cmd.OutOrStdout(), "retrying after processing error: attempt=%d external_id=%s\n", attempt+2, nextExternalID)
		time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
	}
	return fmt.Errorf("channel observe processing failed after %d attempts", controlShortObserveMaxAttempts)
}

func withControlShortObserveLock(fn func() error) error {
	lockName := sanitizeAssignmentToken(controlAddr)
	if lockName == "" {
		lockName = "default"
	}
	lockPath := filepath.Join(os.TempDir(), "mnemon-control-short-"+lockName+".lock")
	for attempt := 0; attempt < 200; attempt++ {
		f, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
			_ = f.Close()
			defer os.Remove(lockPath)
			return fn()
		}
		if !os.IsExist(err) {
			return fmt.Errorf("create short observe lock: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for short observe lock %s", lockPath)
}

func retryExternalID(externalID string, attempt int) string {
	if attempt <= 0 {
		return externalID
	}
	return fmt.Sprintf("%s-retry-%d", externalID, attempt)
}

func retryableShortObserveProcessingError(msg string) bool {
	msg = strings.ToLower(strings.TrimSpace(msg))
	if msg == "" {
		return false
	}
	for _, marker := range []string{
		"read_stale",
		"read stale",
		"stale read",
		"version conflict",
		"resource version",
		"optimistic",
		"conflict",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func shortExternalID(prefix string) string {
	if id := strings.TrimSpace(controlExtID); id != "" {
		return id
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "event"
	}
	return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
}

func defaultShortAssignmentID(scope, assignee string, reportOn []string, work string) string {
	base := issueToken(scope, work)
	if base == "" {
		base = sanitizeAssignmentToken(strings.TrimSuffix(assignee, "@team"))
	}
	if base == "" {
		base = "work"
	}
	topic := assignmentTopic(reportOn, work)
	if topic == "" {
		topic = "task"
	}
	return "assignment-" + base + "-" + topic
}

func issueToken(values ...string) string {
	for _, value := range values {
		lower := strings.ToLower(value)
		for pos := 0; pos < len(lower); {
			idx := strings.Index(lower[pos:], "tea-")
			if idx < 0 {
				break
			}
			idx += pos
			start := idx + len("tea-")
			end := start
			for end < len(lower) && lower[end] >= '0' && lower[end] <= '9' {
				end++
			}
			if end > start {
				return "tea" + lower[start:end]
			}
			pos = idx + 1
		}
	}
	return ""
}

func assignmentTopic(reportOn []string, work string) string {
	combined := strings.ToLower(strings.Join(reportOn, " ") + " " + work)
	switch {
	case strings.Contains(combined, "root") || strings.Contains(combined, "metadata") || strings.Contains(combined, "run visibility"):
		return "root-runtime"
	case strings.Contains(combined, "routing") || strings.Contains(combined, "isolation"):
		return "routing-isolation"
	case strings.Contains(combined, "feedback") || strings.Contains(combined, "status") || strings.Contains(combined, "completion"):
		return "feedback-status"
	default:
		return sanitizeAssignmentToken(combined)
	}
}

func sanitizeAssignmentToken(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	words := 0
	inWord := false
	for _, r := range value {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			if !inWord {
				words++
				inWord = true
			}
			if words > 4 {
				break
			}
			b.WriteRune(r)
			lastDash = false
			continue
		}
		inWord = false
		if b.Len() > 0 && !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func requireShortFields(fields map[string]string) error {
	for name, value := range fields {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	return nil
}

func cleanStrings(values []string) []string {
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func putString(m map[string]any, key, value string) {
	if value = strings.TrimSpace(value); value != "" {
		m[key] = value
	}
}

func putStrings(m map[string]any, key string, values []string) {
	if cleaned := cleanStrings(values); len(cleaned) > 0 {
		m[key] = cleaned
	}
}

func init() {
	controlTeamworkSignalCmd.Flags().StringVar(&controlExtID, "external-id", "", "idempotency external id")
	controlTeamworkSignalCmd.Flags().StringVar(&controlTeamworkSignalID, "signal-id", "", "optional signal id")
	controlTeamworkSignalCmd.Flags().StringVar(&controlTeamworkSignalScope, "scope", "", "teamwork scope")
	controlTeamworkSignalCmd.Flags().StringVar(&controlTeamworkSignalUrgency, "urgency", "normal", "signal urgency: low, normal, or high")
	controlTeamworkSignalCmd.Flags().StringVar(&controlTeamworkSignalTTL, "ttl", "30m", "signal TTL")
	controlTeamworkSignalCmd.Flags().StringVar(&controlTeamworkSignalStatement, "statement", "", "natural-language teamwork need")
	controlTeamworkSignalCmd.Flags().StringVar(&controlTeamworkSignalWhy, "why-teamwork", "", "why this needs teamwork")
	controlTeamworkSignalCmd.Flags().StringArrayVar(&controlTeamworkSignalNeeded, "needed-context", nil, "needed context; may be repeated")
	controlTeamworkSignalCmd.Flags().StringArrayVar(&controlTeamworkSignalEvidence, "evidence", nil, "evidence reference; may be repeated")
	controlTeamworkSignalCmd.Flags().StringArrayVar(&controlTeamworkSignalContextRefs, "context-ref", nil, "context reference; may be repeated")

	controlTeamworkAssignCmd.Flags().StringVar(&controlExtID, "external-id", "", "idempotency external id")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignID, "assignment-id", "", "optional assignment id")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignSignalRef, "signal-ref", "", "source teamwork_signal id")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignAssignee, "assignee", "", "assignee principal")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignScope, "scope", "", "assignment scope")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignTTL, "ttl", "20m", "assignment TTL")
	controlTeamworkAssignCmd.Flags().StringArrayVar(&controlTeamworkAssignReportOn, "report-on", nil, "field or concern to report on; may be repeated")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignWork, "work", "", "natural-language expected work")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignFeedback, "feedback", "progress_digest with result or blocker", "expected feedback")
	controlTeamworkAssignCmd.Flags().StringVar(&controlTeamworkAssignRationale, "rationale", "", "assignment rationale")
	controlTeamworkAssignCmd.Flags().StringArrayVar(&controlTeamworkAssignEvidence, "evidence", nil, "evidence reference; may be repeated")
	controlTeamworkAssignCmd.Flags().StringArrayVar(&controlTeamworkAssignContextRefs, "context-ref", nil, "context reference; may be repeated")

	controlTeamworkProgressCmd.Flags().StringVar(&controlExtID, "external-id", "", "idempotency external id")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressAssignmentRef, "assignment-ref", "", "assignment id this progress reports on")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressScope, "scope", "", "progress scope")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressFeedbackKind, "feedback-kind", "progress", "progress, result, or blocker")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressSummary, "summary", "", "natural-language progress summary")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressBlocker, "blocker", "", "blocker details")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressResult, "result", "", "result details")
	controlTeamworkProgressCmd.Flags().StringArrayVar(&controlTeamworkProgressChanged, "changed-context", nil, "changed context; may be repeated")
	controlTeamworkProgressCmd.Flags().StringVar(&controlTeamworkProgressSuggestedNext, "suggested-next", "", "suggested next action")
	controlTeamworkProgressCmd.Flags().StringArrayVar(&controlTeamworkProgressEvidence, "evidence", nil, "evidence reference; may be repeated")
	controlTeamworkProgressCmd.Flags().StringArrayVar(&controlTeamworkProgressArtifacts, "artifact", nil, "artifact reference; may be repeated")

	controlProfileUpdateCmd.Flags().StringVar(&controlExtID, "external-id", "", "idempotency external id")
	controlProfileUpdateCmd.Flags().StringVar(&controlProfileAvailability, "availability", "available", "available, busy, blocked, or unknown")
	controlProfileUpdateCmd.Flags().StringVar(&controlProfileFreshness, "freshness", "fresh", "freshness marker")
	controlProfileUpdateCmd.Flags().StringVar(&controlProfileTTL, "ttl", "30m", "profile TTL")
	controlProfileUpdateCmd.Flags().StringVar(&controlProfileFocus, "focus", "", "current focus")
	controlProfileUpdateCmd.Flags().StringArrayVar(&controlProfileAdvantages, "advantage", nil, "context advantage; may be repeated")
	controlProfileUpdateCmd.Flags().StringArrayVar(&controlProfileConstraints, "constraint", nil, "constraint; may be repeated")
	controlProfileUpdateCmd.Flags().StringVar(&controlProfileSummary, "summary", "", "profile summary")
	controlProfileUpdateCmd.Flags().StringArrayVar(&controlProfileActiveScopes, "active-scope", nil, "active scope; may be repeated")
	controlProfileUpdateCmd.Flags().StringArrayVar(&controlProfileRecentEvidence, "recent-evidence", nil, "recent evidence; may be repeated")

	controlTeamworkCmd.AddCommand(controlTeamworkSignalCmd, controlTeamworkAssignCmd, controlTeamworkProgressCmd)
	controlProfileCmd.AddCommand(controlProfileUpdateCmd)
}
