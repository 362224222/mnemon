package observer

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	traceSchema   = "mnemon.test.trace"
	traceVersion  = 1
	maxTraceLine  = 16 << 10
	maxTraceFacts = 100000
)

var (
	tokenPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
	digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	tracePattern  = regexp.MustCompile(`^trace:[A-Za-z0-9][A-Za-z0-9._:-]{0,121}$`)
	sourceClasses = []string{"runtime", "r7_authority", "transport", "r8_selector", "oracle", "runner"}
	truthClasses  = []string{"observation", "accepted_local_fact", "derived_projection", "local_preference", "assertion"}
)

func validateWriterRun(run Run) (string, error) {
	if !validTraceToken(run.ID) || !validTraceToken(run.Scenario.ID) ||
		!digestPattern.MatchString(run.Scenario.Digest) {
		return "", fmt.Errorf("trace writer: invalid run or scenario identity")
	}
	if run.CandidateDigest != "" && !digestPattern.MatchString(run.CandidateDigest) {
		return "", fmt.Errorf("trace writer: invalid candidate digest")
	}
	if len(run.Participants) > 32 {
		return "", fmt.Errorf("trace writer: participants exceed 32")
	}
	for _, participant := range run.Participants {
		if !validTraceToken(participant.Node) || !validOptionalTokens(
			participant.Agent, participant.Runtime, participant.Model) {
			return "", fmt.Errorf("trace writer: invalid participant metadata")
		}
	}
	return canonicalTime("run started_at", run.StartedAt)
}

func (writer *Writer) validateFact(fact Fact, sequence int) (string, error) {
	if !tracePattern.MatchString(fact.ID) {
		return "", fmt.Errorf("trace writer: fact %d has invalid id", sequence)
	}
	if _, duplicate := writer.seen[fact.ID]; duplicate {
		return "", fmt.Errorf("trace writer: fact %d repeats id %q", sequence, fact.ID)
	}
	if !validOptionalTokens(fact.Agent, fact.Turn) || !validTraceToken(fact.Source.Node) {
		return "", fmt.Errorf("trace writer: fact %d has invalid source or runtime metadata", sequence)
	}
	classification, known := factClassifications[fact.Kind]
	if !known || classification.source != string(fact.Source.Class) ||
		classification.truth != string(fact.Truth) {
		return "", fmt.Errorf("trace writer: fact %d has invalid kind/source/truth classification", sequence)
	}
	if strings.HasPrefix(fact.Kind, "r8.") && fact.References.Selection == "" {
		return "", fmt.Errorf("trace writer: fact %d has no SelectionID", sequence)
	}
	if err := writer.validateCauses(fact.Causes, sequence); err != nil {
		return "", err
	}
	if err := validateReferences(fact.References, sequence); err != nil {
		return "", err
	}
	if err := validateFactFields(fact.Fields, sequence); err != nil {
		return "", err
	}
	return canonicalTime(fmt.Sprintf("fact %d captured_at", sequence), fact.CapturedAt)
}

func (writer *Writer) validateCauses(causes []string, sequence int) error {
	if len(causes) > 16 {
		return fmt.Errorf("trace writer: fact %d causes exceed 16", sequence)
	}
	unique := make(map[string]struct{}, len(causes))
	for _, cause := range causes {
		if !tracePattern.MatchString(cause) {
			return fmt.Errorf("trace writer: fact %d has invalid cause", sequence)
		}
		if _, duplicate := unique[cause]; duplicate {
			return fmt.Errorf("trace writer: fact %d repeats cause %q", sequence, cause)
		}
		if _, exists := writer.seen[cause]; !exists {
			return fmt.Errorf("trace writer: fact %d cause %q is not an earlier fact", sequence, cause)
		}
		unique[cause] = struct{}{}
	}
	return nil
}

func validateReferences(refs References, sequence int) error {
	for _, value := range []string{refs.Artifact, refs.EventDigest, refs.Selection} {
		if value != "" && !digestPattern.MatchString(value) {
			return fmt.Errorf("trace writer: fact %d has invalid digest reference", sequence)
		}
	}
	if !validOptionalTokens(refs.Correlation, refs.Delivery, refs.Event,
		refs.Handling, refs.ReferenceHead) {
		return fmt.Errorf("trace writer: fact %d has invalid reference", sequence)
	}
	return nil
}

func validateFactFields(fields FactFields, sequence int) error {
	checks := []struct {
		name    string
		value   string
		allowed []string
	}{
		{"action", fields.Action, []string{"current", "submit", "capture", "read", "other"}},
		{"consequence", fields.Consequence, []string{
			"handling.create", "handling.advance", "handling.resolve.completed",
			"handling.resolve.declined", "handling.resolve.unresolved", "reference.publish",
			"reference.supersede", "reference.retract",
		}},
		{"outcome", fields.Outcome, []string{"accepted", "rejected", "replayed", "completed", "declined", "unresolved"}},
		{"phase", fields.Phase, []string{"awaiting_seed", "active", "observed"}},
		{"preference_after", fields.PreferenceAfter, []string{"A", "B"}},
		{"preference_before", fields.PreferenceBefore, []string{"A", "B"}},
		{"result", fields.Result, []string{"threshold_reached", "inconclusive"}},
		{"state", fields.State, []string{"open", "active", "pending", "settled", "expired", "retracted", "resolved"}},
		{"status", fields.Status, []string{"pass", "fail", "incomplete", "unknown", "not_applicable"}},
	}
	for _, check := range checks {
		if check.value != "" && !slices.Contains(check.allowed, check.value) {
			return fmt.Errorf("trace writer: fact %d has invalid %s", sequence, check.name)
		}
	}
	if !validOptionalTokens(fields.Code, fields.GateID, fields.SemanticKind) {
		return fmt.Errorf("trace writer: fact %d has invalid metadata token", sequence)
	}
	integers := []struct {
		name    string
		value   *int
		minimum int
		maximum int
	}{
		{"alpha", fields.Alpha, 1, 64}, {"artifact_count", fields.ArtifactCount, 0, 64},
		{"invalid_votes", fields.InvalidVotes, 0, 128},
		{"margin_after", fields.MarginAfter, -1024, 1024},
		{"margin_before", fields.MarginBefore, -1024, 1024},
		{"no_votes", fields.NoVotes, 0, 64}, {"round", fields.Round, 0, 1024},
		{"sample_size", fields.SampleSize, 0, 64}, {"votes_a", fields.VotesA, 0, 64},
		{"votes_b", fields.VotesB, 0, 64},
	}
	for _, value := range integers {
		if value.value != nil && (*value.value < value.minimum || *value.value > value.maximum) {
			return fmt.Errorf("trace writer: fact %d has out-of-bound %s", sequence, value.name)
		}
	}
	if !validInt64(fields.ByteSize, 0, 16<<20) {
		return fmt.Errorf("trace writer: fact %d has out-of-bound byte_size", sequence)
	}
	if !validInt64(fields.DurationMillis, 0, 3600000) {
		return fmt.Errorf("trace writer: fact %d has out-of-bound duration_ms", sequence)
	}
	return nil
}

func (writer *Writer) validateResult(result Result) (string, error) {
	if !slices.Contains([]ResultStatus{ResultPassed, ResultFailed, ResultIncomplete}, result.Status) {
		return "", fmt.Errorf("trace writer: invalid result status")
	}
	if len(result.Gates) > 64 {
		return "", fmt.Errorf("trace writer: gates exceed 64")
	}
	for _, gate := range result.Gates {
		if !validTraceToken(gate.ID) || !slices.Contains([]GateStatus{
			GatePass, GateFail, GateUnknown, GateNotApplicable,
		}, gate.Status) || len(gate.Evidence) > 32 {
			return "", fmt.Errorf("trace writer: invalid gate")
		}
		unique := make(map[string]struct{}, len(gate.Evidence))
		for _, evidence := range gate.Evidence {
			if _, duplicate := unique[evidence]; duplicate {
				return "", fmt.Errorf("trace writer: gate %q repeats evidence", gate.ID)
			}
			if _, exists := writer.seen[evidence]; !exists {
				return "", fmt.Errorf("trace writer: gate %q cites missing evidence", gate.ID)
			}
			unique[evidence] = struct{}{}
		}
	}
	return canonicalTime("result finished_at", result.FinishedAt)
}

func canonicalTime(label string, value time.Time) (string, error) {
	if value.IsZero() {
		return "", fmt.Errorf("trace writer: %s is required", label)
	}
	formatted := value.UTC().Format(time.RFC3339Nano)
	if len(formatted) > 35 {
		return "", fmt.Errorf("trace writer: %s exceeds timestamp bound", label)
	}
	return formatted, nil
}

func validOptionalTokens(values ...string) bool {
	for _, value := range values {
		if value != "" && !validTraceToken(value) {
			return false
		}
	}
	return true
}

func validTraceToken(value string) bool { return tokenPattern.MatchString(value) }

func validInt64(value *int64, minimum, maximum int64) bool {
	return value == nil || (*value >= minimum && *value <= maximum)
}
