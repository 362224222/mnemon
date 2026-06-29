package drive

import (
	"strings"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

type ManagedWakeMatchMaterial struct {
	MatchTerms            []string
	AssignmentID          string
	AssignmentFingerprint string
	IssueID               string
	Identifier            string
	Title                 string
	Statement             string
	TaskID                string
}

func ManagedWakeCandidateForRender(principal string, resp presentation.Response, material ManagedWakeMatchMaterial) (ManagedWakeCandidate, bool) {
	terms := ManagedWakeMatchTerms(material)
	var fallback ManagedWakeCandidate
	for _, env := range resp.Events {
		candidates := ManagedWakeCandidatesFromEvents(principal, []eventmodel.EventEnvelope{env})
		if len(candidates) == 0 {
			continue
		}
		candidate := candidates[0]
		candidate.RenderAuditID = resp.AuditID
		candidate.RenderBodyDigest = resp.BodyDigest
		if fallback.Principal == "" {
			fallback = candidate
		}
		if len(terms) == 0 || eventNarrativeContainsAny(env, terms) {
			return candidate, true
		}
	}
	if len(terms) == 0 && fallback.Principal != "" {
		return fallback, true
	}
	return ManagedWakeCandidate{}, false
}

func ManagedWakeMatchTerms(material ManagedWakeMatchMaterial) []string {
	if len(material.MatchTerms) > 0 {
		return CleanManagedWakeMatchTerms(material.MatchTerms...)
	}
	if material.AssignmentID != "" || material.AssignmentFingerprint != "" {
		return CleanManagedWakeMatchTerms(material.AssignmentID, material.AssignmentFingerprint)
	}
	raw := []string{material.IssueID, material.Identifier, material.Title, material.TaskID}
	var out []string
	for _, value := range raw {
		value = strings.TrimSpace(value)
		if len(value) >= 3 {
			out = append(out, value)
		}
	}
	if len(out) > 0 {
		return out
	}
	if value := strings.TrimSpace(material.Statement); len(value) >= 3 {
		out = append(out, value)
	}
	return out
}

func CleanManagedWakeMatchTerms(values ...string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if len(value) < 3 || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

func eventNarrativeContainsAny(env eventmodel.EventEnvelope, terms []string) bool {
	body, _ := eventmodel.PayloadNarrative(env.Event.Payload)["body"].(string)
	body = strings.ToLower(strings.Join([]string{body, string(env.Event.Subject), env.Event.ID, env.Event.Type}, "\n"))
	for _, term := range terms {
		if strings.Contains(body, strings.ToLower(term)) {
			return true
		}
	}
	return false
}
