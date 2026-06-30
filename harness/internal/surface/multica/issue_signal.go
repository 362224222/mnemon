package multica

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/interaction"
)

const MulticaExternalSource = "multica"

type IssueSignalMaterial struct {
	ID          string
	Identifier  string
	Title       string
	Description string
}

type IssueSignalOptions struct {
	Scope        string
	TTL          string
	WhyTeamwork  string
	WorkspaceID  string
	TaskID       string
	AgentID      string
	Principal    string
	EvidenceRefs []string
	ContextRefs  []string
	ExternalID   string
}

type ObservedDraft = interaction.EventMaterial

func BuildIssueTeamworkSignal(issue IssueSignalMaterial, opts IssueSignalOptions) (ObservedDraft, error) {
	if strings.TrimSpace(issue.ID) == "" {
		return ObservedDraft{}, fmt.Errorf("multica issue id is required")
	}
	title := strings.TrimSpace(issue.Title)
	if title == "" {
		title = strings.TrimSpace(issue.Identifier)
	}
	if title == "" {
		title = issue.ID
	}
	scope := strings.TrimSpace(opts.Scope)
	if scope == "" {
		scope = "multica/teamwork"
	}
	ttl := strings.TrimSpace(opts.TTL)
	if ttl == "" {
		ttl = "30m"
	}
	correlation := "multica:issue:" + issue.ID
	rule := map[string]any{
		"external_source":           MulticaExternalSource,
		"external_issue_id":         issue.ID,
		"external_issue_identifier": strings.TrimSpace(issue.Identifier),
		"correlation_id":            correlation,
		"scope":                     scope,
		"ttl":                       ttl,
	}
	putRuleString(rule, "external_workspace_id", opts.WorkspaceID)
	putRuleString(rule, "external_task_id", opts.TaskID)
	putRuleString(rule, "external_agent_id", opts.AgentID)
	putRuleString(rule, "principal", opts.Principal)
	narrative := map[string]any{
		"title":        title,
		"statement":    issueSignalStatement(issue, title),
		"why_teamwork": strings.TrimSpace(opts.WhyTeamwork),
	}
	if narrative["why_teamwork"] == "" {
		narrative["why_teamwork"] = "The Multica issue is being bridged into Mnemon so local agents can decide whether and how to coordinate."
	}
	refs := map[string]any{
		"context_refs": append([]string{correlation}, cleanMulticaRefs(opts.ContextRefs)...),
	}
	if evidence := cleanMulticaRefs(opts.EvidenceRefs); len(evidence) > 0 {
		refs["evidence_refs"] = evidence
	} else {
		refs["evidence_refs"] = []string{correlation}
	}
	externalID := strings.TrimSpace(opts.ExternalID)
	if externalID == "" {
		externalID = "multica-issue-" + issue.ID
	}
	return ObservedDraft{
		EventType:  "teamwork_signal.write_candidate.observed",
		ExternalID: externalID,
		Payload:    interaction.BuildPayload(rule, narrative, refs),
	}, nil
}

func issueSignalStatement(issue IssueSignalMaterial, fallback string) string {
	if strings.TrimSpace(issue.Description) != "" {
		return strings.TrimSpace(issue.Description)
	}
	return fallback
}

func putRuleString(rule map[string]any, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		rule[key] = value
	}
}
