package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

const (
	MulticaDefaultCommand = "multica"
	MulticaExternalSource = "multica"
)

type MulticaCLI struct {
	Command     string
	Profile     string
	ServerURL   string
	WorkspaceID string
	Env         []string
	Timeout     time.Duration
}

type MulticaVersion struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
	OS      string `json:"os"`
	Arch    string `json:"arch"`
	Go      string `json:"go"`
}

type MulticaDaemonStatus struct {
	Status       string         `json:"status"`
	PID          int            `json:"pid,omitempty"`
	DaemonID     string         `json:"daemon_id,omitempty"`
	DeviceName   string         `json:"device_name,omitempty"`
	ServerURL    string         `json:"server_url,omitempty"`
	Agents       []string       `json:"agents,omitempty"`
	Profile      string         `json:"profile,omitempty"`
	Raw          map[string]any `json:"raw,omitempty"`
	RawText      string         `json:"raw_text,omitempty"`
	CommandError string         `json:"command_error,omitempty"`
}

type MulticaIssue struct {
	ID          string         `json:"id"`
	Identifier  string         `json:"identifier"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Status      string         `json:"status"`
	Priority    string         `json:"priority"`
	Metadata    map[string]any `json:"metadata"`
}

type MulticaComment struct {
	ID      string `json:"id"`
	IssueID string `json:"issue_id"`
	Content string `json:"content"`
	Type    string `json:"type"`
}

type MulticaCommandResult struct {
	Stdout string
	Stderr string
}

type MulticaCommandError struct {
	Args   []string
	Stdout string
	Stderr string
	Err    error
}

func (e MulticaCommandError) Error() string {
	detail := strings.TrimSpace(e.Stderr)
	if detail == "" {
		detail = strings.TrimSpace(e.Stdout)
	}
	if detail == "" {
		detail = e.Err.Error()
	}
	return fmt.Sprintf("multica %s failed: %s", strings.Join(e.Args, " "), detail)
}

func (e MulticaCommandError) Unwrap() error {
	return e.Err
}

func (c MulticaCLI) Version(ctx context.Context) (MulticaVersion, error) {
	out, err := c.Run(ctx, []string{"version", "--output", "json"}, "")
	if err != nil {
		return MulticaVersion{}, err
	}
	var version MulticaVersion
	if err := json.Unmarshal([]byte(out.Stdout), &version); err != nil {
		return MulticaVersion{}, fmt.Errorf("decode multica version: %w", err)
	}
	return version, nil
}

func (c MulticaCLI) AuthStatus(ctx context.Context) (string, error) {
	out, err := c.Run(ctx, []string{"auth", "status"}, "")
	if err != nil {
		return strings.TrimSpace(out.Stdout + out.Stderr), err
	}
	status := strings.TrimSpace(out.Stdout)
	if status == "" {
		status = strings.TrimSpace(out.Stderr)
	}
	return status, nil
}

func (c MulticaCLI) DaemonStatus(ctx context.Context) (MulticaDaemonStatus, error) {
	out, err := c.Run(ctx, []string{"daemon", "status", "--output", "json"}, "")
	status := MulticaDaemonStatus{RawText: strings.TrimSpace(out.Stdout)}
	if strings.TrimSpace(out.Stdout) != "" {
		var raw map[string]any
		if decodeErr := json.Unmarshal([]byte(out.Stdout), &raw); decodeErr == nil {
			status.Raw = raw
			status.Status, _ = raw["status"].(string)
			status.DaemonID, _ = raw["daemon_id"].(string)
			status.DeviceName, _ = raw["device_name"].(string)
			status.ServerURL, _ = raw["server_url"].(string)
			status.Profile, _ = raw["profile"].(string)
			if pid, ok := raw["pid"].(float64); ok {
				status.PID = int(pid)
			}
			if agents, ok := raw["agents"].([]any); ok {
				for _, agent := range agents {
					if s, ok := agent.(string); ok {
						status.Agents = append(status.Agents, s)
					}
				}
			}
		}
	}
	if err != nil {
		status.CommandError = err.Error()
		return status, err
	}
	return status, nil
}

func (c MulticaCLI) GetIssue(ctx context.Context, issueID string) (MulticaIssue, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return MulticaIssue{}, fmt.Errorf("multica issue id is required")
	}
	out, err := c.Run(ctx, []string{"issue", "get", issueID, "--output", "json"}, "")
	if err != nil {
		return MulticaIssue{}, err
	}
	var issue MulticaIssue
	if err := json.Unmarshal([]byte(out.Stdout), &issue); err != nil {
		return MulticaIssue{}, fmt.Errorf("decode multica issue: %w", err)
	}
	return issue, nil
}

func (c MulticaCLI) AddIssueComment(ctx context.Context, issueID, content string) (MulticaComment, error) {
	issueID = strings.TrimSpace(issueID)
	content = strings.TrimSpace(content)
	if issueID == "" {
		return MulticaComment{}, fmt.Errorf("multica issue id is required")
	}
	if content == "" {
		return MulticaComment{}, fmt.Errorf("multica comment content is required")
	}
	out, err := c.Run(ctx, []string{"issue", "comment", "add", issueID, "--content-stdin", "--output", "json"}, content)
	if err != nil {
		return MulticaComment{}, err
	}
	var comment MulticaComment
	if err := json.Unmarshal([]byte(out.Stdout), &comment); err != nil {
		return MulticaComment{}, fmt.Errorf("decode multica comment: %w", err)
	}
	return comment, nil
}

func (c MulticaCLI) Run(ctx context.Context, args []string, stdin string) (MulticaCommandResult, error) {
	command := strings.TrimSpace(c.Command)
	if command == "" {
		command = MulticaDefaultCommand
	}
	fullArgs := c.globalArgs(args)
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, command, fullArgs...)
	if len(c.Env) > 0 {
		cmd.Env = append([]string(nil), c.Env...)
	}
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := MulticaCommandResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return result, MulticaCommandError{Args: fullArgs, Stdout: result.Stdout, Stderr: result.Stderr, Err: runCtx.Err()}
	}
	if err != nil {
		return result, MulticaCommandError{Args: fullArgs, Stdout: result.Stdout, Stderr: result.Stderr, Err: err}
	}
	return result, nil
}

func (c MulticaCLI) globalArgs(args []string) []string {
	var out []string
	if strings.TrimSpace(c.Profile) != "" {
		out = append(out, "--profile", strings.TrimSpace(c.Profile))
	}
	if strings.TrimSpace(c.ServerURL) != "" {
		out = append(out, "--server-url", strings.TrimSpace(c.ServerURL))
	}
	if strings.TrimSpace(c.WorkspaceID) != "" {
		out = append(out, "--workspace-id", strings.TrimSpace(c.WorkspaceID))
	}
	return append(out, args...)
}

type MulticaIssueSignalOptions struct {
	Scope        string
	TTL          string
	WhyTeamwork  string
	EvidenceRefs []string
	ContextRefs  []string
}

type MulticaObservedDraft struct {
	EventType  string         `json:"event_type"`
	ExternalID string         `json:"external_id"`
	Payload    map[string]any `json:"payload"`
}

func BuildMulticaIssueTeamworkSignal(issue MulticaIssue, opts MulticaIssueSignalOptions) (MulticaObservedDraft, error) {
	if strings.TrimSpace(issue.ID) == "" {
		return MulticaObservedDraft{}, fmt.Errorf("multica issue id is required")
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
	narrative := map[string]any{
		"title":        title,
		"statement":    multicaIssueStatement(issue, title),
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
	return MulticaObservedDraft{
		EventType:  "teamwork_signal.write_candidate.observed",
		ExternalID: "multica-issue-" + issue.ID,
		Payload:    eventmodel.BuildPayload(rule, narrative, refs),
	}, nil
}

func FormatMulticaProjectionComment(title string, body string, eventIDs []string) string {
	title = strings.TrimSpace(title)
	body = strings.TrimSpace(body)
	var b strings.Builder
	if title != "" {
		b.WriteString("Mnemon update: ")
		b.WriteString(title)
		b.WriteString("\n\n")
	} else {
		b.WriteString("Mnemon update\n\n")
	}
	if body != "" {
		b.WriteString(body)
		b.WriteString("\n")
	}
	if ids := cleanMulticaRefs(eventIDs); len(ids) > 0 {
		b.WriteString("\n")
		for _, id := range ids {
			b.WriteString("mnemon:event=")
			b.WriteString(id)
			b.WriteString("\n")
		}
	}
	return strings.TrimSpace(b.String())
}

func DecodeMulticaIssue(r io.Reader) (MulticaIssue, error) {
	var issue MulticaIssue
	if err := json.NewDecoder(r).Decode(&issue); err != nil {
		return MulticaIssue{}, err
	}
	return issue, nil
}

func multicaIssueStatement(issue MulticaIssue, fallback string) string {
	if strings.TrimSpace(issue.Description) != "" {
		return strings.TrimSpace(issue.Description)
	}
	return fallback
}

func cleanMulticaRefs(values []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}
