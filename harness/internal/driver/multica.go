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

type MulticaRuntimeProfile struct {
	ID             string `json:"id"`
	DisplayName    string `json:"display_name"`
	Description    string `json:"description"`
	CommandName    string `json:"command_name"`
	ProtocolFamily string `json:"protocol_family"`
	Enabled        bool   `json:"enabled"`
	Visibility     string `json:"visibility"`
	WorkspaceID    string `json:"workspace_id"`
}

type MulticaRuntime struct {
	ID           string         `json:"id"`
	Name         string         `json:"name"`
	Provider     string         `json:"provider"`
	Status       string         `json:"status"`
	RuntimeMode  string         `json:"runtime_mode"`
	ProfileID    string         `json:"profile_id"`
	LaunchHeader string         `json:"launch_header"`
	Metadata     map[string]any `json:"metadata"`
	WorkspaceID  string         `json:"workspace_id"`
}

type MulticaAgent struct {
	ID            string         `json:"id"`
	Name          string         `json:"name"`
	Description   string         `json:"description"`
	Instructions  string         `json:"instructions"`
	RuntimeID     string         `json:"runtime_id"`
	RuntimeMode   string         `json:"runtime_mode"`
	Status        string         `json:"status"`
	Visibility    string         `json:"visibility"`
	ArchivedAt    string         `json:"archived_at"`
	CustomArgs    []string       `json:"custom_args"`
	RuntimeConfig map[string]any `json:"runtime_config"`
	WorkspaceID   string         `json:"workspace_id"`
}

type MulticaComment struct {
	ID      string `json:"id"`
	IssueID string `json:"issue_id"`
	Content string `json:"content"`
	Type    string `json:"type"`
}

type MulticaIssueRun struct {
	ID          string         `json:"id"`
	IssueID     string         `json:"issue_id"`
	AgentID     string         `json:"agent_id"`
	RuntimeID   string         `json:"runtime_id"`
	Status      string         `json:"status"`
	Kind        string         `json:"kind"`
	Attempt     int            `json:"attempt"`
	Result      map[string]any `json:"result"`
	WorkDir     string         `json:"work_dir"`
	CreatedAt   string         `json:"created_at"`
	StartedAt   string         `json:"started_at"`
	CompletedAt string         `json:"completed_at"`
	WorkspaceID string         `json:"workspace_id"`
}

type MulticaRunMessage struct {
	TaskID    string `json:"task_id"`
	IssueID   string `json:"issue_id"`
	Seq       int64  `json:"seq"`
	Type      string `json:"type"`
	Content   string `json:"content"`
	CreatedAt string `json:"created_at"`
}

type MulticaCreateRuntimeProfileRequest struct {
	DisplayName    string
	Description    string
	ProtocolFamily string
	CommandName    string
}

type MulticaCreateAgentRequest struct {
	Name               string
	Description        string
	Instructions       string
	RuntimeID          string
	Visibility         string
	Model              string
	ThinkingLevel      string
	MaxConcurrentTasks int
	CustomArgsJSON     string
	RuntimeConfigJSON  string
}

type MulticaCreateIssueRequest struct {
	Title          string
	Description    string
	AssigneeID     string
	Assignee       string
	ParentID       string
	Status         string
	Priority       string
	AllowDuplicate bool
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

func (c MulticaCLI) ListRuntimeProfiles(ctx context.Context) ([]MulticaRuntimeProfile, error) {
	out, err := c.Run(ctx, []string{"runtime", "profile", "list", "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	var profiles []MulticaRuntimeProfile
	if err := json.Unmarshal([]byte(out.Stdout), &profiles); err != nil {
		return nil, fmt.Errorf("decode multica runtime profiles: %w", err)
	}
	return profiles, nil
}

func (c MulticaCLI) CreateRuntimeProfile(ctx context.Context, req MulticaCreateRuntimeProfileRequest) (MulticaRuntimeProfile, error) {
	if strings.TrimSpace(req.DisplayName) == "" {
		return MulticaRuntimeProfile{}, fmt.Errorf("multica runtime profile display name is required")
	}
	if strings.TrimSpace(req.ProtocolFamily) == "" {
		return MulticaRuntimeProfile{}, fmt.Errorf("multica runtime profile protocol family is required")
	}
	if strings.TrimSpace(req.CommandName) == "" {
		return MulticaRuntimeProfile{}, fmt.Errorf("multica runtime profile command name is required")
	}
	args := []string{
		"runtime", "profile", "create",
		"--display-name", strings.TrimSpace(req.DisplayName),
		"--protocol-family", strings.TrimSpace(req.ProtocolFamily),
		"--command-name", strings.TrimSpace(req.CommandName),
		"--output", "json",
	}
	if strings.TrimSpace(req.Description) != "" {
		args = append(args, "--description", strings.TrimSpace(req.Description))
	}
	out, err := c.Run(ctx, args, "")
	if err != nil {
		return MulticaRuntimeProfile{}, err
	}
	var profile MulticaRuntimeProfile
	if err := json.Unmarshal([]byte(out.Stdout), &profile); err != nil {
		return MulticaRuntimeProfile{}, fmt.Errorf("decode multica runtime profile: %w", err)
	}
	return profile, nil
}

func (c MulticaCLI) SetRuntimeProfilePath(ctx context.Context, profileID, path string) error {
	profileID = strings.TrimSpace(profileID)
	path = strings.TrimSpace(path)
	if profileID == "" {
		return fmt.Errorf("multica runtime profile id is required")
	}
	if path == "" {
		return fmt.Errorf("multica runtime profile path is required")
	}
	_, err := c.Run(ctx, []string{"runtime", "profile", "set-path", profileID, "--path", path}, "")
	return err
}

func (c MulticaCLI) DeleteRuntimeProfile(ctx context.Context, profileID string) error {
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return fmt.Errorf("multica runtime profile id is required")
	}
	_, err := c.Run(ctx, []string{"runtime", "profile", "delete", profileID}, "")
	return err
}

func (c MulticaCLI) ListRuntimes(ctx context.Context) ([]MulticaRuntime, error) {
	out, err := c.Run(ctx, []string{"runtime", "list", "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	var runtimes []MulticaRuntime
	if err := json.Unmarshal([]byte(out.Stdout), &runtimes); err != nil {
		return nil, fmt.Errorf("decode multica runtimes: %w", err)
	}
	return runtimes, nil
}

func (c MulticaCLI) ListAgents(ctx context.Context, includeArchived bool) ([]MulticaAgent, error) {
	args := []string{"agent", "list", "--output", "json"}
	if includeArchived {
		args = append(args, "--include-archived")
	}
	out, err := c.Run(ctx, args, "")
	if err != nil {
		return nil, err
	}
	var agents []MulticaAgent
	if err := json.Unmarshal([]byte(out.Stdout), &agents); err != nil {
		return nil, fmt.Errorf("decode multica agents: %w", err)
	}
	return agents, nil
}

func (c MulticaCLI) CreateAgent(ctx context.Context, req MulticaCreateAgentRequest) (MulticaAgent, error) {
	if strings.TrimSpace(req.Name) == "" {
		return MulticaAgent{}, fmt.Errorf("multica agent name is required")
	}
	if strings.TrimSpace(req.RuntimeID) == "" {
		return MulticaAgent{}, fmt.Errorf("multica agent runtime id is required")
	}
	args := []string{
		"agent", "create",
		"--name", strings.TrimSpace(req.Name),
		"--runtime-id", strings.TrimSpace(req.RuntimeID),
		"--output", "json",
	}
	if strings.TrimSpace(req.Description) != "" {
		args = append(args, "--description", strings.TrimSpace(req.Description))
	}
	if strings.TrimSpace(req.Instructions) != "" {
		args = append(args, "--instructions", strings.TrimSpace(req.Instructions))
	}
	if strings.TrimSpace(req.Visibility) != "" {
		args = append(args, "--visibility", strings.TrimSpace(req.Visibility))
	}
	if strings.TrimSpace(req.Model) != "" {
		args = append(args, "--model", strings.TrimSpace(req.Model))
	}
	if strings.TrimSpace(req.ThinkingLevel) != "" {
		args = append(args, "--thinking-level", strings.TrimSpace(req.ThinkingLevel))
	}
	if req.MaxConcurrentTasks > 0 {
		args = append(args, "--max-concurrent-tasks", fmt.Sprint(req.MaxConcurrentTasks))
	}
	if strings.TrimSpace(req.CustomArgsJSON) != "" {
		args = append(args, "--custom-args", strings.TrimSpace(req.CustomArgsJSON))
	}
	if strings.TrimSpace(req.RuntimeConfigJSON) != "" {
		args = append(args, "--runtime-config", strings.TrimSpace(req.RuntimeConfigJSON))
	}
	out, err := c.Run(ctx, args, "")
	if err != nil {
		return MulticaAgent{}, err
	}
	var agent MulticaAgent
	if err := json.Unmarshal([]byte(out.Stdout), &agent); err != nil {
		return MulticaAgent{}, fmt.Errorf("decode multica agent: %w", err)
	}
	return agent, nil
}

func (c MulticaCLI) ArchiveAgent(ctx context.Context, agentID string) (MulticaAgent, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MulticaAgent{}, fmt.Errorf("multica agent id is required")
	}
	out, err := c.Run(ctx, []string{"agent", "archive", agentID, "--output", "json"}, "")
	if err != nil {
		return MulticaAgent{}, err
	}
	var agent MulticaAgent
	if err := json.Unmarshal([]byte(out.Stdout), &agent); err != nil {
		return MulticaAgent{}, fmt.Errorf("decode archived multica agent: %w", err)
	}
	return agent, nil
}

func (c MulticaCLI) CreateIssue(ctx context.Context, req MulticaCreateIssueRequest) (MulticaIssue, error) {
	if strings.TrimSpace(req.Title) == "" {
		return MulticaIssue{}, fmt.Errorf("multica issue title is required")
	}
	args := []string{"issue", "create", "--title", strings.TrimSpace(req.Title), "--output", "json"}
	stdin := ""
	if strings.TrimSpace(req.Description) != "" {
		args = append(args, "--description-stdin")
		stdin = req.Description
	}
	if strings.TrimSpace(req.AssigneeID) != "" {
		args = append(args, "--assignee-id", strings.TrimSpace(req.AssigneeID))
	}
	if strings.TrimSpace(req.Assignee) != "" {
		args = append(args, "--assignee", strings.TrimSpace(req.Assignee))
	}
	if strings.TrimSpace(req.ParentID) != "" {
		args = append(args, "--parent", strings.TrimSpace(req.ParentID))
	}
	if strings.TrimSpace(req.Status) != "" {
		args = append(args, "--status", strings.TrimSpace(req.Status))
	}
	if strings.TrimSpace(req.Priority) != "" {
		args = append(args, "--priority", strings.TrimSpace(req.Priority))
	}
	if req.AllowDuplicate {
		args = append(args, "--allow-duplicate")
	}
	out, err := c.Run(ctx, args, stdin)
	if err != nil {
		return MulticaIssue{}, err
	}
	var issue MulticaIssue
	if err := json.Unmarshal([]byte(out.Stdout), &issue); err != nil {
		return MulticaIssue{}, fmt.Errorf("decode created multica issue: %w", err)
	}
	return issue, nil
}

func (c MulticaCLI) AssignIssue(ctx context.Context, issueID, assigneeID string) (MulticaIssue, error) {
	issueID = strings.TrimSpace(issueID)
	assigneeID = strings.TrimSpace(assigneeID)
	if issueID == "" {
		return MulticaIssue{}, fmt.Errorf("multica issue id is required")
	}
	if assigneeID == "" {
		return MulticaIssue{}, fmt.Errorf("multica assignee id is required")
	}
	out, err := c.Run(ctx, []string{"issue", "assign", issueID, "--to-id", assigneeID, "--output", "json"}, "")
	if err != nil {
		return MulticaIssue{}, err
	}
	var issue MulticaIssue
	if err := json.Unmarshal([]byte(out.Stdout), &issue); err != nil {
		return MulticaIssue{}, fmt.Errorf("decode assigned multica issue: %w", err)
	}
	return issue, nil
}

func (c MulticaCLI) SetIssueStatus(ctx context.Context, issueID, status string) (MulticaIssue, error) {
	issueID = strings.TrimSpace(issueID)
	status = strings.TrimSpace(status)
	if issueID == "" {
		return MulticaIssue{}, fmt.Errorf("multica issue id is required")
	}
	if status == "" {
		return MulticaIssue{}, fmt.Errorf("multica issue status is required")
	}
	out, err := c.Run(ctx, []string{"issue", "status", issueID, status, "--output", "json"}, "")
	if err != nil {
		return MulticaIssue{}, err
	}
	var issue MulticaIssue
	if err := json.Unmarshal([]byte(out.Stdout), &issue); err != nil {
		return MulticaIssue{}, fmt.Errorf("decode multica issue status: %w", err)
	}
	return issue, nil
}

func (c MulticaCLI) SetIssueMetadata(ctx context.Context, issueID, key, value, valueType string) error {
	issueID = strings.TrimSpace(issueID)
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if issueID == "" {
		return fmt.Errorf("multica issue id is required")
	}
	if key == "" {
		return fmt.Errorf("multica metadata key is required")
	}
	args := []string{"issue", "metadata", "set", issueID, "--key", key, "--value", value}
	if strings.TrimSpace(valueType) != "" {
		args = append(args, "--type", strings.TrimSpace(valueType))
	}
	args = append(args, "--output", "json")
	_, err := c.Run(ctx, args, "")
	return err
}

func (c MulticaCLI) ListIssueRuns(ctx context.Context, issueID string) ([]MulticaIssueRun, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return nil, fmt.Errorf("multica issue id is required")
	}
	out, err := c.Run(ctx, []string{"issue", "runs", issueID, "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	var runs []MulticaIssueRun
	if err := json.Unmarshal([]byte(out.Stdout), &runs); err != nil {
		return nil, fmt.Errorf("decode multica issue runs: %w", err)
	}
	return runs, nil
}

func (c MulticaCLI) ListRunMessages(ctx context.Context, taskID, issueID string) ([]MulticaRunMessage, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, fmt.Errorf("multica task id is required")
	}
	args := []string{"issue", "run-messages", taskID, "--output", "json"}
	if strings.TrimSpace(issueID) != "" {
		args = append(args, "--issue", strings.TrimSpace(issueID))
	}
	out, err := c.Run(ctx, args, "")
	if err != nil {
		return nil, err
	}
	var messages []MulticaRunMessage
	if err := json.Unmarshal([]byte(out.Stdout), &messages); err != nil {
		return nil, fmt.Errorf("decode multica run messages: %w", err)
	}
	return messages, nil
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
