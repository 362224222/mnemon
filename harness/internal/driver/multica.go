package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const (
	MulticaDefaultCommand     = "multica"
	MulticaRuntimeCommandName = multicasurface.MulticaRuntimeCommandName
	MulticaExternalSource     = multicasurface.MulticaExternalSource
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

func (c MulticaCLI) ListIssueComments(ctx context.Context, issueID string) ([]MulticaComment, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return nil, fmt.Errorf("multica issue id is required")
	}
	out, err := c.Run(ctx, []string{"issue", "comment", "list", issueID, "--full", "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	comments, err := decodeMulticaCommentListOutput([]byte(out.Stdout))
	if err != nil {
		return nil, err
	}
	return comments, nil
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

func (c MulticaCLI) RestoreAgent(ctx context.Context, agentID string) (MulticaAgent, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MulticaAgent{}, fmt.Errorf("multica agent id is required")
	}
	out, err := c.Run(ctx, []string{"agent", "restore", agentID, "--output", "json"}, "")
	if err != nil {
		return MulticaAgent{}, err
	}
	var agent MulticaAgent
	if err := json.Unmarshal([]byte(out.Stdout), &agent); err != nil {
		return MulticaAgent{}, fmt.Errorf("decode restored multica agent: %w", err)
	}
	return agent, nil
}

func (c MulticaCLI) UpdateAgent(ctx context.Context, agentID string, req MulticaCreateAgentRequest) (MulticaAgent, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return MulticaAgent{}, fmt.Errorf("multica agent id is required")
	}
	args := []string{"agent", "update", agentID, "--output", "json"}
	if strings.TrimSpace(req.Name) != "" {
		args = append(args, "--name", strings.TrimSpace(req.Name))
	}
	if strings.TrimSpace(req.Description) != "" {
		args = append(args, "--description", strings.TrimSpace(req.Description))
	}
	if strings.TrimSpace(req.Instructions) != "" {
		args = append(args, "--instructions", strings.TrimSpace(req.Instructions))
	}
	if strings.TrimSpace(req.RuntimeID) != "" {
		args = append(args, "--runtime-id", strings.TrimSpace(req.RuntimeID))
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
		return MulticaAgent{}, fmt.Errorf("decode updated multica agent: %w", err)
	}
	return agent, nil
}

func (c MulticaCLI) GetAgentEnv(ctx context.Context, agentID string) (map[string]string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("multica agent id is required")
	}
	out, err := c.Run(ctx, []string{"agent", "env", "get", agentID, "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	env, err := decodeMulticaAgentEnv([]byte(out.Stdout))
	if err != nil {
		return nil, fmt.Errorf("decode multica agent env: %w", err)
	}
	return env, nil
}

func (c MulticaCLI) SetAgentEnv(ctx context.Context, agentID string, env map[string]string) (map[string]string, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil, fmt.Errorf("multica agent id is required")
	}
	if env == nil {
		env = map[string]string{}
	}
	data, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("encode multica agent env: %w", err)
	}
	out, err := c.Run(ctx, []string{"agent", "env", "set", agentID, "--custom-env-stdin", "--output", "json"}, string(data))
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.Stdout) == "" {
		return env, nil
	}
	updated, err := decodeMulticaAgentEnv([]byte(out.Stdout))
	if err != nil {
		return nil, fmt.Errorf("decode updated multica agent env: %w", err)
	}
	return updated, nil
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

func (c MulticaCLI) SetIssueMetadataMap(ctx context.Context, issueID string, values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil
	}
	const maxMetadataWorkers = 6
	workers := maxMetadataWorkers
	if len(keys) < workers {
		workers = len(keys)
	}
	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				mu.Lock()
				stopped := firstErr != nil
				mu.Unlock()
				if stopped {
					continue
				}
				recordErr(c.SetIssueMetadata(ctx, issueID, key, values[key], "string"))
			}
		}()
	}
	for _, key := range keys {
		jobs <- key
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func (c MulticaCLI) GetIssueMetadata(ctx context.Context, issueID, key string) (string, bool, error) {
	issueID = strings.TrimSpace(issueID)
	key = strings.TrimSpace(key)
	if issueID == "" {
		return "", false, fmt.Errorf("multica issue id is required")
	}
	if key == "" {
		return "", false, fmt.Errorf("multica metadata key is required")
	}
	out, err := c.Run(ctx, []string{"issue", "metadata", "get", issueID, "--key", key, "--output", "json"}, "")
	if err != nil {
		return "", false, err
	}
	meta, err := decodeMulticaMetadataOutput([]byte(out.Stdout))
	if err != nil {
		return "", false, err
	}
	value, ok := meta[key]
	return value, ok, nil
}

func (c MulticaCLI) ListIssueMetadata(ctx context.Context, issueID string) (map[string]string, error) {
	issueID = strings.TrimSpace(issueID)
	if issueID == "" {
		return nil, fmt.Errorf("multica issue id is required")
	}
	out, err := c.Run(ctx, []string{"issue", "metadata", "list", issueID, "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	return decodeMulticaMetadataOutput([]byte(out.Stdout))
}

func (c MulticaCLI) LoadIssueMetadata(ctx context.Context, issue MulticaIssue) (MulticaIssue, int, error) {
	if strings.TrimSpace(issue.ID) == "" {
		return issue, 0, nil
	}
	listed, err := c.ListIssueMetadata(ctx, issue.ID)
	if err != nil {
		return issue, 0, err
	}
	issue.Metadata = multicasurface.MergeIssueMetadata(issue.Metadata, listed)
	return issue, len(listed), nil
}

func (c MulticaCLI) ListIssueChildren(ctx context.Context, parentID string) ([]MulticaIssue, error) {
	parentID = strings.TrimSpace(parentID)
	if parentID == "" {
		return nil, fmt.Errorf("multica parent issue id is required")
	}
	out, err := c.Run(ctx, []string{"issue", "children", parentID, "--output", "json"}, "")
	if err != nil {
		return nil, err
	}
	issues, err := decodeMulticaIssueListOutput([]byte(out.Stdout))
	if err != nil {
		return nil, err
	}
	return issues, nil
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

func DecodeMulticaIssue(r io.Reader) (MulticaIssue, error) {
	var issue MulticaIssue
	if err := json.NewDecoder(r).Decode(&issue); err != nil {
		return MulticaIssue{}, err
	}
	return issue, nil
}

func decodeMulticaMetadataOutput(data []byte) (map[string]string, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode multica metadata: %w", err)
	}
	return NormalizeMulticaMetadata(raw), nil
}

func decodeMulticaIssueListOutput(data []byte) ([]MulticaIssue, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode multica issue children: %w", err)
	}
	var out []MulticaIssue
	seen := map[string]bool{}
	var walk func(any)
	walk = func(v any) {
		switch value := v.(type) {
		case []any:
			for _, item := range value {
				walk(item)
			}
		case map[string]any:
			if issue, ok := multicaIssueFromRawMap(value); ok {
				if !seen[issue.ID] {
					seen[issue.ID] = true
					out = append(out, issue)
				}
				return
			}
			for _, item := range value {
				walk(item)
			}
		}
	}
	walk(raw)
	return out, nil
}

func decodeMulticaCommentListOutput(data []byte) ([]MulticaComment, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode multica comments: %w", err)
	}
	var out []MulticaComment
	seen := map[string]bool{}
	var walk func(any)
	walk = func(v any) {
		switch value := v.(type) {
		case []any:
			for _, item := range value {
				walk(item)
			}
		case map[string]any:
			if comment, ok := multicaCommentFromRawMap(value); ok {
				if !seen[comment.ID] {
					seen[comment.ID] = true
					out = append(out, comment)
				}
				return
			}
			for _, item := range value {
				walk(item)
			}
		}
	}
	walk(raw)
	return out, nil
}

func multicaIssueFromRawMap(raw map[string]any) (MulticaIssue, bool) {
	id, _ := raw["id"].(string)
	if strings.TrimSpace(id) == "" {
		return MulticaIssue{}, false
	}
	if _, ok := raw["title"]; !ok {
		if _, ok := raw["identifier"]; !ok {
			if _, ok := raw["description"]; !ok {
				if _, ok := raw["status"]; !ok {
					return MulticaIssue{}, false
				}
			}
		}
	}
	data, err := json.Marshal(raw)
	if err != nil {
		return MulticaIssue{}, false
	}
	var issue MulticaIssue
	if err := json.Unmarshal(data, &issue); err != nil {
		return MulticaIssue{}, false
	}
	return issue, true
}

func multicaCommentFromRawMap(raw map[string]any) (MulticaComment, bool) {
	id, _ := raw["id"].(string)
	if strings.TrimSpace(id) == "" {
		return MulticaComment{}, false
	}
	if _, ok := raw["content"]; !ok {
		if _, ok := raw["body"]; !ok {
			if _, ok := raw["text"]; !ok {
				return MulticaComment{}, false
			}
		}
	}
	comment := MulticaComment{
		ID:      strings.TrimSpace(id),
		IssueID: firstRawString(raw, "issue_id", "issueId"),
		Content: firstRawString(raw, "content", "body", "text"),
		Type:    firstRawString(raw, "type", "kind"),
	}
	return comment, true
}

func firstRawString(raw map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := raw[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func decodeMulticaAgentEnv(data []byte) (map[string]string, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	if custom, ok := raw["custom_env"]; ok {
		return decodeMulticaStringMap(custom)
	}
	return decodeMulticaStringMap(data)
}

func decodeMulticaStringMap(data []byte) (map[string]string, error) {
	var values map[string]any
	if err := json.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	out := map[string]string{}
	for key, value := range values {
		switch v := value.(type) {
		case string:
			out[key] = v
		case nil:
			out[key] = ""
		default:
			out[key] = fmt.Sprint(v)
		}
	}
	return out, nil
}
