package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

const runtimeVersion = "dev"

var assignedIssuePattern = regexp.MustCompile(`(?i)(?:assigned\s+issue\s+id\s+is|issue[_\s-]*id)\s*[:：]\s*([A-Za-z0-9][A-Za-z0-9._:-]*)`)

type runtimeConfig struct {
	Args   []string
	Env    []string
	CWD    string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
	Now    func() time.Time
}

type rpcMessage struct {
	JSONRPC string         `json:"jsonrpc,omitempty"`
	ID      any            `json:"id,omitempty"`
	Method  string         `json:"method,omitempty"`
	Params  map[string]any `json:"params,omitempty"`
	Result  any            `json:"result,omitempty"`
	Error   any            `json:"error,omitempty"`
}

type runtimeRPCState struct {
	CWD      string
	ThreadID string
	TurnID   string
	Env      []string
	Now      func() time.Time
}

type runtimeImportResult struct {
	IssueID             string
	Identifier          string
	Title               string
	Statement           string
	Principal           string
	TaskID              string
	Status              string
	Receipt             *access.IngestReceipt
	ProjectionStatus    string
	ProjectionCommentID string
	ProjectionErr       error
	WakeStatus          string
	WakeTurnID          string
	WakeErr             error
	Err                 error
}

func main() {
	if err := runRuntime(runtimeConfig{
		Args:   os.Args[1:],
		Env:    os.Environ(),
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		Now:    time.Now,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runRuntime(cfg runtimeConfig) error {
	if cfg.Stdin == nil {
		cfg.Stdin = strings.NewReader("")
	}
	if cfg.Stdout == nil {
		cfg.Stdout = io.Discard
	}
	if cfg.Stderr == nil {
		cfg.Stderr = io.Discard
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cwd := strings.TrimSpace(cfg.CWD)
	if cwd == "" {
		var err error
		cwd, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	if wantsVersion(cfg.Args) {
		fmt.Fprintf(cfg.Stdout, "mnemon-multica-runtime %s\n", runtimeVersion)
		return nil
	}
	return runRuntimeRPC(cfg, cwd)
}

func runRuntimeRPC(cfg runtimeConfig, cwd string) error {
	scanner := bufio.NewScanner(cfg.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	state := runtimeRPCState{CWD: cwd, Env: cfg.Env, Now: cfg.Now}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var msg rpcMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			fmt.Fprintf(cfg.Stderr, "mnemon-multica-runtime: ignoring invalid rpc line: %v\n", err)
			continue
		}
		responses := state.handle(msg)
		for _, response := range responses {
			if err := writeRuntimeRPC(cfg.Stdout, response); err != nil {
				return err
			}
		}
	}
	return scanner.Err()
}

func (s *runtimeRPCState) handle(msg rpcMessage) []rpcMessage {
	switch msg.Method {
	case "initialize":
		return []rpcMessage{{
			ID: msg.ID,
			Result: map[string]any{
				"userAgent":      "mnemon-multica-runtime/" + runtimeVersion,
				"codexHome":      envValue(s.Env, "CODEX_HOME"),
				"platformFamily": "unix",
				"platformOs":     runtime.GOOS,
			},
		}}
	case "initialized":
		return nil
	case "thread/start", "thread/resume":
		s.ThreadID = firstNonEmpty(stringParam(msg.Params, "threadId"), runtimeID("thread", s.now()))
		return []rpcMessage{
			{
				Method: "remoteControl/status/changed",
				Params: map[string]any{
					"status":         "disabled",
					"serverName":     "mnemon-multica-runtime",
					"installationId": "mnemon-multica-runtime",
				},
			},
			{
				ID:     msg.ID,
				Result: s.threadStartResult(msg.Params),
			},
			{
				Method: "thread/started",
				Params: map[string]any{
					"thread": s.threadObject(),
				},
			},
		}
	case "thread/name/set", "session/set_model":
		return []rpcMessage{{ID: msg.ID, Result: map[string]any{}}}
	case "turn/start":
		s.TurnID = runtimeID("turn", s.now())
		input := extractRuntimeInput(msg.Params)
		finalAnswer := s.runTurn(input)
		return []rpcMessage{
			{
				ID: msg.ID,
				Result: map[string]any{
					"turn": s.turnObject("inProgress"),
				},
			},
			{
				Method: "thread/status/changed",
				Params: map[string]any{"threadId": s.ThreadID, "status": map[string]any{"type": "active", "activeFlags": []any{}}},
			},
			{
				Method: "turn/started",
				Params: map[string]any{"threadId": s.ThreadID, "turn": s.turnObject("inProgress")},
			},
			{
				Method: "item/started",
				Params: map[string]any{"threadId": s.ThreadID, "turnId": s.TurnID, "item": runtimeAgentMessage("")},
			},
			{
				Method: "item/agentMessage/delta",
				Params: map[string]any{
					"threadId": s.ThreadID,
					"turnId":   s.TurnID,
					"itemId":   "mnemon-runtime-message",
					"delta":    finalAnswer,
				},
			},
			{
				Method: "item/completed",
				Params: map[string]any{"threadId": s.ThreadID, "turnId": s.TurnID, "item": runtimeAgentMessage(finalAnswer)},
			},
			{
				Method: "thread/status/changed",
				Params: map[string]any{"threadId": s.ThreadID, "status": map[string]any{"type": "idle"}},
			},
			{
				Method: "turn/completed",
				Params: map[string]any{"threadId": s.ThreadID, "turn": s.turnObject("completed")},
			},
		}
	default:
		if msg.ID == nil {
			return nil
		}
		return []rpcMessage{{ID: msg.ID, Result: map[string]any{}}}
	}
}

func (s *runtimeRPCState) runTurn(input string) string {
	result := s.importIssue(input)
	return formatRuntimeFinalAnswer(result)
}

func (s *runtimeRPCState) importIssue(input string) runtimeImportResult {
	taskID := envValue(s.Env, "MULTICA_TASK_ID")
	issueID := firstNonEmpty(envValue(s.Env, "MULTICA_ISSUE_ID"), extractAssignedIssueID(input))
	result := runtimeImportResult{
		IssueID:   issueID,
		Principal: resolveRuntimePrincipal(s.Env, s.CWD),
		TaskID:    taskID,
	}
	if issueID == "" {
		result.Status = "skipped"
		result.Err = fmt.Errorf("no Multica issue id was available in task environment or runtime input")
		return result
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeTimeout(s.Env))
	defer cancel()
	cli := runtimeMulticaCLI(s.Env)
	issue, err := cli.GetIssue(ctx, issueID)
	if err != nil {
		result.Status = "failed"
		result.Err = fmt.Errorf("fetch Multica issue %s: %w", issueID, err)
		return result
	}
	result.IssueID = issue.ID
	result.Identifier = issue.Identifier
	result.Title = issue.Title
	result.Statement = issue.Description
	externalID := ""
	if taskID != "" {
		externalID = "multica-task-" + taskID
	}
	draft, err := driver.BuildMulticaIssueTeamworkSignal(issue, driver.MulticaIssueSignalOptions{
		Scope:       envDefault(s.Env, "MNEMON_MULTICA_SCOPE", "multica/teamwork"),
		TTL:         envDefault(s.Env, "MNEMON_MULTICA_TTL", "30m"),
		WhyTeamwork: "Multica assigned this issue to a Mnemon participant, so Mnemon should admit it through the teamwork protocol.",
		WorkspaceID: firstNonEmpty(envValue(s.Env, "MNEMON_MULTICA_WORKSPACE_ID"), envValue(s.Env, "MULTICA_WORKSPACE_ID")),
		TaskID:      taskID,
		AgentID:     envValue(s.Env, "MULTICA_AGENT_ID"),
		Principal:   result.Principal,
		ContextRefs: []string{
			runtimeRef("issue", issue.ID),
			runtimeRef("task", taskID),
			runtimeRef("agent", envValue(s.Env, "MULTICA_AGENT_ID")),
		},
		EvidenceRefs: []string{runtimeRef("issue", issue.ID)},
		ExternalID:   externalID,
	})
	if err != nil {
		result.Status = "failed"
		result.Err = err
		return result
	}
	addr := strings.TrimSpace(envValue(s.Env, "MNEMON_CONTROL_ADDR"))
	if addr == "" {
		result.Status = "skipped"
		return result
	}
	client, err := runtimeControlClient(s.Env, addr, result.Principal)
	if err != nil {
		result.Status = "failed"
		result.Err = err
		return result
	}
	rec, err := client.IngestObserve(contract.ActorID(result.Principal), contract.ObservationEnvelope{
		ExternalID: draft.ExternalID,
		Event: contract.Event{
			Type:          draft.EventType,
			CorrelationID: payloadRuleString(draft.Payload, "correlation_id"),
			Payload:       draft.Payload,
		},
	})
	if err != nil {
		result.Status = "failed"
		result.Err = fmt.Errorf("ingest Mnemon observation: %w", err)
		return result
	}
	result.Status = "recorded"
	result.Receipt = &rec
	s.wakeManagedAgent(&result)
	s.projectImportComment(ctx, cli, issue, draft.ExternalID, &result)
	return result
}

func (s *runtimeRPCState) wakeManagedAgent(result *runtimeImportResult) {
	if result == nil {
		return
	}
	runtimeName := strings.TrimSpace(envValue(s.Env, "MNEMON_MANAGED_RUNTIME"))
	if runtimeName == "" || strings.EqualFold(runtimeName, "off") || strings.EqualFold(runtimeName, "disabled") || strings.EqualFold(runtimeName, "none") {
		result.WakeStatus = "skipped"
		return
	}
	addr := strings.TrimSpace(envValue(s.Env, "MNEMON_CONTROL_ADDR"))
	if addr == "" {
		result.WakeStatus = "skipped"
		result.WakeErr = fmt.Errorf("MNEMON_CONTROL_ADDR is not set")
		return
	}
	token, err := runtimeControlToken(s.Env)
	if err != nil {
		result.WakeStatus = "failed"
		result.WakeErr = err
		return
	}
	renderCtx, cancel := context.WithTimeout(context.Background(), runtimeTimeout(s.Env))
	defer cancel()
	resp, err := (driver.HTTPRenderClient{
		BaseURL:   addr,
		Token:     token,
		Principal: contract.ActorID(result.Principal),
	}).Render(renderCtx, presentation.Request{
		SchemaVersion: 1,
		Principal:     contract.ActorID(result.Principal),
		Host:          "multica",
		Lifecycle:     envDefault(s.Env, "MNEMON_MANAGED_RENDER_LIFECYCLE", "remind"),
		Surface:       "runtime",
		RenderIntent:  presentation.IntentTeamworkEvents,
	})
	if err != nil {
		result.WakeStatus = "failed"
		result.WakeErr = fmt.Errorf("render managed wake candidates: %w", err)
		return
	}
	candidate, ok := managedWakeCandidateForResult(result.Principal, resp, *result)
	if !ok {
		result.WakeStatus = "skipped"
		result.WakeErr = fmt.Errorf("no managed wake candidate in rendered context")
		return
	}
	client, workspace, err := runtimeManagedTurnClient(s.Env, s.CWD, runtimeName)
	if err != nil {
		result.WakeStatus = "failed"
		result.WakeErr = err
		return
	}
	wakeCtx, cancel := context.WithTimeout(context.Background(), runtimeManagedTurnTimeout(s.Env))
	defer cancel()
	record, err := (&driver.ManagedAgentDriver{
		Principal: result.Principal,
		Client:    client,
		Ledger:    driver.NewFileManagedWakeLedger(runtimeManagedLedgerPath(s.Env, workspace)),
		Now:       func() time.Time { return s.now() },
	}).Wake(wakeCtx, candidate)
	result.WakeTurnID = record.TurnID
	if err != nil {
		result.WakeStatus = "failed"
		result.WakeErr = err
		return
	}
	result.WakeStatus = record.Status
	if result.WakeStatus == "" {
		result.WakeStatus = "completed"
	}
}

func managedWakeCandidateForResult(principal string, resp presentation.Response, result runtimeImportResult) (driver.ManagedWakeCandidate, bool) {
	terms := managedWakeMatchTerms(result)
	var fallback driver.ManagedWakeCandidate
	for _, env := range resp.Events {
		candidates := driver.ManagedWakeCandidatesFromEvents(principal, []eventmodel.EventEnvelope{env})
		if len(candidates) == 0 {
			continue
		}
		candidates[0].RenderAuditID = resp.AuditID
		candidates[0].RenderBodyDigest = resp.BodyDigest
		if fallback.Principal == "" {
			fallback = candidates[0]
		}
		if len(terms) == 0 || eventNarrativeContainsAny(env, terms) {
			return candidates[0], true
		}
	}
	if len(terms) == 0 && fallback.Principal != "" {
		return fallback, true
	}
	return driver.ManagedWakeCandidate{}, false
}

func managedWakeMatchTerms(result runtimeImportResult) []string {
	raw := []string{result.IssueID, result.Identifier, result.Title, result.Statement, result.TaskID}
	var out []string
	for _, value := range raw {
		value = strings.TrimSpace(value)
		if len(value) >= 3 {
			out = append(out, value)
		}
	}
	return out
}

func eventNarrativeContainsAny(env eventmodel.EventEnvelope, terms []string) bool {
	body, _ := eventmodel.PayloadNarrative(env.Event.Payload)["body"].(string)
	body = strings.ToLower(body)
	for _, term := range terms {
		if strings.Contains(body, strings.ToLower(term)) {
			return true
		}
	}
	return false
}

func (s *runtimeRPCState) projectImportComment(ctx context.Context, cli driver.MulticaCLI, issue driver.MulticaIssue, externalID string, result *runtimeImportResult) {
	if result == nil {
		return
	}
	if !runtimeProjectionEnabled(s.Env) {
		result.ProjectionStatus = "skipped"
		return
	}
	body := "Issue admitted into Mnemon teamwork."
	if result.Principal != "" {
		body += "\nPrincipal: " + result.Principal
	}
	if result.TaskID != "" {
		body += "\nMultica task: " + result.TaskID
	}
	if result.Receipt != nil {
		body += fmt.Sprintf("\nMnemon ingest: seq=%d duplicate=%v ticked=%v", result.Receipt.Seq, result.Receipt.Dup, result.Receipt.Ticked)
	}
	if result.WakeStatus != "" {
		body += "\nManaged wake: " + result.WakeStatus
	}
	commentBody := driver.FormatMulticaProjectionComment("issue admitted", body, []string{externalID})
	comment, err := cli.AddIssueComment(ctx, issue.ID, commentBody)
	if err != nil {
		result.ProjectionStatus = "failed"
		result.ProjectionErr = err
		return
	}
	result.ProjectionStatus = "commented"
	result.ProjectionCommentID = comment.ID
}

func runtimeMulticaCLI(env []string) driver.MulticaCLI {
	return driver.MulticaCLI{
		Command:     envValue(env, "MNEMON_MULTICA_BIN"),
		Profile:     envValue(env, "MNEMON_MULTICA_PROFILE"),
		ServerURL:   firstNonEmpty(envValue(env, "MNEMON_MULTICA_SERVER_URL"), envValue(env, "MULTICA_SERVER_URL")),
		WorkspaceID: firstNonEmpty(envValue(env, "MNEMON_MULTICA_WORKSPACE_ID"), envValue(env, "MULTICA_WORKSPACE_ID")),
		Env:         append([]string(nil), env...),
		Timeout:     runtimeTimeout(env),
	}
}

func runtimeControlClient(env []string, addr, principal string) (*access.Client, error) {
	token, err := runtimeControlToken(env)
	if err != nil {
		return nil, err
	}
	if token != "" {
		return access.NewClientWithToken(addr, token), nil
	}
	if strings.TrimSpace(principal) == "" {
		return nil, fmt.Errorf("Mnemon principal is required when MNEMON_CONTROL_TOKEN is not set")
	}
	return access.NewClient(addr, contract.ActorID(principal)), nil
}

func runtimeControlToken(env []string) (string, error) {
	token := envValue(env, "MNEMON_CONTROL_TOKEN")
	if tokenFile := envValue(env, "MNEMON_CONTROL_TOKEN_FILE"); tokenFile != "" {
		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("read MNEMON_CONTROL_TOKEN_FILE: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	return token, nil
}

func runtimeManagedTurnClient(env []string, cwd, runtimeName string) (driver.ManagedTurnClient, string, error) {
	workspace := envDefault(env, "MNEMON_MANAGED_WORKSPACE", cwd)
	if strings.TrimSpace(workspace) == "" {
		workspace = "."
	}
	switch strings.TrimSpace(runtimeName) {
	case "noop":
		return runtimeNoopTurnClient{}, workspace, nil
	case "codex-appserver":
		return driver.CodexAppServerTurnClient{
			Principal:   resolveRuntimePrincipal(env, cwd),
			Command:     envDefault(env, "MNEMON_MANAGED_COMMAND", "codex"),
			Workspace:   workspace,
			Env:         append([]string(nil), env...),
			TurnTimeout: runtimeManagedTurnTimeout(env),
			ClientName:  "mnemon-multica-runtime",
		}, workspace, nil
	default:
		return nil, workspace, fmt.Errorf("unsupported MNEMON_MANAGED_RUNTIME %q", runtimeName)
	}
}

type runtimeNoopTurnClient struct{}

func (runtimeNoopTurnClient) StartTurn(_ context.Context, query string) (driver.ManagedTurnResult, error) {
	if strings.TrimSpace(query) != driver.ManagedWakeQuery {
		return driver.ManagedTurnResult{}, fmt.Errorf("unexpected managed wake query %q", query)
	}
	return driver.ManagedTurnResult{TurnID: "noop-turn", Status: "completed"}, nil
}

func resolveRuntimePrincipal(env []string, cwd string) string {
	agentID := envValue(env, "MULTICA_AGENT_ID")
	agentName := envValue(env, "MULTICA_AGENT_NAME")
	if principal := principalFromRegistry(env, cwd, agentID, agentName); principal != "" {
		return principal
	}
	if principal := envValue(env, "MNEMON_CONTROL_PRINCIPAL"); principal != "" {
		return principal
	}
	if agentName != "" {
		return sanitizePrincipal(agentName) + "@multica"
	}
	return "multica@runtime"
}

func principalFromRegistry(env []string, cwd, agentID, agentName string) string {
	paths := []string{}
	if explicit := envValue(env, "MNEMON_MULTICA_REGISTRY"); explicit != "" {
		paths = append(paths, explicit)
	}
	if strings.TrimSpace(cwd) != "" {
		paths = append(paths, driver.MulticaRegistryPath(cwd, ""))
	}
	for _, path := range paths {
		reg, ok, err := driver.LoadMulticaRegistry(path)
		if err != nil || !ok {
			continue
		}
		for _, participant := range reg.Participants {
			if agentID != "" && participant.AgentID == agentID && strings.TrimSpace(participant.Principal) != "" {
				return strings.TrimSpace(participant.Principal)
			}
			if agentName != "" && participant.AgentName == agentName && strings.TrimSpace(participant.Principal) != "" {
				return strings.TrimSpace(participant.Principal)
			}
		}
	}
	return ""
}

func runtimeRef(kind, id string) string {
	kind = strings.TrimSpace(kind)
	id = strings.TrimSpace(id)
	if kind == "" || id == "" {
		return ""
	}
	return "multica:" + kind + ":" + id
}

func formatRuntimeFinalAnswer(result runtimeImportResult) string {
	var b strings.Builder
	if result.IssueID == "" {
		b.WriteString("Mnemon Multica runtime did not receive a Multica issue id.")
	} else {
		label := strings.TrimSpace(result.Identifier)
		if label == "" {
			label = result.IssueID
		}
		b.WriteString("Mnemon Multica runtime handled issue ")
		b.WriteString(label)
		if title := strings.TrimSpace(result.Title); title != "" {
			b.WriteString(" (")
			b.WriteString(title)
			b.WriteString(")")
		}
		b.WriteString(".")
	}
	if principal := strings.TrimSpace(result.Principal); principal != "" {
		b.WriteString(" Principal: ")
		b.WriteString(principal)
		b.WriteString(".")
	}
	if taskID := strings.TrimSpace(result.TaskID); taskID != "" {
		b.WriteString(" Multica task: ")
		b.WriteString(taskID)
		b.WriteString(".")
	}
	switch result.Status {
	case "recorded":
		b.WriteString(" Mnemon ingest: recorded")
		if result.Receipt != nil {
			b.WriteString(fmt.Sprintf(" seq=%d duplicate=%v ticked=%v", result.Receipt.Seq, result.Receipt.Dup, result.Receipt.Ticked))
		}
		b.WriteString(".")
	case "skipped":
		b.WriteString(" Mnemon ingest: skipped")
		if result.Err != nil {
			b.WriteString(" (")
			b.WriteString(result.Err.Error())
			b.WriteString(")")
		} else {
			b.WriteString(" because MNEMON_CONTROL_ADDR is not set")
		}
		b.WriteString(".")
	case "failed":
		b.WriteString(" Mnemon ingest: failed")
		if result.Err != nil {
			b.WriteString(" (")
			b.WriteString(result.Err.Error())
			b.WriteString(")")
		}
		b.WriteString(".")
	default:
		if result.Err != nil {
			b.WriteString(" Mnemon ingest: failed (")
			b.WriteString(result.Err.Error())
			b.WriteString(").")
		}
	}
	switch result.ProjectionStatus {
	case "commented":
		b.WriteString(" Multica projection: comment")
		if result.ProjectionCommentID != "" {
			b.WriteString("=")
			b.WriteString(result.ProjectionCommentID)
		}
		b.WriteString(".")
	case "skipped":
		b.WriteString(" Multica projection: skipped.")
	case "failed":
		b.WriteString(" Multica projection: failed")
		if result.ProjectionErr != nil {
			b.WriteString(" (")
			b.WriteString(result.ProjectionErr.Error())
			b.WriteString(")")
		}
		b.WriteString(".")
	}
	switch result.WakeStatus {
	case "completed":
		b.WriteString(" Managed wake: completed")
		if result.WakeTurnID != "" {
			b.WriteString(" turn=")
			b.WriteString(result.WakeTurnID)
		}
		b.WriteString(".")
	case "skipped":
		b.WriteString(" Managed wake: skipped")
		if result.WakeErr != nil {
			b.WriteString(" (")
			b.WriteString(result.WakeErr.Error())
			b.WriteString(")")
		}
		b.WriteString(".")
	case "failed":
		b.WriteString(" Managed wake: failed")
		if result.WakeErr != nil {
			b.WriteString(" (")
			b.WriteString(result.WakeErr.Error())
			b.WriteString(")")
		}
		b.WriteString(".")
	default:
		if result.WakeStatus != "" {
			b.WriteString(" Managed wake: ")
			b.WriteString(result.WakeStatus)
			b.WriteString(".")
		}
	}
	return strings.TrimSpace(b.String())
}

func (s *runtimeRPCState) threadStartResult(params map[string]any) map[string]any {
	if cwd := stringParam(params, "cwd"); cwd != "" {
		s.CWD = cwd
	}
	return map[string]any{
		"thread":                s.threadObject(),
		"model":                 "mnemon-runtime",
		"modelProvider":         "mnemon",
		"cwd":                   s.CWD,
		"runtimeWorkspaceRoots": []string{s.CWD},
		"instructionSources":    []string{},
		"approvalPolicy":        "never",
		"sandbox":               map[string]any{"type": "dangerFullAccess"},
		"reasoningEffort":       "",
		"multiAgentMode":        "explicitRequestOnly",
	}
}

func (s *runtimeRPCState) threadObject() map[string]any {
	return map[string]any{
		"id":            s.ThreadID,
		"sessionId":     s.ThreadID,
		"ephemeral":     true,
		"modelProvider": "mnemon",
		"createdAt":     s.now().Unix(),
		"updatedAt":     s.now().Unix(),
		"recencyAt":     s.now().Unix(),
		"status":        map[string]any{"type": "idle"},
		"cwd":           s.CWD,
		"cliVersion":    runtimeVersion,
		"source":        "multica",
		"turns":         []any{},
	}
}

func (s *runtimeRPCState) turnObject(status string) map[string]any {
	turn := map[string]any{
		"id":        s.TurnID,
		"items":     []any{},
		"itemsView": "notLoaded",
		"status":    status,
		"error":     nil,
		"startedAt": s.now().Unix(),
	}
	if status == "completed" {
		turn["completedAt"] = s.now().Unix()
		turn["durationMs"] = 1
	}
	return turn
}

func (s *runtimeRPCState) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func writeRuntimeRPC(w io.Writer, msg rpcMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}

func runtimeAgentMessage(text string) map[string]any {
	return map[string]any{
		"type":           "agentMessage",
		"id":             "mnemon-runtime-message",
		"text":           text,
		"phase":          "final_answer",
		"memoryCitation": nil,
	}
}

func extractRuntimeInput(params map[string]any) string {
	input, ok := params["input"].([]any)
	if !ok {
		return ""
	}
	var parts []string
	for _, item := range input {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if text, _ := obj["text"].(string); strings.TrimSpace(text) != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func extractAssignedIssueID(input string) string {
	match := assignedIssuePattern.FindStringSubmatch(input)
	if len(match) >= 2 {
		return strings.Trim(match[1], " \t\r\n.,;)")
	}
	return ""
}

func wantsVersion(args []string) bool {
	fs := flag.NewFlagSet("mnemon-multica-runtime", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	version := fs.Bool("version", false, "")
	_ = fs.Parse(args)
	if *version {
		return true
	}
	for _, arg := range args {
		switch arg {
		case "version", "--version", "-version", "-v":
			return true
		}
	}
	return false
}

func stringParam(params map[string]any, key string) string {
	if params == nil {
		return ""
	}
	value, _ := params[key].(string)
	return strings.TrimSpace(value)
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(item, prefix))
		}
	}
	return ""
}

func envDefault(env []string, key, fallback string) string {
	if value := envValue(env, key); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func payloadRuleString(payload map[string]any, key string) string {
	rule, _ := payload["rule"].(map[string]any)
	value, _ := rule[key].(string)
	return strings.TrimSpace(value)
}

func runtimeID(prefix string, now time.Time) string {
	if prefix == "" {
		prefix = "id"
	}
	return fmt.Sprintf("%s-%d", prefix, now.UTC().UnixNano())
}

func runtimeTimeout(env []string) time.Duration {
	raw := envValue(env, "MNEMON_MULTICA_RUNTIME_TIMEOUT")
	if raw == "" {
		return 30 * time.Second
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 30 * time.Second
	}
	return d
}

func runtimeManagedTurnTimeout(env []string) time.Duration {
	raw := envValue(env, "MNEMON_MANAGED_TURN_TIMEOUT")
	if raw == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 5 * time.Minute
	}
	return d
}

func runtimeManagedLedgerPath(env []string, workspace string) string {
	if explicit := envValue(env, "MNEMON_MANAGED_LEDGER"); explicit != "" {
		return explicit
	}
	root := strings.TrimSpace(workspace)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, ".mnemon", "harness", "local", "managed-agent", "wake-ledger.jsonl")
}

func runtimeProjectionEnabled(env []string) bool {
	value := strings.ToLower(envDefault(env, "MNEMON_MULTICA_PROJECT_COMMENTS", "true"))
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func sanitizePrincipal(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		ok := unicode.IsLetter(r) || unicode.IsDigit(r)
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "multica-agent"
	}
	return out
}
