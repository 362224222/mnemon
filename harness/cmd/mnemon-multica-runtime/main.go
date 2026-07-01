package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const runtimeVersion = "dev"

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
	ItemSeq  int
	Env      []string
	Now      func() time.Time
}

type runtimeProgressSink func(runtimeProgressEvent)

type runtimeProgressEvent struct {
	Text       string
	Command    string
	CWD        string
	Output     string
	ExitCode   int
	DurationMs int64
}

type runtimeImportResult struct {
	IssueID    string
	Identifier string
	Title      string
	Statement  string
	Principal  string
	TaskID     string
	Status     string
	Receipt    *access.IngestReceipt
	Err        error
}

type providerTurnResult struct {
	Configured bool
	Runtime    string
	Command    string
	CWD        string
	Output     string
	ExitCode   int
	DurationMs int64
	Err        error
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
	if runtimeProbeModeEnabled(cfg.Args) {
		if wantsRuntimeHelp(cfg.Args) {
			writeRuntimeHelp(cfg.Stdout)
			return nil
		}
		return runRuntimeProbe(cfg)
	}
	if wantsVersion(cfg.Args) {
		fmt.Fprintf(cfg.Stdout, "%s %s\n", multicasurface.MulticaRuntimeCommandName, runtimeVersion)
		return nil
	}
	if wantsRuntimeHelp(cfg.Args) {
		writeRuntimeHelp(cfg.Stdout)
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
			fmt.Fprintf(cfg.Stderr, "%s: ignoring invalid rpc line: %v\n", multicasurface.MulticaRuntimeCommandName, err)
			continue
		}
		if err := state.handle(msg, func(response rpcMessage) error {
			return writeRuntimeRPC(cfg.Stdout, response)
		}); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (s *runtimeRPCState) handle(msg rpcMessage, emit func(rpcMessage) error) error {
	emitAll := func(messages ...rpcMessage) error {
		for _, message := range messages {
			if err := emit(message); err != nil {
				return err
			}
		}
		return nil
	}
	switch msg.Method {
	case "initialize":
		return emitAll(rpcMessage{
			ID: msg.ID,
			Result: map[string]any{
				"userAgent":      multicasurface.MulticaRuntimeCommandName + "/" + runtimeVersion,
				"codexHome":      multicasurface.RuntimeEnvValue(s.Env, "CODEX_HOME"),
				"platformFamily": "unix",
				"platformOs":     runtime.GOOS,
			},
		})
	case "initialized":
		return nil
	case "thread/start", "thread/resume":
		s.ThreadID = firstNonEmpty(stringParam(msg.Params, "threadId"), runtimeID("thread", s.now()))
		return emitAll(
			rpcMessage{
				Method: "remoteControl/status/changed",
				Params: map[string]any{
					"status":         "disabled",
					"serverName":     multicasurface.MulticaRuntimeCommandName,
					"installationId": multicasurface.MulticaRuntimeCommandName,
				},
			},
			rpcMessage{
				ID:     msg.ID,
				Result: s.threadStartResult(msg.Params),
			},
			rpcMessage{
				Method: "thread/started",
				Params: map[string]any{
					"thread": s.threadObject(),
				},
			},
		)
	case "thread/name/set", "session/set_model":
		return emitAll(rpcMessage{ID: msg.ID, Result: map[string]any{}})
	case "turn/start":
		s.TurnID = runtimeID("turn", s.now())
		input := multicasurface.RuntimeInputMaterial(msg.Params)
		nowMs := s.now().UnixMilli()
		userItem := multicasurface.RuntimeUserMessage(input.Text)
		if err := emitAll(
			rpcMessage{
				ID: msg.ID,
				Result: map[string]any{
					"turn": s.turnObject("inProgress"),
				},
			},
			rpcMessage{
				Method: "thread/status/changed",
				Params: map[string]any{"threadId": s.ThreadID, "status": map[string]any{"type": "active", "activeFlags": []any{}}},
			},
			rpcMessage{
				Method: "turn/started",
				Params: map[string]any{"threadId": s.ThreadID, "turn": s.turnObject("inProgress")},
			},
			rpcMessage{
				Method: "item/started",
				Params: multicasurface.RuntimeItemParams(s.ThreadID, s.TurnID, userItem, "startedAtMs", nowMs),
			},
			rpcMessage{
				Method: "item/completed",
				Params: multicasurface.RuntimeItemParams(s.ThreadID, s.TurnID, userItem, "completedAtMs", nowMs),
			},
		); err != nil {
			return err
		}
		var progressErr error
		progress := func(event runtimeProgressEvent) {
			if progressErr != nil {
				return
			}
			if strings.TrimSpace(event.Command) != "" {
				progressErr = emitRuntimeMessages(emit, multicasurface.RuntimeCommandExecutionMessages(s.ThreadID, s.TurnID, s.nextItemID("call"), s.CWD, multicasurface.RuntimeCommandExecutionMaterial{
					Command:    event.Command,
					CWD:        event.CWD,
					Output:     event.Output,
					ExitCode:   event.ExitCode,
					DurationMs: event.DurationMs,
				}, s.now()))
				return
			}
			text := strings.TrimSpace(event.Text)
			if text != "" {
				progressErr = emitRuntimeMessages(emit, multicasurface.RuntimeAgentMessageMessages(s.ThreadID, s.TurnID, s.nextItemID("msg"), text, "commentary", s.now()))
			}
		}
		finalAnswer := s.runTurn(input, progress)
		if progressErr != nil {
			return progressErr
		}
		if err := emitRuntimeMessages(emit, multicasurface.RuntimeAgentMessageMessages(s.ThreadID, s.TurnID, s.nextItemID("msg"), finalAnswer, "final_answer", s.now())); err != nil {
			return err
		}
		return emitAll(
			rpcMessage{
				Method: "thread/status/changed",
				Params: map[string]any{"threadId": s.ThreadID, "status": map[string]any{"type": "idle"}},
			},
			rpcMessage{
				Method: "turn/completed",
				Params: map[string]any{"threadId": s.ThreadID, "turn": s.turnObject("completed")},
			},
		)
	default:
		if msg.ID == nil {
			return nil
		}
		return emitAll(rpcMessage{ID: msg.ID, Result: map[string]any{}})
	}
}

func (s *runtimeRPCState) nextItemID(prefix string) string {
	s.ItemSeq++
	if strings.TrimSpace(prefix) == "" {
		prefix = "item"
	}
	return fmt.Sprintf("%s-%d-%d", prefix, s.now().UTC().UnixNano(), s.ItemSeq)
}

func (s *runtimeRPCState) runTurn(input multicasurface.RuntimeInput, progress runtimeProgressSink) string {
	result := s.importIssue(input, progress)
	provider := s.runProviderTurn(input, result, progress)
	if provider.Configured {
		if strings.TrimSpace(provider.Output) != "" && provider.Err == nil {
			return strings.TrimSpace(provider.Output)
		}
		return formatProviderFailureAnswer(provider, result)
	}
	return multicasurface.FormatRuntimeFinalAnswer(runtimeResultSummary(result))
}

func (s *runtimeRPCState) importIssue(input multicasurface.RuntimeInput, progress runtimeProgressSink) runtimeImportResult {
	activation := multicasurface.RuntimeContextFromActivation(s.Env, s.CWD, input)
	taskID := activation.TaskID
	issueID := activation.IssueIdentity
	result := runtimeImportResult{
		IssueID:   issueID,
		Principal: resolveRuntimePrincipalFromContext(s.Env, s.CWD, activation),
		TaskID:    taskID,
	}
	emitRuntimeProgress(progress, "Mnemon runtime accepted the Multica task for "+multicasurface.RuntimePrincipalLabel(result.Principal)+".")
	if issueID == "" {
		result.Status = "skipped"
		result.Err = fmt.Errorf("no Multica issue id was available in task environment or runtime input")
		emitRuntimeProgress(progress, "No assigned Multica issue id was available; Mnemon skipped this turn.")
		return result
	}
	cli := runtimeMulticaCLI(s.Env, activation)
	multicaCtx := context.Background()
	emitRuntimeProgress(progress, "Loading Multica issue "+issueID+".")
	issue, err := cli.GetIssue(multicaCtx, issueID)
	if err != nil {
		result.Status = "failed"
		result.Err = fmt.Errorf("fetch Multica issue %s: %w", issueID, err)
		emitRuntimeCommand(progress, "multica issue get "+issueID, result.Err.Error(), 1)
		emitRuntimeProgress(progress, "Failed to load Multica issue "+issueID+".")
		return result
	}
	emitRuntimeCommand(progress, "multica issue get "+issueID, "Loaded "+runtimeIssueLabel(issue)+".", 0)
	issue = loadRuntimeIssueMetadata(multicaCtx, cli, issue, progress)
	result.IssueID = issue.ID
	result.Identifier = issue.Identifier
	result.Title = issue.Title
	result.Statement = issue.Description
	emitRuntimeProgress(progress, "Loaded "+runtimeIssueLabel(issue)+"; importing Multica surface input.")
	externalID := ""
	if taskID != "" {
		externalID = "multica-task-" + taskID
	}
	draft, err := multicasurface.BuildIssueTeamworkSignal(multicasurface.IssueSignalMaterial{
		ID:          issue.ID,
		Identifier:  issue.Identifier,
		Title:       issue.Title,
		Description: issue.Description,
	}, multicasurface.IssueSignalOptions{
		Scope:       multicasurface.RuntimeEnvDefault(s.Env, "MNEMON_MULTICA_SCOPE", "multica/teamwork"),
		TTL:         multicasurface.RuntimeEnvDefault(s.Env, "MNEMON_MULTICA_TTL", "30m"),
		WhyTeamwork: "Multica assigned this issue to a Mnemon participant, so Mnemon should admit it through the teamwork protocol.",
		WorkspaceID: activation.WorkspaceID,
		TaskID:      taskID,
		AgentID:     activation.AgentID,
		Principal:   result.Principal,
		ContextRefs: []string{
			multicasurface.RuntimeRef("issue", issue.ID),
			multicasurface.RuntimeRef("task", taskID),
			multicasurface.RuntimeRef("agent", activation.AgentID),
		},
		EvidenceRefs: []string{multicasurface.RuntimeRef("issue", issue.ID)},
		ExternalID:   externalID,
	})
	if err != nil {
		result.Status = "failed"
		result.Err = err
		return result
	}
	addr := strings.TrimSpace(activation.ControlAddr)
	if addr == "" {
		result.Status = "skipped"
		emitRuntimeProgress(progress, "Local Mnemon control address is not configured; skipping protocol ingest.")
		return result
	}
	client, err := runtimeControlClient(s.Env, addr, result.Principal)
	if err != nil {
		result.Status = "failed"
		result.Err = err
		emitRuntimeProgress(progress, "Failed to create Local Mnemon control client.")
		return result
	}
	emitRuntimeProgress(progress, "Submitting issue narrative to Mnemon teamwork.")
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
		emitRuntimeCommand(progress, "mnemond ingest observe --principal "+result.Principal, result.Err.Error(), 1)
		emitRuntimeProgress(progress, "Mnemon rejected or failed the issue observation.")
		return result
	}
	result.Status = "recorded"
	result.Receipt = &rec
	emitRuntimeCommand(progress, "mnemond ingest observe --principal "+result.Principal, fmt.Sprintf("recorded seq=%d duplicate=%v ticked=%v", rec.Seq, rec.Dup, rec.Ticked), 0)
	emitRuntimeProgress(progress, fmt.Sprintf("Mnemon recorded the issue observation at seq=%d.", rec.Seq))
	emitRuntimeProgress(progress, "Multica surface input was imported; accepted-state writeback is handled only by explicit Mnemon report commands.")
	return result
}

func (s *runtimeRPCState) runProviderTurn(input multicasurface.RuntimeInput, imported runtimeImportResult, progress runtimeProgressSink) providerTurnResult {
	command := multicasurface.RuntimeEnvValue(s.Env, "MNEMON_MULTICA_PROVIDER_COMMAND")
	if command == "" {
		return providerTurnResult{}
	}
	provider := multicasurface.RuntimeEnvDefault(s.Env, "MNEMON_MULTICA_PROVIDER_RUNTIME", "provider")
	cwd := multicasurface.RuntimeEnvDefault(s.Env, "MNEMON_MULTICA_PROVIDER_WORKSPACE", s.CWD)
	prompt := providerPrompt(input, imported)
	timeout := multicasurface.RuntimeProviderTurnTimeout(s.Env)
	result := providerTurnResult{
		Configured: true,
		Runtime:    provider,
		Command:    command,
		CWD:        cwd,
	}
	emitRuntimeProgress(progress, "Starting Multica-hosted provider turn through "+provider+".")
	started := s.now()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Dir = cwd
	cmd.Env = runtimeProviderEnv(s.Env, imported)
	cmd.Stdin = strings.NewReader(prompt)
	out, err := cmd.CombinedOutput()
	result.DurationMs = s.now().Sub(started).Milliseconds()
	result.Output = strings.TrimSpace(string(out))
	if err != nil {
		result.Err = err
		result.ExitCode = runtimeExitCode(err)
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		if ctx.Err() == context.DeadlineExceeded {
			result.Err = fmt.Errorf("provider turn timed out after %s", timeout)
		}
		emitRuntimeCommandCWD(progress, command, cwd, result.Output, result.ExitCode, result.DurationMs)
		emitRuntimeProgress(progress, "Provider turn failed; Mnemon surface import remains separate from provider execution.")
		return result
	}
	result.ExitCode = 0
	emitRuntimeCommandCWD(progress, command, cwd, result.Output, result.ExitCode, result.DurationMs)
	emitRuntimeProgress(progress, "Provider turn completed; Mnemon did not rewrite provider output.")
	return result
}

func runtimeProviderEnv(env []string, imported runtimeImportResult) []string {
	out := append([]string(nil), env...)
	out = setRuntimeProviderEnv(out, "MULTICA_ISSUE_ID", imported.IssueID)
	out = setRuntimeProviderEnv(out, "MULTICA_TASK_ID", imported.TaskID)
	out = setRuntimeProviderEnv(out, "MNEMON_MULTICA_ISSUE_ID", imported.IssueID)
	out = setRuntimeProviderEnv(out, "MNEMON_MULTICA_ISSUE_IDENTIFIER", imported.Identifier)
	out = setRuntimeProviderEnv(out, "MNEMON_MULTICA_ISSUE_TITLE", imported.Title)
	out = setRuntimeProviderEnv(out, "MNEMON_MULTICA_ISSUE_STATUS", imported.Status)
	out = setRuntimeProviderEnv(out, "MNEMON_MULTICA_PRINCIPAL", imported.Principal)
	return out
}

func setRuntimeProviderEnv(env []string, key, value string) []string {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return env
	}
	next := make([]string, 0, len(env)+1)
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			continue
		}
		next = append(next, entry)
	}
	return append(next, key+"="+value)
}

func providerPrompt(input multicasurface.RuntimeInput, imported runtimeImportResult) string {
	if text := strings.TrimSpace(input.Text); text != "" {
		return text + "\n"
	}
	var b strings.Builder
	label := firstNonEmpty(imported.Identifier, imported.IssueID)
	if label != "" || strings.TrimSpace(imported.Title) != "" {
		b.WriteString("Multica issue")
		if label != "" {
			b.WriteByte(' ')
			b.WriteString(label)
		}
		if strings.TrimSpace(imported.Title) != "" {
			b.WriteString(": ")
			b.WriteString(strings.TrimSpace(imported.Title))
		}
		b.WriteString("\n\n")
	}
	if strings.TrimSpace(imported.Statement) != "" {
		b.WriteString(strings.TrimSpace(imported.Statement))
		b.WriteByte('\n')
	}
	return b.String()
}

func formatProviderFailureAnswer(provider providerTurnResult, imported runtimeImportResult) string {
	var b strings.Builder
	b.WriteString("Provider turn failed")
	if provider.Runtime != "" {
		b.WriteString(" (")
		b.WriteString(provider.Runtime)
		b.WriteString(")")
	}
	if provider.Err != nil {
		b.WriteString(": ")
		b.WriteString(provider.Err.Error())
	}
	if strings.TrimSpace(provider.Output) != "" {
		b.WriteString("\n\n")
		b.WriteString(strings.TrimSpace(provider.Output))
	}
	b.WriteString("\n\n")
	b.WriteString(multicasurface.FormatRuntimeFinalAnswer(runtimeResultSummary(imported)))
	return strings.TrimSpace(b.String())
}

func loadRuntimeIssueMetadata(ctx context.Context, cli driver.MulticaCLI, issue driver.MulticaIssue, progress runtimeProgressSink) driver.MulticaIssue {
	if strings.TrimSpace(issue.ID) == "" {
		return issue
	}
	loaded, count, err := cli.LoadIssueMetadata(ctx, issue)
	if err != nil {
		emitRuntimeCommand(progress, "multica issue metadata list "+issue.ID, err.Error(), 1)
		emitRuntimeProgress(progress, "Multica issue metadata list failed; falling back to metadata returned by issue get.")
		return issue
	}
	emitRuntimeCommand(progress, "multica issue metadata list "+issue.ID, fmt.Sprintf("Loaded %d Multica issue metadata keys.", count), 0)
	return loaded
}

func runtimeMulticaCLI(env []string, activation multicasurface.RuntimeContext) driver.MulticaCLI {
	return driver.MulticaCLI{
		Command:     multicasurface.RuntimeEnvValue(env, "MNEMON_MULTICA_BIN"),
		Profile:     multicasurface.RuntimeEnvValue(env, "MNEMON_MULTICA_PROFILE"),
		ServerURL:   activation.ServerURL,
		WorkspaceID: activation.WorkspaceID,
		Env:         append([]string(nil), env...),
		Timeout:     multicasurface.RuntimeTimeout(env),
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
	token := multicasurface.RuntimeEnvValue(env, "MNEMON_CONTROL_TOKEN")
	if tokenFile := multicasurface.RuntimeEnvValue(env, "MNEMON_CONTROL_TOKEN_FILE"); tokenFile != "" {
		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", fmt.Errorf("read MNEMON_CONTROL_TOKEN_FILE: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	return token, nil
}

func resolveRuntimePrincipal(env []string, cwd string) string {
	return resolveRuntimePrincipalFromContext(env, cwd, multicasurface.RuntimeContextFromActivation(env, cwd, multicasurface.RuntimeInput{}))
}

func resolveRuntimePrincipalFromContext(env []string, cwd string, activation multicasurface.RuntimeContext) string {
	agentID := activation.AgentID
	agentName := activation.AgentName
	if principal := multicasurface.RuntimeMulticaRegistryPrincipal(env, cwd, agentID, agentName); principal != "" {
		return principal
	}
	if principal := activation.ControlPrincipal; principal != "" {
		return principal
	}
	if agentName != "" {
		return sanitizePrincipal(agentName) + "@multica"
	}
	return "multica@runtime"
}

func runtimeResultSummary(result runtimeImportResult) multicasurface.RuntimeResultSummary {
	summary := multicasurface.RuntimeResultSummary{
		IssueID:    result.IssueID,
		Identifier: result.Identifier,
		Title:      result.Title,
		Principal:  result.Principal,
		Status:     result.Status,
	}
	if result.Err != nil {
		summary.Err = result.Err.Error()
	}
	return summary
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

func emitRuntimeMessages(emit func(rpcMessage) error, messages []multicasurface.RuntimeRPCMessage) error {
	for _, message := range messages {
		if err := emit(rpcMessage{Method: message.Method, Params: message.Params}); err != nil {
			return err
		}
	}
	return nil
}

func emitRuntimeProgress(progress runtimeProgressSink, text string) {
	if progress != nil {
		progress(runtimeProgressEvent{Text: text})
	}
}

func emitRuntimeCommand(progress runtimeProgressSink, command, output string, exitCode int) {
	if progress != nil {
		progress(runtimeProgressEvent{Command: command, Output: output, ExitCode: exitCode})
	}
}

func emitRuntimeCommandCWD(progress runtimeProgressSink, command, cwd, output string, exitCode int, durationMs int64) {
	if progress != nil {
		progress(runtimeProgressEvent{Command: command, CWD: cwd, Output: output, ExitCode: exitCode, DurationMs: durationMs})
	}
}

func runtimeExitCode(err error) int {
	if err != nil {
		return 1
	}
	return 0
}

func runtimeIssueLabel(issue driver.MulticaIssue) string {
	return multicasurface.RuntimeIssueLabel(issue.ID, issue.Identifier, issue.Title)
}

func wantsVersion(args []string) bool {
	fs := flag.NewFlagSet(multicasurface.MulticaRuntimeCommandName, flag.ContinueOnError)
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

func wantsRuntimeHelp(args []string) bool {
	for _, arg := range args {
		switch strings.TrimSpace(arg) {
		case "help", "--help", "-help", "-h":
			return true
		}
	}
	return false
}

func writeRuntimeHelp(w io.Writer) {
	fmt.Fprintf(w, "%s is an external Multica runtime adapter for the Mnemon event system.\n", multicasurface.MulticaRuntimeCommandName)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintf(w, "  %s app-server --listen stdio://\n", multicasurface.MulticaRuntimeCommandName)
	fmt.Fprintf(w, "  %s --version\n", multicasurface.MulticaRuntimeCommandName)
	fmt.Fprintf(w, "  %s --probe app-server --listen stdio://\n", multicasurface.MulticaRuntimeCommandName)
	fmt.Fprintln(w)
	fmt.Fprintln(w, "The adapter receives Multica runtime RPC on stdin/stdout, loads issue context through the Multica CLI, and imports Multica surface input into mnemond when configured. Accepted-state Multica writeback is handled only by explicit Mnemon report commands or activation carriers.")
}

func runtimeProbeModeEnabled(args []string) bool {
	for _, arg := range args {
		switch strings.TrimSpace(arg) {
		case "probe", "diagnose", "--probe", "-probe", "--diagnose", "-diagnose":
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
