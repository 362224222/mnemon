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
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const runtimeVersion = "dev"

var (
	assignedIssuePattern       = regexp.MustCompile(`(?i)(?:assigned\s+issue\s+id\s+is|issue[_\s-]*id)\s*[:：]\s*([A-Za-z0-9][A-Za-z0-9._:-]*)`)
	multicaIssueMentionPattern = regexp.MustCompile(`(?i)mention://issue/([A-Za-z0-9][A-Za-z0-9._:-]*)`)
	multicaTaggedIssuePattern  = regexp.MustCompile(`(?i)(?:^|[\s([{"'])[@#]([A-Z][A-Z0-9]+-\d+)(?:$|[\s\])}.,;:'"])`)
)

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
	Trace      *driver.ManagedTurnTraceEvent
}

type runtimeImportResult struct {
	IssueID               string
	Identifier            string
	Title                 string
	Statement             string
	Principal             string
	TaskID                string
	HubMetadata           driver.MulticaHubMetadata
	HubBackend            string
	HubKind               string
	SessionID             string
	CorrelationID         string
	RootIssueID           string
	AssignmentID          string
	AssignmentFingerprint string
	MatchTerms            []string
	Status                string
	Receipt               *access.IngestReceipt
	ProjectionStatus      string
	ProjectionCommentID   string
	ProjectionErr         error
	WakeStatus            string
	WakeTurnID            string
	WakeErr               error
	HubWriteStatus        string
	HubChildIssues        int
	HubFeedbackComments   int
	HubWriteErr           error
	Err                   error
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
		return runRuntimeProbe(cfg)
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
				"userAgent":      "mnemon-multica-runtime/" + runtimeVersion,
				"codexHome":      envValue(s.Env, "CODEX_HOME"),
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
					"serverName":     "mnemon-multica-runtime",
					"installationId": "mnemon-multica-runtime",
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
		input := extractRuntimeInput(msg.Params)
		nowMs := s.now().UnixMilli()
		userItem := multicasurface.RuntimeUserMessage(input)
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
			if event.Trace != nil {
				progressErr = emitRuntimeMessages(emit, multicasurface.RuntimeManagedTraceMessages(s.ThreadID, s.TurnID, *event.Trace, s.now()))
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

func (s *runtimeRPCState) runTurn(input string, progress runtimeProgressSink) string {
	result := s.importIssue(input, progress)
	return formatRuntimeFinalAnswer(result)
}

func (s *runtimeRPCState) importIssue(input string, progress runtimeProgressSink) runtimeImportResult {
	taskID := envValue(s.Env, "MULTICA_TASK_ID")
	issueID := firstNonEmpty(envValue(s.Env, "MULTICA_ISSUE_ID"), extractAssignedIssueID(input))
	result := runtimeImportResult{
		IssueID:   issueID,
		Principal: resolveRuntimePrincipal(s.Env, s.CWD),
		TaskID:    taskID,
	}
	emitRuntimeProgress(progress, "Mnemon runtime accepted the Multica task for "+displayRuntimePrincipal(result.Principal)+".")
	if issueID == "" {
		result.Status = "skipped"
		result.Err = fmt.Errorf("no Multica issue id was available in task environment or runtime input")
		emitRuntimeProgress(progress, "No assigned Multica issue id was available; Mnemon skipped this turn.")
		return result
	}
	cli := runtimeMulticaCLI(s.Env)
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
	result.HubMetadata = driver.MulticaIssueHubMetadata(issue)
	applyMulticaHubMetadata(&result, issue)
	result.HubBackend = firstNonEmpty(result.HubBackend, envValue(s.Env, "MNEMON_HUB_BACKEND"))
	emitRuntimeProgress(progress, "Loaded "+runtimeIssueLabel(issue)+"; classifying Mnemon hub metadata.")
	markIssueInProgress(multicaCtx, cli, issue.ID)
	if result.HubMetadata.IsAssignmentMailbox() {
		return s.correlateAssignmentMailbox(multicaCtx, cli, issue, &result, progress)
	}
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
	if strings.EqualFold(result.HubBackend, driver.MulticaHubBackend) {
		ensureRootSessionHubFields(&result, issue)
		addPayloadRuleString(draft.Payload, "hub_backend", driver.MulticaHubBackend)
		addPayloadRuleString(draft.Payload, "root_issue_id", result.RootIssueID)
		addPayloadRuleString(draft.Payload, "session_id", result.SessionID)
		addPayloadRuleString(draft.Payload, "source_issue_id", issue.ID)
		if err := cli.SetIssueMetadataMap(multicaCtx, issue.ID, rootSessionMetadata(result, draft, s.now())); err != nil {
			metadataErr := fmt.Errorf("set Multica root session metadata: %w", err)
			emitRuntimeCommand(progress, "multica issue metadata set "+issue.ID+" mnemon.root-session", metadataErr.Error(), 1)
			emitRuntimeProgress(progress, "Root session metadata write failed; continuing with Mnemon ingest from issue context.")
		} else {
			emitRuntimeCommand(progress, "multica issue metadata set "+issue.ID+" mnemon.root-session", "Root session metadata written for "+runtimeIssueLabel(issue)+".", 0)
			emitRuntimeProgress(progress, "Root session metadata written for "+runtimeIssueLabel(issue)+".")
		}
	}
	addr := strings.TrimSpace(envValue(s.Env, "MNEMON_CONTROL_ADDR"))
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
	emitRuntimeProgress(progress, "Waking the managed local agent with [mnemon:wake].")
	earlyHubDeltas := s.wakeManagedAgentWithHubProjection(multicaCtx, cli, client, issue, &result, progress)
	emitRuntimeCommand(progress, "mnemond managed wake --principal "+result.Principal+" [mnemon:wake]", runtimeWakeProgress(result), runtimeExitCode(result.WakeErr))
	emitRuntimeProgress(progress, runtimeWakeProgress(result))
	s.writeMulticaHubArtifacts(multicaCtx, cli, client, issue, &result)
	mergeRuntimeHubProjectionDeltas(&result, earlyHubDeltas)
	emitRuntimeCommand(progress, "mnemon multica hub project --issue "+issue.ID, runtimeHubWriteProgress(result), runtimeExitCode(result.HubWriteErr))
	emitRuntimeProgress(progress, runtimeHubWriteProgress(result))
	s.projectImportComment(multicaCtx, cli, issue, draft.ExternalID, &result)
	emitRuntimeCommand(progress, "multica issue comment add "+issue.ID, runtimeProjectionProgress(result), runtimeExitCode(result.ProjectionErr))
	emitRuntimeProgress(progress, runtimeProjectionProgress(result))
	return result
}

func loadRuntimeIssueMetadata(ctx context.Context, cli driver.MulticaCLI, issue driver.MulticaIssue, progress runtimeProgressSink) driver.MulticaIssue {
	if strings.TrimSpace(issue.ID) == "" {
		return issue
	}
	listed, err := cli.ListIssueMetadata(ctx, issue.ID)
	if err != nil {
		emitRuntimeCommand(progress, "multica issue metadata list "+issue.ID, err.Error(), 1)
		emitRuntimeProgress(progress, "Multica issue metadata list failed; falling back to metadata returned by issue get.")
		return issue
	}
	merged := map[string]any{}
	for key, value := range issue.Metadata {
		merged[key] = value
	}
	for key, value := range listed {
		if strings.TrimSpace(key) != "" {
			merged[key] = value
		}
	}
	issue.Metadata = merged
	emitRuntimeCommand(progress, "multica issue metadata list "+issue.ID, fmt.Sprintf("Loaded %d Multica issue metadata keys.", len(listed)), 0)
	return issue
}

func (s *runtimeRPCState) correlateAssignmentMailbox(ctx context.Context, cli driver.MulticaCLI, issue driver.MulticaIssue, result *runtimeImportResult, progress runtimeProgressSink) runtimeImportResult {
	ensureAssignmentHubFields(result, issue)
	result.Status = "correlated"
	result.MatchTerms = cleanRuntimeTerms(
		result.AssignmentID,
		result.AssignmentFingerprint,
		result.HubMetadata.EventID,
		string(eventmodel.Subject("assignment", result.AssignmentID)),
	)
	emitRuntimeCommand(progress, "mnemon multica assignment correlate --issue "+issue.ID, "Assignment mailbox correlated: "+runtimeAssignmentLabel(*result)+".", 0)
	emitRuntimeProgress(progress, "Assignment mailbox correlated: "+runtimeAssignmentLabel(*result)+".")
	addr := strings.TrimSpace(envValue(s.Env, "MNEMON_CONTROL_ADDR"))
	var client *access.Client
	if addr == "" {
		result.WakeStatus = "skipped"
		result.WakeErr = fmt.Errorf("MNEMON_CONTROL_ADDR is not set")
		emitRuntimeProgress(progress, "Local Mnemon control address is not configured; skipping managed wake.")
	} else {
		var err error
		client, err = runtimeControlClient(s.Env, addr, result.Principal)
		if err != nil {
			result.WakeStatus = "failed"
			result.WakeErr = err
			emitRuntimeProgress(progress, "Failed to create Local Mnemon control client.")
		} else {
			emitRuntimeProgress(progress, "Waking assigned local agent with [mnemon:wake].")
			s.wakeManagedAgent(result, progress)
			emitRuntimeCommand(progress, "mnemond managed wake --principal "+result.Principal+" [mnemon:wake]", runtimeWakeProgress(*result), runtimeExitCode(result.WakeErr))
			emitRuntimeProgress(progress, runtimeWakeProgress(*result))
			s.writeMulticaHubArtifacts(ctx, cli, client, issue, result)
			emitRuntimeCommand(progress, "mnemon multica hub project --issue "+issue.ID, runtimeHubWriteProgress(*result), runtimeExitCode(result.HubWriteErr))
			emitRuntimeProgress(progress, runtimeHubWriteProgress(*result))
		}
	}
	s.projectImportComment(ctx, cli, issue, assignmentMailboxMarker(*result), result)
	emitRuntimeCommand(progress, "multica issue comment add "+issue.ID, runtimeProjectionProgress(*result), runtimeExitCode(result.ProjectionErr))
	emitRuntimeProgress(progress, runtimeProjectionProgress(*result))
	return *result
}

func applyMulticaHubMetadata(result *runtimeImportResult, issue driver.MulticaIssue) {
	if result == nil {
		return
	}
	meta := result.HubMetadata
	result.HubBackend = firstNonEmpty(meta.HubBackend, result.HubBackend)
	result.HubKind = firstNonEmpty(meta.Kind, result.HubKind)
	result.SessionID = firstNonEmpty(meta.SessionID, result.SessionID)
	result.CorrelationID = firstNonEmpty(meta.CorrelationID, result.CorrelationID)
	result.RootIssueID = firstNonEmpty(meta.RootIssueID, result.RootIssueID)
	result.AssignmentID = firstNonEmpty(meta.AssignmentID, result.AssignmentID)
	result.AssignmentFingerprint = firstNonEmpty(meta.AssignmentFingerprint, result.AssignmentFingerprint)
	if result.RootIssueID == "" && meta.IsAssignmentMailbox() {
		result.RootIssueID = firstNonEmpty(meta.SourceIssueID, issue.ID)
	}
}

func ensureRootSessionHubFields(result *runtimeImportResult, issue driver.MulticaIssue) {
	if result == nil {
		return
	}
	result.HubBackend = driver.MulticaHubBackend
	result.HubKind = firstNonEmpty(result.HubKind, driver.MulticaHubKindSession)
	result.RootIssueID = firstNonEmpty(result.RootIssueID, issue.ID)
	result.SessionID = firstNonEmpty(result.SessionID, driver.MulticaSessionID(result.RootIssueID))
	result.CorrelationID = firstNonEmpty(result.CorrelationID, "multica:issue:"+issue.ID)
}

func ensureAssignmentHubFields(result *runtimeImportResult, issue driver.MulticaIssue) {
	if result == nil {
		return
	}
	result.HubBackend = firstNonEmpty(result.HubBackend, driver.MulticaHubBackend)
	result.HubKind = firstNonEmpty(result.HubKind, driver.MulticaHubKindAssignmentMailbox)
	result.RootIssueID = firstNonEmpty(result.RootIssueID, result.HubMetadata.SourceIssueID)
	result.SessionID = firstNonEmpty(result.SessionID, driver.MulticaSessionID(result.RootIssueID))
	result.CorrelationID = firstNonEmpty(result.CorrelationID, "multica:issue:"+issue.ID)
}

func rootSessionMetadata(result runtimeImportResult, draft driver.MulticaObservedDraft, now time.Time) map[string]string {
	meta := driver.MulticaHubMetadata{
		SchemaVersion:   "1",
		HubBackend:      driver.MulticaHubBackend,
		Kind:            driver.MulticaHubKindSession,
		SessionID:       result.SessionID,
		CorrelationID:   result.CorrelationID,
		EventID:         draft.ExternalID,
		EventType:       draft.EventType,
		EventPhase:      string(eventmodel.PhaseObserved),
		Principal:       result.Principal,
		SourceIssueID:   result.IssueID,
		RootIssueID:     result.RootIssueID,
		ProjectionOwner: result.Principal,
		ProjectedAt:     now.UTC().Format(time.RFC3339),
	}
	return meta.Map()
}

func addPayloadRuleString(payload map[string]any, key, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if payload == nil || key == "" || value == "" {
		return
	}
	rule, _ := payload[eventmodel.PayloadRuleKey].(map[string]any)
	if rule == nil {
		rule = map[string]any{}
		payload[eventmodel.PayloadRuleKey] = rule
	}
	rule[key] = value
}

func cleanRuntimeTerms(values ...string) []string {
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

func assignmentMailboxMarker(result runtimeImportResult) string {
	if result.HubMetadata.EventID != "" {
		return result.HubMetadata.EventID
	}
	if result.AssignmentID != "" {
		return "multica-assignment-" + result.AssignmentID
	}
	return "multica-issue-" + result.IssueID
}

func (s *runtimeRPCState) wakeManagedAgent(result *runtimeImportResult, progress runtimeProgressSink) {
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
		TraceSink: driver.ManagedTurnTraceSinkFunc(func(event driver.ManagedTurnTraceEvent) {
			if progress != nil {
				progress(runtimeProgressEvent{Trace: &event})
			}
		}),
		Now: func() time.Time { return s.now() },
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

type runtimeHubProjectionDelta struct {
	ChildIssues      int
	FeedbackComments int
	Status           string
	Err              error
}

func (d runtimeHubProjectionDelta) active() bool {
	return d.ChildIssues > 0 || d.FeedbackComments > 0
}

func (s *runtimeRPCState) wakeManagedAgentWithHubProjection(ctx context.Context, cli driver.MulticaCLI, client *access.Client, rootIssue driver.MulticaIssue, result *runtimeImportResult, progress runtimeProgressSink) []runtimeHubProjectionDelta {
	if result == nil || client == nil ||
		!strings.EqualFold(result.HubBackend, driver.MulticaHubBackend) ||
		result.HubKind != driver.MulticaHubKindSession ||
		!runtimeMulticaHubWriteEnabled(s.Env) {
		s.wakeManagedAgent(result, progress)
		return nil
	}
	done := make(chan struct{})
	deltas := make(chan runtimeHubProjectionDelta, 16)
	go func(snapshot runtimeImportResult) {
		defer close(deltas)
		ticker := time.NewTicker(runtimeHubProjectionInterval(s.Env))
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				delta := s.writeMulticaHubArtifactsDelta(ctx, cli, client, rootIssue, snapshot)
				if delta.active() {
					select {
					case deltas <- delta:
					default:
					}
				}
			}
		}
	}(*result)
	s.wakeManagedAgent(result, progress)
	close(done)
	var out []runtimeHubProjectionDelta
	for delta := range deltas {
		out = append(out, delta)
	}
	return out
}

func (s *runtimeRPCState) writeMulticaHubArtifactsDelta(ctx context.Context, cli driver.MulticaCLI, client *access.Client, rootIssue driver.MulticaIssue, base runtimeImportResult) runtimeHubProjectionDelta {
	snapshot := base
	snapshot.HubChildIssues = 0
	snapshot.HubFeedbackComments = 0
	snapshot.HubWriteStatus = ""
	snapshot.HubWriteErr = nil
	s.writeMulticaHubArtifacts(ctx, cli, client, rootIssue, &snapshot)
	return runtimeHubProjectionDelta{
		ChildIssues:      snapshot.HubChildIssues,
		FeedbackComments: snapshot.HubFeedbackComments,
		Status:           snapshot.HubWriteStatus,
		Err:              snapshot.HubWriteErr,
	}
}

func mergeRuntimeHubProjectionDeltas(result *runtimeImportResult, deltas []runtimeHubProjectionDelta) {
	if result == nil || len(deltas) == 0 {
		return
	}
	for _, delta := range deltas {
		result.HubChildIssues += delta.ChildIssues
		result.HubFeedbackComments += delta.FeedbackComments
	}
	if result.HubWriteErr != nil {
		return
	}
	switch {
	case result.HubChildIssues > 0 && result.HubFeedbackComments > 0:
		result.HubWriteStatus = "updated"
	case result.HubChildIssues > 0:
		result.HubWriteStatus = "created"
	case result.HubFeedbackComments > 0:
		result.HubWriteStatus = "commented"
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
	if len(result.MatchTerms) > 0 {
		return cleanRuntimeTerms(result.MatchTerms...)
	}
	if result.AssignmentID != "" || result.AssignmentFingerprint != "" {
		return cleanRuntimeTerms(result.AssignmentID, result.AssignmentFingerprint)
	}
	raw := []string{result.IssueID, result.Identifier, result.Title, result.TaskID}
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
	if value := strings.TrimSpace(result.Statement); len(value) >= 3 {
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

func (s *runtimeRPCState) projectImportComment(ctx context.Context, cli driver.MulticaCLI, issue driver.MulticaIssue, externalID string, result *runtimeImportResult) {
	if result == nil {
		return
	}
	if !runtimeProjectionEnabled(s.Env) {
		result.ProjectionStatus = "skipped"
		return
	}
	title := "issue admitted"
	if result.HubKind == driver.MulticaHubKindAssignmentMailbox || result.Status == "correlated" {
		title = "assignment mailbox correlated"
	}
	commentBody := projection.FormatComment(projection.CommentMaterial{
		Title:    title,
		Body:     multicasurface.RuntimeProjectionCommentBody(runtimeProjectionMaterial(issue, *result)),
		EventIDs: []string{externalID},
	})
	comment, err := cli.AddIssueComment(ctx, issue.ID, commentBody)
	if err != nil {
		result.ProjectionStatus = "failed"
		result.ProjectionErr = err
		return
	}
	result.ProjectionStatus = "commented"
	result.ProjectionCommentID = comment.ID
}

func runtimeProjectionMaterial(issue driver.MulticaIssue, result runtimeImportResult) multicasurface.RuntimeProjectionMaterial {
	material := multicasurface.RuntimeProjectionMaterial{
		AssignmentMailbox:  result.HubKind == driver.MulticaHubKindAssignmentMailbox || result.Status == "correlated",
		Status:             result.Status,
		IssueID:            issue.ID,
		IssueLabel:         firstNonEmpty(issue.Identifier, issue.ID),
		Principal:          result.Principal,
		TaskID:             result.TaskID,
		HubBackend:         result.HubBackend,
		SessionID:          result.SessionID,
		RootIssueID:        result.RootIssueID,
		RootIssueLabel:     firstNonEmpty(result.RootIssueID, issue.Identifier),
		AssignmentID:       result.AssignmentID,
		WakeStatus:         result.WakeStatus,
		WakeTurnID:         result.WakeTurnID,
		HubWriteStatus:     result.HubWriteStatus,
		HubChildIssues:     result.HubChildIssues,
		HubFeedbackComment: result.HubFeedbackComments,
	}
	if result.Receipt != nil {
		material.HasIngestReceipt = true
		material.IngestSeq = result.Receipt.Seq
		material.IngestDuplicate = result.Receipt.Dup
		material.IngestTicked = result.Receipt.Ticked
	}
	return material
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
	case "correlated":
		b.WriteString(" Mnemon assignment mailbox: correlated")
		if result.AssignmentID != "" {
			b.WriteString(" assignment=")
			b.WriteString(result.AssignmentID)
		}
		if result.SessionID != "" {
			b.WriteString(" session=")
			b.WriteString(result.SessionID)
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
	switch result.HubWriteStatus {
	case "created", "commented", "updated", "noop":
		b.WriteString(" Multica hub write: ")
		b.WriteString(result.HubWriteStatus)
		if result.HubChildIssues > 0 {
			b.WriteString(fmt.Sprintf(" child_issues=%d", result.HubChildIssues))
		}
		if result.HubFeedbackComments > 0 {
			b.WriteString(fmt.Sprintf(" feedback_comments=%d", result.HubFeedbackComments))
		}
		b.WriteString(".")
	case "skipped":
		b.WriteString(" Multica hub write: skipped.")
	case "failed":
		b.WriteString(" Multica hub write: failed")
		if result.HubWriteErr != nil {
			b.WriteString(" (")
			b.WriteString(result.HubWriteErr.Error())
			b.WriteString(")")
		}
		b.WriteString(".")
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

func runtimeExitCode(err error) int {
	if err != nil {
		return 1
	}
	return 0
}

func displayRuntimePrincipal(principal string) string {
	principal = strings.TrimSpace(principal)
	if principal == "" {
		return "the resolved principal"
	}
	return principal
}

func runtimeIssueLabel(issue driver.MulticaIssue) string {
	if strings.TrimSpace(issue.Identifier) != "" && strings.TrimSpace(issue.Title) != "" {
		return issue.Identifier + " (" + issue.Title + ")"
	}
	if strings.TrimSpace(issue.Identifier) != "" {
		return issue.Identifier
	}
	if strings.TrimSpace(issue.ID) != "" {
		return issue.ID
	}
	return "the Multica issue"
}

func runtimeAssignmentLabel(result runtimeImportResult) string {
	if strings.TrimSpace(result.AssignmentID) != "" {
		return result.AssignmentID
	}
	if strings.TrimSpace(result.AssignmentFingerprint) != "" {
		return result.AssignmentFingerprint
	}
	return "assignment mailbox"
}

func runtimeWakeProgress(result runtimeImportResult) string {
	switch strings.TrimSpace(result.WakeStatus) {
	case "":
		if result.WakeErr != nil {
			return "Managed wake failed: " + result.WakeErr.Error()
		}
		return "Managed wake did not run."
	case "completed":
		if strings.TrimSpace(result.WakeTurnID) != "" {
			return "Managed wake completed: turn=" + result.WakeTurnID + "."
		}
		return "Managed wake completed."
	case "skipped":
		if result.WakeErr != nil {
			return "Managed wake skipped: " + result.WakeErr.Error()
		}
		return "Managed wake skipped."
	case "failed":
		if result.WakeErr != nil {
			return "Managed wake failed: " + result.WakeErr.Error()
		}
		return "Managed wake failed."
	default:
		return "Managed wake status: " + result.WakeStatus + "."
	}
}

func runtimeHubWriteProgress(result runtimeImportResult) string {
	switch strings.TrimSpace(result.HubWriteStatus) {
	case "":
		return ""
	case "failed":
		if result.HubWriteErr != nil {
			return "Multica hub projection failed: " + result.HubWriteErr.Error()
		}
		return "Multica hub projection failed."
	case "skipped":
		return "Multica hub projection skipped."
	default:
		return fmt.Sprintf("Multica hub projection %s: child issues=%d, feedback comments=%d.", result.HubWriteStatus, result.HubChildIssues, result.HubFeedbackComments)
	}
}

func runtimeProjectionProgress(result runtimeImportResult) string {
	switch strings.TrimSpace(result.ProjectionStatus) {
	case "":
		return ""
	case "failed":
		if result.ProjectionErr != nil {
			return "Multica comment projection failed: " + result.ProjectionErr.Error()
		}
		return "Multica comment projection failed."
	case "skipped":
		return "Multica comment projection skipped."
	default:
		if strings.TrimSpace(result.ProjectionCommentID) != "" {
			return "Multica comment projection completed: comment=" + result.ProjectionCommentID + "."
		}
		return "Multica comment projection completed."
	}
}

func markIssueInProgress(ctx context.Context, cli driver.MulticaCLI, issueID string) {
	_, _ = cli.SetIssueStatus(ctx, issueID, "in_progress")
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
	match := multicaIssueMentionPattern.FindStringSubmatch(input)
	if len(match) >= 2 {
		return strings.Trim(match[1], " \t\r\n.,;)")
	}
	match = assignedIssuePattern.FindStringSubmatch(input)
	if len(match) >= 2 {
		return strings.Trim(match[1], " \t\r\n.,;)")
	}
	match = multicaTaggedIssuePattern.FindStringSubmatch(input)
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

func envValue(env []string, key string) string {
	prefix := key + "="
	for i := len(env) - 1; i >= 0; i-- {
		item := env[i]
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
		raw = envValue(env, "MULTICA_HTTP_TIMEOUT")
	}
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

func runtimeHubProjectionInterval(env []string) time.Duration {
	raw := envValue(env, "MNEMON_MULTICA_HUB_PROJECT_INTERVAL")
	if raw == "" {
		return 5 * time.Second
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 5 * time.Second
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
