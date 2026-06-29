package drive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/codexapp"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

type HTTPRenderClient struct {
	BaseURL   string
	Token     string
	Principal contract.ActorID
	HTTP      *http.Client
}

func (c HTTPRenderClient) Render(ctx context.Context, req presentation.Request) (presentation.Response, error) {
	base := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if base == "" {
		return presentation.Response{}, fmt.Errorf("render client requires base URL")
	}
	req.Principal = c.Principal
	body, err := json.Marshal(req)
	if err != nil {
		return presentation.Response{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/render", bytes.NewReader(body))
	if err != nil {
		return presentation.Response{}, err
	}
	if strings.TrimSpace(c.Token) != "" {
		httpReq.Header.Set("Authorization", "Bearer "+strings.TrimSpace(c.Token))
	} else {
		httpReq.Header.Set(access.PrincipalHeader, string(c.Principal))
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return presentation.Response{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return presentation.Response{}, fmt.Errorf("render failed: %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var out presentation.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return presentation.Response{}, err
	}
	return out, nil
}

func ManagedWakeCandidatesFromRender(principal string, resp presentation.Response) []ManagedWakeCandidate {
	candidates := ManagedWakeCandidatesFromEvents(principal, resp.Events)
	for i := range candidates {
		candidates[i].RenderAuditID = resp.AuditID
		candidates[i].RenderBodyDigest = resp.BodyDigest
	}
	return candidates
}

type FileManagedWakeLedger struct {
	path    string
	mu      sync.Mutex
	loaded  bool
	loadErr error
	seen    map[string]ManagedWakeRecord
}

func NewFileManagedWakeLedger(path string) *FileManagedWakeLedger {
	return &FileManagedWakeLedger{path: path, seen: map[string]ManagedWakeRecord{}}
}

func (l *FileManagedWakeLedger) Seen(candidate ManagedWakeCandidate) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loadLocked()
	record, ok := l.seen[managedWakeKey(candidate)]
	return ok && managedWakeRecordHandled(record)
}

func (l *FileManagedWakeLedger) Record(record ManagedWakeRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loadLocked()
	if l.loadErr != nil {
		return l.loadErr
	}
	if strings.TrimSpace(l.path) == "" {
		return fmt.Errorf("managed wake ledger path is required")
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(record); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	l.seen[managedWakeKey(ManagedWakeCandidate{
		Principal:      record.Principal,
		DerivedEventID: record.DerivedEventID,
		BodyDigest:     record.BodyDigest,
	})] = record
	return nil
}

func (l *FileManagedWakeLedger) loadLocked() {
	if l.loaded {
		return
	}
	l.loaded = true
	if strings.TrimSpace(l.path) == "" {
		l.loadErr = fmt.Errorf("managed wake ledger path is required")
		return
	}
	f, err := os.Open(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		l.loadErr = err
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record ManagedWakeRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			l.loadErr = err
			return
		}
		l.seen[managedWakeKey(ManagedWakeCandidate{
			Principal:      record.Principal,
			DerivedEventID: record.DerivedEventID,
			BodyDigest:     record.BodyDigest,
		})] = record
	}
	if err := scanner.Err(); err != nil {
		l.loadErr = err
	}
}

type CodexAppServerTurnClient struct {
	Principal      string
	Command        string
	Workspace      string
	Env            []string
	TurnTimeout    time.Duration
	RequestTimeout time.Duration
	ClientName     string
	ClientVersion  string
}

func (c CodexAppServerTurnClient) StartTurn(ctx context.Context, query string) (ManagedTurnResult, error) {
	return c.StartTurnWithTrace(ctx, query, nil)
}

func (c CodexAppServerTurnClient) StartTurnWithTrace(ctx context.Context, query string, sink ManagedTurnTraceSink) (ManagedTurnResult, error) {
	if strings.TrimSpace(query) != ManagedWakeQuery {
		return ManagedTurnResult{}, fmt.Errorf("managed codex appserver client only accepts %q queries", ManagedWakeQuery)
	}
	command := strings.TrimSpace(c.Command)
	if command == "" {
		command = "codex"
	}
	workspace := strings.TrimSpace(c.Workspace)
	if workspace == "" {
		workspace = "."
	}
	requestTimeout := c.RequestTimeout
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	turnTimeout := c.TurnTimeout
	if turnTimeout <= 0 {
		turnTimeout = 5 * time.Minute
	}
	clientName := strings.TrimSpace(c.ClientName)
	if clientName == "" {
		clientName = "mnemond-managed-agent"
	}
	clientVersion := strings.TrimSpace(c.ClientVersion)
	if clientVersion == "" {
		clientVersion = "dev"
	}
	server := codexapp.New(command, workspace)
	if len(c.Env) > 0 {
		server.SetEnv(c.Env)
	}
	if err := server.Start(); err != nil {
		return ManagedTurnResult{}, err
	}
	defer server.Close()
	if _, err := server.Request("initialize", map[string]any{
		"clientInfo":   map[string]any{"name": clientName, "version": clientVersion},
		"capabilities": codexAppServerCapabilities(),
	}, requestTimeout); err != nil {
		return ManagedTurnResult{}, fmt.Errorf("initialize: %w", err)
	}
	developerInstructions, err := CodexAppServerDeveloperInstructions(ctx, workspace, c.Env, ManagedWakeQuery)
	if err != nil {
		return ManagedTurnResult{}, fmt.Errorf("load hook context: %w", err)
	}
	threadParams := map[string]any{
		"cwd":            workspace,
		"approvalPolicy": "never",
		"sandbox":        "danger-full-access",
		"ephemeral":      true,
	}
	if developerInstructions != "" {
		threadParams["developerInstructions"] = developerInstructions
	}
	thread, err := server.Request("thread/start", threadParams, requestTimeout)
	if err != nil {
		return ManagedTurnResult{}, fmt.Errorf("thread/start: %w", err)
	}
	threadID := codexapp.ThreadID(thread)
	if threadID == "" {
		return ManagedTurnResult{}, fmt.Errorf("thread/start returned no thread id")
	}
	turnParams := map[string]any{
		"threadId":       threadID,
		"input":          []map[string]any{{"type": "text", "text": ManagedWakeQuery}},
		"cwd":            workspace,
		"approvalPolicy": "never",
		"sandboxPolicy":  map[string]any{"type": "dangerFullAccess"},
	}
	before := server.NotificationCount()
	if _, err := server.Request("turn/start", turnParams, requestTimeout); err != nil {
		return ManagedTurnResult{}, fmt.Errorf("turn/start: %w", err)
	}
	emitTrace := func(notifications []map[string]any) {
		if sink == nil {
			return
		}
		for _, event := range ManagedTurnTraceEventsFromCodexNotifications(c.Principal, notifications) {
			sink.OnManagedTurnTrace(event)
		}
	}
	if _, err := server.WaitNotificationWithCallback("turn/completed", turnTimeout, before, emitTrace); err != nil {
		text := codexapp.CombinedText(server.NotificationsSince(before))
		return ManagedTurnResult{TurnID: threadID, Status: "failed", FinalAnswer: text}, fmt.Errorf("wait turn/completed: %w", err)
	}
	notifications := server.NotificationsSince(before)
	answer := codexapp.FinalAnswer(notifications)
	if answer == "" {
		answer = codexapp.CombinedText(notifications)
	}
	return ManagedTurnResult{TurnID: threadID, Status: "completed", FinalAnswer: answer}, nil
}

func CodexAppServerDeveloperInstructions(ctx context.Context, workspace string, env []string, query string) (string, error) {
	additionalContext, err := CodexAppServerAdditionalContext(ctx, workspace, env, query)
	if err != nil {
		return "", err
	}
	return formatCodexAppServerDeveloperInstructions(additionalContext), nil
}

func CodexAppServerAdditionalContext(ctx context.Context, workspace string, env []string, query string) (map[string]any, error) {
	out := map[string]any{}
	for _, lifecycle := range []string{"prime", "remind"} {
		message, err := runCodexLifecycleHook(ctx, workspace, env, lifecycle, query)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(message) == "" {
			continue
		}
		out["mnemon.hook."+lifecycle] = map[string]any{
			"kind":  "application",
			"value": message,
		}
	}
	return out, nil
}

func formatCodexAppServerDeveloperInstructions(entries map[string]any) string {
	if len(entries) == 0 {
		return ""
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("Mnemon managed wake context.\n")
	b.WriteString("The user input for this turn is only the local wake sentinel `[mnemon:wake]`. Use the application context below as the governed Mnemon context for deciding whether to read or record state. If a teamwork signal says assignment or self-assignment may be useful, act through the Mnemon commands described in the guide instead of answering that no task was supplied.\n")
	for _, key := range keys {
		value := additionalContextEntryValue(entries[key])
		if value == "" {
			continue
		}
		b.WriteString("\n## ")
		b.WriteString(key)
		b.WriteString("\n")
		b.WriteString(value)
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func additionalContextEntryValue(entry any) string {
	switch v := entry.(type) {
	case string:
		return strings.TrimSpace(v)
	case map[string]any:
		if value, ok := v["value"].(string); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func runCodexLifecycleHook(ctx context.Context, workspace string, env []string, lifecycle string, query string) (string, error) {
	hook := filepath.Join(workspace, ".codex", "hooks", "mnemon-r1", lifecycle+".sh")
	if _, err := os.Stat(hook); errors.Is(err, os.ErrNotExist) {
		return "", nil
	} else if err != nil {
		return "", err
	}
	stdin := "{}"
	if lifecycle == "remind" {
		raw, err := json.Marshal(map[string]string{"prompt": query})
		if err != nil {
			return "", err
		}
		stdin = string(raw)
	}
	cmd := exec.CommandContext(ctx, "bash", hook)
	cmd.Dir = workspace
	if len(env) > 0 {
		cmd.Env = append([]string(nil), env...)
	}
	cmd.Stdin = strings.NewReader(stdin)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail != "" {
			return "", fmt.Errorf("%s hook failed: %w: %s", lifecycle, err, detail)
		}
		return "", fmt.Errorf("%s hook failed: %w", lifecycle, err)
	}
	return codexHookSystemMessage(stdout.String()), nil
}

func codexHookSystemMessage(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}
	var envelope map[string]any
	if err := json.Unmarshal([]byte(text), &envelope); err == nil {
		if message, ok := envelope["systemMessage"].(string); ok {
			return strings.TrimSpace(message)
		}
	}
	return text
}

func codexAppServerCapabilities() map[string]any {
	return map[string]any{
		"experimentalApi":    true,
		"requestAttestation": false,
	}
}
