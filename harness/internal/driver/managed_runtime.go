package driver

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
	"path/filepath"
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
	_, ok := l.seen[managedWakeKey(candidate)]
	return ok
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
	Principal             string
	Command               string
	Workspace             string
	Env                   []string
	DeveloperInstructions string
	TurnTimeout           time.Duration
	RequestTimeout        time.Duration
	ClientName            string
	ClientVersion         string
}

func (c CodexAppServerTurnClient) StartTurn(_ context.Context, query string) (ManagedTurnResult, error) {
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
		"clientInfo": map[string]any{"name": clientName, "version": clientVersion},
	}, requestTimeout); err != nil {
		return ManagedTurnResult{}, fmt.Errorf("initialize: %w", err)
	}
	thread, err := server.Request("thread/start", map[string]any{
		"cwd":                   workspace,
		"approvalPolicy":        "never",
		"sandbox":               "danger-full-access",
		"ephemeral":             true,
		"developerInstructions": c.developerInstructions(),
	}, requestTimeout)
	if err != nil {
		return ManagedTurnResult{}, fmt.Errorf("thread/start: %w", err)
	}
	threadID := codexapp.ThreadID(thread)
	if threadID == "" {
		return ManagedTurnResult{}, fmt.Errorf("thread/start returned no thread id")
	}
	before := server.NotificationCount()
	if _, err := server.Request("turn/start", map[string]any{
		"threadId":       threadID,
		"input":          []map[string]any{{"type": "text", "text": ManagedWakeQuery}},
		"cwd":            workspace,
		"approvalPolicy": "never",
		"sandboxPolicy":  map[string]any{"type": "dangerFullAccess"},
	}, requestTimeout); err != nil {
		return ManagedTurnResult{}, fmt.Errorf("turn/start: %w", err)
	}
	if _, err := server.WaitNotification("turn/completed", turnTimeout, before); err != nil {
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

func (c CodexAppServerTurnClient) developerInstructions() string {
	if strings.TrimSpace(c.DeveloperInstructions) != "" {
		return c.DeveloperInstructions
	}
	principal := strings.TrimSpace(c.Principal)
	if principal == "" {
		principal = "the local managed principal"
	}
	return fmt.Sprintf(`You are %s in a mnemond-managed Mnemon runtime.
When the user input is [mnemon:wake], treat it only as a local wake signal.
Use the normal Mnemon hooks/skills and Local Mnemon commands in this workspace to inspect governed context and decide whether to act.
Do not expect assignment details in the raw wake query. Keep any governed event drafts short and emit them through Local Mnemon.`, principal)
}
