package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

type probeRecorder struct {
	path string
	now  func() time.Time
}

type probeEvent struct {
	TS     string         `json:"ts"`
	Kind   string         `json:"kind"`
	Args   []string       `json:"args,omitempty"`
	CWD    string         `json:"cwd,omitempty"`
	Env    map[string]any `json:"env,omitempty"`
	Line   string         `json:"line,omitempty"`
	Method string         `json:"method,omitempty"`
	ID     any            `json:"id,omitempty"`
	Params map[string]any `json:"params,omitempty"`
	Error  string         `json:"error,omitempty"`
}

func runRuntimeProbe(cfg runtimeConfig) error {
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
	rec, err := newProbeRecorder(cfg.Env, cwd, cfg.Now)
	if err != nil {
		return err
	}
	if err := rec.record(probeEvent{
		Kind: "process_start",
		Args: append([]string(nil), cfg.Args...),
		CWD:  cwd,
		Env:  redactProbeEnv(cfg.Env),
	}); err != nil {
		return err
	}
	if wantsVersion(cfg.Args) {
		fmt.Fprintf(cfg.Stdout, "%s %s (probe mode)\n", multicasurface.MulticaRuntimeCommandName, runtimeVersion)
		return rec.record(probeEvent{Kind: "version"})
	}
	return runProbeRPC(cfg, rec, cwd)
}

func runProbeRPC(cfg runtimeConfig, rec *probeRecorder, cwd string) error {
	scanner := bufio.NewScanner(cfg.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	state := probeRPCState{CWD: cwd, Now: cfg.Now}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if err := rec.record(probeEvent{Kind: "rpc_line", Line: line}); err != nil {
			return err
		}
		var msg rpcMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			_ = rec.record(probeEvent{Kind: "rpc_decode_error", Line: line, Error: err.Error()})
			continue
		}
		if err := rec.record(probeEvent{Kind: "rpc_request", ID: msg.ID, Method: msg.Method, Params: msg.Params}); err != nil {
			return err
		}
		responses := state.handle(msg)
		for _, response := range responses {
			if err := writeProbeRPC(cfg.Stdout, response); err != nil {
				return err
			}
			if err := rec.record(probeEvent{Kind: "rpc_response", ID: response.ID, Method: response.Method}); err != nil {
				return err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		_ = rec.record(probeEvent{Kind: "stdin_error", Error: err.Error()})
		return err
	}
	return rec.record(probeEvent{Kind: "process_exit"})
}

type probeRPCState struct {
	CWD      string
	ThreadID string
	TurnID   string
	Now      func() time.Time
}

func (s *probeRPCState) handle(msg rpcMessage) []rpcMessage {
	switch msg.Method {
	case "initialize":
		return []rpcMessage{{
			ID: msg.ID,
			Result: map[string]any{
				"userAgent":      multicasurface.MulticaRuntimeCommandName + "/" + runtimeVersion + " probe",
				"codexHome":      os.Getenv("CODEX_HOME"),
				"platformFamily": "unix",
				"platformOs":     runtime.GOOS,
			},
		}}
	case "thread/start", "thread/resume":
		s.ThreadID = firstNonEmpty(stringParam(msg.Params, "threadId"), probeID("thread", s.now()))
		return []rpcMessage{
			{
				Method: "remoteControl/status/changed",
				Params: map[string]any{
					"status":         "disabled",
					"serverName":     multicasurface.MulticaRuntimeCommandName,
					"installationId": "mnemon-runtime-probe",
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
		s.TurnID = probeID("turn", s.now())
		input := extractProbeInput(msg.Params)
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
				Params: map[string]any{"threadId": s.ThreadID, "turnId": s.TurnID, "item": probeAgentMessage("")},
			},
			{
				Method: "item/agentMessage/delta",
				Params: map[string]any{
					"threadId": s.ThreadID,
					"turnId":   s.TurnID,
					"itemId":   "mnemon-probe-message",
					"delta":    probeFinalAnswer(input),
				},
			},
			{
				Method: "item/completed",
				Params: map[string]any{"threadId": s.ThreadID, "turnId": s.TurnID, "item": probeAgentMessage(probeFinalAnswer(input))},
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

func (s *probeRPCState) threadStartResult(params map[string]any) map[string]any {
	if cwd := stringParam(params, "cwd"); cwd != "" {
		s.CWD = cwd
	}
	return map[string]any{
		"thread":                s.threadObject(),
		"model":                 "mnemon-probe",
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

func (s *probeRPCState) threadObject() map[string]any {
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

func (s *probeRPCState) turnObject(status string) map[string]any {
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

func (s *probeRPCState) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func writeProbeRPC(w io.Writer, msg rpcMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}

func probeAgentMessage(text string) map[string]any {
	return map[string]any{
		"type":           "agentMessage",
		"id":             "mnemon-probe-message",
		"text":           text,
		"phase":          "final_answer",
		"memoryCitation": nil,
	}
}

func probeFinalAnswer(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		input = "no input"
	}
	return "Mnemon Multica runtime probe completed. Observed turn input: " + input
}

func extractProbeInput(params map[string]any) string {
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

func newProbeRecorder(env []string, cwd string, now func() time.Time) (*probeRecorder, error) {
	path := multicasurface.RuntimeEnvValue(env, "MNEMON_MULTICA_PROBE_LOG")
	if path == "" {
		dir := multicasurface.RuntimeEnvValue(env, "MNEMON_MULTICA_PROBE_DIR")
		if dir == "" {
			home, err := os.UserHomeDir()
			if err != nil || strings.TrimSpace(home) == "" {
				home = cwd
			}
			dir = filepath.Join(home, ".mnemon", "harness", "multica", "probe")
		}
		if now == nil {
			now = time.Now
		}
		path = filepath.Join(dir, "probe-"+now().UTC().Format("20060102T150405Z")+"-"+fmt.Sprint(os.Getpid())+".jsonl")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return &probeRecorder{path: path, now: now}, nil
}

func (r *probeRecorder) record(event probeEvent) error {
	if r == nil || strings.TrimSpace(r.path) == "" {
		return nil
	}
	now := time.Now
	if r.now != nil {
		now = r.now
	}
	event.TS = now().UTC().Format(time.RFC3339Nano)
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(event); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func redactProbeEnv(env []string) map[string]any {
	out := map[string]any{}
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		if isSensitiveProbeEnv(key) {
			if value == "" {
				out[key] = ""
			} else {
				out[key] = "[redacted]"
			}
			continue
		}
		out[key] = value
	}
	return out
}

func isSensitiveProbeEnv(key string) bool {
	key = strings.ToUpper(key)
	for _, marker := range []string{"TOKEN", "SECRET", "PASSWORD", "API_KEY", "AUTH", "CREDENTIAL", "COOKIE"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return false
}

func probeID(prefix string, ts time.Time) string {
	return fmt.Sprintf("%s-%d", prefix, ts.UTC().UnixNano())
}
