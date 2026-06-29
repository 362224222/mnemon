// Package codexapp drives a real Codex CLI app-server over JSON-RPC (stdio) and parses its
// output. It is the reusable "run a real Codex turn from Go" adapter — an external-tool
// integration with zero knowledge of mnemon's governance or any demo. It
// depends only on the standard library.
package codexapp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// AppServer is a running `codex app-server` process spoken to over JSON-RPC on stdio. It is
// driven from a SINGLE goroutine (request/waitNotification drain the same message channel);
// callers must not invoke its methods concurrently.
type AppServer struct {
	command       string
	cwd           string
	env           []string
	proc          *exec.Cmd
	stdin         io.WriteCloser
	messages      chan map[string]any
	responses     map[int]map[string]any
	notifications []map[string]any
	nextID        int
	stderr        lockedOutput
}

// New returns an unstarted AppServer that will launch `command app-server` in cwd.
func New(command, cwd string) *AppServer {
	return &AppServer{
		command:   command,
		cwd:       cwd,
		messages:  make(chan map[string]any, 256),
		responses: map[int]map[string]any{},
		nextID:    1,
	}
}

// SetEnv sets the exact process environment used when Start launches the app-server.
// When unset, the child inherits the current process environment.
func (s *AppServer) SetEnv(env []string) {
	s.env = append([]string(nil), env...)
}

// Start launches the app-server subprocess and begins reading its stdio.
func (s *AppServer) Start() error {
	parts := commandParts(s.command)
	args := append(append([]string(nil), parts[1:]...), "app-server", "--listen", "stdio://")
	if err := s.startProcess(exec.Command(parts[0], args...)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		cmdline := shellJoin(append(parts, "app-server", "--listen", "stdio://")...)
		return s.startProcess(exec.Command("sh", "-lc", cmdline))
	}
	return nil
}

func commandParts(command string) []string {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return []string{"codex"}
	}
	return parts
}

func (s *AppServer) startProcess(cmd *exec.Cmd) error {
	cmd.Dir = s.cwd
	if len(s.env) > 0 {
		cmd.Env = append([]string(nil), s.env...)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	s.proc = cmd
	s.stdin = stdin
	go s.readStdout(stdout)
	go func() { _, _ = io.Copy(&s.stderr, stderr) }()
	return nil
}

func shellJoin(args ...string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuote(arg))
	}
	return strings.Join(quoted, " ")
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// Close interrupts (then kills) the app-server subprocess.
func (s *AppServer) Close() {
	if s.proc == nil || s.proc.Process == nil {
		return
	}
	if s.proc.ProcessState != nil && s.proc.ProcessState.Exited() {
		return
	}
	_ = s.proc.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() {
		_ = s.proc.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = s.proc.Process.Kill()
		<-done
	}
}

func (s *AppServer) readStdout(stdout io.Reader) {
	defer close(s.messages)
	reader := bufio.NewReaderSize(stdout, 1024*1024)
	for {
		line, err := reader.ReadString('\n')
		if strings.TrimSpace(line) != "" {
			var msg map[string]any
			if jerr := json.Unmarshal([]byte(line), &msg); jerr == nil {
				s.messages <- msg
			} else {
				s.messages <- map[string]any{"method": "codexapp/invalid-json", "params": map[string]any{"line": line, "error": jerr.Error()}}
			}
		}
		if err != nil {
			return
		}
	}
}

// Request sends a JSON-RPC request and waits up to timeout for its response.
func (s *AppServer) Request(method string, params map[string]any, timeout time.Duration) (map[string]any, error) {
	if s.stdin == nil {
		return nil, fmt.Errorf("codex app-server is not running")
	}
	id := s.nextID
	s.nextID++
	req := map[string]any{"jsonrpc": "2.0", "id": id, "method": method}
	if params != nil {
		req["params"] = params
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := s.stdin.Write(append(data, '\n')); err != nil {
		return nil, err
	}
	return s.waitResponse(id, timeout)
}

func (s *AppServer) waitResponse(id int, timeout time.Duration) (map[string]any, error) {
	deadline := time.After(timeout)
	for {
		if resp, ok := s.responses[id]; ok {
			delete(s.responses, id)
			if raw, ok := resp["error"]; ok {
				return nil, fmt.Errorf("codex app-server error: %s", jsonString(raw))
			}
			if result, ok := resp["result"].(map[string]any); ok {
				return result, nil
			}
			return map[string]any{}, nil
		}
		select {
		case <-deadline:
			return nil, fmt.Errorf("timed out waiting for response id %d", id)
		case msg, ok := <-s.messages:
			if !ok {
				return nil, fmt.Errorf("codex app-server stdout closed: %s", s.stderr.String())
			}
			s.acceptMessage(msg)
		}
	}
}

// WaitNotification waits up to timeout for a notification with the given method, starting from
// startIndex into the notification log (use NotificationCount before the action that triggers it).
func (s *AppServer) WaitNotification(method string, timeout time.Duration, startIndex int) (map[string]any, error) {
	return s.WaitNotificationWithCallback(method, timeout, startIndex, nil)
}

// WaitNotificationWithCallback waits for a notification while calling onNotifications for every
// notification observed from startIndex onward. The callback runs on the AppServer driving goroutine;
// callers should keep it lightweight and must not call back into AppServer methods from it.
func (s *AppServer) WaitNotificationWithCallback(method string, timeout time.Duration, startIndex int, onNotifications func([]map[string]any)) (map[string]any, error) {
	deadline := time.After(timeout)
	cursor := startIndex
	if cursor < 0 || cursor > len(s.notifications) {
		cursor = len(s.notifications)
	}
	for {
		for cursor < len(s.notifications) {
			n := s.notifications[cursor]
			cursor++
			if onNotifications != nil {
				onNotifications([]map[string]any{n})
			}
			if n["method"] == method {
				return n, nil
			}
		}
		select {
		case <-deadline:
			return nil, fmt.Errorf("timed out waiting for notification %s", method)
		case msg, ok := <-s.messages:
			if !ok {
				return nil, fmt.Errorf("codex app-server stdout closed: %s", s.stderr.String())
			}
			s.acceptMessage(msg)
		}
	}
}

func (s *AppServer) acceptMessage(msg map[string]any) {
	if id, ok := messageID(msg); ok {
		s.responses[id] = msg
		return
	}
	s.notifications = append(s.notifications, msg)
}

// NotificationCount returns the number of notifications received so far (the cursor a caller
// passes to NotificationsSince/WaitNotification to scope to events after an action).
func (s *AppServer) NotificationCount() int { return len(s.notifications) }

// NotificationsSince returns a copy of the notifications received after index.
func (s *AppServer) NotificationsSince(index int) []map[string]any {
	if index < 0 || index > len(s.notifications) {
		index = len(s.notifications)
	}
	return append([]map[string]any(nil), s.notifications[index:]...)
}

func messageID(msg map[string]any) (int, bool) {
	raw, ok := msg["id"]
	if !ok || raw == nil {
		return 0, false
	}
	switch v := raw.(type) {
	case float64:
		return int(v), true
	case int:
		return v, true
	case json.Number:
		i, err := v.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

type lockedOutput struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (o *lockedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.Write(p)
}

func (o *lockedOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buf.String()
}

func jsonString(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	return string(data)
}

// ---- output parsing ----

// ThreadID extracts the thread id from a thread/start result.
func ThreadID(result map[string]any) string {
	if thread, ok := result["thread"].(map[string]any); ok {
		if id, ok := thread["id"].(string); ok {
			return id
		}
	}
	if id, ok := result["threadId"].(string); ok {
		return id
	}
	if id, ok := result["id"].(string); ok {
		return id
	}
	return ""
}

// FinalAnswer extracts the agent's final-answer text from a turn's notifications.
func FinalAnswer(notifications []map[string]any) string {
	var out []string
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if x["type"] == "agentMessage" && x["phase"] == "final_answer" {
				if text, ok := x["text"].(string); ok && strings.TrimSpace(text) != "" {
					out = append(out, text)
				}
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	for _, n := range notifications {
		walk(n)
	}
	return strings.TrimSpace(strings.Join(out, "\n\n"))
}

// CombinedText flattens every string value across the notifications (the fallback when there is
// no structured final-answer phase).
func CombinedText(values []map[string]any) string {
	var parts []string
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			parts = append(parts, x)
		case map[string]any:
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	for _, v := range values {
		walk(v)
	}
	return strings.Join(parts, "\n")
}
