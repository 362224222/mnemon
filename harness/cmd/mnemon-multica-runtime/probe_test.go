package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProbeVersionRedactsEnvironmentInLog(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "probe.jsonl")
	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args: []string{"--probe", "--version"},
		Env: []string{
			"MNEMON_MULTICA_PROBE_LOG=" + logPath,
			"MULTICA_TASK_ID=task-1",
			"MULTICA_TOKEN=mat_secret",
		},
		CWD:    t.TempDir(),
		Stdin:  strings.NewReader(""),
		Stdout: &out,
		Now:    fixedProbeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "mnemon-multica-runtime") || !strings.Contains(out.String(), "probe mode") {
		t.Fatalf("version output = %q", out.String())
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logData), "mat_secret") {
		t.Fatalf("log leaked token:\n%s", logData)
	}
	if !strings.Contains(string(logData), `"MULTICA_TASK_ID":"task-1"`) || !strings.Contains(string(logData), `"MULTICA_TOKEN":"[redacted]"`) {
		t.Fatalf("log missing expected env capture:\n%s", logData)
	}
}

func TestProbeSpeaksMinimalCodexAppServerRPC(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "probe.jsonl")
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"multica","version":"test"}}}`,
		`{"jsonrpc":"2.0","method":"initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"thread/start","params":{"cwd":"/tmp/work","ephemeral":true}}`,
		`{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"threadId":"thread-known","input":[{"type":"text","text":"hello"}]}}`,
	}, "\n") + "\n"
	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args:   []string{"--diagnose", "app-server", "--listen", "stdio://"},
		Env:    []string{"MNEMON_MULTICA_PROBE_LOG=" + logPath},
		CWD:    t.TempDir(),
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedProbeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) < 8 {
		t.Fatalf("expected several rpc responses/notifications, got %d:\n%s", len(lines), out.String())
	}
	var sawInit, sawThread, sawTurnComplete bool
	for _, line := range lines {
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("invalid rpc line %q: %v", line, err)
		}
		switch {
		case msg["id"] == float64(1):
			sawInit = true
		case msg["id"] == float64(2):
			sawThread = true
		case msg["method"] == "turn/completed":
			sawTurnComplete = true
		}
	}
	if !sawInit || !sawThread || !sawTurnComplete {
		t.Fatalf("missing expected rpc responses init=%v thread=%v turnComplete=%v:\n%s", sawInit, sawThread, sawTurnComplete, out.String())
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"method":"initialize"`, `"method":"initialized"`, `"method":"thread/start"`, `"method":"turn/start"`, `"kind":"process_exit"`} {
		if !strings.Contains(string(logData), want) {
			t.Fatalf("log missing %s:\n%s", want, logData)
		}
	}
}

func fixedProbeTime() time.Time {
	return time.Date(2026, 6, 28, 8, 0, 0, 0, time.UTC)
}
