package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

func TestRuntimeSkipsWithoutIssueID(t *testing.T) {
	result := (&runtimeRPCState{Env: nil, CWD: t.TempDir(), Now: fixedRuntimeTime}).importIssue(multicasurface.RuntimeInput{}, nil)
	if result.Status != "skipped" {
		t.Fatalf("status = %q, want skipped", result.Status)
	}
	if result.Err == nil || !strings.Contains(result.Err.Error(), "no Multica issue id") {
		t.Fatalf("unexpected error: %v", result.Err)
	}
}

func TestRuntimeImportsMulticaIssueWithoutHubWritebackOrWake(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "multica-args.log")
	multicaBin := writeFakeMulticaCLI(t, logPath, fakeMulticaScript{
		IssueJSON:    `{"id":"issue-1","identifier":"TEA-1","title":"中文验收-退款规则澄清","description":"请澄清退款规则。","status":"todo","priority":"medium"}`,
		MetadataJSON: `[]`,
	})
	server, received := fakeIngestServer(t)
	defer server.Close()

	result := (&runtimeRPCState{
		Env: []string{
			"MULTICA_ISSUE_ID=issue-1",
			"MULTICA_TASK_ID=task-1",
			"MULTICA_AGENT_ID=agent-1",
			"MULTICA_AGENT_NAME=Reviewer",
			"MNEMON_MULTICA_BIN=" + multicaBin,
			"MNEMON_CONTROL_ADDR=" + server.URL,
			"MNEMON_CONTROL_PRINCIPAL=reviewer@team",
			"FAKE_MULTICA_LOG=" + logPath,
		},
		CWD: t.TempDir(),
		Now: fixedRuntimeTime,
	}).importIssue(multicasurface.RuntimeInput{}, nil)

	if result.Status != "recorded" {
		t.Fatalf("status = %q err=%v", result.Status, result.Err)
	}
	if result.Receipt == nil || result.Receipt.Seq != 7 {
		t.Fatalf("receipt mismatch: %+v", result.Receipt)
	}
	env := <-received
	if env.ExternalID != "multica-task-task-1" ||
		env.Event.Type != "teamwork_signal.write_candidate.observed" {
		t.Fatalf("observation mismatch: %+v", env)
	}
	rule, _ := env.Event.Payload["rule"].(map[string]any)
	if rule["external_issue_id"] != "issue-1" || rule["external_source"] != "multica" {
		t.Fatalf("rule payload mismatch: %+v", rule)
	}
	argsLog := readFile(t, logPath)
	for _, forbidden := range []string{
		"issue comment add",
		"issue metadata set",
		"issue status set",
		"issue assign",
		"hub-write",
		"[mnemon:wake]",
	} {
		if strings.Contains(argsLog, forbidden) {
			t.Fatalf("runtime performed forbidden R2 side effect %q:\n%s", forbidden, argsLog)
		}
	}
}

func TestRuntimeTreatsLegacyMailboxMetadataAsSurfaceInputOnly(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "multica-args.log")
	multicaBin := writeFakeMulticaCLI(t, logPath, fakeMulticaScript{
		IssueJSON:    `{"id":"issue-legacy","identifier":"TEA-2","title":"旧 assignment mailbox","description":"旧元数据不应触发 wake。","status":"todo","priority":"medium"}`,
		MetadataJSON: `[{"key":"mnemon.hub_backend","value":"multica"},{"key":"mnemon.kind","value":"assignment_mailbox"},{"key":"mnemon.assignment_id","value":"asg-1"}]`,
	})
	server, received := fakeIngestServer(t)
	defer server.Close()

	result := (&runtimeRPCState{
		Env: []string{
			"MULTICA_ISSUE_ID=issue-legacy",
			"MNEMON_MULTICA_BIN=" + multicaBin,
			"MNEMON_CONTROL_ADDR=" + server.URL,
			"MNEMON_CONTROL_PRINCIPAL=reviewer@team",
			"FAKE_MULTICA_LOG=" + logPath,
		},
		CWD: t.TempDir(),
		Now: fixedRuntimeTime,
	}).importIssue(multicasurface.RuntimeInput{}, nil)

	if result.Status != "recorded" {
		t.Fatalf("status = %q err=%v", result.Status, result.Err)
	}
	env := <-received
	rule, _ := env.Event.Payload["rule"].(map[string]any)
	if _, ok := rule["assignment_id"]; ok {
		t.Fatalf("legacy mailbox metadata leaked into canonical rule payload: %+v", rule)
	}
	argsLog := readFile(t, logPath)
	if strings.Contains(argsLog, "issue comment add") ||
		strings.Contains(argsLog, "issue metadata set") ||
		strings.Contains(argsLog, "issue status set") {
		t.Fatalf("legacy mailbox metadata triggered R2 writeback:\n%s", argsLog)
	}
}

type fakeMulticaScript struct {
	IssueJSON    string
	MetadataJSON string
}

func writeFakeMulticaCLI(t *testing.T, logPath string, script fakeMulticaScript) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "multica")
	body := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$FAKE_MULTICA_LOG"
case "$*" in
  *"issue get "*)
    printf '%s\n' '` + script.IssueJSON + `'
    ;;
  *"issue metadata list "*)
    printf '%s\n' '` + script.MetadataJSON + `'
    ;;
  *)
    echo "unexpected multica args: $*" >&2
    exit 42
    ;;
esac
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func fakeIngestServer(t *testing.T) (*httptest.Server, <-chan contract.ObservationEnvelope) {
	t.Helper()
	received := make(chan contract.ObservationEnvelope, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("X-Mnemon-Principal"); got != "reviewer@team" {
			http.Error(w, "unexpected principal "+got, http.StatusUnauthorized)
			return
		}
		var env contract.ObservationEnvelope
		if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		received <- env
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"seq":7,"dup":false,"ticked":true}`))
	}))
	return server, received
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func fixedRuntimeTime() time.Time {
	return time.Date(2026, 7, 1, 8, 0, 0, 0, time.UTC)
}
