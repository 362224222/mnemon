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

func TestRuntimeRunsProviderCommandWithOriginalTurnInput(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "multica-args.log")
	multicaBin := writeFakeMulticaCLI(t, logPath, fakeMulticaScript{
		IssueJSON:    `{"id":"issue-provider","identifier":"TEA-9","title":"中文验收-多角色决策","description":"请协调多角色完成退款策略复盘。","status":"todo","priority":"high"}`,
		MetadataJSON: `[]`,
	})
	server, received := fakeIngestServer(t)
	defer server.Close()
	providerInputPath := filepath.Join(tmp, "provider.stdin")
	providerOutput := "原生 provider 输出：已读取中文 turn"
	providerBin := writeFakeProvider(t)
	progress := []runtimeProgressEvent{}

	final := (&runtimeRPCState{
		Env: []string{
			"MULTICA_ISSUE_ID=issue-provider",
			"MULTICA_TASK_ID=task-provider",
			"MULTICA_AGENT_ID=agent-1",
			"MULTICA_AGENT_NAME=Reviewer",
			"MNEMON_MULTICA_BIN=" + multicaBin,
			"MNEMON_CONTROL_ADDR=" + server.URL,
			"MNEMON_CONTROL_PRINCIPAL=reviewer@team",
			"MNEMON_MULTICA_PROVIDER_RUNTIME=codex",
			"MNEMON_MULTICA_PROVIDER_COMMAND=" + providerBin,
			"MNEMON_MULTICA_PROVIDER_WORKSPACE=" + tmp,
			"MNEMON_MULTICA_PROVIDER_TURN_TIMEOUT=5s",
			"FAKE_PROVIDER_INPUT=" + providerInputPath,
			"FAKE_PROVIDER_OUTPUT=" + providerOutput,
			"FAKE_MULTICA_LOG=" + logPath,
		},
		CWD: tmp,
		Now: fixedRuntimeTime,
	}).runTurn(multicasurface.RuntimeInput{Text: "请基于当前 issue 继续推进中文 ReAct 协作。"}, func(event runtimeProgressEvent) {
		progress = append(progress, event)
	})

	if final != providerOutput {
		t.Fatalf("final answer = %q", final)
	}
	if got := readFile(t, providerInputPath); got != "请基于当前 issue 继续推进中文 ReAct 协作。\n" {
		t.Fatalf("provider input = %q", got)
	}
	env := <-received
	if env.ExternalID != "multica-task-task-provider" {
		t.Fatalf("unexpected ingest external id: %+v", env)
	}
	foundProviderCommand := false
	for _, event := range progress {
		if event.Command == providerBin {
			foundProviderCommand = true
			if event.CWD != tmp {
				t.Fatalf("provider cwd = %q, want %q", event.CWD, tmp)
			}
		}
	}
	if !foundProviderCommand {
		t.Fatalf("provider command was not emitted in progress: %+v", progress)
	}
	argsLog := readFile(t, logPath)
	if strings.Contains(argsLog, "issue comment add") ||
		strings.Contains(argsLog, "issue status set") ||
		strings.Contains(argsLog, "[mnemon:wake]") {
		t.Fatalf("provider wrapper performed forbidden R2 side effect:\n%s", argsLog)
	}
}

func TestRuntimeProviderEnvAddsImportedIssueContext(t *testing.T) {
	env := runtimeProviderEnv([]string{
		"MULTICA_TASK_ID=daemon-task",
		"MULTICA_AGENT_ID=agent-1",
	}, runtimeImportResult{
		IssueID:    "issue-imported",
		Identifier: "TEA-42",
		Title:      "中文协作验收",
		Principal:  "planner@team",
		TaskID:     "task-imported",
		Status:     "recorded",
	})
	got := map[string]string{}
	counts := map[string]int{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			got[key] = value
			counts[key]++
		}
	}
	for key, want := range map[string]string{
		"MULTICA_ISSUE_ID":                "issue-imported",
		"MULTICA_TASK_ID":                 "task-imported",
		"MNEMON_MULTICA_ISSUE_IDENTIFIER": "TEA-42",
		"MNEMON_MULTICA_ISSUE_TITLE":      "中文协作验收",
		"MNEMON_MULTICA_ISSUE_STATUS":     "recorded",
		"MNEMON_MULTICA_PRINCIPAL":        "planner@team",
	} {
		if got[key] != want {
			t.Fatalf("%s = %q, want %q (env=%v)", key, got[key], want, env)
		}
		if counts[key] != 1 {
			t.Fatalf("%s appears %d times in env=%v", key, counts[key], env)
		}
	}
}

func TestProviderPromptFallsBackToIssueWhenTurnInputIsEmpty(t *testing.T) {
	got := providerPrompt(multicasurface.RuntimeInput{}, runtimeImportResult{
		IssueID:    "issue-1",
		Identifier: "TEA-1",
		Title:      "中文复用上下文评审",
		Statement:  "请复用上一轮风险结论，并给出下一步。",
	})
	for _, want := range []string{"TEA-1", "中文复用上下文评审", "请复用上一轮风险结论"} {
		if !strings.Contains(got, want) {
			t.Fatalf("provider prompt missing %q:\n%s", want, got)
		}
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

func writeFakeProvider(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "provider")
	body := `#!/bin/sh
set -eu
cat > "$FAKE_PROVIDER_INPUT"
printf '%s\n' "$FAKE_PROVIDER_OUTPUT"
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
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
