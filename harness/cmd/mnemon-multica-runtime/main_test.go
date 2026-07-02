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

func TestRuntimeProxiesProviderAppServerWhenNoIssueGateIsPresent(t *testing.T) {
	tmp := t.TempDir()
	providerLogPath := filepath.Join(tmp, "provider-rpc.jsonl")
	providerBin := writeFakeCodexAppServer(t, providerLogPath)
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"multica","version":"test"}}}`,
		`{"jsonrpc":"2.0","method":"initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"thread/start","params":{"cwd":"` + tmp + `","ephemeral":true}}`,
		`{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"threadId":"thread-fake","input":[{"type":"text","text":"你好，请保持原生 Codex 流。"}]}}`,
	}, "\n") + "\n"
	var out strings.Builder
	err := runRuntime(runtimeConfig{
		Env: []string{
			"MNEMON_MULTICA_PROVIDER_RUNTIME=codex",
			"MNEMON_MULTICA_PROVIDER_COMMAND=" + providerBin,
			"FAKE_CODEX_APP_SERVER_LOG=" + providerLogPath,
		},
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	providerLog := readFile(t, providerLogPath)
	for _, want := range []string{`"method":"initialize"`, `"method":"thread/start"`, `"method":"turn/start"`, "你好，请保持原生 Codex 流。"} {
		if !strings.Contains(providerLog, want) {
			t.Fatalf("provider did not receive %q:\n%s", want, providerLog)
		}
	}
	for _, want := range []string{`"provider":"fake-codex"`, `"method":"item/agentMessage/delta"`, "原生 provider 输出：已读取中文 turn"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("runtime did not proxy provider output %q:\n%s", want, out.String())
		}
	}
}

func TestRuntimeGateDoesNotStartProviderForIssueActivation(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "multica-args.log")
	providerLogPath := filepath.Join(tmp, "provider-rpc.jsonl")
	multicaBin := writeFakeMulticaCLI(t, logPath, fakeMulticaScript{
		IssueJSON:    `{"id":"issue-provider","identifier":"TEA-9","title":"中文验收-多角色决策","description":"请协调多角色完成退款策略复盘。","status":"todo","priority":"high"}`,
		MetadataJSON: `[]`,
	})
	server, received := fakeIngestServer(t)
	defer server.Close()
	providerBin := writeFakeCodexAppServer(t, providerLogPath)

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
			"FAKE_CODEX_APP_SERVER_LOG=" + providerLogPath,
			"FAKE_MULTICA_LOG=" + logPath,
		},
		CWD: tmp,
		Now: fixedRuntimeTime,
	}).runTurn(multicasurface.RuntimeInput{Text: "请基于当前 issue 继续推进中文 ReAct 协作。"}, nil)

	if !strings.Contains(final, "Mnemon Multica runtime handled issue") {
		t.Fatalf("final answer = %q", final)
	}
	env := <-received
	if env.ExternalID != "multica-task-task-provider" {
		t.Fatalf("unexpected ingest external id: %+v", env)
	}
	if got := readFile(t, providerLogPath); strings.TrimSpace(got) != "" {
		t.Fatalf("issue activation bypassed gate and started provider:\n%s", got)
	}
}

func TestRuntimeGateUsesTurnIssueTagBeforeStartingProvider(t *testing.T) {
	tmp := t.TempDir()
	logPath := filepath.Join(tmp, "multica-args.log")
	providerLogPath := filepath.Join(tmp, "provider-rpc.jsonl")
	multicaBin := writeFakeMulticaCLI(t, logPath, fakeMulticaScript{
		IssueJSON:    `{"id":"issue-tagged","identifier":"TEA-9","title":"中文验收-由 @tag 触发","description":"请根据 @TEA-9 进入 Mnemon gate。","status":"todo","priority":"high"}`,
		MetadataJSON: `[]`,
	})
	server, received := fakeIngestServer(t)
	defer server.Close()
	providerBin := writeFakeCodexAppServer(t, providerLogPath)
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"multica","version":"test"}}}`,
		`{"jsonrpc":"2.0","id":2,"method":"thread/start","params":{"cwd":"` + tmp + `","ephemeral":true}}`,
		`{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"threadId":"thread-fake","input":[{"type":"text","text":"请处理 @TEA-9 并进入 Mnemon 协作流程。"}]}}`,
	}, "\n") + "\n"
	var out strings.Builder

	err := runRuntime(runtimeConfig{
		Env: []string{
			"MULTICA_TASK_ID=task-tagged",
			"MULTICA_AGENT_ID=agent-1",
			"MULTICA_AGENT_NAME=Reviewer",
			"MNEMON_MULTICA_BIN=" + multicaBin,
			"MNEMON_CONTROL_ADDR=" + server.URL,
			"MNEMON_CONTROL_PRINCIPAL=reviewer@team",
			"MNEMON_MULTICA_PROVIDER_RUNTIME=codex",
			"MNEMON_MULTICA_PROVIDER_COMMAND=" + providerBin,
			"FAKE_CODEX_APP_SERVER_LOG=" + providerLogPath,
			"FAKE_MULTICA_LOG=" + logPath,
		},
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	env := <-received
	if env.ExternalID != "multica-task-task-tagged" {
		t.Fatalf("unexpected ingest external id: %+v", env)
	}
	if got := readFile(t, providerLogPath); strings.TrimSpace(got) != "" {
		t.Fatalf("turn issue gate started provider before mnemond import:\n%s", got)
	}
	if !strings.Contains(out.String(), "Mnemon Multica runtime handled issue") {
		t.Fatalf("runtime output did not include Mnemon result:\n%s", out.String())
	}
}

func TestRuntimeProviderCommandDefaultsToCodexJSONRPC(t *testing.T) {
	if got := runtimeProviderCommand(nil); got != "codex app-server --listen stdio://" {
		t.Fatalf("default provider command = %q", got)
	}
	if got := runtimeProviderCommand([]string{"MNEMON_MULTICA_PROVIDER_RUNTIME=codex-jsonrpc"}); got != "codex app-server --listen stdio://" {
		t.Fatalf("codex-jsonrpc provider command = %q", got)
	}
	if got := runtimeProviderCommand([]string{"MNEMON_MULTICA_PROVIDER_RUNTIME=unknown"}); got != "" {
		t.Fatalf("unknown provider command = %q", got)
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

func writeFakeCodexAppServer(t *testing.T, logPath string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-codex-app-server")
	body := `#!/bin/sh
set -eu
: > "$FAKE_CODEX_APP_SERVER_LOG"
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$FAKE_CODEX_APP_SERVER_LOG"
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":1,"result":{"provider":"fake-codex","userAgent":"fake-codex/0.1"}}'
      ;;
    *'"method":"thread/start"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-fake","sessionId":"thread-fake","status":{"type":"idle"}}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"thread/started","params":{"thread":{"id":"thread-fake","sessionId":"thread-fake","status":{"type":"idle"}}}}'
      ;;
    *'"method":"turn/start"'*)
      printf '%s\n' '{"jsonrpc":"2.0","id":3,"result":{"turn":{"id":"turn-fake","status":"inProgress","items":[]}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"turn/started","params":{"threadId":"thread-fake","turn":{"id":"turn-fake","status":"inProgress","items":[]}}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"threadId":"thread-fake","turnId":"turn-fake","itemId":"msg-fake","delta":"原生 provider 输出：已读取中文 turn"}}'
      printf '%s\n' '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-fake","turn":{"id":"turn-fake","status":"completed","items":[]}}}'
      exit 0
      ;;
  esac
done
`
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_CODEX_APP_SERVER_LOG", logPath)
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
