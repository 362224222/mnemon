package driver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMulticaCLIAddCommentUsesStdin(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" > "$MULTICA_ARGS_PATH"
cat > "$MULTICA_STDIN_PATH"
printf '{"id":"comment-1","issue_id":"iss-1","content":"ok","type":"comment"}\n'
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cli := MulticaCLI{
		Command: bin,
		Profile: "desktop-api.multica.ai",
		Env: append(os.Environ(),
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_STDIN_PATH="+stdinPath,
		),
	}
	comment, err := cli.AddIssueComment(context.Background(), "iss-1", "progress with sensitive details")
	if err != nil {
		t.Fatal(err)
	}
	if comment.ID != "comment-1" {
		t.Fatalf("comment = %+v", comment)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--profile desktop-api.multica.ai issue comment add iss-1 --content-stdin --output json") {
		t.Fatalf("args did not use expected CLI shape: %q", string(args))
	}
	if strings.Contains(string(args), "sensitive details") {
		t.Fatalf("comment content must not be passed as an argument: %q", string(args))
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stdin) != "progress with sensitive details" {
		t.Fatalf("stdin = %q", string(stdin))
	}
}

func TestDecodeMulticaCommentListOutputDedupesNestedComments(t *testing.T) {
	comments, err := decodeMulticaCommentListOutput([]byte(`{
		"comments": [
			{"id":"comment-1","issue_id":"issue-1","content":"ok","type":"comment"},
			{"id":"comment-1","issue_id":"issue-1","content":"ok","type":"comment"}
		]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(comments) != 1 || comments[0].ID != "comment-1" {
		t.Fatalf("comments = %+v", comments)
	}
}

func TestMulticaCLIAuthStatusFallsBackToStderr(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf 'Server:  https://api.multica.ai\nUser:    Test User\nToken:   mul_test...\n' >&2
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	status, err := (MulticaCLI{Command: bin}).AuthStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "Test User") {
		t.Fatalf("auth status did not include stderr content: %q", status)
	}
}

func TestMulticaCLIAgentEnvUsesStdin(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" > "$MULTICA_ARGS_PATH"
cat > "$MULTICA_STDIN_PATH"
case "$*" in
  *"agent env get agent-1"*) printf '{"agent_id":"agent-1","custom_env":{"EXISTING":"****"}}\n' ;;
  *"agent env set agent-1"*) printf '{"agent_id":"agent-1","custom_env":'; cat "$MULTICA_STDIN_PATH"; printf '}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cli := MulticaCLI{
		Command: bin,
		Env: append(os.Environ(),
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_STDIN_PATH="+stdinPath,
		),
	}
	env, err := cli.GetAgentEnv(context.Background(), "agent-1")
	if err != nil {
		t.Fatal(err)
	}
	if env["EXISTING"] != "****" {
		t.Fatalf("env = %+v", env)
	}
	updated, err := cli.SetAgentEnv(context.Background(), "agent-1", map[string]string{
		"EXISTING":                "****",
		"MNEMON_CONTROL_TOKEN":    "secret-token",
		"MNEMON_MULTICA_REGISTRY": "/tmp/registry.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated["MNEMON_CONTROL_TOKEN"] != "secret-token" {
		t.Fatalf("updated env = %+v", updated)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(args), "secret-token") || !strings.Contains(string(args), "agent env set agent-1 --custom-env-stdin --output json") {
		t.Fatalf("env update used unsafe args: %q", string(args))
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stdin), `"MNEMON_CONTROL_TOKEN":"secret-token"`) {
		t.Fatalf("env JSON was not written to stdin: %s", stdin)
	}
}

func TestMulticaCLIAgentEnvFallsBackToAPI(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf 'Error: unknown command "env" for "multica agent"\n' >&2
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	var sawPut bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer token-1" {
			t.Fatalf("Authorization header = %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Workspace-ID") != "ws-1" {
			t.Fatalf("X-Workspace-ID header = %q", r.Header.Get("X-Workspace-ID"))
		}
		if r.URL.Path != "/api/agents/agent-1/env" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":         "agent-1",
				"custom_env": map[string]string{"EXISTING": "yes"},
			})
		case http.MethodPut:
			sawPut = true
			var body struct {
				CustomEnv map[string]string `json:"custom_env"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.CustomEnv["MNEMON_CONTROL_ADDR"] != "http://127.0.0.1:8787" {
				t.Fatalf("custom_env body = %+v", body.CustomEnv)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":         "agent-1",
				"custom_env": body.CustomEnv,
			})
		default:
			t.Fatalf("method = %s", r.Method)
		}
	}))
	defer server.Close()
	cli := MulticaCLI{
		Command:     bin,
		ServerURL:   server.URL,
		WorkspaceID: "ws-1",
		Env: append(os.Environ(),
			"MULTICA_TOKEN=token-1",
		),
	}
	env, err := cli.GetAgentEnv(context.Background(), "agent-1")
	if err != nil {
		t.Fatal(err)
	}
	if env["EXISTING"] != "yes" {
		t.Fatalf("env = %+v", env)
	}
	updated, err := cli.SetAgentEnv(context.Background(), "agent-1", map[string]string{
		"MNEMON_CONTROL_ADDR": "http://127.0.0.1:8787",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawPut || updated["MNEMON_CONTROL_ADDR"] != "http://127.0.0.1:8787" {
		t.Fatalf("updated=%+v sawPut=%v", updated, sawPut)
	}
}

func TestMulticaCLICreateIssueFallsBackToAPI(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf 'Error: unknown flag: --description-stdin\n' >&2
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/issues" {
			t.Fatalf("%s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["title"] != "R3 中文 case" || body["description"] != "详细说明" || body["assignee_type"] != "agent" || body["assignee_id"] != "agent-1" {
			t.Fatalf("body = %+v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":          "issue-1",
			"identifier":  "TEA-1",
			"title":       body["title"],
			"description": body["description"],
			"status":      "todo",
		})
	}))
	defer server.Close()
	cli := MulticaCLI{
		Command:     bin,
		ServerURL:   server.URL,
		WorkspaceID: "ws-1",
		Env: append(os.Environ(),
			"MULTICA_TOKEN=token-1",
		),
	}
	issue, err := cli.CreateIssue(context.Background(), MulticaCreateIssueRequest{
		Title:       "R3 中文 case",
		Description: "详细说明",
		AssigneeID:  "agent-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if issue.ID != "issue-1" || issue.Identifier != "TEA-1" {
		t.Fatalf("issue = %+v", issue)
	}
}

func TestMulticaCLIListIssueChildrenFallsBackToAPI(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf 'Error: unknown command "children" for "multica issue"\n' >&2
exit 1
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/issues/root-1/children" {
			t.Fatalf("%s %s", r.Method, r.URL.Path)
		}
		fmt.Fprint(w, `{"issues":[{"id":"child-1","identifier":"TEA-2","title":"子任务","status":"done"}]}`)
	}))
	defer server.Close()
	cli := MulticaCLI{
		Command:     bin,
		ServerURL:   server.URL,
		WorkspaceID: "ws-1",
		Env: append(os.Environ(),
			"MULTICA_TOKEN=token-1",
		),
	}
	children, err := cli.ListIssueChildren(context.Background(), "root-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].ID != "child-1" || children[0].Status != "done" {
		t.Fatalf("children = %+v", children)
	}
}

func TestMulticaCLIProvisioningCommandsUseExpectedShapes(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "args.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" > "$MULTICA_ARGS_PATH"
case "$*" in
  *"runtime profile create"*) printf '{"id":"profile-1","display_name":"Mnemon","command_name":"mnemon-multica-runtime","protocol_family":"codex","enabled":true,"visibility":"workspace","workspace_id":"ws-1"}\n' ;;
  *"runtime list"*) printf '[{"id":"runtime-1","name":"Mnemon (Mac)","provider":"codex","status":"online","profile_id":"profile-1","workspace_id":"ws-1"}]\n' ;;
  *"agent create"*) printf '{"id":"agent-1","name":"mnemon-planner","runtime_id":"runtime-1","status":"idle","visibility":"private","workspace_id":"ws-1"}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cli := MulticaCLI{
		Command:     bin,
		Profile:     "desktop-api.multica.ai",
		WorkspaceID: "ws-1",
		Env: append(os.Environ(),
			"MULTICA_ARGS_PATH="+argsPath,
		),
	}
	profile, err := cli.CreateRuntimeProfile(context.Background(), MulticaCreateRuntimeProfileRequest{
		DisplayName:    "Mnemon",
		Description:    "Mnemon runtime",
		ProtocolFamily: "codex",
		CommandName:    "mnemon-multica-runtime",
	})
	if err != nil {
		t.Fatal(err)
	}
	if profile.ID != "profile-1" || profile.ProtocolFamily != "codex" {
		t.Fatalf("profile = %+v", profile)
	}
	args := mustReadDriverTestFile(t, argsPath)
	for _, want := range []string{
		"--profile desktop-api.multica.ai --workspace-id ws-1 runtime profile create",
		"--display-name Mnemon",
		"--protocol-family codex",
		"--command-name mnemon-multica-runtime",
		"--output json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("create profile args missing %q: %q", want, args)
		}
	}
	runtimes, err := cli.ListRuntimes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(runtimes) != 1 || runtimes[0].ProfileID != "profile-1" {
		t.Fatalf("runtimes = %+v", runtimes)
	}
	agent, err := cli.CreateAgent(context.Background(), MulticaCreateAgentRequest{
		Name:               "mnemon-planner",
		RuntimeID:          "runtime-1",
		Instructions:       "Use Mnemon render context.",
		Visibility:         "private",
		MaxConcurrentTasks: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if agent.ID != "agent-1" {
		t.Fatalf("agent = %+v", agent)
	}
	args = mustReadDriverTestFile(t, argsPath)
	for _, want := range []string{
		"agent create",
		"--name mnemon-planner",
		"--runtime-id runtime-1",
		"--instructions Use Mnemon render context.",
		"--max-concurrent-tasks 1",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("create agent args missing %q: %q", want, args)
		}
	}
}

func TestMulticaCLICreateIssueUsesDescriptionStdin(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" > "$MULTICA_ARGS_PATH"
cat > "$MULTICA_STDIN_PATH"
printf '{"id":"issue-1","identifier":"TEA-9","title":"Run teamwork","status":"todo","priority":"low"}\n'
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cli := MulticaCLI{
		Command: bin,
		Env: append(os.Environ(),
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_STDIN_PATH="+stdinPath,
		),
	}
	issue, err := cli.CreateIssue(context.Background(), MulticaCreateIssueRequest{
		Title:          "Run teamwork",
		Description:    "Long task description with context that must not live in argv.",
		AssigneeID:     "agent-1",
		Status:         "todo",
		Priority:       "low",
		AllowDuplicate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if issue.ID != "issue-1" {
		t.Fatalf("issue = %+v", issue)
	}
	args := mustReadDriverTestFile(t, argsPath)
	if !strings.Contains(args, "issue create --title Run teamwork --output json --description-stdin --assignee-id agent-1 --status todo --priority low --allow-duplicate") {
		t.Fatalf("issue create args mismatch: %q", args)
	}
	if strings.Contains(args, "Long task description") {
		t.Fatalf("description must not be passed in argv: %q", args)
	}
	stdin := mustReadDriverTestFile(t, stdinPath)
	if stdin != "Long task description with context that must not live in argv." {
		t.Fatalf("stdin = %q", stdin)
	}
}

func TestMulticaCLILoadIssueMetadataMergesListedMetadata(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "args.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue metadata list issue-1"*) printf '[{"key":"mnemon.surface_role","value":"display"},{"key":"listed","value":"yes"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cli := MulticaCLI{
		Command: bin,
		Env: append(os.Environ(),
			"MULTICA_ARGS_PATH="+argsPath,
		),
	}
	loaded, count, err := cli.LoadIssueMetadata(context.Background(), MulticaIssue{
		ID: "issue-1",
		Metadata: map[string]any{
			MulticaMetadataSurfaceRole: "input",
			"base":                     "kept",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 || loaded.Metadata[MulticaMetadataSurfaceRole] != MulticaSurfaceRoleDisplay || loaded.Metadata["base"] != "kept" || loaded.Metadata["listed"] != "yes" {
		t.Fatalf("loaded metadata count=%d metadata=%+v", count, loaded.Metadata)
	}
	empty, count, err := cli.LoadIssueMetadata(context.Background(), MulticaIssue{Title: "missing id"})
	if err != nil || count != 0 || empty.Title != "missing id" {
		t.Fatalf("empty metadata load = count:%d issue:%+v err:%v", count, empty, err)
	}
	args := mustReadDriverTestFile(t, argsPath)
	if !strings.Contains(args, "issue metadata list issue-1 --output json") || strings.Contains(args, "missing id") {
		t.Fatalf("metadata load args mismatch:\n%s", args)
	}
}

func TestMulticaCLIChildrenAndMetadataCommands(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "args.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue children root-1"*) printf '{"children":[{"id":"child-1","identifier":"TEA-2","title":"Surface","metadata":{"mnemon.surface_role":"display"}}]}\n' ;;
  *"issue metadata list child-1"*) printf '[{"key":"mnemon.surface_role","value":"display"},{"key":"mnemon.event_ref","value":"event-1"}]\n' ;;
  *"issue metadata get child-1 --key mnemon.surface_role"*) printf '{"key":"mnemon.surface_role","value":"display"}\n' ;;
  *"issue metadata set child-1"*) printf '{}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	cli := MulticaCLI{
		Command: bin,
		Env: append(os.Environ(),
			"MULTICA_ARGS_PATH="+argsPath,
		),
	}
	children, err := cli.ListIssueChildren(context.Background(), "root-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].ID != "child-1" {
		t.Fatalf("children = %+v", children)
	}
	meta, err := cli.ListIssueMetadata(context.Background(), "child-1")
	if err != nil {
		t.Fatal(err)
	}
	if meta[MulticaMetadataSurfaceRole] != MulticaSurfaceRoleDisplay || meta[MulticaMetadataEventRef] != "event-1" {
		t.Fatalf("metadata = %+v", meta)
	}
	value, ok, err := cli.GetIssueMetadata(context.Background(), "child-1", MulticaMetadataSurfaceRole)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || value != MulticaSurfaceRoleDisplay {
		t.Fatalf("metadata get = %q ok=%v", value, ok)
	}
	if err := cli.SetIssueMetadataMap(context.Background(), "child-1", map[string]string{
		"b": "two",
		"a": "one",
	}); err != nil {
		t.Fatal(err)
	}
	args := mustReadDriverTestFile(t, argsPath)
	for _, want := range []string{
		"issue children root-1 --output json",
		"issue metadata list child-1 --output json",
		"issue metadata get child-1 --key mnemon.surface_role --output json",
		"issue metadata set child-1 --key a --value one --type string --output json",
		"issue metadata set child-1 --key b --value two --type string --output json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("args missing %q:\n%s", want, args)
		}
	}
}

func mustReadDriverTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}
