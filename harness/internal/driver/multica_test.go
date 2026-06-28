package driver

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildMulticaIssueTeamworkSignalSeparatesRuleAndNarrative(t *testing.T) {
	draft, err := BuildMulticaIssueTeamworkSignal(MulticaIssue{
		ID:          "iss-123",
		Identifier:  "MUL-123",
		Title:       "Validate bridge",
		Description: "Check that Multica issue context can start Mnemon teamwork.",
	}, MulticaIssueSignalOptions{
		Scope:        "multica/poc",
		TTL:          "45m",
		WhyTeamwork:  "The task needs more than one local agent.",
		EvidenceRefs: []string{"multica:issue/iss-123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if draft.EventType != "teamwork_signal.write_candidate.observed" {
		t.Fatalf("event type = %q", draft.EventType)
	}
	rule := draft.Payload["rule"].(map[string]any)
	if rule["external_source"] != MulticaExternalSource || rule["external_issue_id"] != "iss-123" || rule["scope"] != "multica/poc" {
		t.Fatalf("rule mapping mismatch: %+v", rule)
	}
	narrative := draft.Payload["narrative"].(map[string]any)
	if narrative["statement"] != "Check that Multica issue context can start Mnemon teamwork." {
		t.Fatalf("narrative statement mismatch: %+v", narrative)
	}
	if _, ok := narrative["external_issue_id"]; ok {
		t.Fatalf("narrative must not carry rule ids: %+v", narrative)
	}
}

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

func TestFormatMulticaProjectionCommentCarriesStableMarkers(t *testing.T) {
	got := FormatMulticaProjectionComment("assignment finished", "Worker reported passing evidence.", []string{"ev-1", "ev-1", "ev-2"})
	for _, want := range []string{
		"Mnemon update: assignment finished",
		"Worker reported passing evidence.",
		"mnemon:event=ev-1",
		"mnemon:event=ev-2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("projection comment missing %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "mnemon:event=ev-1") != 1 {
		t.Fatalf("projection comment should dedupe markers:\n%s", got)
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
