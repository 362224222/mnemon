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
