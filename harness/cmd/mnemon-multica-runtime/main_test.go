package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

func TestRuntimeImportsAssignedIssueIntoMnemon(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "multica.args")
	commentPath := filepath.Join(tmp, "comment.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" > "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue get iss-7"*) printf '{"id":"iss-7","identifier":"TEA-7","title":"Prepare teamwork acceptance","description":"Validate runtime issue import through real Multica task context.","status":"todo","priority":"medium"}\n' ;;
  *"issue comment add iss-7"*) cat > "$MULTICA_COMMENT_PATH"; printf '{"id":"comment-1","issue_id":"iss-7","content":"ok","type":"comment"}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(tmp, "registry.json")
	if err := driver.SaveMulticaRegistry(registryPath, driver.MulticaRegistry{
		SchemaVersion:    1,
		WorkspaceID:      "ws-1",
		RuntimeProfileID: "profile-1",
		RuntimeID:        "runtime-1",
		Participants: []driver.MulticaParticipantRecord{{
			Principal: "planner@team",
			AgentName: "mnemon-planner",
			AgentID:   "agent-1",
			Role:      "planner",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	var gotPrincipal string
	var got contract.ObservationEnvelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ingest":
			gotPrincipal = r.Header.Get(access.PrincipalHeader)
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(access.IngestReceipt{Seq: 17, Dup: false, Ticked: true})
		case "/render":
			if r.Header.Get(access.PrincipalHeader) != "planner@team" {
				t.Fatalf("render principal header = %q", r.Header.Get(access.PrincipalHeader))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(presentation.Response{
				SchemaVersion: 1,
				Status:        presentation.StatusOK,
				AuditID:       "render-audit-1",
				BodyDigest:    "sha256:render-body",
				Events: []eventmodel.EventEnvelope{{
					SchemaVersion: eventmodel.SchemaVersion,
					Phase:         eventmodel.PhaseDerived,
					Event: eventmodel.Event{
						SchemaVersion: eventmodel.SchemaVersion,
						ID:            "derived-1",
						Type:          "assignment.brief.derived",
						Subject:       "assignment/asg-1",
						Actor:         "mnemond",
						Audience:      "planner@team",
						Payload: eventmodel.BuildPayload(nil, map[string]any{
							"body": "assignment for iss-7",
						}, nil),
					},
					Meta: map[string]any{"presentation_hint": "work"},
				}},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","method":"initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"thread/start","params":{"cwd":"` + tmp + `","ephemeral":true}}`,
		`{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"threadId":"thread-1","input":[{"type":"text","text":"Your assigned issue ID is: iss-7\nPlease work on it."}]}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args: []string{"app-server", "--listen", "stdio://"},
		Env: append(os.Environ(),
			"MNEMON_MULTICA_BIN="+bin,
			"MNEMON_MULTICA_REGISTRY="+registryPath,
			"MNEMON_CONTROL_ADDR="+srv.URL,
			"MNEMON_CONTROL_PRINCIPAL=wrong@team",
			"MNEMON_MANAGED_RUNTIME=noop",
			"MNEMON_MANAGED_LEDGER="+filepath.Join(tmp, "wake-ledger.jsonl"),
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_COMMENT_PATH="+commentPath,
			"MULTICA_WORKSPACE_ID=ws-1",
			"MULTICA_TASK_ID=task-1",
			"MULTICA_AGENT_ID=agent-1",
			"MULTICA_AGENT_NAME=mnemon-planner",
			"MULTICA_SERVER_URL=https://desktop-api.multica.ai",
		),
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}

	if gotPrincipal != "planner@team" {
		t.Fatalf("principal header = %q", gotPrincipal)
	}
	if got.ExternalID != "multica-task-task-1" {
		t.Fatalf("external id = %q", got.ExternalID)
	}
	if got.Event.Type != "teamwork_signal.write_candidate.observed" {
		t.Fatalf("event type = %q", got.Event.Type)
	}
	rule := got.Event.Payload["rule"].(map[string]any)
	if rule["external_issue_id"] != "iss-7" || rule["external_task_id"] != "task-1" || rule["external_agent_id"] != "agent-1" || rule["external_workspace_id"] != "ws-1" || rule["principal"] != "planner@team" {
		t.Fatalf("rule mismatch: %+v", rule)
	}
	narrative := got.Event.Payload["narrative"].(map[string]any)
	if narrative["statement"] != "Validate runtime issue import through real Multica task context." {
		t.Fatalf("narrative mismatch: %+v", narrative)
	}
	if _, ok := narrative["external_task_id"]; ok {
		t.Fatalf("narrative leaked rule ids: %+v", narrative)
	}
	if got.Event.CorrelationID != "multica:issue:iss-7" {
		t.Fatalf("correlation id = %q", got.Event.CorrelationID)
	}

	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(args), "Validate runtime issue import") {
		t.Fatalf("issue narrative leaked into CLI arguments: %q", string(args))
	}
	if !strings.Contains(string(args), "issue comment add iss-7 --content-stdin --output json") {
		t.Fatalf("unexpected multica args: %q", string(args))
	}
	comment, err := os.ReadFile(commentPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Mnemon update: issue admitted", "Principal: planner@team", "Managed wake: completed", "mnemon:event=multica-task-task-1"} {
		if !strings.Contains(string(comment), want) {
			t.Fatalf("comment missing %s:\n%s", want, comment)
		}
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	var sawComplete, sawAnswer bool
	for _, line := range lines {
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("invalid rpc line %q: %v", line, err)
		}
		if msg["method"] == "turn/completed" {
			sawComplete = true
		}
		if msg["method"] == "item/agentMessage/delta" && strings.Contains(line, "Mnemon ingest: recorded seq=17") && strings.Contains(line, "Multica projection: comment=comment-1") && strings.Contains(line, "Managed wake: completed turn=noop-turn") {
			sawAnswer = true
		}
	}
	if !sawComplete || !sawAnswer {
		t.Fatalf("missing expected runtime response complete=%v answer=%v:\n%s", sawComplete, sawAnswer, out.String())
	}
}

func TestRuntimeSkipsIngestWhenControlAddressIsUnset(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "multica.args")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" > "$MULTICA_ARGS_PATH"
printf '{"id":"iss-8","identifier":"TEA-8","title":"No local mnemond","description":"The runtime can still complete a Multica turn when local ingest is not configured."}\n'
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	input := `{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"input":[{"type":"text","text":"Your assigned issue ID is: iss-8"}]}}` + "\n"
	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args: []string{"app-server", "--listen", "stdio://"},
		Env: append(os.Environ(),
			"MNEMON_MULTICA_BIN="+bin,
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_TASK_ID=task-8",
			"MULTICA_AGENT_NAME=mnemon-reviewer",
		),
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Mnemon ingest: skipped because MNEMON_CONTROL_ADDR is not set") {
		t.Fatalf("runtime output did not report skipped ingest:\n%s", out.String())
	}
}

func fixedRuntimeTime() time.Time {
	return time.Date(2026, 6, 28, 9, 0, 0, 0, time.UTC)
}
