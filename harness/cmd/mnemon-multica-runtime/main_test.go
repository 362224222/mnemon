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
	pview "github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

func TestRuntimeEnvValueUsesLastValue(t *testing.T) {
	env := []string{
		"MNEMON_MANAGED_RUNTIME=codex-appserver",
		"MNEMON_MANAGED_RUNTIME=off",
	}
	if got := envValue(env, "MNEMON_MANAGED_RUNTIME"); got != "off" {
		t.Fatalf("envValue = %q, want off", got)
	}
}

func TestRuntimeMulticaHubLedgerPathDefaultsToManagedWorkspace(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "managed-workspace")
	cwd := filepath.Join(tmp, "task-workdir")
	got := runtimeMulticaHubLedgerPath([]string{"MNEMON_MANAGED_WORKSPACE=" + workspace}, cwd)
	want := filepath.Join(workspace, driver.MulticaDefaultHubLedgerRelPath)
	if got != want {
		t.Fatalf("hub ledger path = %q, want %q", got, want)
	}
	explicit := filepath.Join(tmp, "explicit.jsonl")
	got = runtimeMulticaHubLedgerPath([]string{
		"MNEMON_MANAGED_WORKSPACE=" + workspace,
		"MNEMON_MULTICA_HUB_LEDGER=" + explicit,
	}, cwd)
	if got != explicit {
		t.Fatalf("explicit hub ledger path = %q, want %q", got, explicit)
	}
}

func TestMergeRuntimeHubProjectionDeltasPreservesEarlyDispatchCounts(t *testing.T) {
	result := runtimeImportResult{HubWriteStatus: "noop"}
	mergeRuntimeHubProjectionDeltas(&result, []runtimeHubProjectionDelta{
		{ChildIssues: 2},
		{FeedbackComments: 1},
	})
	if result.HubChildIssues != 2 || result.HubFeedbackComments != 1 || result.HubWriteStatus != "updated" {
		t.Fatalf("merged projection result = %+v", result)
	}
	failed := runtimeImportResult{HubWriteStatus: "failed", HubWriteErr: os.ErrInvalid}
	mergeRuntimeHubProjectionDeltas(&failed, []runtimeHubProjectionDelta{{ChildIssues: 1}})
	if failed.HubWriteStatus != "failed" || failed.HubChildIssues != 1 {
		t.Fatalf("failed projection merge should preserve final failure while carrying counts: %+v", failed)
	}
}

func TestManagedWakeMatchTermsPreferStableIssueIdentity(t *testing.T) {
	got := managedWakeMatchTerms(runtimeImportResult{
		IssueID:    "issue-123",
		Identifier: "TEA-27",
		Title:      "Mnemon R2 hub-flow completion drill",
		Statement:  "Run a small hub-flow readiness drill.",
		TaskID:     "task-123",
	})
	joined := strings.Join(got, "\n")
	for _, want := range []string{"issue-123", "TEA-27", "Mnemon R2 hub-flow completion drill", "task-123"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("match terms missing %q: %+v", want, got)
		}
	}
	if strings.Contains(joined, "Run a small hub-flow readiness drill.") {
		t.Fatalf("root issue matching must not prefer reusable statement text: %+v", got)
	}
}

func runtimeTestEnv(values ...string) []string {
	env := make([]string, 0, len(os.Environ())+len(values))
	for _, item := range os.Environ() {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		if strings.HasPrefix(key, "MNEMON_") || strings.HasPrefix(key, "MULTICA_") {
			continue
		}
		env = append(env, item)
	}
	return append(env, values...)
}

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
						ID:            "derived-stale",
						Type:          "assignment.brief.derived",
						Subject:       "assignment/asg-stale",
						Actor:         "mnemond",
						Audience:      "planner@team",
						Payload: eventmodel.BuildPayload(nil, map[string]any{
							"body": "assignment for an older issue",
						}, nil),
					},
					Meta: map[string]any{"presentation_hint": "work"},
				}, {
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
		Env: runtimeTestEnv(
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
	var sawComplete, sawProgress, sawAnswer, sawUserItem, sawAgentStart, sawAgentCompletedAt bool
	var sawCommandStart, sawCommandComplete bool
	var sawCommentaryProgress, sawFinalPhaseAnswer bool
	agentCompleted := map[string]string{}
	progressItemIDs := map[string]bool{}
	answerItemID := ""
	for _, line := range lines {
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("invalid rpc line %q: %v", line, err)
		}
		if msg["method"] == "turn/completed" {
			sawComplete = true
		}
		if msg["method"] == "item/started" {
			params, _ := msg["params"].(map[string]any)
			item, _ := params["item"].(map[string]any)
			switch item["type"] {
			case "userMessage":
				_, sawUserItem = params["startedAtMs"]
			case "agentMessage":
				id, _ := item["id"].(string)
				_, hasStarted := params["startedAtMs"]
				if hasStarted && strings.HasPrefix(id, "msg-") {
					sawAgentStart = true
				}
			case "commandExecution":
				command, _ := item["command"].(string)
				if strings.Contains(command, "multica issue get iss-7") && item["status"] == "inProgress" {
					sawCommandStart = true
				}
			}
		}
		if msg["method"] == "item/completed" {
			params, _ := msg["params"].(map[string]any)
			item, _ := params["item"].(map[string]any)
			if item["type"] == "agentMessage" {
				_, sawAgentCompletedAt = params["completedAtMs"]
				id, _ := item["id"].(string)
				text, _ := item["text"].(string)
				if id != "" {
					agentCompleted[id] = text
				}
				phase, _ := item["phase"].(string)
				if strings.Contains(text, "Loading Multica issue iss-7") && phase == "commentary" {
					sawCommentaryProgress = true
				}
				if strings.Contains(text, "Mnemon ingest: recorded seq=17") && phase == "final_answer" {
					sawFinalPhaseAnswer = true
				}
			}
			if item["type"] == "commandExecution" {
				command, _ := item["command"].(string)
				output, _ := item["aggregatedOutput"].(string)
				if strings.Contains(command, "multica issue get iss-7") && item["status"] == "completed" && strings.Contains(output, "Loaded TEA-7") {
					sawCommandComplete = true
				}
			}
		}
		if msg["method"] == "item/agentMessage/delta" && strings.Contains(line, "Loading Multica issue iss-7") {
			sawProgress = true
			params, _ := msg["params"].(map[string]any)
			if itemID, _ := params["itemId"].(string); itemID != "" {
				progressItemIDs[itemID] = true
			}
		}
		if msg["method"] == "item/agentMessage/delta" && strings.Contains(line, "Mnemon ingest: recorded seq=17") && strings.Contains(line, "Multica projection: comment=comment-1") && strings.Contains(line, "Managed wake: completed turn=noop-turn") {
			sawAnswer = true
			params, _ := msg["params"].(map[string]any)
			answerItemID, _ = params["itemId"].(string)
		}
	}
	if !sawComplete || !sawProgress || !sawAnswer || !sawUserItem || !sawAgentStart || !sawAgentCompletedAt {
		t.Fatalf("missing expected runtime response complete=%v progress=%v answer=%v user=%v agent_start=%v agent_completed_at=%v:\n%s",
			sawComplete, sawProgress, sawAnswer, sawUserItem, sawAgentStart, sawAgentCompletedAt, out.String())
	}
	if !sawCommandStart || !sawCommandComplete {
		t.Fatalf("missing commandExecution projection start=%v complete=%v:\n%s", sawCommandStart, sawCommandComplete, out.String())
	}
	if !sawCommentaryProgress || !sawFinalPhaseAnswer {
		t.Fatalf("unexpected agent message phases commentary=%v final=%v:\n%s", sawCommentaryProgress, sawFinalPhaseAnswer, out.String())
	}
	if len(agentCompleted) < 4 {
		t.Fatalf("runtime progress should complete multiple agent messages, got %d:\n%s", len(agentCompleted), out.String())
	}
	if answerItemID == "" || progressItemIDs[answerItemID] {
		t.Fatalf("final answer should be a separate agent message item from progress, answer=%q progress=%v", answerItemID, progressItemIDs)
	}
}

func TestRuntimeForwardsManagedCodexTraceItemsOnOuterTurn(t *testing.T) {
	var messages []rpcMessage
	emit := func(message rpcMessage) error {
		messages = append(messages, message)
		return nil
	}
	traceStarted := driver.ManagedTurnTraceEvent{
		SourceRuntime: driver.ManagedTurnTraceSourceCodexAppServer,
		Principal:     "planner@team",
		TurnID:        "inner-turn",
		ItemID:        "inner-msg",
		Method:        "item/started",
		ItemType:      "agentMessage",
		Item: map[string]any{
			"type":  "agentMessage",
			"id":    "inner-msg",
			"text":  "",
			"phase": "commentary",
		},
	}
	traceDelta := driver.ManagedTurnTraceEvent{
		SourceRuntime: driver.ManagedTurnTraceSourceCodexAppServer,
		Principal:     "planner@team",
		TurnID:        "inner-turn",
		ItemID:        "inner-msg",
		Method:        "item/agentMessage/delta",
		Text:          "native Codex agent detail",
	}
	traceCompleted := traceStarted
	traceCompleted.Method = "item/completed"
	traceCompleted.Item = map[string]any{
		"type":  "agentMessage",
		"id":    "inner-msg",
		"text":  "native Codex agent detail",
		"phase": "commentary",
	}
	for _, event := range []driver.ManagedTurnTraceEvent{traceStarted, traceDelta, traceCompleted} {
		if err := emitRuntimeManagedTraceEvent(emit, "outer-thread", "outer-turn", event, fixedRuntimeTime()); err != nil {
			t.Fatal(err)
		}
	}
	if len(messages) != 3 {
		t.Fatalf("messages = %+v, want 3", messages)
	}
	var itemID string
	for i, message := range messages {
		params := message.Params
		if params["threadId"] != "outer-thread" || params["turnId"] != "outer-turn" {
			t.Fatalf("message %d not attached to outer turn: %+v", i, message)
		}
		switch message.Method {
		case "item/started", "item/completed":
			item, _ := params["item"].(map[string]any)
			if item["id"] == "inner-msg" {
				t.Fatalf("message %d leaked inner item id: %+v", i, message)
			}
			if item["mnemonManagedTurnId"] != "inner-turn" || item["mnemonPrincipal"] != "planner@team" {
				t.Fatalf("message %d missing trace metadata: %+v", i, item)
			}
			if itemID == "" {
				itemID, _ = item["id"].(string)
			} else if item["id"] != itemID {
				t.Fatalf("trace item id changed across lifecycle: first=%q got=%q", itemID, item["id"])
			}
		case "item/agentMessage/delta":
			if params["itemId"] != itemID {
				t.Fatalf("delta item id = %q, want %q", params["itemId"], itemID)
			}
			if params["delta"] != "native Codex agent detail" {
				t.Fatalf("delta = %+v", params)
			}
		default:
			t.Fatalf("unexpected method %q", message.Method)
		}
	}
}

func TestRuntimeInitializesMulticaRootSessionMetadata(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "multica.args")
	commentPath := filepath.Join(tmp, "comment.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue get root-1"*) printf '{"id":"root-1","identifier":"TEA-1","title":"Coordinate release validation","description":"Plan and split release validation across the team.","status":"todo","priority":"medium"}\n' ;;
  *"issue metadata set root-1"*) printf '{}\n' ;;
  *"issue comment add root-1"*) cat > "$MULTICA_COMMENT_PATH"; printf '{"id":"comment-root","issue_id":"root-1","content":"ok","type":"comment"}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	var got contract.ObservationEnvelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ingest":
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(access.IngestReceipt{Seq: 21, Dup: false, Ticked: true})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	input := `{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"input":[{"type":"text","text":"Your assigned issue ID is: root-1"}]}}` + "\n"
	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args: []string{"app-server", "--listen", "stdio://"},
		Env: runtimeTestEnv(
			"MNEMON_MULTICA_BIN="+bin,
			"MNEMON_HUB_BACKEND=multica",
			"MNEMON_MULTICA_HUB_WRITE=off",
			"MNEMON_CONTROL_ADDR="+srv.URL,
			"MNEMON_CONTROL_PRINCIPAL=planner@team",
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_COMMENT_PATH="+commentPath,
			"MULTICA_TASK_ID=task-root",
			"MULTICA_AGENT_ID=agent-planner",
		),
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	rule := got.Event.Payload["rule"].(map[string]any)
	if rule["hub_backend"] != driver.MulticaHubBackend || rule["root_issue_id"] != "root-1" || rule["source_issue_id"] != "root-1" {
		t.Fatalf("root hub rule mismatch: %+v", rule)
	}
	if rule["session_id"] != driver.MulticaSessionID("root-1") {
		t.Fatalf("session id = %q", rule["session_id"])
	}
	args := mustReadRuntimeTestFile(t, argsPath)
	for _, want := range []string{
		"issue metadata set root-1 --key mnemon.hub_backend --value multica --type string --output json",
		"issue metadata set root-1 --key mnemon.kind --value session_mailbox --type string --output json",
		"issue metadata set root-1 --key mnemon.root_issue_id --value root-1 --type string --output json",
		"issue comment add root-1 --content-stdin --output json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("args missing %q:\n%s", want, args)
		}
	}
	comment := mustReadRuntimeTestFile(t, commentPath)
	for _, want := range []string{"Mnemon update: issue admitted", "Hub backend: multica", "Session: multica:session:root-1"} {
		if !strings.Contains(comment, want) {
			t.Fatalf("comment missing %q:\n%s", want, comment)
		}
	}
	if !strings.Contains(out.String(), "Mnemon ingest: recorded seq=21") {
		t.Fatalf("runtime output missing ingest evidence:\n%s", out.String())
	}
}

func TestRuntimeWritesAssignmentMailboxChildIssueFromView(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "multica.args")
	commentPath := filepath.Join(tmp, "comment.txt")
	createDescriptionPath := filepath.Join(tmp, "child-description.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue get root-2"*) printf '{"id":"root-2","identifier":"TEA-10","title":"Coordinate docs release","description":"Split docs release validation across the team.","status":"todo","priority":"medium"}\n' ;;
  *"issue metadata set root-2"*) printf '{}\n' ;;
  *"issue children root-2"*) printf '{"children":[]}\n' ;;
  *"issue create"*) cat > "$MULTICA_CREATE_DESCRIPTION_PATH"; printf '{"id":"child-2","identifier":"TEA-11","title":"Mnemon assignment asg-writer","status":"todo","metadata":{}}\n' ;;
  *"issue metadata set child-2"*) printf '{}\n' ;;
  *"issue comment add root-2"*) cat > "$MULTICA_COMMENT_PATH"; printf '{"id":"comment-root-2","issue_id":"root-2","content":"ok","type":"comment"}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(tmp, "registry.json")
	if err := driver.SaveMulticaRegistry(registryPath, driver.MulticaRegistry{
		SchemaVersion: 1,
		WorkspaceID:   "ws-1",
		Participants: []driver.MulticaParticipantRecord{{
			Principal: "worker@team",
			AgentName: "mnemon-worker",
			AgentID:   "agent-worker",
			Role:      "implementer",
		}},
	}); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ingest":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(access.IngestReceipt{Seq: 31, Dup: false, Ticked: true})
		case "/presentation-view":
			var sub contract.Subscription
			if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
				t.Fatal(err)
			}
			if sub.Actor != "planner@team" {
				t.Fatalf("presentation actor = %q", sub.Actor)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pview.View{
				Ref:    "view-assignments",
				Digest: "digest-assignments",
				Content: []pview.ResourceContent{{
					Ref: contract.ResourceRef{Kind: "assignment", ID: "project"},
					Fields: map[string]any{"items": []any{map[string]any{
						"id":         "asg-stale",
						"ingest_seq": float64(30),
						"actor":      "planner@team",
						"rule": map[string]any{
							"assignment_id": "asg-stale",
							"assignee":      "worker@team",
							"scope":         "old release notes",
						},
						"narrative": map[string]any{
							"expected_work":     "stale assignment from an older Multica session",
							"expected_feedback": "progress_digest with result or blocker",
						},
					}, map[string]any{
						"id":         "asg-writer",
						"ingest_seq": float64(32),
						"actor":      "planner@team",
						"rule": map[string]any{
							"assignment_id": "asg-writer",
							"assignee":      "worker@team",
							"scope":         "check release notes",
							"ttl":           "30m",
							"signal_ref":    "teamwork_signal/root-2",
						},
						"narrative": map[string]any{
							"expected_work":     "check release notes against the public changelog",
							"expected_feedback": "progress_digest with result or blocker",
						},
						"refs": map[string]any{
							"context_refs":  []any{"multica:issue:root-2"},
							"evidence_refs": []any{"multica:issue:root-2"},
						},
					}}},
				}},
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	input := `{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"input":[{"type":"text","text":"Your assigned issue ID is: root-2"}]}}` + "\n"
	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args: []string{"app-server", "--listen", "stdio://"},
		Env: runtimeTestEnv(
			"MNEMON_MULTICA_BIN="+bin,
			"MNEMON_HUB_BACKEND=multica",
			"MNEMON_CONTROL_ADDR="+srv.URL,
			"MNEMON_CONTROL_PRINCIPAL=planner@team",
			"MNEMON_MULTICA_REGISTRY="+registryPath,
			"MNEMON_MULTICA_HUB_LEDGER="+filepath.Join(tmp, "hub-ledger.jsonl"),
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_COMMENT_PATH="+commentPath,
			"MULTICA_CREATE_DESCRIPTION_PATH="+createDescriptionPath,
			"MULTICA_TASK_ID=task-root-2",
			"MULTICA_AGENT_ID=agent-planner",
		),
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Multica hub write: created child_issues=1") {
		t.Fatalf("runtime output missing hub write evidence:\n%s", out.String())
	}
	args := mustReadRuntimeTestFile(t, argsPath)
	for _, want := range []string{
		"issue children root-2 --output json",
		"issue create --title Mnemon assignment asg-writer: check release notes --output json --description-stdin --parent root-2 --status in_progress --priority medium",
		"issue metadata set child-2 --key mnemon.kind --value assignment_mailbox --type string --output json",
		"issue metadata set child-2 --key mnemon.assignment_id --value asg-writer --type string --output json",
		"issue metadata set child-2 --key mnemon.principal --value worker@team --type string --output json",
		"issue assign child-2 --to-id agent-worker --output json",
	} {
		if !strings.Contains(args, want) {
			t.Fatalf("args missing %q:\n%s", want, args)
		}
	}
	if strings.Contains(args, "check release notes against") {
		t.Fatalf("assignment description leaked into argv:\n%s", args)
	}
	if strings.Contains(args, "asg-stale") {
		t.Fatalf("stale pre-root assignment must not be projected into current root session:\n%s", args)
	}
	createIdx := strings.Index(args, "issue create --title Mnemon assignment asg-writer")
	metaIdx := strings.Index(args, "issue metadata set child-2 --key mnemon.kind")
	assignIdx := strings.Index(args, "issue assign child-2 --to-id agent-worker")
	if createIdx < 0 || metaIdx < 0 || assignIdx < 0 || !(createIdx < metaIdx && metaIdx < assignIdx) {
		t.Fatalf("assignment mailbox must be created, tagged, then assigned; args:\n%s", args)
	}
	supplementalIdx := strings.Index(args, "issue metadata set child-2 --key mnemon.projection_owner")
	if supplementalIdx < 0 || !(assignIdx < supplementalIdx) {
		t.Fatalf("non-dispatch metadata must not block child assignment; args:\n%s", args)
	}
	if strings.Contains(args, "issue status child-2 in_progress") {
		t.Fatalf("assignment mailbox should be created in progress without a separate status call:\n%s", args)
	}
	description := mustReadRuntimeTestFile(t, createDescriptionPath)
	for _, want := range []string{"Mnemon assignment mailbox", "Expected work: check release notes against the public changelog", "Expected feedback: progress_digest with result or blocker"} {
		if !strings.Contains(description, want) {
			t.Fatalf("description missing %q:\n%s", want, description)
		}
	}
	comment := mustReadRuntimeTestFile(t, commentPath)
	if !strings.Contains(comment, "Multica hub write: created child_issues=1") {
		t.Fatalf("root comment missing hub write summary:\n%s", comment)
	}
}

func TestRuntimeCorrelatesAssignmentMailboxWithoutNewIngest(t *testing.T) {
	tmp := t.TempDir()
	argsPath := filepath.Join(tmp, "multica.args")
	commentPath := filepath.Join(tmp, "comment.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue get child-1"*) printf '{"id":"child-1","identifier":"TEA-2","title":"Assignment mailbox","description":"Visible assignment summary only.","status":"todo","metadata":{"mnemon.hub_backend":"multica","mnemon.kind":"assignment_mailbox","mnemon.session_id":"multica:session:root-1","mnemon.root_issue_id":"root-1","mnemon.assignment_id":"asg-1","mnemon.assignment_fingerprint":"sha256:abc","mnemon.event_id":"event-assignment-1","mnemon.principal":"worker@team"}}\n' ;;
  *"issue comment add child-1"*) cat > "$MULTICA_COMMENT_PATH"; printf '{"id":"comment-child","issue_id":"child-1","content":"ok","type":"comment"}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	var ingestCalled bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ingest":
			ingestCalled = true
			http.Error(w, "assignment mailbox must not ingest a new signal", http.StatusInternalServerError)
		case "/render":
			if r.Header.Get(access.PrincipalHeader) != "worker@team" {
				t.Fatalf("render principal header = %q", r.Header.Get(access.PrincipalHeader))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(presentation.Response{
				SchemaVersion: 1,
				Status:        presentation.StatusOK,
				AuditID:       "render-audit-child",
				BodyDigest:    "sha256:render-child",
				Events: []eventmodel.EventEnvelope{{
					SchemaVersion: eventmodel.SchemaVersion,
					Phase:         eventmodel.PhaseDerived,
					Event: eventmodel.Event{
						SchemaVersion: eventmodel.SchemaVersion,
						ID:            "derived-work-asg-1",
						Type:          "assignment.work_available",
						Subject:       "assignment/asg-1",
						Actor:         "mnemond",
						Audience:      "worker@team",
						Payload: eventmodel.BuildPayload(nil, map[string]any{
							"body": "Assignment asg-1 is yours: release validation. Expected work: check the edge cases.",
						}, nil),
					},
					Meta: map[string]any{"presentation_hint": "work"},
				}},
			})
		case "/presentation-view":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(pview.View{
				Ref:    "view-empty",
				Digest: "digest-empty",
			})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer srv.Close()

	input := `{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"input":[{"type":"text","text":"Your assigned issue ID is: child-1"}]}}` + "\n"
	var out bytes.Buffer
	err := runRuntime(runtimeConfig{
		Args: []string{"app-server", "--listen", "stdio://"},
		Env: runtimeTestEnv(
			"MNEMON_MULTICA_BIN="+bin,
			"MNEMON_HUB_BACKEND=multica",
			"MNEMON_CONTROL_ADDR="+srv.URL,
			"MNEMON_CONTROL_PRINCIPAL=worker@team",
			"MNEMON_MANAGED_RUNTIME=noop",
			"MNEMON_MANAGED_LEDGER="+filepath.Join(tmp, "wake-ledger.jsonl"),
			"MULTICA_ARGS_PATH="+argsPath,
			"MULTICA_COMMENT_PATH="+commentPath,
			"MULTICA_TASK_ID=task-child",
			"MULTICA_AGENT_ID=agent-worker",
		),
		CWD:    tmp,
		Stdin:  strings.NewReader(input),
		Stdout: &out,
		Now:    fixedRuntimeTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ingestCalled {
		t.Fatal("assignment mailbox dispatch must not ingest a new teamwork signal")
	}
	if !strings.Contains(out.String(), "Mnemon assignment mailbox: correlated assignment=asg-1") || !strings.Contains(out.String(), "Managed wake: completed turn=noop-turn") {
		t.Fatalf("runtime output missing correlation/wake evidence:\n%s", out.String())
	}
	comment := mustReadRuntimeTestFile(t, commentPath)
	for _, want := range []string{
		"Mnemon update: assignment mailbox correlated",
		"Assignment: asg-1",
		"mnemon:event=event-assignment-1",
		"Managed wake: completed",
	} {
		if !strings.Contains(comment, want) {
			t.Fatalf("comment missing %q:\n%s", want, comment)
		}
	}
}

func TestMulticaStatusForProgressIsRuleBased(t *testing.T) {
	for _, tc := range []struct {
		name string
		item runtimeProgress
		want string
	}{
		{name: "progress", item: runtimeProgress{FeedbackKind: "progress"}, want: "in_progress"},
		{name: "result", item: runtimeProgress{FeedbackKind: "result"}, want: "done"},
		{name: "blocker", item: runtimeProgress{FeedbackKind: "blocker"}, want: "blocked"},
		{name: "blocker narrative fallback", item: runtimeProgress{Blocker: "waiting on access"}, want: "blocked"},
		{name: "result narrative fallback", item: runtimeProgress{Result: "validated"}, want: "done"},
		{name: "unknown", item: runtimeProgress{Summary: "not enough signal"}, want: ""},
	} {
		if got := multicasurface.ProgressIssueStatus(progressFeedbackMaterial(tc.item)); got != tc.want {
			t.Fatalf("%s: status = %q, want %q", tc.name, got, tc.want)
		}
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
		Env: runtimeTestEnv(
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

func mustReadRuntimeTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(data))
}
