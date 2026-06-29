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

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
)

func TestMulticaImportIssueWritesObservedDraftToLocalIngest(t *testing.T) {
	restoreMulticaFlags(t)

	issuePath := filepath.Join(t.TempDir(), "issue.json")
	if err := os.WriteFile(issuePath, []byte(`{
		"id": "iss-7",
		"identifier": "MUL-7",
		"title": "Coordinate adapter validation",
		"description": "Validate that a Multica issue can be handed to local teamwork without leaking rule ids into narrative."
	}`), 0o600); err != nil {
		t.Fatal(err)
	}

	var got contract.ObservationEnvelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest" {
			t.Fatalf("path = %q", r.URL.Path)
		}
		if r.Header.Get("X-Mnemon-Principal") != "codex@project" {
			t.Fatalf("principal header = %q", r.Header.Get("X-Mnemon-Principal"))
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"seq": 7, "dup": false, "ticked": true})
	}))
	defer srv.Close()

	multicaIssueJSON = issuePath
	multicaScope = "multica/poc"
	multicaTTL = "45m"
	multicaWhyTeamwork = "Adapter validation needs multiple local agents."
	multicaContextRefs = []string{"multica:workspace:test"}
	multicaEvidenceRefs = []string{"multica:issue:iss-7"}
	multicaLocalAddr = srv.URL
	multicaLocalPrincipal = "codex@project"
	multicaJSON = true

	var out bytes.Buffer
	multicaImportIssueCmd.SetOut(&out)
	t.Cleanup(func() {
		multicaImportIssueCmd.SetOut(os.Stdout)
	})
	if err := runMulticaImportIssue(multicaImportIssueCmd, nil); err != nil {
		t.Fatalf("import issue: %v", err)
	}

	if got.ExternalID != "multica-issue-iss-7" {
		t.Fatalf("external id = %q", got.ExternalID)
	}
	if got.Event.Type != "teamwork_signal.write_candidate.observed" {
		t.Fatalf("event type = %q", got.Event.Type)
	}
	rule, _ := got.Event.Payload["rule"].(map[string]any)
	if rule["external_source"] != "multica" || rule["external_issue_id"] != "iss-7" || rule["ttl"] != "45m" {
		t.Fatalf("rule payload mismatch: %+v", rule)
	}
	narrative, _ := got.Event.Payload["narrative"].(map[string]any)
	if narrative["title"] != "Coordinate adapter validation" {
		t.Fatalf("narrative title mismatch: %+v", narrative)
	}
	if _, ok := narrative["external_issue_id"]; ok {
		t.Fatalf("narrative must not carry rule ids: %+v", narrative)
	}
	refs, _ := got.Event.Payload["refs"].(map[string]any)
	if refs == nil || len(refs) == 0 {
		t.Fatalf("refs payload missing: %+v", got.Event.Payload)
	}
}

func TestMulticaParticipantRegisterAdoptsExistingUIAgent(t *testing.T) {
	restoreMulticaFlags(t)

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	envStdinPath := filepath.Join(tmp, "agent-env.jsonl")
	argsPath := filepath.Join(tmp, "args.log")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"version --output json"*) printf '{"version":"v0.3.31","commit":"test","date":"now","os":"darwin","arch":"arm64","go":"go"}\n' ;;
  *"auth status"*) printf 'Server: https://api.multica.ai\nUser: Test\n' >&2 ;;
  *"daemon status --output json"*) printf '{"status":"running"}\n' ;;
  *"agent list"*) printf '[{"id":"agent-ui-reviewer","name":"ui-reviewer","description":"Created in UI","runtime_id":"runtime-ui","status":"idle","visibility":"workspace","workspace_id":"ws-ui"}]\n' ;;
  *"agent env get"*) printf '{}\n' ;;
  *"agent env set"*) cat >> "$MULTICA_ENV_STDIN_PATH"; printf '\n' >> "$MULTICA_ENV_STDIN_PATH"; printf '{}\n' ;;
  *"agent create"*|*"agent update"*|*"agent restore"*) printf 'unexpected mutation: %s\n' "$*" >&2; exit 42 ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(tmp, "registry.json")
	multicaBin = bin
	multicaProfile = "desktop-api.multica.ai"
	multicaWorkspaceID = "ws-ui"
	multicaParticipantRegistry = registryPath
	multicaParticipantAgentName = "ui-reviewer"
	multicaParticipantPrincipal = "reviewer@team"
	multicaParticipantRole = "reviewer"
	multicaParticipantControlAddr = "http://127.0.0.1:8791"
	multicaParticipantHarnessBin = "/abs/mnemon-harness"
	multicaParticipantManagedRuntime = "codex-appserver"
	multicaParticipantManagedCommand = "codex"
	multicaParticipantManagedWorkspace = tmp
	multicaJSON = true
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_ENV_STDIN_PATH", envStdinPath)

	var out bytes.Buffer
	multicaParticipantRegisterCmd.SetOut(&out)
	t.Cleanup(func() {
		multicaParticipantRegisterCmd.SetOut(os.Stdout)
	})
	if err := runMulticaParticipantRegister(multicaParticipantRegisterCmd, nil); err != nil {
		t.Fatalf("participant register: %v", err)
	}
	var report multicaParticipantRegisterReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("register output must be JSON: %v\n%s", err, out.String())
	}
	if report.AgentAction != "reused" || !report.UpdatedEnv {
		t.Fatalf("register report mismatch: %+v", report)
	}
	if report.Participant.Principal != "reviewer@team" || report.Participant.AgentID != "agent-ui-reviewer" || report.Participant.AgentName != "ui-reviewer" {
		t.Fatalf("participant mismatch: %+v", report.Participant)
	}
	reg, ok, err := driver.LoadMulticaRegistry(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || reg.WorkspaceID != "ws-ui" || reg.RuntimeID != "runtime-ui" || len(reg.Participants) != 1 {
		t.Fatalf("registry mismatch: ok=%v reg=%+v", ok, reg)
	}
	envStdin, err := os.ReadFile(envStdinPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"MNEMON_CONTROL_PRINCIPAL":"reviewer@team"`,
		`"MNEMON_HUB_BACKEND":"multica"`,
		`"MNEMON_MULTICA_REGISTRY":"` + registryPath + `"`,
		`"MNEMON_MANAGED_RUNTIME":"codex-appserver"`,
		`"MNEMON_MANAGED_COMMAND":"codex"`,
	} {
		if !strings.Contains(string(envStdin), want) {
			t.Fatalf("agent env stdin missing %s:\n%s", want, envStdin)
		}
	}
	argsLog, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(argsLog), "agent create") || strings.Contains(string(argsLog), "agent update") {
		t.Fatalf("register must adopt without creating/updating the UI agent:\n%s", argsLog)
	}
}

func TestMulticaProvisionCreatesParticipantRegistry(t *testing.T) {
	restoreMulticaFlags(t)

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	envStdinPath := filepath.Join(tmp, "agent-env.jsonl")
	script := `#!/usr/bin/env sh
case "$*" in
  *"version --output json"*) printf '{"version":"v0.3.31","commit":"test","date":"now","os":"darwin","arch":"arm64","go":"go"}\n' ;;
  *"auth status"*) printf 'Server: https://api.multica.ai\nUser: Test\n' >&2 ;;
  *"daemon status --output json"*) printf '{"status":"running"}\n' ;;
  *"runtime profile list"*) printf '[]\n' ;;
  *"runtime profile create"*) printf '{"id":"profile-1","display_name":"mnemon-runtime","command_name":"mnemon-multica-runtime","protocol_family":"codex","enabled":true,"workspace_id":"ws-1"}\n' ;;
  *"runtime profile set-path"*) printf '{}\n' ;;
  *"runtime list"*) printf '[{"id":"runtime-1","name":"Mnemon (Mac)","provider":"codex","status":"online","profile_id":"profile-1","workspace_id":"ws-1"}]\n' ;;
  *"agent list"*) printf '[]\n' ;;
  *"agent create"*) name=""; prev=""; for arg in "$@"; do if [ "$prev" = "--name" ]; then name="$arg"; fi; prev="$arg"; done; printf '{"id":"agent-%s","name":"%s","runtime_id":"runtime-1","status":"idle","visibility":"private","workspace_id":"ws-1"}\n' "$name" "$name" ;;
  *"agent env get"*) printf '{}\n' ;;
  *"agent env set"*) cat >> "$MULTICA_ENV_STDIN_PATH"; printf '\n' >> "$MULTICA_ENV_STDIN_PATH"; printf '{}\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(tmp, "registry.json")
	credentialsDir := filepath.Join(tmp, ".mnemon", "harness", "channel", "credentials")
	if err := os.MkdirAll(credentialsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	plannerTokenFile := filepath.Join(credentialsDir, "planner-team.token")
	implementerTokenFile := filepath.Join(credentialsDir, "implementer-team.token")
	for _, path := range []string{plannerTokenFile, implementerTokenFile} {
		if err := os.WriteFile(path, []byte("token\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	multicaBin = bin
	multicaProfile = "desktop-api.multica.ai"
	multicaWorkspaceID = "ws-1"
	multicaProvisionRegistry = registryPath
	multicaProvisionProjectRoot = tmp
	multicaProvisionProfileName = "mnemon-runtime"
	multicaProvisionRuntimeCommand = "mnemon-multica-runtime"
	multicaProvisionRuntimePath = "/abs/mnemon-multica-runtime"
	multicaProvisionAgentPrefix = "mnemon"
	multicaProvisionRestartDaemon = false
	multicaProvisionWait = 0
	multicaProvisionControlAddr = "http://127.0.0.1:8787"
	multicaProvisionHarnessBin = "/abs/mnemon-harness"
	multicaProvisionManagedRuntime = "noop"
	multicaProvisionManagedWorkspace = tmp
	multicaJSON = true
	t.Setenv("MULTICA_ENV_STDIN_PATH", envStdinPath)

	var out bytes.Buffer
	multicaProvisionCmd.SetOut(&out)
	t.Cleanup(func() {
		multicaProvisionCmd.SetOut(os.Stdout)
	})
	if err := runMulticaProvision(multicaProvisionCmd, nil); err != nil {
		t.Fatalf("provision: %v", err)
	}
	var report multicaProvisionReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("provision output must be JSON: %v\n%s", err, out.String())
	}
	if report.RuntimeProfile.ID != "profile-1" || report.Runtime.ID != "runtime-1" || len(report.Participants) != 5 {
		t.Fatalf("report mismatch: %+v", report)
	}
	if len(report.UpdatedEnv) != 5 {
		t.Fatalf("expected env updates for every participant: %+v", report)
	}
	reg, ok, err := driver.LoadMulticaRegistry(registryPath)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("registry was not written")
	}
	if reg.WorkspaceID != "ws-1" || reg.RuntimeProfileID != "profile-1" || reg.RuntimeID != "runtime-1" || len(reg.Participants) != 5 {
		t.Fatalf("registry mismatch: %+v", reg)
	}
	for _, participant := range reg.Participants {
		if !strings.HasPrefix(participant.AgentID, "agent-mnemon-") {
			t.Fatalf("participant missing agent id: %+v", participant)
		}
	}
	envStdin, err := os.ReadFile(envStdinPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"MNEMON_HUB_BACKEND":"multica"`,
		`"MNEMON_MULTICA_REGISTRY":"` + registryPath + `"`,
		`"MNEMON_MULTICA_WORKSPACE_ID":"ws-1"`,
		`"MNEMON_CONTROL_ADDR":"http://127.0.0.1:8787"`,
		`"MNEMON_CONTROL_PRINCIPAL":"planner@team"`,
		`"MNEMON_CONTROL_TOKEN_FILE":"` + plannerTokenFile + `"`,
		`"MNEMON_CONTROL_TOKEN_FILE":"` + implementerTokenFile + `"`,
		`"MNEMON_HARNESS_BIN":"/abs/mnemon-harness"`,
		`"MNEMON_MANAGED_RUNTIME":"noop"`,
		`"MNEMON_MANAGED_WORKSPACE":"` + tmp + `"`,
	} {
		if !strings.Contains(string(envStdin), want) {
			t.Fatalf("agent env stdin missing %s:\n%s", want, envStdin)
		}
	}
}

func TestMergeMulticaParticipantRuntimeEnvPrunesStaleManagedKeys(t *testing.T) {
	merged := mergeMulticaParticipantRuntimeEnv(map[string]string{
		"MNEMON_CONTROL_TOKEN":      "old-token",
		"MNEMON_CONTROL_TOKEN_FILE": "/old/token",
		"MNEMON_MANAGED_RUNTIME":    "codex-appserver",
		"MNEMON_HUB_BACKEND":        "old",
		"CUSTOM_USER_ENV":           "keep",
	}, map[string]string{
		"MNEMON_HUB_BACKEND":          "multica",
		"MNEMON_CONTROL_ADDR":         "http://127.0.0.1:8791",
		"MNEMON_CONTROL_PRINCIPAL":    "planner@team",
		"MNEMON_MANAGED_WORKSPACE":    "/workspace",
		"MNEMON_MULTICA_REGISTRY":     "/registry.json",
		"MNEMON_MULTICA_WORKSPACE_ID": "ws-1",
	})
	for _, stale := range []string{"MNEMON_CONTROL_TOKEN", "MNEMON_CONTROL_TOKEN_FILE", "MNEMON_MANAGED_RUNTIME"} {
		if _, ok := merged[stale]; ok {
			t.Fatalf("stale managed key %s should be pruned: %+v", stale, merged)
		}
	}
	if merged["CUSTOM_USER_ENV"] != "keep" {
		t.Fatalf("unmanaged env should be preserved: %+v", merged)
	}
	if merged["MNEMON_HUB_BACKEND"] != "multica" || merged["MNEMON_CONTROL_PRINCIPAL"] != "planner@team" {
		t.Fatalf("desired managed env not applied: %+v", merged)
	}
}

func restoreMulticaFlags(t *testing.T) {
	t.Helper()
	oldBin := multicaBin
	oldProfile := multicaProfile
	oldServerURL := multicaServerURL
	oldWorkspaceID := multicaWorkspaceID
	oldTimeout := multicaTimeout
	oldJSON := multicaJSON
	oldIssueID := multicaIssueID
	oldIssueJSON := multicaIssueJSON
	oldScope := multicaScope
	oldTTL := multicaTTL
	oldWhy := multicaWhyTeamwork
	oldEvidence := multicaEvidenceRefs
	oldContext := multicaContextRefs
	oldDryRun := multicaDryRun
	oldAddr := multicaLocalAddr
	oldPrincipal := multicaLocalPrincipal
	oldToken := multicaLocalToken
	oldTokenFile := multicaLocalTokenFile
	oldContent := multicaCommentContent
	oldFile := multicaCommentFile
	oldStdin := multicaCommentStdin
	oldTitle := multicaCommentTitle
	oldEvents := multicaCommentEvents
	oldProvisionRegistry := multicaProvisionRegistry
	oldProvisionProjectRoot := multicaProvisionProjectRoot
	oldProvisionProfileName := multicaProvisionProfileName
	oldProvisionRuntimeCommand := multicaProvisionRuntimeCommand
	oldProvisionRuntimePath := multicaProvisionRuntimePath
	oldProvisionAgentPrefix := multicaProvisionAgentPrefix
	oldProvisionRestartDaemon := multicaProvisionRestartDaemon
	oldProvisionWait := multicaProvisionWait
	oldProvisionControlAddr := multicaProvisionControlAddr
	oldProvisionControlToken := multicaProvisionControlToken
	oldProvisionControlTokenFile := multicaProvisionControlTokenFile
	oldProvisionHarnessBin := multicaProvisionHarnessBin
	oldProvisionManagedRuntime := multicaProvisionManagedRuntime
	oldProvisionManagedCommand := multicaProvisionManagedCommand
	oldProvisionManagedWorkspace := multicaProvisionManagedWorkspace
	oldProvisionManagedTimeout := multicaProvisionManagedTimeout
	oldParticipantRegistry := multicaParticipantRegistry
	oldParticipantProjectRoot := multicaParticipantProjectRoot
	oldParticipantAgentID := multicaParticipantAgentID
	oldParticipantAgentName := multicaParticipantAgentName
	oldParticipantPrincipal := multicaParticipantPrincipal
	oldParticipantRole := multicaParticipantRole
	oldParticipantRuntimeID := multicaParticipantRuntimeID
	oldParticipantCreateIfMissing := multicaParticipantCreateIfMissing
	oldParticipantSyncAgent := multicaParticipantSyncAgent
	oldParticipantControlAddr := multicaParticipantControlAddr
	oldParticipantControlToken := multicaParticipantControlToken
	oldParticipantControlTokenFile := multicaParticipantControlTokenFile
	oldParticipantHarnessBin := multicaParticipantHarnessBin
	oldParticipantManagedRuntime := multicaParticipantManagedRuntime
	oldParticipantManagedCommand := multicaParticipantManagedCommand
	oldParticipantManagedWorkspace := multicaParticipantManagedWorkspace
	oldParticipantManagedTimeout := multicaParticipantManagedTimeout
	t.Cleanup(func() {
		multicaBin = oldBin
		multicaProfile = oldProfile
		multicaServerURL = oldServerURL
		multicaWorkspaceID = oldWorkspaceID
		multicaTimeout = oldTimeout
		multicaJSON = oldJSON
		multicaIssueID = oldIssueID
		multicaIssueJSON = oldIssueJSON
		multicaScope = oldScope
		multicaTTL = oldTTL
		multicaWhyTeamwork = oldWhy
		multicaEvidenceRefs = oldEvidence
		multicaContextRefs = oldContext
		multicaDryRun = oldDryRun
		multicaLocalAddr = oldAddr
		multicaLocalPrincipal = oldPrincipal
		multicaLocalToken = oldToken
		multicaLocalTokenFile = oldTokenFile
		multicaCommentContent = oldContent
		multicaCommentFile = oldFile
		multicaCommentStdin = oldStdin
		multicaCommentTitle = oldTitle
		multicaCommentEvents = oldEvents
		multicaProvisionRegistry = oldProvisionRegistry
		multicaProvisionProjectRoot = oldProvisionProjectRoot
		multicaProvisionProfileName = oldProvisionProfileName
		multicaProvisionRuntimeCommand = oldProvisionRuntimeCommand
		multicaProvisionRuntimePath = oldProvisionRuntimePath
		multicaProvisionAgentPrefix = oldProvisionAgentPrefix
		multicaProvisionRestartDaemon = oldProvisionRestartDaemon
		multicaProvisionWait = oldProvisionWait
		multicaProvisionControlAddr = oldProvisionControlAddr
		multicaProvisionControlToken = oldProvisionControlToken
		multicaProvisionControlTokenFile = oldProvisionControlTokenFile
		multicaProvisionHarnessBin = oldProvisionHarnessBin
		multicaProvisionManagedRuntime = oldProvisionManagedRuntime
		multicaProvisionManagedCommand = oldProvisionManagedCommand
		multicaProvisionManagedWorkspace = oldProvisionManagedWorkspace
		multicaProvisionManagedTimeout = oldProvisionManagedTimeout
		multicaParticipantRegistry = oldParticipantRegistry
		multicaParticipantProjectRoot = oldParticipantProjectRoot
		multicaParticipantAgentID = oldParticipantAgentID
		multicaParticipantAgentName = oldParticipantAgentName
		multicaParticipantPrincipal = oldParticipantPrincipal
		multicaParticipantRole = oldParticipantRole
		multicaParticipantRuntimeID = oldParticipantRuntimeID
		multicaParticipantCreateIfMissing = oldParticipantCreateIfMissing
		multicaParticipantSyncAgent = oldParticipantSyncAgent
		multicaParticipantControlAddr = oldParticipantControlAddr
		multicaParticipantControlToken = oldParticipantControlToken
		multicaParticipantControlTokenFile = oldParticipantControlTokenFile
		multicaParticipantHarnessBin = oldParticipantHarnessBin
		multicaParticipantManagedRuntime = oldParticipantManagedRuntime
		multicaParticipantManagedCommand = oldParticipantManagedCommand
		multicaParticipantManagedWorkspace = oldParticipantManagedWorkspace
		multicaParticipantManagedTimeout = oldParticipantManagedTimeout
	})
	multicaBin = ""
	multicaProfile = ""
	multicaServerURL = ""
	multicaWorkspaceID = ""
	multicaJSON = false
	multicaIssueID = ""
	multicaIssueJSON = ""
	multicaScope = "multica/teamwork"
	multicaTTL = "30m"
	multicaWhyTeamwork = ""
	multicaEvidenceRefs = nil
	multicaContextRefs = nil
	multicaDryRun = false
	multicaLocalAddr = "http://127.0.0.1:8787"
	multicaLocalPrincipal = ""
	multicaLocalToken = ""
	multicaLocalTokenFile = ""
	multicaCommentContent = ""
	multicaCommentFile = ""
	multicaCommentStdin = false
	multicaCommentTitle = ""
	multicaCommentEvents = nil
	multicaProvisionRegistry = ""
	multicaProvisionProjectRoot = "."
	multicaProvisionProfileName = "mnemon-runtime"
	multicaProvisionRuntimeCommand = "mnemon-multica-runtime"
	multicaProvisionRuntimePath = ""
	multicaProvisionAgentPrefix = "mnemon"
	multicaProvisionRestartDaemon = false
	multicaProvisionWait = 30 * 1_000_000_000
	multicaProvisionControlAddr = ""
	multicaProvisionControlToken = ""
	multicaProvisionControlTokenFile = ""
	multicaProvisionManagedRuntime = ""
	multicaProvisionManagedCommand = ""
	multicaProvisionManagedWorkspace = ""
	multicaProvisionManagedTimeout = 0
	multicaParticipantRegistry = ""
	multicaParticipantProjectRoot = "."
	multicaParticipantAgentID = ""
	multicaParticipantAgentName = ""
	multicaParticipantPrincipal = ""
	multicaParticipantRole = ""
	multicaParticipantRuntimeID = ""
	multicaParticipantCreateIfMissing = false
	multicaParticipantSyncAgent = false
	multicaParticipantControlAddr = ""
	multicaParticipantControlToken = ""
	multicaParticipantControlTokenFile = ""
	multicaParticipantHarnessBin = ""
	multicaParticipantManagedRuntime = ""
	multicaParticipantManagedCommand = ""
	multicaParticipantManagedWorkspace = ""
	multicaParticipantManagedTimeout = 0
}
