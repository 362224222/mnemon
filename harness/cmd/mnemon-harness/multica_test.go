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
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
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

func TestMulticaSurfaceReportWritesDisplayOnlyState(t *testing.T) {
	restoreMulticaFlags(t)

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	argsPath := filepath.Join(tmp, "args.log")
	commentPath := filepath.Join(tmp, "comment.md")
	script := `#!/usr/bin/env sh
set -eu
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue comment add iss-report"*) cat > "$MULTICA_COMMENT_PATH"; printf '{"id":"comment-1","issue_id":"iss-report","content":"ok"}\n' ;;
  *"issue metadata set iss-report"*) printf '{}\n' ;;
  *"issue status iss-report done"*) printf '{"id":"iss-report","status":"done"}\n' ;;
  *) printf 'unexpected multica args: %s\n' "$*" >&2; exit 42 ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	multicaBin = bin
	multicaProfile = "desktop-api.multica.ai"
	multicaIssueID = "iss-report"
	multicaCommentTitle = "中文进展报告"
	multicaSurfaceStatusLabel = "已完成第一轮"
	multicaSurfaceSummary = "完成退款规则和客服话术复盘。"
	multicaCommentContent = "结论：需要补充异常退款审批人。"
	multicaSurfaceDesiredStatus = "done"
	multicaSurfaceEventRef = "event:accepted-1"
	multicaSurfaceResourceRef = "assignment:refund-review"
	multicaSurfaceRef = "multica:iss-report:comment"
	multicaSurfaceSourceArtifactRef = "artifact:refund.md"
	multicaSurfaceEvidenceRefs = []string{"evidence:log-1"}
	multicaSurfaceArtifactRefs = []string{"artifact:refund.md"}
	multicaJSON = true
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_COMMENT_PATH", commentPath)

	var out bytes.Buffer
	multicaSurfaceReportCmd.SetOut(&out)
	t.Cleanup(func() {
		multicaSurfaceReportCmd.SetOut(os.Stdout)
	})
	if err := runMulticaSurfaceReport(multicaSurfaceReportCmd, nil); err != nil {
		t.Fatalf("surface report: %v", err)
	}
	comment := readTestFile(t, commentPath)
	for _, want := range []string{"Mnemon 更新: 中文进展报告", "## 摘要", "完成退款规则", "## 事件引用", "event:accepted-1"} {
		if !strings.Contains(comment, want) {
			t.Fatalf("surface report comment missing %q:\n%s", want, comment)
		}
	}
	argsLog := readTestFile(t, argsPath)
	for _, want := range []string{
		"issue comment add iss-report --content-stdin --output json",
		"issue metadata set iss-report --key " + multicasurface.MulticaMetadataEventRef + " --value event:accepted-1",
		"issue metadata set iss-report --key " + multicasurface.MulticaMetadataNoAutoDispatch + " --value true",
		"issue status iss-report done --output json",
	} {
		if !strings.Contains(argsLog, want) {
			t.Fatalf("surface report args missing %q:\n%s", want, argsLog)
		}
	}
	for _, forbidden := range []string{"issue assign", "[mnemon:wake]"} {
		if strings.Contains(argsLog, forbidden) {
			t.Fatalf("display report must not trigger execution via %q:\n%s", forbidden, argsLog)
		}
	}
	var report multicaSurfaceReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("surface report output must be JSON: %v\n%s", err, out.String())
	}
	if report.Status != "done" || !report.NoAutoDispatch || report.Metadata[multicasurface.MulticaMetadataSurfaceRole] != string(multicasurface.SurfaceRoleDisplay) {
		t.Fatalf("surface report mismatch: %+v", report)
	}
}

func TestMulticaActivationCarrierCreatesTriggerIssue(t *testing.T) {
	restoreMulticaFlags(t)

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "multica")
	argsPath := filepath.Join(tmp, "args.log")
	descriptionPath := filepath.Join(tmp, "activation.md")
	script := `#!/usr/bin/env sh
set -eu
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"issue create"*) cat > "$MULTICA_DESCRIPTION_PATH"; printf '{"id":"iss-activation","identifier":"ACT-1","title":"激活下一轮协作","status":"todo"}\n' ;;
  *"issue metadata set iss-activation"*) printf '{}\n' ;;
  *) printf 'unexpected multica args: %s\n' "$*" >&2; exit 42 ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	multicaBin = bin
	multicaProfile = "desktop-api.multica.ai"
	multicaIssueID = "iss-parent"
	multicaCommentTitle = "激活下一轮协作"
	multicaCommentContent = "请根据已接受事件继续处理客户退款争议。"
	multicaSurfaceEventRef = "event:accepted-2"
	multicaSurfaceResourceRef = "assignment:refund-follow-up"
	multicaSurfaceTargetAgentID = "agent-reviewer"
	multicaJSON = true
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_DESCRIPTION_PATH", descriptionPath)

	var out bytes.Buffer
	multicaActivationCarrierCmd.SetOut(&out)
	t.Cleanup(func() {
		multicaActivationCarrierCmd.SetOut(os.Stdout)
	})
	if err := runMulticaActivationCarrier(multicaActivationCarrierCmd, nil); err != nil {
		t.Fatalf("activation carrier: %v", err)
	}
	argsLog := readTestFile(t, argsPath)
	for _, want := range []string{
		"issue create --title 激活下一轮协作 --output json --description-stdin --assignee-id agent-reviewer --parent iss-parent --status todo --priority medium --allow-duplicate",
		"issue metadata set iss-activation --key " + multicasurface.MulticaMetadataEventRef + " --value event:accepted-2",
		"issue metadata set iss-activation --key " + multicasurface.MulticaMetadataSurfaceRole + " --value activate",
	} {
		if !strings.Contains(argsLog, want) {
			t.Fatalf("activation carrier args missing %q:\n%s", want, argsLog)
		}
	}
	description := readTestFile(t, descriptionPath)
	for _, want := range []string{"请根据已接受事件继续处理客户退款争议", "Mnemon 激活载体", "event:accepted-2"} {
		if !strings.Contains(description, want) {
			t.Fatalf("activation description missing %q:\n%s", want, description)
		}
	}
	var report multicaActivationCarrierReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("activation carrier output must be JSON: %v\n%s", err, out.String())
	}
	if report.ParentIssueID != "iss-parent" || report.Issue.ID != "iss-activation" || report.Metadata[multicasurface.MulticaMetadataSurfaceRole] != string(multicasurface.SurfaceRoleActivate) {
		t.Fatalf("activation report mismatch: %+v", report)
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
	multicaParticipantProviderRuntime = "codex"
	multicaParticipantProviderCommand = "codex"
	multicaParticipantProviderWorkspace = tmp
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
		`"MNEMON_MULTICA_REGISTRY":"` + registryPath + `"`,
		`"MNEMON_MULTICA_PROVIDER_RUNTIME":"codex"`,
		`"MNEMON_MULTICA_PROVIDER_COMMAND":"codex"`,
	} {
		if !strings.Contains(string(envStdin), want) {
			t.Fatalf("agent env stdin missing %s:\n%s", want, envStdin)
		}
	}
	if strings.Contains(string(envStdin), "MNEMON_HUB_BACKEND") {
		t.Fatalf("R3 runtime env must not configure Multica as hub backend:\n%s", envStdin)
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
	multicaProvisionProviderRuntime = "noop"
	multicaProvisionProviderWorkspace = tmp
	multicaProvisionAcceptanceBridge = true
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
		`"MNEMON_MULTICA_REGISTRY":"` + registryPath + `"`,
		`"MNEMON_MULTICA_WORKSPACE_ID":"ws-1"`,
		`"MNEMON_CONTROL_ADDR":"http://127.0.0.1:8787"`,
		`"MNEMON_CONTROL_PRINCIPAL":"planner@team"`,
		`"MNEMON_CONTROL_TOKEN_FILE":"` + plannerTokenFile + `"`,
		`"MNEMON_CONTROL_TOKEN_FILE":"` + implementerTokenFile + `"`,
		`"MNEMON_HARNESS_BIN":"/abs/mnemon-harness"`,
		`"MNEMON_MULTICA_PROVIDER_RUNTIME":"noop"`,
		`"MNEMON_MULTICA_PROVIDER_WORKSPACE":"` + tmp + `"`,
	} {
		if !strings.Contains(string(envStdin), want) {
			t.Fatalf("agent env stdin missing %s:\n%s", want, envStdin)
		}
	}
	if strings.Contains(string(envStdin), "MNEMON_HUB_BACKEND") {
		t.Fatalf("R3 provision env must not configure Multica as hub backend:\n%s", envStdin)
	}
}

func TestMulticaProvisionRejectsDirectHarnessUse(t *testing.T) {
	restoreMulticaFlags(t)

	multicaProvisionAcceptanceBridge = false
	err := runMulticaProvision(multicaProvisionCmd, nil)
	if err == nil {
		t.Fatal("direct hidden harness provision should be rejected")
	}
	if !strings.Contains(err.Error(), "mnemon-acceptance multica-provision") {
		t.Fatalf("unexpected direct provision error: %v", err)
	}
}

func TestMergeMulticaParticipantRuntimeEnvPrunesStaleManagedKeys(t *testing.T) {
	merged := mergeMulticaParticipantRuntimeEnv(map[string]string{
		"MNEMON_CONTROL_TOKEN":            "old-token",
		"MNEMON_CONTROL_TOKEN_FILE":       "/old/token",
		"MNEMON_MANAGED_RUNTIME":          "codex-appserver",
		"MNEMON_MULTICA_PROVIDER_RUNTIME": "old-provider",
		"MNEMON_HUB_BACKEND":              "old",
		"CUSTOM_USER_ENV":                 "keep",
	}, map[string]string{
		"MNEMON_CONTROL_ADDR":               "http://127.0.0.1:8791",
		"MNEMON_CONTROL_PRINCIPAL":          "planner@team",
		"MNEMON_MULTICA_PROVIDER_WORKSPACE": "/workspace",
		"MNEMON_MULTICA_REGISTRY":           "/registry.json",
		"MNEMON_MULTICA_WORKSPACE_ID":       "ws-1",
	})
	for _, stale := range []string{"MNEMON_CONTROL_TOKEN", "MNEMON_CONTROL_TOKEN_FILE", "MNEMON_MANAGED_RUNTIME", "MNEMON_HUB_BACKEND", "MNEMON_MULTICA_PROVIDER_RUNTIME"} {
		if _, ok := merged[stale]; ok {
			t.Fatalf("stale runtime env key %s should be pruned: %+v", stale, merged)
		}
	}
	if merged["CUSTOM_USER_ENV"] != "keep" {
		t.Fatalf("unmanaged env should be preserved: %+v", merged)
	}
	if merged["MNEMON_CONTROL_PRINCIPAL"] != "planner@team" {
		t.Fatalf("desired managed env not applied: %+v", merged)
	}
}

func TestMulticaParticipantRuntimeEnvUsesAbsoluteLocalPaths(t *testing.T) {
	restoreMulticaFlags(t)

	tmp := t.TempDir()
	t.Chdir(tmp)
	workspace := "provider-workspace"
	tokenDir := filepath.Join(workspace, ".mnemon", "harness", "channel", "credentials")
	if err := os.MkdirAll(tokenDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tokenDir, "planner-team.token"), []byte("token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	multicaProfile = "desktop-api.multica.ai"
	env := multicaParticipantRuntimeEnv(driver.MulticaCLI{Command: "multica"}, driver.MulticaParticipantRecord{
		Principal: "planner@team",
		AgentName: "mnemon-planner",
		AgentID:   "agent-planner",
	}, filepath.Join("state", "registry.json"), "ws-1", multicaParticipantEnvOptions{
		ProviderWorkspace: workspace,
	})

	for _, key := range []string{"MNEMON_MULTICA_REGISTRY", "MNEMON_MULTICA_PROVIDER_WORKSPACE", "MNEMON_CONTROL_TOKEN_FILE"} {
		value := env[key]
		if value == "" || !filepath.IsAbs(value) {
			t.Fatalf("%s should be absolute, got %q in %+v", key, value, env)
		}
	}
	if want := filepath.Join(tmp, workspace, ".mnemon", "harness", "channel", "credentials", "planner-team.token"); env["MNEMON_CONTROL_TOKEN_FILE"] != want {
		t.Fatalf("token file = %q, want %q", env["MNEMON_CONTROL_TOKEN_FILE"], want)
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
	oldStatusLabel := multicaSurfaceStatusLabel
	oldSurfaceSummary := multicaSurfaceSummary
	oldDesiredStatus := multicaSurfaceDesiredStatus
	oldEventRef := multicaSurfaceEventRef
	oldResourceRef := multicaSurfaceResourceRef
	oldSurfaceRef := multicaSurfaceRef
	oldSourceArtifactRef := multicaSurfaceSourceArtifactRef
	oldSurfaceEvidence := multicaSurfaceEvidenceRefs
	oldSurfaceArtifacts := multicaSurfaceArtifactRefs
	oldAssigneeAgentID := multicaSurfaceAssigneeAgentID
	oldAssignedToProvider := multicaSurfaceAssignedToProvider
	oldTargetAgentID := multicaSurfaceTargetAgentID
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
	oldProvisionProviderRuntime := multicaProvisionProviderRuntime
	oldProvisionProviderCommand := multicaProvisionProviderCommand
	oldProvisionProviderWorkspace := multicaProvisionProviderWorkspace
	oldProvisionProviderTimeout := multicaProvisionProviderTimeout
	oldProvisionAcceptanceBridge := multicaProvisionAcceptanceBridge
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
	oldParticipantProviderRuntime := multicaParticipantProviderRuntime
	oldParticipantProviderCommand := multicaParticipantProviderCommand
	oldParticipantProviderWorkspace := multicaParticipantProviderWorkspace
	oldParticipantProviderTimeout := multicaParticipantProviderTimeout
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
		multicaSurfaceStatusLabel = oldStatusLabel
		multicaSurfaceSummary = oldSurfaceSummary
		multicaSurfaceDesiredStatus = oldDesiredStatus
		multicaSurfaceEventRef = oldEventRef
		multicaSurfaceResourceRef = oldResourceRef
		multicaSurfaceRef = oldSurfaceRef
		multicaSurfaceSourceArtifactRef = oldSourceArtifactRef
		multicaSurfaceEvidenceRefs = oldSurfaceEvidence
		multicaSurfaceArtifactRefs = oldSurfaceArtifacts
		multicaSurfaceAssigneeAgentID = oldAssigneeAgentID
		multicaSurfaceAssignedToProvider = oldAssignedToProvider
		multicaSurfaceTargetAgentID = oldTargetAgentID
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
		multicaProvisionProviderRuntime = oldProvisionProviderRuntime
		multicaProvisionProviderCommand = oldProvisionProviderCommand
		multicaProvisionProviderWorkspace = oldProvisionProviderWorkspace
		multicaProvisionProviderTimeout = oldProvisionProviderTimeout
		multicaProvisionAcceptanceBridge = oldProvisionAcceptanceBridge
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
		multicaParticipantProviderRuntime = oldParticipantProviderRuntime
		multicaParticipantProviderCommand = oldParticipantProviderCommand
		multicaParticipantProviderWorkspace = oldParticipantProviderWorkspace
		multicaParticipantProviderTimeout = oldParticipantProviderTimeout
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
	multicaSurfaceStatusLabel = ""
	multicaSurfaceSummary = ""
	multicaSurfaceDesiredStatus = ""
	multicaSurfaceEventRef = ""
	multicaSurfaceResourceRef = ""
	multicaSurfaceRef = ""
	multicaSurfaceSourceArtifactRef = ""
	multicaSurfaceEvidenceRefs = nil
	multicaSurfaceArtifactRefs = nil
	multicaSurfaceAssigneeAgentID = ""
	multicaSurfaceAssignedToProvider = false
	multicaSurfaceTargetAgentID = ""
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
	multicaProvisionProviderRuntime = ""
	multicaProvisionProviderCommand = ""
	multicaProvisionProviderWorkspace = ""
	multicaProvisionProviderTimeout = 0
	multicaProvisionAcceptanceBridge = false
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
	multicaParticipantProviderRuntime = ""
	multicaParticipantProviderCommand = ""
	multicaParticipantProviderWorkspace = ""
	multicaParticipantProviderTimeout = 0
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
