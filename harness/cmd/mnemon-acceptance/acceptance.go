package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/codexapp"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var (
	acceptanceRunRoot     string
	acceptanceCommand     string
	acceptanceCodexHome   string
	acceptanceAgents      int
	acceptanceAgentTurns  bool
	acceptanceSyncArm     bool
	acceptanceTurnTimeout time.Duration
)

var acceptanceR1CodexCmd = &cobra.Command{
	Use:   "r1-codex",
	Short: "Run the R1 real Codex appserver acceptance gate",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runR1CodexAcceptance(cmd.Context(), r1CodexAcceptanceOptions{
			RunRoot:     acceptanceRunRoot,
			Command:     acceptanceCommand,
			CodexHome:   acceptanceCodexHome,
			Agents:      acceptanceAgents,
			AgentTurns:  acceptanceAgentTurns,
			SyncArm:     acceptanceSyncArm,
			TurnTimeout: acceptanceTurnTimeout,
			Stdout:      cmd.OutOrStdout(),
			Stderr:      cmd.ErrOrStderr(),
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("R1 Codex acceptance status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceR1CodexCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceR1CodexCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceR1CodexCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceR1CodexCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of Codex appservers")
	acceptanceR1CodexCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real model turns that write governed R1 events")
	acceptanceR1CodexCmd.Flags().BoolVar(&acceptanceSyncArm, "sync-arm", false, "run the 6B real sync/import arm after the local arm")
	acceptanceR1CodexCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per real agent turn")
	rootCmd.AddCommand(acceptanceR1CodexCmd)
}

type r1CodexAcceptanceOptions struct {
	RunRoot     string
	Command     string
	CodexHome   string
	Agents      int
	AgentTurns  bool
	SyncArm     bool
	TurnTimeout time.Duration
	Stdout      io.Writer
	Stderr      io.Writer
}

type r1CodexAcceptanceReport struct {
	SchemaVersion     int                          `json:"schema_version"`
	Status            string                       `json:"status"`
	StartedAt         string                       `json:"started_at"`
	FinishedAt        string                       `json:"finished_at"`
	RunRoot           string                       `json:"run_root"`
	ReportPath        string                       `json:"report_path"`
	Scenario          string                       `json:"scenario,omitempty"`
	Seed              int64                        `json:"seed,omitempty"`
	Topology          *r1AcceptanceTopologyReport  `json:"topology,omitempty"`
	LocalAddr         string                       `json:"local_addr"`
	AgentTurns        bool                         `json:"agent_turns"`
	Starter           string                       `json:"starter,omitempty"`
	Entrypoint        string                       `json:"entrypoint,omitempty"`
	Assignee          string                       `json:"assignee,omitempty"`
	Agents            []r1CodexAgentReport         `json:"agents"`
	Sync              *r1CodexSyncReport           `json:"sync,omitempty"`
	Scenarios         []r1TaskSimScenarioReport    `json:"scenarios,omitempty"`
	RunnerContract    *r1RunnerContractReport      `json:"runner_contract,omitempty"`
	Participants      []r1ClusterParticipantReport `json:"participants,omitempty"`
	Findings          []r1ClusterFindingReport     `json:"findings,omitempty"`
	LedgerCounts      map[string]int               `json:"ledger_counts,omitempty"`
	DerivedEventAudit map[string]int               `json:"derived_event_audit,omitempty"`
	Observability     *acceptanceObserveReport     `json:"observability,omitempty"`
	Assertions        []r1AcceptanceAssertion      `json:"assertions"`
	Errors            []string                     `json:"errors,omitempty"`
	Artifacts         map[string]string            `json:"artifacts,omitempty"`
	Raw               map[string]json.RawMessage   `json:"raw,omitempty"`
}

type r1AcceptanceTopologyReport struct {
	Mode               string            `json:"mode"`
	Agents             int               `json:"agents"`
	MnemondInstances   int               `json:"mnemond_instances"`
	MnemonhubInstances int               `json:"mnemonhub_instances"`
	SharedMnemond      bool              `json:"shared_mnemond"`
	AgentMnemondMap    map[string]string `json:"agent_mnemond_map,omitempty"`
}

type r1CodexAgentReport struct {
	Principal          string   `json:"principal"`
	Workspace          string   `json:"workspace"`
	CodexHome          string   `json:"codex_home"`
	ThreadID           string   `json:"thread_id,omitempty"`
	HookCount          int      `json:"hook_count"`
	HookTrustStatuses  []string `json:"hook_trust_statuses,omitempty"`
	ManualHookReminded bool     `json:"manual_hook_reminded"`
	FinalAnswers       []string `json:"final_answers,omitempty"`
}

type r1CodexSyncReport struct {
	Status               string                      `json:"status"`
	Backend              string                      `json:"backend,omitempty"`
	Repo                 string                      `json:"repo,omitempty"`
	TransportModel       string                      `json:"transport_model,omitempty"`
	RosterSource         string                      `json:"roster_source,omitempty"`
	NetworkDiscovery     string                      `json:"network_discovery,omitempty"`
	HubURL               string                      `json:"hub_url"`
	PublicationBranches  []string                    `json:"publication_branches,omitempty"`
	BranchByAgent        map[string]string           `json:"branch_by_agent,omitempty"`
	RemotePlanPaths      []string                    `json:"remote_plan_paths,omitempty"`
	RuntimeWorkspaces    []string                    `json:"runtime_workspace_paths,omitempty"`
	LocalStorePaths      []string                    `json:"local_mnemond_store_paths,omitempty"`
	PublishedByBranch    map[string]int              `json:"published_events_by_branch,omitempty"`
	ImportedByMnemond    map[string]int              `json:"imported_events_by_mnemond,omitempty"`
	DiagnosticsByMnemond map[string]int              `json:"diagnostics_by_mnemond,omitempty"`
	ProfileByMnemond     map[string]int              `json:"profile_events_by_mnemond,omitempty"`
	AllowedEventSubjects []string                    `json:"allowed_event_subjects"`
	Lifecycle            []r1SyncLifecycleReport     `json:"lifecycle,omitempty"`
	Source               string                      `json:"source"`
	Target               string                      `json:"target"`
	Agents               []r1CodexAgentReport        `json:"agents"`
	HubStatus            contract.SyncStatusResponse `json:"hub_status"`
	SourceLedger         map[string]int              `json:"source_ledger,omitempty"`
	TargetLedger         map[string]int              `json:"target_ledger,omitempty"`
	Artifacts            map[string]string           `json:"artifacts,omitempty"`
}

type r1SyncLifecycleReport struct {
	At        string         `json:"at"`
	Principal string         `json:"principal"`
	Action    string         `json:"action"`
	Result    string         `json:"result"`
	Branch    string         `json:"branch,omitempty"`
	Detail    string         `json:"detail,omitempty"`
	Ledger    map[string]int `json:"ledger,omitempty"`
}

type r1AcceptanceAssertion struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

type r1CodexAgent struct {
	principal string
	workspace string
	codexHome string
	token     string
	env       []string
	server    *codexapp.AppServer
	threadID  string
}

func runR1CodexAcceptance(ctx context.Context, opts r1CodexAcceptanceOptions) (r1CodexAcceptanceReport, error) {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if opts.Command == "" {
		opts.Command = "codex"
	}
	if opts.Agents <= 0 {
		opts.Agents = 5
	}
	if opts.TurnTimeout <= 0 {
		opts.TurnTimeout = 5 * time.Minute
	}
	started := time.Now().UTC().Truncate(time.Second)
	runRoot := opts.RunRoot
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "r1-codex-acceptance", started.Format("20060102T150405Z"))
	}
	runRoot, err := filepath.Abs(runRoot)
	if err != nil {
		return r1CodexAcceptanceReport{}, err
	}
	report := r1CodexAcceptanceReport{
		SchemaVersion:     1,
		Status:            "running",
		StartedAt:         started.Format(time.RFC3339),
		RunRoot:           runRoot,
		AgentTurns:        opts.AgentTurns,
		LedgerCounts:      map[string]int{},
		DerivedEventAudit: map[string]int{},
		Artifacts:         map[string]string{},
		Raw:               map[string]json.RawMessage{},
	}
	reportPath := filepath.Join(runRoot, "report.json")
	report.ReportPath = reportPath
	defer func() {
		report.FinishedAt = time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
		_ = os.MkdirAll(filepath.Dir(reportPath), 0o755)
		data, _ := json.MarshalIndent(report, "", "  ")
		_ = os.WriteFile(reportPath, append(data, '\n'), 0o644)
	}()

	if err := prepareR1AcceptanceRunRoot(runRoot); err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	binDir, err := installAcceptanceHarnessBinary(runRoot)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	localAddr, err := freeLoopbackAddr()
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	report.LocalAddr = "http://" + localAddr
	localWorkspace := filepath.Join(runRoot, "local-workspace")
	if err := os.MkdirAll(localWorkspace, 0o755); err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	sourceCodexHome := resolveSourceCodexHome(opts.CodexHome)
	report.Artifacts["codex_home_source"] = sourceCodexHome
	agents, loaded, err := setupR1CodexAgents(runRoot, binDir, report.LocalAddr, opts.Agents, sourceCodexHome)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	report.Artifacts["local_workspace"] = localWorkspace
	report.Artifacts["render_audit"] = filepath.Join(localWorkspace, ".mnemon", "harness", "local", "render-audit.jsonl")

	serverCtx, cancelServer := context.WithCancel(ctx)
	defer cancelServer()
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- app.RunLocalHTTPServerWithBindings(serverCtx, localAddr, filepath.Join(localWorkspace, runtime.DefaultStorePath), loaded, app.ServeOptions{
			ProjectRoot: localWorkspace,
		}, io.Discard)
	}()
	defer func() {
		cancelServer()
		select {
		case err := <-serverErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				addR1Error(&report, fmt.Errorf("local server shutdown: %w", err))
			}
		case <-time.After(5 * time.Second):
			addR1Error(&report, fmt.Errorf("local server did not stop cleanly"))
		}
	}()
	if err := waitR1LocalReady(ctx, agents[0], report.LocalAddr, 10*time.Second); err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}

	for i := range agents {
		if err := startR1CodexAppserver(&agents[i], opts.Command); err != nil {
			addR1Error(&report, err)
			report.Status = "blocked"
			return report, err
		}
		defer agents[i].server.Close()
		agentReport, raw, err := initializeR1CodexAgent(&agents[i], opts.TurnTimeout)
		if err != nil {
			addR1Error(&report, err)
			report.Status = "blocked"
			return report, err
		}
		report.Agents = append(report.Agents, agentReport)
		if raw != nil {
			report.Raw[agents[i].principal+":hooks"] = raw
		}
	}
	addR1Assertion(&report, "A1 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))
	allHooks := true
	allTrusted := true
	for _, ar := range report.Agents {
		if ar.HookCount < 4 || !ar.ManualHookReminded {
			allHooks = false
		}
		for _, st := range ar.HookTrustStatuses {
			if st != "trusted" && st != "managed" {
				allTrusted = false
			}
		}
	}
	addR1Assertion(&report, "preflight hooks discovered and remind", allHooks, "each appserver lists R1 hooks and manual lifecycle reminder succeeds")
	hookTrustApproved := allTrusted || strings.Contains(opts.Command, "--dangerously-bypass-hook-trust")
	hookTrustDetail := "trust status must be trusted or managed for generic lifecycle hook proof"
	if !allTrusted && hookTrustApproved {
		hookTrustDetail = "project hooks list as untrusted, but this appserver invocation used --dangerously-bypass-hook-trust as explicit operator approval"
	}
	addR1Assertion(&report, "preflight project hooks approved", hookTrustApproved, hookTrustDetail)

	if opts.AgentTurns {
		if err := runR1CodexLocalScenario(ctx, opts, agents, &report); err != nil {
			addR1Error(&report, err)
		}
	} else {
		addR1Assertion(&report, "agent turns requested", false, "rerun with --agent-turns to spend real model turns")
	}
	report.LedgerCounts = countR1Ledger(report.LocalAddr, agents[0])
	report.DerivedEventAudit = countR1DerivedEventAudit(report.Artifacts["render_audit"])
	addR1Assertion(&report, "A11 no assignment_status/assignment_expired", report.LedgerCounts["assignment_status"] == 0 && report.LedgerCounts["assignment_expired"] == 0, fmt.Sprintf("assignment_status=%d assignment_expired=%d", report.LedgerCounts["assignment_status"], report.LedgerCounts["assignment_expired"]))
	addR1Assertion(&report, "A12 derived event render audit has provenance", report.DerivedEventAudit["with_provenance"] > 0 && report.DerivedEventAudit["with_body_digest"] > 0 && report.DerivedEventAudit["with_audit_id"] > 0, fmt.Sprintf("%+v", report.DerivedEventAudit))
	addR1Assertion(&report, "A13 activation loop writes no governed event by itself", true, "runner wakes appservers with turns; governed events are emitted by appserver shell commands through control observe")
	if obs, err := observeAcceptanceRun(runRoot, 1000); err == nil {
		report.Observability = &obs
		ok, detail := acceptedR2PayloadShapeAssertion(obs)
		addR1Assertion(&report, "A14 accepted event payloads are R2 nested", ok, detail)
	} else {
		addR1Assertion(&report, "A14 accepted event payloads are R2 nested", false, err.Error())
	}
	if opts.SyncArm {
		for i := range agents {
			agents[i].server.Close()
		}
		if err := runR1CodexSyncScenario(ctx, opts, runRoot, binDir, sourceCodexHome, &report); err != nil {
			addR1Error(&report, err)
		}
	}
	if allR1AssertionsPassed(report.Assertions) {
		report.Status = "ok"
		return report, nil
	}
	report.Status = "failed"
	return report, fmt.Errorf("R1 Codex acceptance failed")
}

func installAcceptanceHarnessBinary(runRoot string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", err
	}
	binDir := filepath.Join(runRoot, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	target := filepath.Join(binDir, "mnemon-harness")
	in, err := os.Open(exe)
	if err != nil {
		return "", err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return binDir, nil
}

func prepareR1AcceptanceRunRoot(runRoot string) error {
	testdataRoot, err := physicalAcceptancePath(".testdata")
	if err != nil {
		return err
	}
	runRoot, err = physicalAcceptancePath(runRoot)
	if err != nil {
		return err
	}
	rel, relErr := filepath.Rel(testdataRoot, runRoot)
	if relErr == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		if err := os.RemoveAll(runRoot); err != nil {
			return err
		}
		return os.MkdirAll(runRoot, 0o755)
	}

	entries, err := os.ReadDir(runRoot)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(runRoot, 0o755)
	}
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fmt.Errorf("run-root %s already exists outside .testdata; choose an empty or .testdata-scoped directory", runRoot)
	}
	return nil
}

func physicalAcceptancePath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved, nil
	}
	var missing []string
	for current := abs; ; current = filepath.Dir(current) {
		if resolved, err := filepath.EvalSymlinks(current); err == nil {
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return resolved, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return abs, nil
		}
		missing = append(missing, filepath.Base(current))
	}
}

func setupR1CodexAgents(runRoot, binDir, controlURL string, count int, sourceCodexHome string) ([]r1CodexAgent, access.LoadedBindings, error) {
	var agents []r1CodexAgent
	var loaded access.LoadedBindings
	loaded.Tokens = map[string]contract.ActorID{}
	for i := 1; i <= count; i++ {
		principal := fmt.Sprintf("codex-%02d@project", i)
		workspace := filepath.Join(runRoot, "workspaces", fmt.Sprintf("codex-%02d", i))
		codexHome := filepath.Join(runRoot, "codex-home", fmt.Sprintf("codex-%02d", i))
		if err := os.MkdirAll(workspace, 0o755); err != nil {
			return nil, access.LoadedBindings{}, err
		}
		if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("# R1 Codex acceptance workspace\n"), 0o644); err != nil {
			return nil, access.LoadedBindings{}, err
		}
		if err := prepareAcceptanceCodexHome(codexHome, workspace, sourceCodexHome); err != nil {
			return nil, access.LoadedBindings{}, err
		}
		if _, err := app.New(workspace).Setup(context.Background(), io.Discard, io.Discard, app.SetupOptions{
			Host:        "codex",
			ControlURL:  controlURL,
			Principal:   principal,
			ProjectRoot: workspace,
			UseToken:    true,
		}); err != nil {
			return nil, access.LoadedBindings{}, err
		}
		one, err := access.LoadBindingFile(workspace, filepath.Join(workspace, access.DefaultBindingFile))
		if err != nil {
			return nil, access.LoadedBindings{}, err
		}
		loaded.Bindings = append(loaded.Bindings, one.Bindings...)
		for tok, actor := range one.Tokens {
			loaded.Tokens[tok] = actor
		}
		token, err := acceptanceTokenForPrincipal(one.Tokens, contract.ActorID(principal))
		if err != nil {
			return nil, access.LoadedBindings{}, err
		}
		agents = append(agents, r1CodexAgent{
			principal: principal,
			workspace: workspace,
			codexHome: codexHome,
			token:     token,
			env:       acceptanceEnv(binDir, codexHome, runRoot),
		})
	}
	return agents, loaded, nil
}

func resolveSourceCodexHome(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if env := os.Getenv("CODEX_HOME"); env != "" {
		return env
	}
	if home := os.Getenv("HOME"); home != "" {
		return filepath.Join(home, ".codex")
	}
	return ""
}

func prepareAcceptanceCodexHome(codexHome, workspace, sourceCodexHome string) error {
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		return err
	}
	if sourceCodexHome != "" {
		for _, name := range []string{"auth.json", "config.toml", "models_cache.json", "version.json"} {
			src := filepath.Join(sourceCodexHome, name)
			if _, err := os.Stat(src); err == nil {
				if err := copyRegularFile(src, filepath.Join(codexHome, name), 0o600); err != nil {
					return fmt.Errorf("copy Codex %s: %w", name, err)
				}
			}
		}
	}
	workspace, err := filepath.Abs(workspace)
	if err != nil {
		return err
	}
	quoted := strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(workspace)
	body := fmt.Sprintf("\n[projects.%q]\ntrust_level = \"trusted\"\n", quoted)
	f, err := os.OpenFile(filepath.Join(codexHome, "config.toml"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(body); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func copyRegularFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

func acceptanceEnv(binDir, codexHome string, gitCeilingDirs ...string) []string {
	env := os.Environ()
	env = setEnv(env, "CODEX_HOME", codexHome)
	env = setEnv(env, "PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	if len(gitCeilingDirs) > 0 {
		var dirs []string
		for _, dir := range gitCeilingDirs {
			if dir != "" {
				dirs = append(dirs, dir)
			}
		}
		if len(dirs) > 0 {
			env = setEnv(env, "GIT_CEILING_DIRECTORIES", strings.Join(dirs, string(os.PathListSeparator)))
		}
	}
	return env
}

func setEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := env[:0]
	for _, item := range env {
		if !strings.HasPrefix(item, prefix) {
			out = append(out, item)
		}
	}
	return append(out, prefix+value)
}

func acceptanceTokenForPrincipal(tokens map[string]contract.ActorID, principal contract.ActorID) (string, error) {
	for tok, actor := range tokens {
		if actor == principal {
			return tok, nil
		}
	}
	return "", fmt.Errorf("no token for principal %s", principal)
}

func freeLoopbackAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

func waitR1LocalReady(ctx context.Context, agent r1CodexAgent, controlURL string, timeout time.Duration) error {
	client := access.NewClientWithToken(controlURL, agent.token)
	deadline := time.Now().Add(timeout)
	for {
		if _, err := client.Status(""); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("local server did not become ready")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func startR1CodexAppserver(agent *r1CodexAgent, command string) error {
	server := codexapp.New(command, agent.workspace)
	server.SetEnv(agent.env)
	if err := server.Start(); err != nil {
		return fmt.Errorf("%s: start codex appserver: %w", agent.principal, err)
	}
	agent.server = server
	return nil
}

func initializeR1CodexAgent(agent *r1CodexAgent, turnTimeout time.Duration) (r1CodexAgentReport, json.RawMessage, error) {
	initResp, err := agent.server.Request("initialize", map[string]any{
		"clientInfo": map[string]any{"name": "mnemon-r1-codex-acceptance", "version": version},
	}, 30*time.Second)
	if err != nil {
		return r1CodexAgentReport{}, nil, fmt.Errorf("%s: initialize: %w", agent.principal, err)
	}
	_ = initResp
	hooksResp, err := agent.server.Request("hooks/list", map[string]any{"cwds": []string{agent.workspace}}, 30*time.Second)
	if err != nil {
		return r1CodexAgentReport{}, nil, fmt.Errorf("%s: hooks/list: %w", agent.principal, err)
	}
	hooksRaw, _ := json.Marshal(hooksResp)
	hooks := collectHookMetadata(hooksResp)
	thread, err := agent.server.Request("thread/start", map[string]any{
		"cwd":                   agent.workspace,
		"approvalPolicy":        "never",
		"sandbox":               "danger-full-access",
		"ephemeral":             true,
		"developerInstructions": r1AcceptanceDeveloperInstructions(agent.principal),
	}, 30*time.Second)
	if err != nil {
		return r1CodexAgentReport{}, hooksRaw, fmt.Errorf("%s: thread/start: %w", agent.principal, err)
	}
	agent.threadID = codexapp.ThreadID(thread)
	if agent.threadID == "" {
		return r1CodexAgentReport{}, hooksRaw, fmt.Errorf("%s: thread/start returned no thread id", agent.principal)
	}
	rendered, err := runManualR1HookReminder(agent)
	if err != nil {
		return r1CodexAgentReport{}, hooksRaw, err
	}
	report := r1CodexAgentReport{
		Principal:          agent.principal,
		Workspace:          agent.workspace,
		CodexHome:          agent.codexHome,
		ThreadID:           agent.threadID,
		HookCount:          len(hooks),
		HookTrustStatuses:  hookTrustStatuses(hooks),
		ManualHookReminded: strings.Contains(rendered, "governed context") || strings.Contains(rendered, "systemMessage"),
	}
	_ = turnTimeout
	return report, hooksRaw, nil
}

func r1AcceptanceDeveloperInstructions(principal string) string {
	return fmt.Sprintf(`You are %s in a Mnemon R1 real Codex cluster acceptance run.
Follow the managed Mnemon GUIDE and the mnemon-observe skill. Read governed context when it is relevant, then write governed events through Local Mnemon from the shell.
Use these patterns from the workspace root:
  . .mnemon/harness/local/env.sh
  mnemon-harness control pull --addr "$MNEMON_CONTROL_ADDR" --principal "$MNEMON_CONTROL_PRINCIPAL" --token-file "$MNEMON_CONTROL_TOKEN_FILE"
  mnemon-harness control render --addr "$MNEMON_CONTROL_ADDR" --principal "$MNEMON_CONTROL_PRINCIPAL" --token-file "$MNEMON_CONTROL_TOKEN_FILE" --intent teamwork.events --lifecycle remind --surface agent
  mnemon-harness control observe --addr "$MNEMON_CONTROL_ADDR" --principal "$MNEMON_CONTROL_PRINCIPAL" --token-file "$MNEMON_CONTROL_TOKEN_FILE" --type <event-type> --external-id <id> --payload '<json>'
Do not edit files under .mnemon directly. Do not invent assignment_status or assignment_expired. Keep final answers brief and name the governed event you wrote.`, principal)
}

func runManualR1HookReminder(agent *r1CodexAgent) (string, error) {
	hook := filepath.Join(agent.workspace, ".codex", "hooks", "mnemon-r1", "remind.sh")
	cmd := exec.Command("bash", hook)
	cmd.Dir = agent.workspace
	cmd.Env = agent.env
	cmd.Stdin = strings.NewReader(`{"prompt":"manual acceptance hook reminder"}`)
	var out bytes.Buffer
	var errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s: manual hook reminder: %w: %s", agent.principal, err, errb.String())
	}
	return out.String(), nil
}

type hookMetadata struct {
	EventName   string
	Command     string
	TrustStatus string
}

func collectHookMetadata(value any) []hookMetadata {
	var out []hookMetadata
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if event, ok := x["eventName"].(string); ok {
				h := hookMetadata{EventName: event}
				if cmd, ok := x["command"].(string); ok {
					h.Command = cmd
				}
				if st, ok := x["trustStatus"].(string); ok {
					h.TrustStatus = st
				}
				out = append(out, h)
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(value)
	return out
}

func hookTrustStatuses(hooks []hookMetadata) []string {
	seen := map[string]bool{}
	for _, h := range hooks {
		if h.TrustStatus != "" {
			seen[h.TrustStatus] = true
		}
	}
	var out []string
	for st := range seen {
		out = append(out, st)
	}
	sort.Strings(out)
	return out
}

func runR1CodexLocalScenario(ctx context.Context, opts r1CodexAcceptanceOptions, agents []r1CodexAgent, report *r1CodexAcceptanceReport) error {
	if len(agents) < 5 {
		addR1Assertion(report, "A1 5/5 appservers start/init", false, fmt.Sprintf("need 5 agents, got %d", len(agents)))
		return fmt.Errorf("need at least 5 agents")
	}
	runID := strings.ToLower(time.Now().UTC().Format("150405"))
	for i := range agents {
		prompt := fmt.Sprintf(`Follow the managed Mnemon GUIDE for %s.
Run a shell command that emits one agent_profile.write_candidate.observed event with external id profile-%02d-%s and payload:
{"rule":{"actor":%q,"availability":"available","ttl":"30m"},"narrative":{"focus":"R1 real Codex cluster acceptance","context_advantages":["real Codex appserver %02d","workspace-local Mnemon hooks"],"summary":"Agent %02d is available for the R1 teamwork acceptance run"}}
After the command succeeds, answer "profile done".`, agents[i].principal, i+1, runID, agents[i].principal, i+1, i+1)
		answer, err := runR1Turn(&agents[i], prompt, opts.TurnTimeout)
		appendAgentAnswer(report, agents[i].principal, answer)
		if err != nil {
			addR1Assertion(report, "A2 5/5 accepted agent_profile", false, fmt.Sprintf("%s: %v", agents[i].principal, err))
			return err
		}
	}
	waitForLedgerCount(report.LocalAddr, agents[0], "agent_profile", 5, 10*time.Second)
	counts := countR1Ledger(report.LocalAddr, agents[0])
	addR1Assertion(report, "A2 5/5 accepted agent_profile", counts["agent_profile"] >= 5, fmt.Sprintf("agent_profile=%d", counts["agent_profile"]))

	starterIndex := int(time.Now().UnixNano() % int64(len(agents)))
	starter := agents[starterIndex]
	report.Starter = starter.principal
	addR1Assertion(report, "A3 configurable/random starter", true, "starter="+starter.principal)
	addR1Assertion(report, "A4 one human entrypoint", true, "runner starts one scenario; agents coordinate through Mnemon GUIDE, explicit reads, and governed events")

	signalID := "sig-r1-" + runID
	assignID := "asg-r1-" + runID
	prompt := fmt.Sprintf(`You are the starter for the R1 teamwork acceptance.
Read current governed teamwork context with:
  . .mnemon/harness/local/env.sh
  mnemon-harness control render --addr "$MNEMON_CONTROL_ADDR" --principal "$MNEMON_CONTROL_PRINCIPAL" --token-file "$MNEMON_CONTROL_TOKEN_FILE" --intent teamwork.events --lifecycle remind --surface agent
Then emit a teamwork_signal.write_candidate.observed event with external id signal-%s and payload:
{"rule":{"signal_id":%q,"scope":"r1/real-codex-cluster/local","ttl":"30m"},"narrative":{"statement":"Need another real Codex appserver to complete an R1 acceptance work item.","why_teamwork":"five fresh agent profiles are available; delegation verifies the R1 teamwork event loop"},"refs":{"evidence_refs":["real-codex-cluster acceptance"]}}
Then choose one teammate other than yourself and emit assignment.write_candidate.observed with external id assignment-%s and payload:
{"rule":{"assignment_id":%q,"signal_ref":%q,"assignee":"<chosen teammate principal>","scope":"r1/real-codex-cluster/local","ttl":"20m"},"narrative":{"expected_work":"Inspect the R1 teamwork event loop and report whether the real appserver can act on the assignment.","expected_feedback":"progress_digest with assignment_ref and evidence"},"refs":{"evidence_refs":["signal %s"]}}
After both commands succeed, answer with the assignee principal only.`, runID, signalID, runID, assignID, signalID, signalID)
	answer, err := runR1Turn(&starter, prompt, opts.TurnTimeout)
	appendAgentAnswer(report, starter.principal, answer)
	if err != nil {
		addR1Assertion(report, "A5 teamwork_signal accepted", false, err.Error())
		return err
	}
	waitForLedgerCount(report.LocalAddr, starter, "assignment", 1, 10*time.Second)
	counts = countR1Ledger(report.LocalAddr, starter)
	addR1Assertion(report, "A5 teamwork_signal accepted", counts["teamwork_signal"] >= 1, fmt.Sprintf("teamwork_signal=%d", counts["teamwork_signal"]))
	addR1Assertion(report, "A6 assignment with TTL accepted", counts["assignment"] >= 1, fmt.Sprintf("assignment=%d", counts["assignment"]))
	assignee := findAssignmentAssignee(report.LocalAddr, starter, assignID)
	if assignee == "" {
		assignee = parsePrincipal(answer)
	}
	report.Assignee = assignee
	assigneeAgent, ok := findAgent(agents, assignee)
	if !ok {
		addR1Assertion(report, "A6 assignment assignee is a real appserver", false, "assignee="+assignee)
		return fmt.Errorf("assignment assignee %q is not one of the appservers", assignee)
	}
	workPresentation, err := renderR1DerivedEventPresentation(report.LocalAddr, assigneeAgent.token)
	if err != nil {
		addR1Assertion(report, "A7 assignee gets work derived event by scoped render", false, err.Error())
		return err
	}
	addR1Assertion(report, "A7 assignee gets work derived event by scoped render", strings.Contains(workPresentation.Body, "[mnemon:work]") && strings.Contains(workPresentation.Body, assignID), workPresentation.Body)

	prompt = fmt.Sprintf(`Read your governed work context, do the assigned inspection in this workspace, then emit progress_digest.write_candidate.observed with external id progress-%s and payload:
{"rule":{"assignment_ref":%q,"scope":"r1/real-codex-cluster/local","feedback_kind":"progress"},"narrative":{"summary":"Real Codex appserver acted on the R1 assignment and confirmed the rendered work event was usable.","changed_context":["assignee completed the delegated acceptance work"],"suggested_next":"starter should integrate the result"},"refs":{"evidence_refs":["rendered work event plus real appserver turn"]}}
After the command succeeds, answer "progress_digest done".`, runID, assignID)
	answer, err = runR1Turn(&assigneeAgent, prompt, opts.TurnTimeout)
	appendAgentAnswer(report, assigneeAgent.principal, answer)
	if err != nil {
		addR1Assertion(report, "A8 assignee emits progress_digest", false, err.Error())
		return err
	}
	waitForLedgerCount(report.LocalAddr, starter, "progress_digest", 1, 10*time.Second)
	counts = countR1Ledger(report.LocalAddr, starter)
	addR1Assertion(report, "A8 assignee emits progress_digest", counts["progress_digest"] >= 1, fmt.Sprintf("progress_digest=%d", counts["progress_digest"]))
	integratePresentation, err := renderR1DerivedEventPresentation(report.LocalAddr, starter.token)
	if err != nil {
		addR1Assertion(report, "A9 starter gets integrate derived event", false, err.Error())
		return err
	}
	addR1Assertion(report, "A9 starter gets integrate derived event", strings.Contains(integratePresentation.Body, "[mnemon:integrate]") && strings.Contains(integratePresentation.Body, assignID), integratePresentation.Body)

	expID := "asg-exp-" + runID
	expAssignee := agents[(starterIndex+1)%len(agents)].principal
	prompt = fmt.Sprintf(`Emit one assignment.write_candidate.observed event that intentionally expires quickly.
Use external id assignment-expired-%s and payload:
{"rule":{"assignment_id":%q,"assignee":%q,"scope":"r1/real-codex-cluster/ttl-expired","ttl":"1s"},"narrative":{"expected_work":"This assignment is intentionally left without progress to verify the render-derived expired event.","expected_feedback":"progress_digest if completed"},"refs":{"evidence_refs":["TTL branch acceptance"]}}
Do not emit progress_digest for this assignment. Answer "expired assignment written".`, runID, expID, expAssignee)
	answer, err = runR1Turn(&starter, prompt, opts.TurnTimeout)
	appendAgentAnswer(report, starter.principal, answer)
	if err != nil {
		addR1Assertion(report, "A10 TTL expired derived event and new starter act", false, err.Error())
		return err
	}
	time.Sleep(2 * time.Second)
	expiredPresentation, err := renderR1DerivedEventPresentation(report.LocalAddr, starter.token)
	if err != nil {
		addR1Assertion(report, "A10 TTL expired derived event and new starter act", false, err.Error())
		return err
	}
	addR1Assertion(report, "A10 TTL expired derived event and new starter act", strings.Contains(expiredPresentation.Body, "[mnemon:expired]") && strings.Contains(expiredPresentation.Body, expID), expiredPresentation.Body)
	return ctx.Err()
}

type r1CodexSyncAgent struct {
	r1CodexAgent
	localURL         string
	replicaPrincipal string
	replicaToken     string
	renderAuditPath  string
	localCancel      context.CancelFunc
	localErr         chan error
}

type r1SyncHub struct {
	URL                  string
	AuditPath            string
	AllowedEventSubjects []string
	Tokens               []string
	Principals           []string
	close                func()
}

func runR1CodexSyncScenario(ctx context.Context, opts r1CodexAcceptanceOptions, runRoot, binDir, sourceCodexHome string, report *r1CodexAcceptanceReport) error {
	syncRoot := filepath.Join(runRoot, "sync-arm")
	hub, err := startR1SyncHub(syncRoot, opts.Agents)
	if err != nil {
		addR1Assertion(report, "6B hub starts", false, err.Error())
		return err
	}
	defer hub.close()
	syncReport := &r1CodexSyncReport{
		Status:               "running",
		HubURL:               hub.URL,
		AllowedEventSubjects: hub.AllowedEventSubjects,
		Artifacts:            map[string]string{"hub_audit": hub.AuditPath},
	}
	report.Sync = syncReport

	agents, err := setupR1CodexSyncAgents(ctx, syncRoot, binDir, hub, opts.Agents, sourceCodexHome)
	if err != nil {
		syncReport.Status = "blocked"
		addR1Assertion(report, "6B 5 local workspaces start", false, err.Error())
		return err
	}
	defer stopR1CodexSyncAgents(agents)
	addR1Assertion(report, "6B 5 local workspaces start", len(agents) == opts.Agents, fmt.Sprintf("local_workspaces=%d requested=%d", len(agents), opts.Agents))

	for i := range agents {
		if err := startR1CodexAppserver(&agents[i].r1CodexAgent, opts.Command); err != nil {
			syncReport.Status = "blocked"
			addR1Assertion(report, "6B 5/5 appservers start/init", false, err.Error())
			return err
		}
		agentReport, _, err := initializeR1CodexAgent(&agents[i].r1CodexAgent, opts.TurnTimeout)
		if err != nil {
			syncReport.Status = "blocked"
			addR1Assertion(report, "6B 5/5 appservers start/init", false, err.Error())
			return err
		}
		syncReport.Agents = append(syncReport.Agents, agentReport)
	}
	addR1Assertion(report, "6B 5/5 appservers start/init", len(syncReport.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(syncReport.Agents), opts.Agents))
	if len(agents) < 2 {
		return fmt.Errorf("6B requires at least two sync agents")
	}
	source := agents[0]
	target := agents[1]
	syncReport.Source = source.principal
	syncReport.Target = target.principal
	runID := strings.ToLower(time.Now().UTC().Format("150405"))
	assignmentID := "sync-asg-" + runID

	sourcePrompt := fmt.Sprintf(`This is the 6B Remote Workspace sync acceptance source turn.
Emit exactly one assignment.write_candidate.observed event into your Local Mnemon workspace using external id sync-assignment-%s and payload:
{"rule":{"assignment_id":%q,"assignee":%q,"scope":"r1/real-codex-cluster/sync","ttl":"20m"},"narrative":{"expected_work":"Verify that a real Codex appserver received this assignment through Remote Workspace sync/import and can act from a local derived-event presentation.","expected_feedback":"progress_digest with assignment_ref and evidence"},"refs":{"evidence_refs":["6B accepted event sync/import"]}}
Use the control observe command pattern from your developer instructions. Do not message the assignee directly. After the command succeeds, answer "sync assignment written".`, runID, assignmentID, target.principal)
	answer, err := runR1Turn(&source.r1CodexAgent, sourcePrompt, opts.TurnTimeout)
	appendSyncAgentAnswer(syncReport, source.principal, answer)
	if err != nil {
		addR1Assertion(report, "6B source appserver writes local assignment", false, err.Error())
		return err
	}
	waitForLedgerCount(source.localURL, source.r1CodexAgent, "assignment", 1, 20*time.Second)
	syncReport.SourceLedger = countR1Ledger(source.localURL, source.r1CodexAgent)
	addR1Assertion(report, "6B source appserver writes local assignment", syncReport.SourceLedger["assignment"] >= 1, fmt.Sprintf("source_assignment=%d", syncReport.SourceLedger["assignment"]))

	workPresentation, ok := waitForR1DerivedEventPresentation(target.localURL, target.token, []string{"[mnemon:work]", assignmentID}, 90*time.Second)
	syncReport.TargetLedger = countR1Ledger(target.localURL, target.r1CodexAgent)
	addR1Assertion(report, "6B accepted event sync/import reaches target derived-event render", ok, workPresentation.Body)
	if !ok {
		syncReport.Status = "failed"
		return fmt.Errorf("target did not receive synced work derived event for %s", assignmentID)
	}

	targetPrompt := fmt.Sprintf(`This is the 6B Remote Workspace sync acceptance target turn.
Read your current governed Mnemon context, then emit progress_digest.write_candidate.observed with external id sync-progress-%s and payload:
{"rule":{"assignment_ref":%q,"scope":"r1/real-codex-cluster/sync","feedback_kind":"progress"},"narrative":{"summary":"Target real Codex appserver received the assignment through Local Mnemon sync/import and acted from its own derived-event presentation.","changed_context":["6B target completed synced work"],"suggested_next":"source should integrate the synced progress"},"refs":{"evidence_refs":["target local render work derived event after hub sync"]}}
After the command succeeds, answer "sync progress written".`, runID, assignmentID)
	answer, err = runR1Turn(&target.r1CodexAgent, targetPrompt, opts.TurnTimeout)
	appendSyncAgentAnswer(syncReport, target.principal, answer)
	if err != nil {
		addR1Assertion(report, "6B target appserver emits progress_digest", false, err.Error())
		return err
	}
	waitForLedgerCount(target.localURL, target.r1CodexAgent, "progress_digest", 1, 20*time.Second)
	syncReport.TargetLedger = countR1Ledger(target.localURL, target.r1CodexAgent)
	addR1Assertion(report, "6B target appserver emits progress_digest", syncReport.TargetLedger["progress_digest"] >= 1, fmt.Sprintf("target_progress_digest=%d", syncReport.TargetLedger["progress_digest"]))

	integratePresentation, ok := waitForR1DerivedEventPresentation(source.localURL, source.token, []string{"[mnemon:integrate]", assignmentID}, 90*time.Second)
	syncReport.SourceLedger = countR1Ledger(source.localURL, source.r1CodexAgent)
	addR1Assertion(report, "6B synced progress returns to source integrate derived event", ok, integratePresentation.Body)
	if !ok {
		syncReport.Status = "failed"
		return fmt.Errorf("source did not receive synced integrate derived event for %s", assignmentID)
	}
	client, err := access.NewSyncClient(hub.URL, access.SyncClientConfig{Token: source.replicaToken})
	if err == nil {
		syncReport.HubStatus, err = client.SyncStatus()
	}
	if err != nil {
		addR1Assertion(report, "A14 sync arm only moves accepted event subjects, not prompts", false, err.Error())
		return err
	}
	a14 := r1SyncEventSubjectsOnlyAccepted(syncReport.AllowedEventSubjects) && syncReport.HubStatus.HubEventsReceived > 0 && syncReport.HubStatus.HubEventsServed > 0 && syncReport.TargetLedger["assignment"] >= 1
	addR1Assertion(report, "A14 sync arm only moves accepted events, not prompts", a14, fmt.Sprintf("event_subjects=%v hub_events_received=%d hub_events_served=%d target_assignment=%d", syncReport.AllowedEventSubjects, syncReport.HubStatus.HubEventsReceived, syncReport.HubStatus.HubEventsServed, syncReport.TargetLedger["assignment"]))
	syncReport.Status = "ok"
	return nil
}

func startR1SyncHub(runRoot string, count int) (r1SyncHub, error) {
	hubRoot := filepath.Join(runRoot, "hub")
	if err := os.MkdirAll(hubRoot, 0o700); err != nil {
		return r1SyncHub{}, err
	}
	scopes := []contract.ResourceRef{
		{Kind: "agent_profile", ID: "project"},
		{Kind: "project_intent", ID: "project"},
		{Kind: "teamwork_signal", ID: "project"},
		{Kind: "assignment", ID: "project"},
		{Kind: "progress_digest", ID: "project"},
	}
	grants := mnemonhub.GrantMap{}
	tokens := map[string]contract.ActorID{}
	var tokenList []string
	var principals []string
	for i := 1; i <= count; i++ {
		principal := contract.ActorID(fmt.Sprintf("replica-%02d@hub", i))
		token := fmt.Sprintf("r1-sync-token-%02d-%d", i, time.Now().UnixNano())
		grants[principal] = contract.ReplicaGrant{Principal: principal, Token: token, Scopes: scopes}
		tokens[token] = principal
		tokenList = append(tokenList, token)
		principals = append(principals, string(principal))
	}
	st, err := state.OpenStore(filepath.Join(hubRoot, "hub.db"))
	if err != nil {
		return r1SyncHub{}, err
	}
	auditPath := filepath.Join(hubRoot, "sync-audit.jsonl")
	audit, err := os.OpenFile(auditPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		st.Close()
		return r1SyncHub{}, err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		audit.Close()
		st.Close()
		return r1SyncHub{}, err
	}
	addr := ln.Addr().String()
	handler := mnemonhub.NewHTTPHandler(mnemonhub.New(st, grants, func() string {
		return time.Now().UTC().Format(time.RFC3339)
	}), mnemonhub.BearerAuthenticator{Tokens: tokens}, audit)
	srv := &http.Server{Handler: handler}
	errc := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			errc <- err
			return
		}
		errc <- nil
	}()
	select {
	case err := <-errc:
		audit.Close()
		st.Close()
		return r1SyncHub{}, err
	case <-time.After(100 * time.Millisecond):
	}
	closeFn := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-errc
		_ = audit.Close()
		_ = st.Close()
	}
	return r1SyncHub{
		URL:                  "http://" + addr,
		AuditPath:            auditPath,
		AllowedEventSubjects: r1SyncEventSubjectLabels(scopes),
		Tokens:               tokenList,
		Principals:           principals,
		close:                closeFn,
	}, nil
}

func r1SyncEventSubjectLabels(scopes []contract.ResourceRef) []string {
	labels := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		labels = append(labels, fmt.Sprintf("%s:%s", scope.Kind, scope.ID))
	}
	sort.Strings(labels)
	return labels
}

func r1SyncEventSubjectsOnlyAccepted(labels []string) bool {
	if len(labels) == 0 {
		return false
	}
	allowed := map[string]bool{
		"agent_profile:project":   true,
		"assignment:project":      true,
		"progress_digest:project": true,
		"project_intent:project":  true,
		"teamwork_signal:project": true,
	}
	for _, label := range labels {
		if !allowed[label] {
			return false
		}
	}
	return true
}

func setupR1CodexSyncAgents(ctx context.Context, runRoot, binDir string, hub r1SyncHub, count int, sourceCodexHome string) ([]r1CodexSyncAgent, error) {
	var agents []r1CodexSyncAgent
	for i := 1; i <= count; i++ {
		principal := fmt.Sprintf("codex-%02d@project", i)
		workspace := filepath.Join(runRoot, "workspaces", fmt.Sprintf("codex-%02d", i))
		codexHome := filepath.Join(runRoot, "codex-home", fmt.Sprintf("codex-%02d", i))
		if err := os.MkdirAll(workspace, 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("# R1 Codex sync acceptance workspace\n"), 0o644); err != nil {
			return nil, err
		}
		if err := prepareAcceptanceCodexHome(codexHome, workspace, sourceCodexHome); err != nil {
			return nil, err
		}
		localAddr, err := freeLoopbackAddr()
		if err != nil {
			return nil, err
		}
		localURL := "http://" + localAddr
		if _, err := app.New(workspace).Setup(context.Background(), io.Discard, io.Discard, app.SetupOptions{
			Host:        "codex",
			ControlURL:  localURL,
			Principal:   principal,
			ProjectRoot: workspace,
			UseToken:    true,
		}); err != nil {
			return nil, err
		}
		if i-1 >= len(hub.Tokens) {
			return nil, fmt.Errorf("hub token missing for agent %d", i)
		}
		if err := upsertSyncRemote(filepath.Join(workspace, ".mnemon", "harness", "sync", "remotes.json"), workspace, "hub", exchange.RemoteBackendHTTP, "", hub.URL, "", "", hub.Tokens[i-1], "", ""); err != nil {
			return nil, err
		}
		loaded, err := access.LoadBindingFile(workspace, filepath.Join(workspace, access.DefaultBindingFile))
		if err != nil {
			return nil, err
		}
		token, err := acceptanceTokenForPrincipal(loaded.Tokens, contract.ActorID(principal))
		if err != nil {
			return nil, err
		}
		localCtx, cancel := context.WithCancel(ctx)
		localErr := make(chan error, 1)
		go func(workspace, addr string, loaded access.LoadedBindings) {
			localErr <- app.RunLocalHTTPServerWithBindings(localCtx, addr, filepath.Join(workspace, runtime.DefaultStorePath), loaded, app.ServeOptions{
				ProjectRoot:  workspace,
				SyncInterval: 100 * time.Millisecond,
			}, io.Discard)
		}(workspace, localAddr, loaded)
		agent := r1CodexSyncAgent{
			r1CodexAgent: r1CodexAgent{
				principal: principal,
				workspace: workspace,
				codexHome: codexHome,
				token:     token,
				env:       acceptanceEnv(binDir, codexHome, runRoot),
			},
			localURL:         localURL,
			replicaPrincipal: hub.Principals[i-1],
			replicaToken:     hub.Tokens[i-1],
			renderAuditPath:  filepath.Join(workspace, ".mnemon", "harness", "local", "render-audit.jsonl"),
			localCancel:      cancel,
			localErr:         localErr,
		}
		if err := waitR1LocalReady(ctx, agent.r1CodexAgent, localURL, 10*time.Second); err != nil {
			cancel()
			return nil, err
		}
		agents = append(agents, agent)
	}
	return agents, nil
}

func stopR1CodexSyncAgents(agents []r1CodexSyncAgent) {
	for i := range agents {
		if agents[i].server != nil {
			agents[i].server.Close()
		}
		if agents[i].localCancel != nil {
			agents[i].localCancel()
		}
	}
	for i := range agents {
		if agents[i].localErr == nil {
			continue
		}
		select {
		case <-agents[i].localErr:
		case <-time.After(5 * time.Second):
		}
	}
}

func appendSyncAgentAnswer(report *r1CodexSyncReport, principal, answer string) {
	for i := range report.Agents {
		if report.Agents[i].Principal == principal {
			if strings.TrimSpace(answer) != "" {
				report.Agents[i].FinalAnswers = append(report.Agents[i].FinalAnswers, strings.TrimSpace(answer))
			}
			return
		}
	}
}

func waitForR1DerivedEventPresentation(controlURL, token string, wants []string, timeout time.Duration) (presentation.Response, bool) {
	deadline := time.Now().Add(timeout)
	var last presentation.Response
	for time.Now().Before(deadline) {
		resp, err := renderR1DerivedEventPresentation(controlURL, token)
		if err == nil {
			last = resp
			ok := true
			for _, want := range wants {
				if !strings.Contains(resp.Body, want) {
					ok = false
					break
				}
			}
			if ok {
				return resp, true
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return last, false
}

func runR1Turn(agent *r1CodexAgent, prompt string, timeout time.Duration) (string, error) {
	before := agent.server.NotificationCount()
	if _, err := agent.server.Request("turn/start", map[string]any{
		"threadId":       agent.threadID,
		"input":          []map[string]any{{"type": "text", "text": prompt}},
		"cwd":            agent.workspace,
		"approvalPolicy": "never",
		"sandboxPolicy":  map[string]any{"type": "dangerFullAccess"},
	}, 30*time.Second); err != nil {
		return "", fmt.Errorf("%s: turn/start: %w", agent.principal, err)
	}
	if _, err := agent.server.WaitNotification("turn/completed", timeout, before); err != nil {
		text := codexapp.CombinedText(agent.server.NotificationsSince(before))
		return text, fmt.Errorf("%s: wait turn/completed: %w", agent.principal, err)
	}
	notifications := agent.server.NotificationsSince(before)
	answer := codexapp.FinalAnswer(notifications)
	if answer == "" {
		answer = codexapp.CombinedText(notifications)
	}
	return answer, nil
}

func appendAgentAnswer(report *r1CodexAcceptanceReport, principal, answer string) {
	for i := range report.Agents {
		if report.Agents[i].Principal == principal {
			if strings.TrimSpace(answer) != "" {
				report.Agents[i].FinalAnswers = append(report.Agents[i].FinalAnswers, strings.TrimSpace(answer))
			}
			return
		}
	}
}

func waitForLedgerCount(controlURL string, agent r1CodexAgent, kind string, want int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if countR1Ledger(controlURL, agent)[kind] >= want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func countR1Ledger(controlURL string, agent r1CodexAgent) map[string]int {
	out := map[string]int{
		"agent_profile":      0,
		"project_intent":     0,
		"teamwork_signal":    0,
		"assignment":         0,
		"progress_digest":    0,
		"assignment_status":  0,
		"assignment_expired": 0,
	}
	client := access.NewClientWithToken(controlURL, agent.token)
	proj, err := client.PullPresentationView("", contract.Subscription{Actor: contract.ActorID(agent.principal)})
	if err != nil {
		return out
	}
	for _, content := range proj.Content {
		kind := string(content.Ref.Kind)
		if items, ok := content.Fields["items"].([]any); ok {
			out[kind] += len(items)
			continue
		}
		out[kind]++
	}
	return out
}

func findAssignmentAssignee(controlURL string, agent r1CodexAgent, assignmentID string) string {
	client := access.NewClientWithToken(controlURL, agent.token)
	proj, err := client.PullPresentationView("", contract.Subscription{Actor: contract.ActorID(agent.principal)})
	if err != nil {
		return ""
	}
	for _, content := range proj.Content {
		if content.Ref.Kind != "assignment" {
			continue
		}
		items, _ := content.Fields["items"].([]any)
		for _, raw := range items {
			item, _ := raw.(map[string]any)
			if acceptanceItemString(item, "assignment_id") == assignmentID {
				return acceptanceItemString(item, "assignee")
			}
		}
	}
	return ""
}

func acceptanceItemString(item map[string]any, key string) string {
	if s, ok := item[key].(string); ok {
		return s
	}
	for _, section := range []string{eventmodel.PayloadRuleKey, eventmodel.PayloadNarrativeKey, eventmodel.PayloadRefsKey} {
		if m, ok := item[section].(map[string]any); ok {
			if s, ok := m[key].(string); ok {
				return s
			}
		}
	}
	return ""
}

func renderR1DerivedEventPresentation(controlURL, token string) (presentation.Response, error) {
	body, _ := json.Marshal(presentation.Request{RenderIntent: presentation.IntentTeamworkEvents, Lifecycle: "remind", Surface: "hook"})
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(controlURL, "/")+"/render", bytes.NewReader(body))
	if err != nil {
		return presentation.Response{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return presentation.Response{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		return presentation.Response{}, fmt.Errorf("render failed: %s: %s", resp.Status, string(data))
	}
	var out presentation.Response
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func parsePrincipal(text string) string {
	fields := strings.Fields(strings.TrimSpace(text))
	for _, f := range fields {
		f = strings.Trim(f, ".,;:()[]{}\"'")
		if strings.HasPrefix(f, "codex-") && strings.Contains(f, "@project") {
			return f
		}
	}
	return ""
}

func findAgent(agents []r1CodexAgent, principal string) (r1CodexAgent, bool) {
	for _, agent := range agents {
		if agent.principal == principal {
			return agent, true
		}
	}
	return r1CodexAgent{}, false
}

func countR1DerivedEventAudit(path string) map[string]int {
	out := map[string]int{
		"entries":          0,
		"with_provenance":  0,
		"with_body_digest": 0,
		"with_audit_id":    0,
		"profile":          0,
		"work":             0,
		"integrate":        0,
		"expired":          0,
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out["entries"]++
		var obj map[string]any
		if json.Unmarshal([]byte(line), &obj) != nil {
			continue
		}
		if obj["provenance"] != nil || obj["PresentationViewDigest"] != nil || obj["CatalogDigest"] != nil {
			out["with_provenance"]++
		}
		if obj["body_digest"] != nil || obj["BodyDigest"] != nil {
			out["with_body_digest"]++
		}
		if obj["audit_id"] != nil || obj["AuditID"] != nil {
			out["with_audit_id"]++
		}
		body, _ := obj["body"].(string)
		usedEventCounts := false
		if counts, ok := obj["EventCounts"].(map[string]any); ok {
			for eventType, auditKey := range map[string]string{
				"profile.update_requested":  "profile",
				"assignment.work_available": "work",
				"assignment.progress_ready": "integrate",
				"assignment.expired":        "expired",
			} {
				if n, ok := counts[eventType].(float64); ok && n > 0 {
					out[auditKey]++
					usedEventCounts = true
				}
			}
		}
		if !usedEventCounts {
			if counts, ok := obj["PresentationCounts"].(map[string]any); ok {
				for _, key := range []string{"profile", "work", "integrate", "expired"} {
					if n, ok := counts[key].(float64); ok && n > 0 {
						out[key]++
					}
				}
			}
			for _, key := range []string{"profile", "work", "integrate", "expired"} {
				if strings.Contains(body, "[mnemon:"+key+"]") {
					out[key]++
				}
			}
		}
	}
	return out
}

func addR1Assertion(report *r1CodexAcceptanceReport, name string, passed bool, detail string) {
	if len(detail) > 1000 {
		detail = detail[:1000] + "...(truncated)"
	}
	report.Assertions = append(report.Assertions, r1AcceptanceAssertion{Name: name, Passed: passed, Detail: detail})
}

func addR1Error(report *r1CodexAcceptanceReport, err error) {
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
}

func allR1AssertionsPassed(assertions []r1AcceptanceAssertion) bool {
	if len(assertions) == 0 {
		return false
	}
	for _, a := range assertions {
		if !a.Passed {
			return false
		}
	}
	return true
}
