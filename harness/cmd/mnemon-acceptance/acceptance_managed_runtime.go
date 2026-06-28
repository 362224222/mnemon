package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/codexapp"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/spf13/cobra"
)

var (
	acceptanceManagedExchange        string
	acceptanceManagedMnemondBin      string
	acceptanceManagedRuntimeAdapter  string
	acceptanceManagedGitHubRepo      string
	acceptanceManagedGitHubTokenFile string
)

var acceptanceManagedRuntimeCmd = &cobra.Command{
	Use:   "managed-runtime",
	Short: "Run managed-runtime seed-and-observe acceptance",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runManagedRuntimeAcceptance(cmd.Context(), managedRuntimeAcceptanceOptions{
			RunRoot:         acceptanceRunRoot,
			Command:         acceptanceCommand,
			CodexHome:       acceptanceCodexHome,
			Agents:          acceptanceAgents,
			AgentTurns:      acceptanceAgentTurns,
			Exchange:        acceptanceManagedExchange,
			MnemondBin:      acceptanceManagedMnemondBin,
			Runtime:         acceptanceManagedRuntimeAdapter,
			GitHubRepo:      acceptanceManagedGitHubRepo,
			GitHubTokenFile: acceptanceManagedGitHubTokenFile,
			TurnTimeout:     acceptanceTurnTimeout,
			Stdout:          cmd.OutOrStdout(),
			Stderr:          cmd.ErrOrStderr(),
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("managed-runtime acceptance status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceManagedRuntimeCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of managed agent nodes")
	acceptanceManagedRuntimeCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real managed Codex turns after the seed")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedExchange, "exchange", "mnemonhub", "exchange mode: mnemonhub or github")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedMnemondBin, "mnemond-bin", "mnemond", "mnemond binary used for product-path wake checks")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedRuntimeAdapter, "runtime", "codex-appserver", "managed runtime adapter: codex-appserver or noop")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedGitHubRepo, "github-repo", "mnemon-dev/mnemon-teamwork-example", "GitHub Remote Workspace repository (owner/name)")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedGitHubTokenFile, "github-token-file", "", "GitHub token file for real GitHub exchange validation")
	acceptanceManagedRuntimeCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per managed wake check")
	rootCmd.AddCommand(acceptanceManagedRuntimeCmd)
}

type managedRuntimeAcceptanceOptions struct {
	RunRoot         string
	Command         string
	CodexHome       string
	Agents          int
	AgentTurns      bool
	Exchange        string
	MnemondBin      string
	Runtime         string
	GitHubRepo      string
	GitHubTokenFile string
	TurnTimeout     time.Duration
	Stdout          io.Writer
	Stderr          io.Writer
	Wake            managedRuntimeWakeFunc
}

type managedRuntimeWakeFunc func(context.Context, managedRuntimeAcceptanceOptions, string) (string, error)

type managedRuntimeAcceptanceReport struct {
	SchemaVersion int                              `json:"schema_version"`
	Status        string                           `json:"status"`
	Layer         string                           `json:"layer"`
	RunnerRole    string                           `json:"runner_role"`
	Exchange      string                           `json:"exchange"`
	GitHubRepo    string                           `json:"github_repo,omitempty"`
	Runtime       string                           `json:"runtime"`
	StartedAt     string                           `json:"started_at"`
	FinishedAt    string                           `json:"finished_at"`
	RunRoot       string                           `json:"run_root"`
	ReportPath    string                           `json:"report_path"`
	Topology      *r1AcceptanceTopologyReport      `json:"topology,omitempty"`
	Agents        []managedRuntimeAgentReport      `json:"agents"`
	DriverWakes   []driver.ManagedWakeRecord       `json:"driver_wakes,omitempty"`
	Scenarios     []r1TaskSimScenarioReport        `json:"scenarios,omitempty"`
	LedgerCounts  map[string]int                   `json:"ledger_counts,omitempty"`
	Artifacts     map[string]string                `json:"artifacts,omitempty"`
	Assertions    []managedRuntimeAssertion        `json:"assertions"`
	PromptAudit   []managedRuntimePromptAuditEntry `json:"prompt_audit"`
	Errors        []string                         `json:"errors,omitempty"`
}

type managedRuntimeAgentReport struct {
	Principal string `json:"principal"`
	Workspace string `json:"workspace,omitempty"`
	ThreadID  string `json:"thread_id,omitempty"`
	RawQuery  string `json:"raw_query"`
	Status    string `json:"status"`
}

type managedRuntimePromptAuditEntry struct {
	Principal string `json:"principal"`
	Kind      string `json:"kind"`
	Query     string `json:"query"`
}

type managedRuntimeAssertion struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

func runManagedRuntimeAcceptance(ctx context.Context, opts managedRuntimeAcceptanceOptions) (managedRuntimeAcceptanceReport, error) {
	started := time.Now().UTC()
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if opts.Agents <= 0 {
		opts.Agents = 5
	}
	if opts.Command == "" {
		opts.Command = "codex --dangerously-bypass-hook-trust"
	}
	if opts.TurnTimeout <= 0 {
		opts.TurnTimeout = 5 * time.Minute
	}
	if strings.TrimSpace(opts.GitHubRepo) == "" {
		opts.GitHubRepo = "mnemon-dev/mnemon-teamwork-example"
	}
	exchangeMode := strings.TrimSpace(opts.Exchange)
	if exchangeMode == "" {
		exchangeMode = "mnemonhub"
	}
	runtimeName := strings.TrimSpace(opts.Runtime)
	if runtimeName == "" {
		runtimeName = "codex-appserver"
	}
	runRoot := opts.RunRoot
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "managed-runtime", started.Format("20060102T150405Z"))
	}
	absRunRoot, err := filepath.Abs(runRoot)
	if err != nil {
		return managedRuntimeAcceptanceReport{}, err
	}
	runRoot = absRunRoot
	report := managedRuntimeAcceptanceReport{
		SchemaVersion: 1,
		Status:        "ok",
		Layer:         "managed_runtime_acceptance",
		RunnerRole:    "seed_and_observe",
		Exchange:      exchangeMode,
		GitHubRepo:    strings.TrimSpace(opts.GitHubRepo),
		Runtime:       runtimeName,
		StartedAt:     started.Format(time.RFC3339),
		RunRoot:       runRoot,
		Artifacts:     map[string]string{},
	}
	if err := os.MkdirAll(runRoot, 0o755); err != nil {
		report.Status = "blocked"
		report.Errors = append(report.Errors, err.Error())
		return finishManagedRuntimeReport(report), err
	}
	if exchangeMode != "mnemonhub" && exchangeMode != "github" {
		err := fmt.Errorf("managed-runtime acceptance exchange must be mnemonhub or github, got %q", exchangeMode)
		report.Status = "blocked"
		report.Errors = append(report.Errors, err.Error())
		written, writeErr := finishAndWriteManagedRuntimeReport(report)
		if writeErr != nil {
			return written, writeErr
		}
		return written, err
	}
	if err := prepareR1AcceptanceRunRoot(runRoot); err != nil {
		report.Status = "blocked"
		report.Errors = append(report.Errors, err.Error())
		return finishManagedRuntimeReport(report), err
	}
	if exchangeMode == "github" && strings.TrimSpace(opts.GitHubTokenFile) == "" {
		err := fmt.Errorf("managed-runtime github acceptance requires --github-token-file for real GitHub access")
		report.Status = "blocked"
		report.Errors = append(report.Errors, err.Error())
		addManagedAssertion(&report, "github token file provided", false, err.Error())
		written, writeErr := finishAndWriteManagedRuntimeReport(report)
		if writeErr != nil {
			return written, writeErr
		}
		return written, err
	}
	if opts.Wake != nil {
		return runManagedRuntimeInjectedWakeAcceptance(ctx, opts, report)
	}
	if !opts.AgentTurns {
		err := fmt.Errorf("managed-runtime acceptance requires --agent-turns for real seed-and-observe validation")
		report.Status = "failed"
		report.Errors = append(report.Errors, err.Error())
		addManagedAssertion(&report, "managed-runtime real agent turns requested", false, "rerun with --agent-turns")
		written, writeErr := finishAndWriteManagedRuntimeReport(report)
		if writeErr != nil {
			return written, writeErr
		}
		return written, err
	}
	switch exchangeMode {
	case "mnemonhub":
		return runManagedRuntimeMnemonhubAcceptance(ctx, opts, report)
	case "github":
		return runManagedRuntimeGitHubAcceptance(ctx, opts, report)
	default:
		panic("validated exchange mode escaped switch")
	}
}

func runManagedRuntimeInjectedWakeAcceptance(ctx context.Context, opts managedRuntimeAcceptanceOptions, report managedRuntimeAcceptanceReport) (managedRuntimeAcceptanceReport, error) {
	allSentinel := true
	for i := 1; i <= opts.Agents; i++ {
		principal := fmt.Sprintf("codex-%02d@project", i)
		query, err := opts.Wake(ctx, opts, principal)
		status := "ok"
		if err != nil {
			status = "failed"
			allSentinel = false
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", principal, err))
		}
		if strings.TrimSpace(query) != driver.ManagedWakeQuery {
			allSentinel = false
		}
		report.Agents = append(report.Agents, managedRuntimeAgentReport{Principal: principal, RawQuery: strings.TrimSpace(query), Status: status})
		report.PromptAudit = append(report.PromptAudit, managedRuntimePromptAuditEntry{Principal: principal, Kind: "raw_managed_wake", Query: strings.TrimSpace(query)})
	}
	addManagedAssertion(&report, "raw managed queries are sentinel only", allSentinel, fmt.Sprintf("agents=%d", len(report.Agents)))
	addManagedAssertion(&report, "runner role is seed-and-observe", report.RunnerRole == "seed_and_observe", report.RunnerRole)
	addManagedAssertion(&report, "no direct worker business prompts", managedRuntimeDirectWorkerPromptCount(report) == 0, "prompt_audit contains raw_managed_wake only")
	if len(report.Errors) > 0 || !managedRuntimeAssertionsPassed(report) {
		report.Status = "failed"
	}
	return finishAndWriteManagedRuntimeReport(report)
}

func runManagedRuntimeMnemonhubAcceptance(ctx context.Context, opts managedRuntimeAcceptanceOptions, report managedRuntimeAcceptanceReport) (managedRuntimeAcceptanceReport, error) {
	binDir, err := installAcceptanceHarnessBinary(report.RunRoot)
	if err != nil {
		return managedRuntimeBlocked(report, err)
	}
	hub, err := startR1SyncHub(report.RunRoot, opts.Agents)
	if err != nil {
		return managedRuntimeBlocked(report, err)
	}
	defer hub.close()
	sourceCodexHome := resolveSourceCodexHome(opts.CodexHome)
	report.Artifacts["codex_home_source"] = sourceCodexHome
	report.Artifacts["hub_db"] = filepath.Join(report.RunRoot, "hub", "hub.db")
	report.Artifacts["hub_audit"] = hub.AuditPath
	agents, err := setupR1CodexSyncAgents(ctx, report.RunRoot, binDir, hub, opts.Agents, sourceCodexHome)
	if err != nil {
		return managedRuntimeBlocked(report, err)
	}
	defer stopR1CodexSyncAgents(agents)
	report.Topology = buildR1ProdSimTopology(agents)
	addManagedAssertion(&report, "managed-runtime strict per-hostagent mnemond topology", prodSimStrictTopology(report.Topology), fmt.Sprintf("%+v", report.Topology))
	for _, agent := range agents {
		report.Artifacts["mnemond:"+agent.principal] = prodSimMnemondPath(agent)
		report.Artifacts["render_audit:"+agent.principal] = agent.renderAuditPath
	}
	return runManagedRuntimeRealScenario(ctx, opts, report, agents)
}

func runManagedRuntimeGitHubAcceptance(ctx context.Context, opts managedRuntimeAcceptanceOptions, report managedRuntimeAcceptanceReport) (managedRuntimeAcceptanceReport, error) {
	tokenFile, err := filepath.Abs(opts.GitHubTokenFile)
	if err != nil {
		return managedRuntimeBlocked(report, err)
	}
	if _, err := os.Stat(tokenFile); err != nil {
		return managedRuntimeBlocked(report, fmt.Errorf("github token file: %w", err))
	}
	binDir, err := installAcceptanceHarnessBinary(report.RunRoot)
	if err != nil {
		return managedRuntimeBlocked(report, err)
	}
	started, _ := time.Parse(time.RFC3339, report.StartedAt)
	branchPrefix := r1GitHubMeshBranchPrefix("", started)
	branches := r1GitHubMeshBranches(branchPrefix, opts.Agents)
	if err := ensureR1GitHubMeshBranches(ctx, report.GitHubRepo, tokenFile, branches); err != nil {
		return managedRuntimeBlocked(report, err)
	}
	sourceCodexHome := resolveSourceCodexHome(opts.CodexHome)
	report.Artifacts["codex_home_source"] = sourceCodexHome
	report.Artifacts["github_repo"] = report.GitHubRepo
	report.Artifacts["github_token_file"] = tokenFile
	report.Artifacts["github_branch_prefix"] = branchPrefix
	agents, err := setupR1CodexGitHubMeshAgents(ctx, report.RunRoot, binDir, report.GitHubRepo, tokenFile, branchPrefix, opts.Agents, sourceCodexHome, 30*time.Second, opts.Agents)
	if err != nil {
		return managedRuntimeBlocked(report, err)
	}
	defer stopR1CodexSyncAgents(agents)
	report.Topology = buildR1ProdSimTopology(agents)
	report.Topology.MnemonhubInstances = 0
	addManagedAssertion(&report, "managed-runtime github per-hostagent mnemond topology", r1GitHubMeshStrictTopology(report.Topology), fmt.Sprintf("%+v", report.Topology))
	addManagedAssertion(&report, "managed-runtime github has no central hub", report.Topology.MnemonhubInstances == 0, "backend=github")
	for _, agent := range agents {
		report.Artifacts["mnemond:"+agent.principal] = prodSimMnemondPath(agent)
		report.Artifacts["render_audit:"+agent.principal] = agent.renderAuditPath
	}
	return runManagedRuntimeRealScenario(ctx, opts, report, agents)
}

func runManagedRuntimeRealScenario(ctx context.Context, opts managedRuntimeAcceptanceOptions, report managedRuntimeAcceptanceReport, agents []r1CodexSyncAgent) (managedRuntimeAcceptanceReport, error) {
	if len(agents) < 5 {
		return managedRuntimeFailed(report, fmt.Errorf("managed-runtime requires at least 5 agents, got %d", len(agents)))
	}
	for i := range agents {
		if err := startR1CodexAppserver(&agents[i].r1CodexAgent, opts.Command); err != nil {
			return managedRuntimeBlocked(report, err)
		}
		agentReport, _, err := initializeManagedRuntimeCodexAgent(&agents[i].r1CodexAgent)
		if err != nil {
			return managedRuntimeBlocked(report, err)
		}
		report.Agents = append(report.Agents, managedRuntimeAgentReport{
			Principal: agentReport.Principal,
			Workspace: agentReport.Workspace,
			ThreadID:  agentReport.ThreadID,
			Status:    "initialized",
		})
	}
	addManagedAssertion(&report, "managed-runtime 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))

	runID := strings.ToLower(time.Now().UTC().Format("150405"))
	entry := &agents[0]
	worker := &agents[1]
	assignmentID := "managed-asg-" + runID
	signalID := "managed-signal-" + runID
	seedPrompt := fmt.Sprintf(`You are the entry agent for a managed-runtime Mnemon acceptance run.
Emit one teamwork_signal.write_candidate.observed event with external id seed-signal-%s and payload:
{"rule":{"signal_id":%q,"scope":"managed-runtime/seeded-teamwork","ttl":"30m"},"narrative":{"statement":"Validate that managed local mnemond drivers can continue teamwork after one seed prompt.","why_teamwork":"a worker should act from hook-rendered Mnemon context after receiving only the managed wake sentinel"},"refs":{"evidence_refs":["managed-runtime acceptance seed"]}}
Then emit one assignment.write_candidate.observed event with external id seed-assignment-%s and payload:
{"rule":{"assignment_id":%q,"signal_ref":%q,"assignee":%q,"scope":"managed-runtime/seeded-teamwork","ttl":"20m"},"narrative":{"expected_work":"Inspect the hook-rendered managed work brief and report whether the sentinel-driven turn can advance the teamwork task.","expected_feedback":"progress_digest with evidence from the managed wake turn"},"refs":{"evidence_refs":["seed signal %s"]}}
Do not contact the worker directly. After both commands succeed, answer "seed written".`, runID, signalID, runID, assignmentID, signalID, worker.principal, signalID)
	report.PromptAudit = append(report.PromptAudit, managedRuntimePromptAuditEntry{Principal: entry.principal, Kind: "entry_seed", Query: seedPrompt})
	answer, err := runR1Turn(&entry.r1CodexAgent, seedPrompt, opts.TurnTimeout)
	_ = answer
	if err != nil {
		addManagedAssertion(&report, "managed-runtime entry seed accepted", false, err.Error())
		return managedRuntimeFailed(report, err)
	}
	waitForLedgerCount(entry.localURL, entry.r1CodexAgent, "assignment", 1, 30*time.Second)
	entryCounts := countR1Ledger(entry.localURL, entry.r1CodexAgent)
	addManagedAssertion(&report, "managed-runtime entry seed accepted", entryCounts["teamwork_signal"] >= 1 && entryCounts["assignment"] >= 1, fmt.Sprintf("teamwork_signal=%d assignment=%d", entryCounts["teamwork_signal"], entryCounts["assignment"]))

	workerRecord, err := managedRuntimeWakeExistingAgent(ctx, opts, worker, []string{assignmentID})
	if err != nil {
		addManagedAssertion(&report, "managed-runtime worker woke from local mnemond driver", false, err.Error())
		return managedRuntimeFailed(report, err)
	}
	report.DriverWakes = append(report.DriverWakes, workerRecord)
	report.PromptAudit = append(report.PromptAudit, managedRuntimePromptAuditEntry{Principal: worker.principal, Kind: "raw_managed_wake", Query: workerRecord.Query})
	addManagedAssertion(&report, "managed-runtime worker woke from local mnemond driver", workerRecord.Query == driver.ManagedWakeQuery && workerRecord.RenderAuditID != "", fmt.Sprintf("query=%q render_audit=%s", workerRecord.Query, workerRecord.RenderAuditID))
	waitForLedgerCount(worker.localURL, worker.r1CodexAgent, "progress_digest", 1, 60*time.Second)
	workerCounts := countR1Ledger(worker.localURL, worker.r1CodexAgent)
	addManagedAssertion(&report, "managed-runtime worker emitted progress through normal events", workerCounts["progress_digest"] >= 1, fmt.Sprintf("progress_digest=%d", workerCounts["progress_digest"]))

	leadRecord, err := managedRuntimeWakeExistingAgent(ctx, opts, entry, []string{assignmentID})
	if err != nil {
		addManagedAssertion(&report, "managed-runtime lead woke for integrate", false, err.Error())
		return managedRuntimeFailed(report, err)
	}
	report.DriverWakes = append(report.DriverWakes, leadRecord)
	report.PromptAudit = append(report.PromptAudit, managedRuntimePromptAuditEntry{Principal: entry.principal, Kind: "raw_managed_wake", Query: leadRecord.Query})
	addManagedAssertion(&report, "managed-runtime lead woke for integrate", leadRecord.Query == driver.ManagedWakeQuery && leadRecord.RenderAuditID != "", fmt.Sprintf("query=%q render_audit=%s", leadRecord.Query, leadRecord.RenderAuditID))

	report.LedgerCounts = countR1Ledger(entry.localURL, entry.r1CodexAgent)
	report.Scenarios = append(report.Scenarios, r1TaskSimScenarioReport{
		Name:   "seeded_teamwork_managed_wake",
		Status: statusFromBool(managedRuntimeAssertionsPassed(report)),
		Actors: []string{entry.principal, worker.principal},
		Evidence: map[string]any{
			"assignment":   assignmentID,
			"driver_wakes": len(report.DriverWakes),
			"raw_query":    driver.ManagedWakeQuery,
		},
	})
	addManagedAssertion(&report, "raw managed queries are sentinel only", managedRuntimeRawQueriesSentinel(report), fmt.Sprintf("wakes=%d", len(report.DriverWakes)))
	addManagedAssertion(&report, "runner role is seed-and-observe", report.RunnerRole == "seed_and_observe", report.RunnerRole)
	addManagedAssertion(&report, "no direct worker business prompts", managedRuntimeDirectWorkerPromptCount(report) == 0, "only entry_seed and raw_managed_wake prompts recorded")
	if len(report.Errors) == 0 && managedRuntimeAssertionsPassed(report) {
		report.Status = "ok"
		return finishAndWriteManagedRuntimeReport(report)
	}
	return managedRuntimeFailed(report, fmt.Errorf("managed-runtime acceptance failed"))
}

func initializeManagedRuntimeCodexAgent(agent *r1CodexAgent) (r1CodexAgentReport, json.RawMessage, error) {
	initResp, err := agent.server.Request("initialize", map[string]any{
		"clientInfo":   map[string]any{"name": "mnemon-managed-runtime-acceptance", "version": version},
		"capabilities": codexAppServerCapabilities(),
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
		"cwd":            agent.workspace,
		"approvalPolicy": "never",
		"sandbox":        "danger-full-access",
		"ephemeral":      true,
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
	return r1CodexAgentReport{
		Principal:          agent.principal,
		Workspace:          agent.workspace,
		CodexHome:          agent.codexHome,
		ThreadID:           agent.threadID,
		HookCount:          len(hooks),
		HookTrustStatuses:  hookTrustStatuses(hooks),
		ManualHookReminded: strings.Contains(rendered, "governed context") || strings.Contains(rendered, "systemMessage"),
	}, hooksRaw, nil
}

type managedRuntimeExistingCodexClient struct {
	agent   *r1CodexAgent
	timeout time.Duration
}

func (c managedRuntimeExistingCodexClient) StartTurn(ctx context.Context, query string) (driver.ManagedTurnResult, error) {
	if strings.TrimSpace(query) != driver.ManagedWakeQuery {
		return driver.ManagedTurnResult{}, fmt.Errorf("managed acceptance client only accepts %q queries", driver.ManagedWakeQuery)
	}
	additionalContext, err := driver.CodexAppServerAdditionalContext(ctx, c.agent.workspace, c.agent.env, driver.ManagedWakeQuery)
	if err != nil {
		return driver.ManagedTurnResult{}, err
	}
	answer, err := runR1TurnWithAdditionalContext(c.agent, driver.ManagedWakeQuery, c.timeout, additionalContext)
	status := "completed"
	if err != nil {
		status = "failed"
	}
	return driver.ManagedTurnResult{TurnID: c.agent.threadID, Status: status, FinalAnswer: answer}, err
}

func managedRuntimeWakeExistingAgent(ctx context.Context, opts managedRuntimeAcceptanceOptions, agent *r1CodexSyncAgent, bodyWants []string) (driver.ManagedWakeRecord, error) {
	resp, ok := waitForR1DerivedEventPresentation(agent.localURL, agent.token, bodyWants, 90*time.Second)
	if !ok {
		return driver.ManagedWakeRecord{}, fmt.Errorf("%s did not receive derived presentation containing %v", agent.principal, bodyWants)
	}
	candidate, ok := managedRuntimeCandidateFromRender(agent.principal, resp, bodyWants)
	if !ok {
		return driver.ManagedWakeRecord{}, fmt.Errorf("%s render had no wake candidate matching %v", agent.principal, bodyWants)
	}
	ledgerPath := filepath.Join(agent.workspace, ".mnemon", "harness", "local", "managed-agent", "wake-ledger.jsonl")
	managed := &driver.ManagedAgentDriver{
		Principal: agent.principal,
		Client:    managedRuntimeExistingCodexClient{agent: &agent.r1CodexAgent, timeout: opts.TurnTimeout},
		Ledger:    driver.NewFileManagedWakeLedger(ledgerPath),
		Now:       func() time.Time { return time.Now().UTC() },
	}
	return managed.Wake(ctx, candidate)
}

func managedRuntimeCandidateFromRender(principal string, resp presentation.Response, bodyWants []string) (driver.ManagedWakeCandidate, bool) {
	for _, env := range resp.Events {
		if string(env.Event.Audience) != principal {
			continue
		}
		body, _ := eventmodel.PayloadNarrative(env.Event.Payload)["body"].(string)
		if !managedRuntimeContainsAll(body, bodyWants) {
			continue
		}
		candidates := driver.ManagedWakeCandidatesFromEvents(principal, []eventmodel.EventEnvelope{env})
		if len(candidates) == 0 {
			continue
		}
		candidates[0].RenderAuditID = resp.AuditID
		candidates[0].RenderBodyDigest = resp.BodyDigest
		return candidates[0], true
	}
	return driver.ManagedWakeCandidate{}, false
}

func managedRuntimeContainsAll(body string, wants []string) bool {
	for _, want := range wants {
		if strings.TrimSpace(want) != "" && !strings.Contains(body, want) {
			return false
		}
	}
	return true
}

func managedRuntimeRawQueriesSentinel(report managedRuntimeAcceptanceReport) bool {
	for _, entry := range report.PromptAudit {
		if entry.Kind != "raw_managed_wake" {
			continue
		}
		if strings.TrimSpace(entry.Query) != driver.ManagedWakeQuery {
			return false
		}
	}
	return true
}

func managedRuntimeBlocked(report managedRuntimeAcceptanceReport, err error) (managedRuntimeAcceptanceReport, error) {
	report.Status = "blocked"
	report.Errors = append(report.Errors, err.Error())
	written, writeErr := finishAndWriteManagedRuntimeReport(report)
	if writeErr != nil {
		return written, writeErr
	}
	return written, err
}

func managedRuntimeFailed(report managedRuntimeAcceptanceReport, err error) (managedRuntimeAcceptanceReport, error) {
	report.Status = "failed"
	if err != nil {
		report.Errors = append(report.Errors, err.Error())
	}
	written, writeErr := finishAndWriteManagedRuntimeReport(report)
	if writeErr != nil {
		return written, writeErr
	}
	return written, err
}

func managedRuntimeMnemondDryRunWake(ctx context.Context, opts managedRuntimeAcceptanceOptions, principal string) (string, error) {
	timeout := opts.TurnTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	wakeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	bin := strings.TrimSpace(opts.MnemondBin)
	if bin == "" {
		bin = "mnemond"
	}
	cmd := exec.CommandContext(wakeCtx, bin, "agent", "run", "--principal", principal, "--dry-run")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return strings.TrimSpace(string(out)), fmt.Errorf("mnemond managed wake dry-run: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

func managedRuntimeDirectWorkerPromptCount(report managedRuntimeAcceptanceReport) int {
	count := 0
	for _, entry := range report.PromptAudit {
		if entry.Kind == "worker_business" {
			count++
		}
	}
	return count
}

func addManagedAssertion(report *managedRuntimeAcceptanceReport, name string, passed bool, detail string) {
	report.Assertions = append(report.Assertions, managedRuntimeAssertion{Name: name, Passed: passed, Detail: detail})
}

func managedRuntimeAssertionsPassed(report managedRuntimeAcceptanceReport) bool {
	for _, assertion := range report.Assertions {
		if !assertion.Passed {
			return false
		}
	}
	return true
}

func finishAndWriteManagedRuntimeReport(report managedRuntimeAcceptanceReport) (managedRuntimeAcceptanceReport, error) {
	report = finishManagedRuntimeReport(report)
	path := filepath.Join(report.RunRoot, "acceptance-report.json")
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return report, err
	}
	report.ReportPath = path
	raw, err = json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, err
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		return report, err
	}
	return report, nil
}

func finishManagedRuntimeReport(report managedRuntimeAcceptanceReport) managedRuntimeAcceptanceReport {
	report.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if report.Status == "" {
		report.Status = "ok"
	}
	return report
}
