package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var (
	acceptanceGitHubRepo         string
	acceptanceGitHubTokenFile    string
	acceptanceGitHubBranchPrefix string
	acceptanceGitHubScenarios    []string
)

var acceptanceR1GitHubMeshCmd = &cobra.Command{
	Use:   "r1-github-mesh-task-suite",
	Short: "Run GitHub-backed Remote Workspace task-suite acceptance",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runR1GitHubMeshAcceptance(cmd.Context(), r1GitHubMeshAcceptanceOptions{
			r1CodexAcceptanceOptions: r1CodexAcceptanceOptions{
				RunRoot:     acceptanceRunRoot,
				Command:     acceptanceCommand,
				CodexHome:   acceptanceCodexHome,
				Agents:      acceptanceAgents,
				AgentTurns:  acceptanceAgentTurns,
				TurnTimeout: acceptanceTurnTimeout,
				Stdout:      cmd.OutOrStdout(),
				Stderr:      cmd.ErrOrStderr(),
			},
			Repo:         acceptanceGitHubRepo,
			TokenFile:    acceptanceGitHubTokenFile,
			BranchPrefix: acceptanceGitHubBranchPrefix,
			Scenarios:    acceptanceGitHubScenarios,
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("GitHub mesh task-suite acceptance status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceR1GitHubMeshCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of Codex appservers")
	acceptanceR1GitHubMeshCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real model turns that write governed GitHub mesh events")
	acceptanceR1GitHubMeshCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per real agent turn")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceGitHubRepo, "github-repo", "mnemon-dev/mnemon-teamwork-example", "GitHub Remote Workspace repository (owner/name)")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceGitHubTokenFile, "github-token-file", "", "GitHub token file for publication store access")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceGitHubBranchPrefix, "github-branch-prefix", "mnemon/agent-", "GitHub publication branch prefix")
	acceptanceR1GitHubMeshCmd.Flags().StringArrayVar(&acceptanceGitHubScenarios, "scenario", nil, "natural scenario to run; repeatable")
	acceptanceCmd.AddCommand(acceptanceR1GitHubMeshCmd)
}

type r1GitHubMeshAcceptanceOptions struct {
	r1CodexAcceptanceOptions
	Repo         string
	TokenFile    string
	BranchPrefix string
	Scenarios    []string
}

func runR1GitHubMeshAcceptance(ctx context.Context, opts r1GitHubMeshAcceptanceOptions) (r1CodexAcceptanceReport, error) {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if opts.Command == "" {
		opts.Command = "codex"
	}
	if opts.Agents < 5 {
		opts.Agents = 5
	}
	if opts.TurnTimeout <= 0 {
		opts.TurnTimeout = 5 * time.Minute
	}
	if opts.Repo == "" {
		opts.Repo = "mnemon-dev/mnemon-teamwork-example"
	}
	if opts.BranchPrefix == "" {
		opts.BranchPrefix = "mnemon/agent-"
	}
	started := time.Now().UTC().Truncate(time.Second)
	runRoot := opts.RunRoot
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "r1-github-mesh-task-suite", started.Format("20060102T150405Z"))
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
		Scenario:          "github-mesh-task-suite",
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
	if opts.TokenFile == "" {
		err := fmt.Errorf("--github-token-file is required")
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	tokenFile, err := filepath.Abs(opts.TokenFile)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	if _, err := os.Stat(tokenFile); err != nil {
		err = fmt.Errorf("github token file: %w", err)
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
	sourceCodexHome := resolveSourceCodexHome(opts.CodexHome)
	report.Artifacts["codex_home_source"] = sourceCodexHome
	report.Artifacts["github_repo"] = opts.Repo
	report.Artifacts["github_token_file"] = tokenFile

	agents, err := setupR1CodexGitHubMeshAgents(ctx, runRoot, binDir, opts.Repo, tokenFile, opts.BranchPrefix, opts.Agents, sourceCodexHome)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	defer stopR1CodexSyncAgents(agents)
	report.Topology = buildR1ProdSimTopology(agents)
	addR1Assertion(&report, "github-mesh strict per-hostagent mnemond topology", prodSimStrictTopology(report.Topology), fmt.Sprintf("%+v", report.Topology))

	syncReport := buildR1GitHubMeshSyncReport(opts.Repo, agents)
	report.Sync = syncReport
	for _, agent := range agents {
		report.Artifacts["mnemond:"+agent.principal] = prodSimMnemondPath(agent)
		report.Artifacts["render_audit:"+agent.principal] = agent.renderAuditPath
	}
	addR1Assertion(&report, "github-mesh no central mnemon-hub endpoint", syncReport.HubURL == "" && syncReport.HubStatus.HubEventsReceived == 0, "backend=github repo-mediated publication")
	addR1Assertion(&report, "github-mesh publication branches configured", len(syncReport.PublicationBranches) == opts.Agents && len(syncReport.BranchByAgent) == opts.Agents, fmt.Sprintf("branches=%v", syncReport.PublicationBranches))
	addR1Assertion(&report, "github-mesh local stores isolated", distinctStrings(syncReport.LocalStorePaths) && len(syncReport.LocalStorePaths) == opts.Agents, fmt.Sprintf("stores=%v", syncReport.LocalStorePaths))
	addR1Assertion(&report, "github-mesh runtime workspaces isolated", distinctStrings(syncReport.RuntimeWorkspaces) && len(syncReport.RuntimeWorkspaces) == opts.Agents, fmt.Sprintf("workspaces=%v", syncReport.RuntimeWorkspaces))

	if !opts.AgentTurns {
		addR1Assertion(&report, "github-mesh real agent turns requested", false, "rerun with --agent-turns")
		report.Status = "failed"
		return report, fmt.Errorf("GitHub mesh task-suite acceptance requires --agent-turns")
	}
	for i := range agents {
		if err := startR1CodexAppserver(&agents[i].r1CodexAgent, opts.Command); err != nil {
			addR1Error(&report, err)
			report.Status = "blocked"
			return report, err
		}
		agentReport, raw, err := initializeR1CodexAgent(&agents[i].r1CodexAgent, opts.TurnTimeout)
		if err != nil {
			addR1Error(&report, err)
			report.Status = "blocked"
			return report, err
		}
		report.Agents = append(report.Agents, agentReport)
		syncReport.Agents = append(syncReport.Agents, agentReport)
		if raw != nil {
			report.Raw[agents[i].principal+":hooks"] = raw
		}
	}
	addR1Assertion(&report, "github-mesh 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))
	addR1Assertion(&report, "github-mesh natural suite implementation pending", false, "P8 topology is wired; natural Teamwork-ReAct scenarios still need execution wiring")
	report.Status = "failed"
	return report, fmt.Errorf("GitHub mesh natural task suite is not fully wired yet")
}

func setupR1CodexGitHubMeshAgents(ctx context.Context, runRoot, binDir, repo, tokenFile, branchPrefix string, count int, sourceCodexHome string) ([]r1CodexSyncAgent, error) {
	var agents []r1CodexSyncAgent
	branches := r1GitHubMeshBranches(branchPrefix, count)
	for i := 1; i <= count; i++ {
		principal := fmt.Sprintf("codex-%02d@project", i)
		workspace := filepath.Join(runRoot, "workspaces", fmt.Sprintf("codex-%02d", i))
		codexHome := filepath.Join(runRoot, "codex-home", fmt.Sprintf("codex-%02d", i))
		if err := os.MkdirAll(workspace, 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(workspace, "README.md"), []byte("# R1 GitHub mesh acceptance workspace\n"), 0o644); err != nil {
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
		if err := writeR1GitHubMeshRemotes(workspace, repo, tokenFile, branches, i-1); err != nil {
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
			replicaPrincipal: principal,
			replicaToken:     tokenFile,
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

func writeR1GitHubMeshRemotes(workspace, repo, tokenFile string, branches []string, self int) error {
	if self < 0 || self >= len(branches) {
		return fmt.Errorf("self index %d outside branches", self)
	}
	repo, err := exchange.NormalizeGitHubRepo(repo)
	if err != nil {
		return err
	}
	if strings.TrimSpace(tokenFile) == "" {
		return fmt.Errorf("github token file is required")
	}
	normalized := make([]string, 0, len(branches))
	for _, branch := range branches {
		branch, err := exchange.NormalizePublicationBranch(branch)
		if err != nil {
			return err
		}
		normalized = append(normalized, branch)
	}
	branches = normalized
	remotesPath := filepath.Join(workspace, ".mnemon", "harness", "sync", "remotes.json")
	if err := upsertSyncRemote(remotesPath, workspace, "self", exchange.RemoteBackendGitHub, exchange.RemoteDirectionPublish, "", repo, branches[self], "", tokenFile, ""); err != nil {
		return err
	}
	for i, branch := range branches {
		if i == self {
			continue
		}
		id := fmt.Sprintf("stream-%02d", i+1)
		if err := upsertSyncRemote(remotesPath, workspace, id, exchange.RemoteBackendGitHub, exchange.RemoteDirectionSubscribe, "", repo, branch, "", tokenFile, ""); err != nil {
			return err
		}
	}
	return nil
}

func r1GitHubMeshBranches(prefix string, count int) []string {
	if prefix == "" {
		prefix = "mnemon/agent-"
	}
	out := make([]string, 0, count)
	for i := 0; i < count; i++ {
		suffix := fmt.Sprintf("%02d", i+1)
		if strings.HasSuffix(prefix, "agent-") && i < 26 {
			suffix = string(rune('a' + i))
		}
		out = append(out, prefix+suffix)
	}
	return out
}

func buildR1GitHubMeshSyncReport(repo string, agents []r1CodexSyncAgent) *r1CodexSyncReport {
	report := &r1CodexSyncReport{
		Status:               "running",
		Backend:              exchange.RemoteBackendGitHub,
		Repo:                 repo,
		AllowedEventSubjects: r1SyncEventSubjectLabels(r1GitHubMeshScopes()),
		Artifacts:            map[string]string{},
		BranchByAgent:        map[string]string{},
	}
	for i, agent := range agents {
		branch := ""
		if remote, err := exchange.LoadRemoteEntry(filepath.Join(agent.workspace, ".mnemon", "harness", "sync", "remotes.json"), "self"); err == nil {
			branch = remote.Branch
		} else if agent.replicaPrincipal != "" {
			branch = agent.replicaPrincipal
		}
		if branch != "" {
			report.PublicationBranches = append(report.PublicationBranches, branch)
			report.BranchByAgent[agent.principal] = branch
		}
		report.RuntimeWorkspaces = append(report.RuntimeWorkspaces, agent.workspace)
		report.LocalStorePaths = append(report.LocalStorePaths, filepath.Join(agent.workspace, runtime.DefaultStorePath))
		report.Artifacts[fmt.Sprintf("remotes:%s", agent.principal)] = filepath.Join(agent.workspace, ".mnemon", "harness", "sync", "remotes.json")
		if i == 0 {
			report.Source = agent.principal
		}
	}
	sort.Strings(report.PublicationBranches)
	return report
}

func r1GitHubMeshScopes() []contract.ResourceRef {
	return []contract.ResourceRef{
		{Kind: "agent_profile", ID: "project"},
		{Kind: "project_intent", ID: "project"},
		{Kind: "teamwork_signal", ID: "project"},
		{Kind: "assignment", ID: "project"},
		{Kind: "progress_digest", ID: "project"},
	}
}

func distinctStrings(values []string) bool {
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			return false
		}
		seen[value] = true
	}
	return true
}
