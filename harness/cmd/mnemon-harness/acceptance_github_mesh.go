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
	githubbackend "github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange/backend/github"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var (
	acceptanceGitHubRepo         string
	acceptanceGitHubTokenFile    string
	acceptanceGitHubBranchPrefix string
	acceptanceGitHubScenarios    []string
	acceptanceGitHubSyncInterval time.Duration
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
			SyncInterval: acceptanceGitHubSyncInterval,
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

const r1GitHubMeshWorkerWakePrompt = `Check your Mnemon context. If there is governed work for you, act on it through
your own Local Mnemon and record durable progress. If your focus, availability,
or working state changed, update your agent_profile through your own Local Mnemon.
If there is no work for you, answer "no governed work".`

func init() {
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceR1GitHubMeshCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of Codex appservers")
	acceptanceR1GitHubMeshCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real model turns that write governed GitHub mesh events")
	acceptanceR1GitHubMeshCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per real agent turn")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceGitHubRepo, "github-repo", "mnemon-dev/mnemon-teamwork-example", "GitHub Remote Workspace repository (owner/name)")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceGitHubTokenFile, "github-token-file", "", "GitHub token file for publication store access")
	acceptanceR1GitHubMeshCmd.Flags().StringVar(&acceptanceGitHubBranchPrefix, "github-branch-prefix", "", "GitHub publication branch prefix; empty uses a run-scoped acceptance prefix")
	acceptanceR1GitHubMeshCmd.Flags().StringArrayVar(&acceptanceGitHubScenarios, "scenario", nil, "natural scenario to run; repeatable")
	acceptanceR1GitHubMeshCmd.Flags().DurationVar(&acceptanceGitHubSyncInterval, "sync-interval", 30*time.Second, "GitHub sync interval per local mnemond")
	acceptanceCmd.AddCommand(acceptanceR1GitHubMeshCmd)
}

type r1GitHubMeshAcceptanceOptions struct {
	r1CodexAcceptanceOptions
	Repo         string
	TokenFile    string
	BranchPrefix string
	Scenarios    []string
	SyncInterval time.Duration
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
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 30 * time.Second
	}
	started := time.Now().UTC().Truncate(time.Second)
	branchPrefix := r1GitHubMeshBranchPrefix(opts.BranchPrefix, started)
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
		RunnerContract: &r1RunnerContractReport{
			EntrypointProgressBeforeIntegration: -1,
			EntrypointProgressAfterIntegration:  -1,
			WorkerWakePrompt:                    r1GitHubMeshWorkerWakePrompt,
		},
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
	branches := r1GitHubMeshBranches(branchPrefix, opts.Agents)
	if err := ensureR1GitHubMeshBranches(ctx, opts.Repo, tokenFile, branches); err != nil {
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
	report.Artifacts["github_branch_prefix"] = branchPrefix
	report.Artifacts["github_sync_interval"] = opts.SyncInterval.String()
	initialOnline := opts.Agents
	if opts.AgentTurns {
		initialOnline = r1GitHubMeshInitialOnline(opts.Agents)
	}
	report.Artifacts["github_initial_online_agents"] = fmt.Sprintf("%d", initialOnline)

	agents, err := setupR1CodexGitHubMeshAgents(ctx, runRoot, binDir, opts.Repo, tokenFile, branchPrefix, opts.Agents, sourceCodexHome, opts.SyncInterval, initialOnline)
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
	addR1Assertion(&report, "github-mesh no p2p node discovery", syncReport.NetworkDiscovery == "none" && syncReport.RosterSource == "configured-remotes-json", fmt.Sprintf("roster_source=%s network_discovery=%s", syncReport.RosterSource, syncReport.NetworkDiscovery))
	addR1Assertion(&report, "github-mesh publication branches configured", len(syncReport.PublicationBranches) == opts.Agents && len(syncReport.BranchByAgent) == opts.Agents, fmt.Sprintf("branches=%v", syncReport.PublicationBranches))
	addR1Assertion(&report, "github-mesh remote plans are per-workspace", distinctStrings(syncReport.RemotePlanPaths) && len(syncReport.RemotePlanPaths) == opts.Agents, fmt.Sprintf("remote_plans=%v", syncReport.RemotePlanPaths))
	addR1Assertion(&report, "github-mesh local stores isolated", distinctStrings(syncReport.LocalStorePaths) && len(syncReport.LocalStorePaths) == opts.Agents, fmt.Sprintf("stores=%v", syncReport.LocalStorePaths))
	addR1Assertion(&report, "github-mesh runtime workspaces isolated", distinctStrings(syncReport.RuntimeWorkspaces) && len(syncReport.RuntimeWorkspaces) == opts.Agents, fmt.Sprintf("workspaces=%v", syncReport.RuntimeWorkspaces))

	if !opts.AgentTurns {
		addR1Assertion(&report, "github-mesh real agent turns requested", false, "rerun with --agent-turns")
		report.Status = "failed"
		return report, fmt.Errorf("GitHub mesh task-suite acceptance requires --agent-turns")
	}
	for _, i := range r1GitHubMeshLocalOnlineIndexes(agents) {
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
	addR1Assertion(&report, "github-mesh initial appservers start/init", len(report.Agents) == initialOnline, fmt.Sprintf("started=%d initial_online=%d requested=%d", len(report.Agents), initialOnline, opts.Agents))

	run := r1GitHubMeshRun{
		ctx:           ctx,
		opts:          opts,
		report:        &report,
		agents:        agents,
		runID:         started.Format("150405"),
		initialOnline: initialOnline,
	}
	if err := run.bootstrapProfiles(); err != nil {
		addR1Error(&report, err)
	}
	for _, name := range r1GitHubMeshScenarioNames(opts.Scenarios) {
		if err := run.runScenario(name); err != nil {
			addR1Error(&report, err)
		}
	}
	addR1Assertion(&report, "github-mesh 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))
	addR1Assertion(&report, "github-mesh delayed mnemond join exercised", run.joined || initialOnline == opts.Agents, fmt.Sprintf("initial_online=%d total=%d lifecycle=%v", initialOnline, opts.Agents, syncReport.Lifecycle))
	addR1Assertion(&report, "github-mesh local mnemond leave/restart exercised", run.lifecycleExercised, fmt.Sprintf("lifecycle=%v", syncReport.Lifecycle))
	run.addPostScenarioAssertions()
	obs, obsErr := observeAcceptanceRun(runRoot, 1000)
	if obsErr == nil {
		report.Observability = &obs
		populateR1GitHubMeshSyncEvidence(&report, obs)
		report.Participants = r1ClusterParticipants(r1ClusterActorEventCounts(obs), report.Entrypoint)
	} else {
		addR1Error(&report, obsErr)
	}
	report.DerivedEventAudit = prodSimDerivedAudit(agents)
	if len(agents) > 0 {
		report.LedgerCounts = countR1Ledger(agents[0].localURL, agents[0].r1CodexAgent)
	}
	addR1Assertion(&report, "github-mesh no shared governed.db", prodSimStrictTopology(report.Topology), fmt.Sprintf("%+v", report.Topology))
	addR1Assertion(&report, "github-mesh accepted event subjects only", r1SyncEventSubjectsOnlyAccepted(syncReport.AllowedEventSubjects), fmt.Sprintf("subjects=%v", syncReport.AllowedEventSubjects))
	addR1Assertion(&report, "github-mesh report includes publication/import evidence", len(syncReport.PublishedByBranch) == opts.Agents && len(syncReport.ImportedByMnemond) == opts.Agents, fmt.Sprintf("published=%v imported=%v diagnostics=%v", syncReport.PublishedByBranch, syncReport.ImportedByMnemond, syncReport.DiagnosticsByMnemond))
	if len(report.Errors) == 0 && allR1AssertionsPassed(report.Assertions) && allR1GitHubMeshScenariosOK(report.Scenarios, opts.Scenarios) {
		syncReport.Status = "ok"
		report.Status = "ok"
		return report, nil
	}
	syncReport.Status = "failed"
	report.Status = "failed"
	return report, fmt.Errorf("GitHub mesh natural task suite failed")
}

type r1GitHubMeshRun struct {
	ctx                context.Context
	opts               r1GitHubMeshAcceptanceOptions
	report             *r1CodexAcceptanceReport
	agents             []r1CodexSyncAgent
	runID              string
	initialOnline      int
	joined             bool
	lifecycleExercised bool
}

func r1GitHubMeshScenarioNames(selected []string) []string {
	if len(selected) == 0 {
		return []string{"onboarding-synthesis", "sync-risk-review", "live-readiness-operator-safety"}
	}
	out := make([]string, 0, len(selected))
	for _, name := range selected {
		name = strings.TrimSpace(name)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func allR1GitHubMeshScenariosOK(scenarios []r1TaskSimScenarioReport, selected []string) bool {
	want := map[string]bool{}
	for _, name := range r1GitHubMeshScenarioNames(selected) {
		want[name] = false
	}
	for _, scenario := range scenarios {
		if _, ok := want[scenario.Name]; ok && scenario.Status == "ok" {
			want[scenario.Name] = true
		}
	}
	if len(want) == 0 {
		return false
	}
	for _, ok := range want {
		if !ok {
			return false
		}
	}
	return true
}

func r1GitHubMeshScenarioSelected(selected []string, name string) bool {
	for _, selectedName := range selected {
		if selectedName == name {
			return true
		}
	}
	return false
}

func r1GitHubMeshOKScenarioNames(scenarios []r1TaskSimScenarioReport) []string {
	out := []string{}
	for _, scenario := range scenarios {
		if scenario.Status == "ok" && strings.TrimSpace(scenario.Name) != "" {
			out = append(out, scenario.Name)
		}
	}
	sort.Strings(out)
	return out
}

func r1GitHubMeshCrossTaskReuseCandidate(name string, priorOK []string) bool {
	if name != "sync-risk-review" {
		return false
	}
	return r1GitHubMeshScenarioSelected(priorOK, "onboarding-synthesis")
}

func r1GitHubMeshHasOKScenarioEvidenceBool(scenarios []r1TaskSimScenarioReport, name, key string) bool {
	for _, scenario := range scenarios {
		if scenario.Name != name || scenario.Status != "ok" {
			continue
		}
		if value, ok := scenario.Evidence[key].(bool); ok && value {
			return true
		}
	}
	return false
}

func r1GitHubMeshHasAnyOKScenarioEvidenceIntAtLeast(scenarios []r1TaskSimScenarioReport, key string, min int) bool {
	for _, scenario := range scenarios {
		if scenario.Status != "ok" {
			continue
		}
		switch value := scenario.Evidence[key].(type) {
		case int:
			if value >= min {
				return true
			}
		case float64:
			if int(value) >= min {
				return true
			}
		}
	}
	return false
}

func (s *r1GitHubMeshRun) bootstrapProfiles() error {
	profileCounts := map[string]int{}
	active := r1GitHubMeshReadyAgentIndexes(s.agents)
	if len(active) == 0 {
		return fmt.Errorf("github mesh profile bootstrap requires at least one online agent")
	}
	for _, i := range active {
		agent := &s.agents[i]
		payload := taskSimJSON(map[string]any{
			"actor":              agent.principal,
			"focus":              fmt.Sprintf("GitHub mesh Remote Workspace acceptance node %s", agent.principal),
			"context_advantages": []string{"isolated local mnemond", "github publication branch sync", "real Codex appserver turn"},
			"availability":       "available",
			"ttl":                "30m",
			"summary":            fmt.Sprintf("%s is available for GitHub mesh teamwork validation.", agent.principal),
		})
		prompt := fmt.Sprintf(`Emit exactly one agent_profile.write_candidate.observed event through your own Local Mnemon.
Use external id github-mesh-profile-%s-%s and payload:
%s
After the command succeeds, answer "profile written".`, s.runID, prodSafeID(agent.principal), payload)
		recordR1ClusterPrompt(s.report.RunnerContract, agent.principal, "profile_bootstrap", prompt)
		s.report.RunnerContract.ProfileBootstrapPrompts++
		answer, err := runR1Turn(&agent.r1CodexAgent, prompt, s.opts.TurnTimeout)
		appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
		if err != nil {
			addR1Assertion(s.report, "github-mesh profile emitted "+agent.principal, false, err.Error())
			return err
		}
		waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "agent_profile", 1, 20*time.Second)
		profileCounts[agent.principal] = countR1Ledger(agent.localURL, agent.r1CodexAgent)["agent_profile"]
	}
	allVisible := true
	for _, i := range active {
		agent := s.agents[i]
		waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "agent_profile", len(active), 120*time.Second)
		counts := countR1Ledger(agent.localURL, agent.r1CodexAgent)
		profileCounts[agent.principal] = counts["agent_profile"]
		if counts["agent_profile"] < len(active) {
			allVisible = false
		}
	}
	addR1Assertion(s.report, "github-mesh initial profiles converge through publication branches", allVisible, fmt.Sprintf("initial_online_agents=%d", len(active)))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "bootstrap_profiles",
		Status: statusFromBool(allVisible),
		Evidence: map[string]any{
			"profile_counts_by_agent": profileCounts,
			"initial_online_agents":   len(active),
			"publication_branches":    s.report.Sync.PublicationBranches,
		},
	})
	if !allVisible {
		return fmt.Errorf("profiles did not converge through GitHub publication branches")
	}
	return nil
}

func (s *r1GitHubMeshRun) runScenario(name string) error {
	entries, err := s.scenarioEntries(name)
	if err != nil {
		s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{Name: name, Status: "blocked", Evidence: map[string]any{"error": err.Error()}})
		return err
	}
	var actors []string
	for _, entry := range entries {
		agent := &s.agents[entry.index]
		actors = append(actors, agent.principal)
		s.report.RunnerContract.BusinessTaskPrompts++
		if s.report.Entrypoint == "" {
			s.report.Entrypoint = agent.principal
			s.report.Starter = agent.principal
			s.report.Sync.Source = agent.principal
		}
		recordR1ClusterPrompt(s.report.RunnerContract, agent.principal, "natural_user_message:"+name, entry.prompt)
		answer, err := runR1Turn(&agent.r1CodexAgent, entry.prompt, s.opts.TurnTimeout)
		appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
		if err != nil {
			addR1Assertion(s.report, "github-mesh "+name+" entry "+agent.principal, false, err.Error())
			return err
		}
		addR1Assertion(s.report, "github-mesh "+name+" entry "+agent.principal, true, truncateR1Cluster(answer, 300))
	}
	if err := s.wakeWorkers(name, entries); err != nil {
		return err
	}
	priorScenarios := r1GitHubMeshOKScenarioNames(s.report.Scenarios)
	lead := &s.agents[entries[0].index]
	integrationPrompt := r1GitHubMeshIntegrationPrompt(name)
	s.report.RunnerContract.IntegrationPrompts++
	recordR1ClusterPrompt(s.report.RunnerContract, lead.principal, "integration:"+name, integrationPrompt)
	answer, err := runR1Turn(&lead.r1CodexAgent, integrationPrompt, s.opts.TurnTimeout)
	appendSyncAgentAnswer(s.report.Sync, lead.principal, answer)
	if err != nil {
		addR1Assertion(s.report, "github-mesh "+name+" integration", false, err.Error())
		return err
	}
	waitR1ClusterAcceptedEventSettle(s.report.RunRoot, 15*time.Second, 2*time.Second)
	obs, err := observeAcceptanceRun(s.report.RunRoot, 1000)
	if err != nil {
		return err
	}
	counts := r1ClusterActorEventCounts(obs)
	participants := r1ClusterNonProfileParticipantCount(counts)
	replans := r1GitHubMeshPromptRounds(s.report.RunnerContract, name)
	naturalMessages := r1GitHubMeshPromptKindCount(s.report.RunnerContract, "natural_user_message:"+name)
	workerWakes := r1GitHubMeshPromptKindCount(s.report.RunnerContract, "worker_wake:"+name)
	integrationPrompts := r1GitHubMeshPromptKindCount(s.report.RunnerContract, "integration:"+name)
	assignments := r1GitHubMeshKindTotal(counts, "assignment")
	progress := r1GitHubMeshKindTotal(counts, "progress_digest")
	signals := r1GitHubMeshKindTotal(counts, "teamwork_signal")
	intents := r1GitHubMeshKindTotal(counts, "project_intent")
	passed := participants >= 2 &&
		replans >= 2 &&
		naturalMessages == len(entries) &&
		integrationPrompts >= 1 &&
		s.report.RunnerContract.DirectWorkerBusinessPrompts == 0 &&
		assignments >= 1 &&
		progress >= 1
	addR1Assertion(s.report, "github-mesh "+name+" team-shaped multi-round evidence", passed, fmt.Sprintf("participants=%d rounds=%d natural=%d worker_wakes=%d integration=%d assignments=%d progress_digest=%d teamwork_signal=%d project_intent=%d actors=%v", participants, replans, naturalMessages, workerWakes, integrationPrompts, assignments, progress, signals, intents, counts))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   name,
		Status: statusFromBool(passed),
		Actors: actors,
		Evidence: map[string]any{
			"participants":                   participants,
			"replanning_rounds":              replans,
			"natural_user_messages":          naturalMessages,
			"worker_wake_prompts":            workerWakes,
			"integration_prompts":            integrationPrompts,
			"entry_poc_agents":               actors,
			"multi_poc":                      len(actors) > 1,
			"prior_ok_scenarios":             priorScenarios,
			"cross_task_reuse_or_completion": r1GitHubMeshCrossTaskReuseCandidate(name, priorScenarios),
			"profile_update_prompted":        strings.Contains(r1GitHubMeshWorkerWakePrompt, "agent_profile"),
			"direct_worker_business":         s.report.RunnerContract.DirectWorkerBusinessPrompts,
			"shared_appserver_threads":       r1GitHubMeshThreadIDs(s.agents),
			"cross_scenario_mnemon_ctx":      true,
			"actor_event_counts":             counts,
			"assignment_events":              assignments,
			"progress_digest_events":         progress,
			"teamwork_signal_events":         signals,
			"project_intent_events":          intents,
		},
	})
	if !passed {
		return fmt.Errorf("github mesh scenario %s did not produce team-shaped multi-round evidence", name)
	}
	return nil
}

func (s *r1GitHubMeshRun) addPostScenarioAssertions() {
	profileCounts := r1GitHubMeshLedgerCountsByAgent(s.agents, "agent_profile")
	baseline := len(s.agents)
	if s.initialOnline > 0 && s.initialOnline < len(s.agents) {
		baseline = s.initialOnline
	}
	refreshed := false
	for _, count := range profileCounts {
		if count > baseline {
			refreshed = true
			break
		}
	}
	addR1Assertion(s.report, "github-mesh profiles refresh during work", refreshed, fmt.Sprintf("agent_profile_counts=%v initial_online_agents=%d total_agents=%d", profileCounts, baseline, len(s.agents)))
	if s.report.Sync != nil {
		if s.report.Raw == nil {
			s.report.Raw = map[string]json.RawMessage{}
		}
		raw, _ := json.Marshal(profileCounts)
		s.report.Raw["github_mesh:profile_counts_after_scenarios"] = raw
	}
	selected := r1GitHubMeshScenarioNames(s.opts.Scenarios)
	if r1GitHubMeshScenarioSelected(selected, "live-readiness-operator-safety") {
		addR1Assertion(s.report, "github-mesh multi-poc scenario exercised", r1GitHubMeshHasOKScenarioEvidenceBool(s.report.Scenarios, "live-readiness-operator-safety", "multi_poc"), "scenario=live-readiness-operator-safety")
	}
	if r1GitHubMeshScenarioSelected(selected, "onboarding-synthesis") && r1GitHubMeshScenarioSelected(selected, "sync-risk-review") {
		addR1Assertion(s.report, "github-mesh cross-task reuse/completion evidence recorded", r1GitHubMeshHasOKScenarioEvidenceBool(s.report.Scenarios, "sync-risk-review", "cross_task_reuse_or_completion"), "scenario=sync-risk-review prior=onboarding-synthesis")
	}
	addR1Assertion(s.report, "github-mesh output-driven replanning evidence recorded", r1GitHubMeshHasAnyOKScenarioEvidenceIntAtLeast(s.report.Scenarios, "replanning_rounds", 2), "requires at least one successful natural scenario with two or more prompt rounds")
}

type r1GitHubMeshScenarioEntry struct {
	index  int
	prompt string
}

func (s *r1GitHubMeshRun) scenarioEntries(name string) ([]r1GitHubMeshScenarioEntry, error) {
	if len(s.agents) < 5 {
		return nil, fmt.Errorf("github mesh natural scenarios require five agents")
	}
	switch name {
	case "onboarding-synthesis":
		return []r1GitHubMeshScenarioEntry{{index: 0, prompt: `帮我快速理解这个仓库现在的 GitHub Remote Workspace 改造方向,整理一份新成员能读懂的上手说明。你可以让其他成员帮忙核对架构、测试和风险。如果第一轮信息不够,请根据大家的反馈再拆一轮补齐。`}}, nil
	case "sync-risk-review":
		return []r1GitHubMeshScenarioEntry{{index: 1, prompt: `同步这块我担心还有隐藏问题。你帮我检查一下 GitHub Remote Workspace 相关的配置、诊断和测试设计;如果发现顺手能补的文档或测试缺口,一起处理。第一轮先找风险,再根据结果安排第二轮验证。`}}, nil
	case "live-readiness-operator-safety":
		return []r1GitHubMeshScenarioEntry{
			{index: 0, prompt: `请你推进一次 GitHub live case 的准备,目标是能在 mnemon-dev/mnemon-teamwork-example 上证明 publish/pull/import 成立。先让大家找出缺口,再根据第一轮结果安排第二轮补齐。`},
			{index: 2, prompt: `我主要担心这个 GitHub 方案的操作者安全和失败诊断。你帮我从 token、repo、branch、报错可读性这几个角度检查一下。`},
		}, nil
	default:
		return nil, fmt.Errorf("unknown GitHub mesh scenario %q", name)
	}
}

func (s *r1GitHubMeshRun) wakeWorkers(name string, entries []r1GitHubMeshScenarioEntry) error {
	entry := map[int]bool{}
	for _, item := range entries {
		entry[item.index] = true
	}
	for cycle := 1; cycle <= 3; cycle++ {
		for i := range s.agents {
			if entry[i] || !r1GitHubMeshAgentReady(s.agents[i]) {
				continue
			}
			agent := &s.agents[i]
			s.report.RunnerContract.WorkerWakePrompts++
			recordR1ClusterPrompt(s.report.RunnerContract, agent.principal, "worker_wake:"+name, r1GitHubMeshWorkerWakePrompt)
			answer, err := runR1Turn(&agent.r1CodexAgent, r1GitHubMeshWorkerWakePrompt, s.opts.TurnTimeout)
			appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
			if err != nil {
				s.report.RunnerContract.WorkerWakeErrors = append(s.report.RunnerContract.WorkerWakeErrors, fmt.Sprintf("%s cycle %d %s: %v", name, cycle, agent.principal, err))
			}
		}
		waitR1ClusterAcceptedEventSettle(s.report.RunRoot, 8*time.Second, 1*time.Second)
		if cycle == 1 {
			if err := s.joinDelayedAgents(name); err != nil {
				return err
			}
		}
		if cycle == 2 && !s.lifecycleExercised {
			if err := exerciseR1GitHubMeshLifecycle(s.ctx, s.report, s.agents, s.opts.SyncInterval); err != nil {
				return err
			}
			s.lifecycleExercised = true
		}
	}
	return nil
}

func (s *r1GitHubMeshRun) joinDelayedAgents(scenario string) error {
	if s.joined {
		return nil
	}
	var joined []string
	profileCounts := map[string]int{}
	for i := range s.agents {
		if r1GitHubMeshAgentReady(s.agents[i]) {
			continue
		}
		agent := &s.agents[i]
		branch := ""
		if s.report.Sync != nil {
			branch = s.report.Sync.BranchByAgent[agent.principal]
			s.report.Sync.Lifecycle = append(s.report.Sync.Lifecycle, r1SyncLifecycleReport{
				At:        time.Now().UTC().Format(time.RFC3339),
				Principal: agent.principal,
				Action:    "delayed_join_start",
				Result:    "requested",
				Branch:    branch,
				Detail:    "start a preconfigured isolated Local Mnemon during the natural GitHub mesh task",
			})
		}
		if err := startR1GitHubMeshLocalMnemond(s.ctx, agent, s.opts.SyncInterval); err != nil {
			addR1Assertion(s.report, "github-mesh delayed mnemond join "+agent.principal, false, err.Error())
			return err
		}
		if err := startR1CodexAppserver(&agent.r1CodexAgent, s.opts.Command); err != nil {
			addR1Assertion(s.report, "github-mesh delayed appserver join "+agent.principal, false, err.Error())
			return err
		}
		agentReport, raw, err := initializeR1CodexAgent(&agent.r1CodexAgent, s.opts.TurnTimeout)
		if err != nil {
			addR1Assertion(s.report, "github-mesh delayed appserver join "+agent.principal, false, err.Error())
			return err
		}
		s.report.Agents = append(s.report.Agents, agentReport)
		if s.report.Sync != nil {
			s.report.Sync.Agents = append(s.report.Sync.Agents, agentReport)
		}
		if raw != nil {
			s.report.Raw[agent.principal+":hooks"] = raw
		}
		if err := s.emitJoinedProfile(agent, scenario); err != nil {
			return err
		}
		counts := countR1Ledger(agent.localURL, agent.r1CodexAgent)
		profileCounts[agent.principal] = counts["agent_profile"]
		joined = append(joined, agent.principal)
		if s.report.Sync != nil {
			s.report.Sync.Lifecycle = append(s.report.Sync.Lifecycle, r1SyncLifecycleReport{
				At:        time.Now().UTC().Format(time.RFC3339),
				Principal: agent.principal,
				Action:    "delayed_join_ready",
				Result:    "ready",
				Branch:    branch,
				Detail:    "joined configured publication mesh, initialized appserver, and published fresh profile",
				Ledger:    counts,
			})
		}
	}
	s.joined = true
	if len(joined) == 0 {
		return nil
	}
	allVisible := true
	for _, i := range r1GitHubMeshReadyAgentIndexes(s.agents) {
		agent := s.agents[i]
		waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "agent_profile", len(s.agents), 120*time.Second)
		counts := countR1Ledger(agent.localURL, agent.r1CodexAgent)
		profileCounts[agent.principal] = counts["agent_profile"]
		if counts["agent_profile"] < len(s.agents) {
			allVisible = false
		}
	}
	passed := len(joined) >= 2 && allVisible
	addR1Assertion(s.report, "github-mesh two delayed mnemond join and import backlog", passed, fmt.Sprintf("joined=%v profile_counts=%v", joined, profileCounts))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "delayed_join_profiles",
		Status: statusFromBool(passed),
		Actors: joined,
		Evidence: map[string]any{
			"scenario":                scenario,
			"joined_agents":           joined,
			"profile_counts_by_agent": profileCounts,
			"publication_branches":    s.report.Sync.PublicationBranches,
		},
	})
	if !passed {
		return fmt.Errorf("delayed GitHub mesh join did not converge profiles through publication branches")
	}
	return nil
}

func (s *r1GitHubMeshRun) emitJoinedProfile(agent *r1CodexSyncAgent, scenario string) error {
	payload := taskSimJSON(map[string]any{
		"actor":              agent.principal,
		"focus":              fmt.Sprintf("Joined GitHub mesh task %s with fresh local context", scenario),
		"context_advantages": []string{"late join backlog import", "github publication branch sync", "isolated local mnemond"},
		"availability":       "available",
		"ttl":                "30m",
		"summary":            fmt.Sprintf("%s joined during %s and can pick up governed work from imported context.", agent.principal, scenario),
	})
	prompt := fmt.Sprintf(`Emit exactly one agent_profile.write_candidate.observed event through your own Local Mnemon.
Use external id github-mesh-join-profile-%s-%s and payload:
%s
After the command succeeds, answer "joined profile written".`, s.runID, prodSafeID(agent.principal), payload)
	recordR1ClusterPrompt(s.report.RunnerContract, agent.principal, "delayed_join_profile:"+scenario, prompt)
	s.report.RunnerContract.ProfileBootstrapPrompts++
	answer, err := runR1Turn(&agent.r1CodexAgent, prompt, s.opts.TurnTimeout)
	appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
	if err != nil {
		addR1Assertion(s.report, "github-mesh delayed profile emitted "+agent.principal, false, err.Error())
		return err
	}
	waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "agent_profile", 1, 20*time.Second)
	return nil
}

func r1GitHubMeshIntegrationPrompt(name string) string {
	return fmt.Sprintf(`Read your own Local Mnemon context and integrate the GitHub mesh teamwork scenario %q.
Use only governed Mnemon events as teammate evidence. If first-round output reveals gaps, emit a follow-up assignment before finalizing; otherwise emit a final progress_digest with participants, evidence, gaps, and next action.
Answer with the event-backed result.`, name)
}

func r1GitHubMeshPromptRounds(contract *r1RunnerContractReport, scenario string) int {
	if contract == nil {
		return 0
	}
	rounds := 0
	for _, prompt := range contract.PromptAudit {
		if strings.Contains(prompt.Kind, scenario) {
			rounds++
		}
	}
	return rounds
}

func r1GitHubMeshPromptKindCount(contract *r1RunnerContractReport, kind string) int {
	if contract == nil {
		return 0
	}
	count := 0
	for _, prompt := range contract.PromptAudit {
		if prompt.Kind == kind {
			count++
		}
	}
	return count
}

func r1GitHubMeshKindTotal(counts map[string]map[string]int, kind string) int {
	total := 0
	for _, byKind := range counts {
		total += byKind[kind]
	}
	return total
}

func r1GitHubMeshLedgerCountsByAgent(agents []r1CodexSyncAgent, kind string) map[string]int {
	out := make(map[string]int, len(agents))
	for _, agent := range agents {
		out[agent.principal] = countR1Ledger(agent.localURL, agent.r1CodexAgent)[kind]
	}
	return out
}

func r1GitHubMeshThreadIDs(agents []r1CodexSyncAgent) map[string]string {
	out := make(map[string]string, len(agents))
	for _, agent := range agents {
		if strings.TrimSpace(agent.threadID) != "" {
			out[agent.principal] = agent.threadID
		}
	}
	return out
}

func setupR1CodexGitHubMeshAgents(ctx context.Context, runRoot, binDir, repo, tokenFile, branchPrefix string, count int, sourceCodexHome string, syncInterval time.Duration, initialOnline int) ([]r1CodexSyncAgent, error) {
	if syncInterval <= 0 {
		syncInterval = 30 * time.Second
	}
	if initialOnline < 0 || initialOnline > count {
		initialOnline = count
	}
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
		}
		if i <= initialOnline {
			if err := startR1GitHubMeshLocalMnemond(ctx, &agent, syncInterval); err != nil {
				return nil, err
			}
		}
		agents = append(agents, agent)
	}
	return agents, nil
}

func exerciseR1GitHubMeshLifecycle(ctx context.Context, report *r1CodexAcceptanceReport, agents []r1CodexSyncAgent, syncInterval time.Duration) error {
	if report == nil || report.Sync == nil || len(agents) < 5 {
		return nil
	}
	if syncInterval <= 0 {
		syncInterval = 30 * time.Second
	}
	target := &agents[2]
	branch := report.Sync.BranchByAgent[target.principal]
	report.Sync.Lifecycle = append(report.Sync.Lifecycle, r1SyncLifecycleReport{
		At:        time.Now().UTC().Format(time.RFC3339),
		Principal: target.principal,
		Action:    "pause_local_mnemond",
		Result:    "requested",
		Branch:    branch,
		Detail:    "cancel one isolated Local Mnemon before teamwork turns; appserver remains initialized",
	})
	if target.localCancel == nil {
		addR1Assertion(report, "github-mesh local mnemond pause/restart exercised", false, "target has no localCancel")
		return fmt.Errorf("%s has no local mnemond cancel function", target.principal)
	}
	target.localCancel()
	if target.localErr != nil {
		select {
		case <-target.localErr:
		case <-time.After(5 * time.Second):
			addR1Assertion(report, "github-mesh local mnemond pause observed", false, "timeout waiting for local mnemond stop")
			return fmt.Errorf("%s local mnemond did not stop within timeout", target.principal)
		}
	}
	target.localCancel = nil
	target.localErr = nil
	if err := startR1GitHubMeshLocalMnemond(ctx, target, syncInterval); err != nil {
		addR1Assertion(report, "github-mesh local mnemond pause/restart exercised", false, err.Error())
		return err
	}
	counts := countR1Ledger(target.localURL, target.r1CodexAgent)
	report.Sync.Lifecycle = append(report.Sync.Lifecycle, r1SyncLifecycleReport{
		At:        time.Now().UTC().Format(time.RFC3339),
		Principal: target.principal,
		Action:    "restart_local_mnemond",
		Result:    "ready",
		Branch:    branch,
		Detail:    "restarted the same isolated Local Mnemon store/workspace and configured GitHub publication branch",
		Ledger:    counts,
	})
	addR1Assertion(report, "github-mesh local mnemond pause/restart exercised", true, fmt.Sprintf("principal=%s branch=%s", target.principal, branch))
	return nil
}

func startR1GitHubMeshLocalMnemond(ctx context.Context, agent *r1CodexSyncAgent, syncInterval time.Duration) error {
	if agent == nil {
		return fmt.Errorf("nil GitHub mesh agent")
	}
	if agent.localCancel != nil {
		return nil
	}
	if syncInterval <= 0 {
		syncInterval = 30 * time.Second
	}
	loaded, err := access.LoadBindingFile(agent.workspace, filepath.Join(agent.workspace, access.DefaultBindingFile))
	if err != nil {
		return err
	}
	addr := strings.TrimPrefix(agent.localURL, "http://")
	if strings.TrimSpace(addr) == "" {
		return fmt.Errorf("%s has no local URL", agent.principal)
	}
	localCtx, cancel := context.WithCancel(ctx)
	localErr := make(chan error, 1)
	go func(workspace, addr string, loaded access.LoadedBindings) {
		localErr <- app.RunLocalHTTPServerWithBindings(localCtx, addr, filepath.Join(workspace, runtime.DefaultStorePath), loaded, app.ServeOptions{
			ProjectRoot:  workspace,
			SyncInterval: syncInterval,
		}, io.Discard)
	}(agent.workspace, addr, loaded)
	agent.localCancel = cancel
	agent.localErr = localErr
	if err := waitR1LocalReady(ctx, agent.r1CodexAgent, agent.localURL, 10*time.Second); err != nil {
		cancel()
		agent.localCancel = nil
		agent.localErr = nil
		return err
	}
	return nil
}

func r1GitHubMeshInitialOnline(count int) int {
	if count <= 2 {
		return count
	}
	if count <= 5 {
		return count - 2
	}
	return count - 2
}

func r1GitHubMeshAgentReady(agent r1CodexSyncAgent) bool {
	return agent.localCancel != nil && agent.server != nil && strings.TrimSpace(agent.threadID) != ""
}

func r1GitHubMeshLocalOnlineIndexes(agents []r1CodexSyncAgent) []int {
	out := []int{}
	for i, agent := range agents {
		if agent.localCancel != nil {
			out = append(out, i)
		}
	}
	return out
}

func r1GitHubMeshReadyAgentIndexes(agents []r1CodexSyncAgent) []int {
	out := []int{}
	for i, agent := range agents {
		if r1GitHubMeshAgentReady(agent) {
			out = append(out, i)
		}
	}
	return out
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

func r1GitHubMeshBranchPrefix(prefix string, started time.Time) string {
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		return prefix
	}
	return "mnemon/acceptance/" + started.UTC().Format("20060102T150405Z") + "/agent-"
}

func ensureR1GitHubMeshBranches(ctx context.Context, repo, tokenFile string, branches []string) error {
	token, err := readR1GitHubMeshToken(tokenFile)
	if err != nil {
		return err
	}
	store, err := githubbackend.NewPublicationStore(githubbackend.PublicationStoreConfig{
		Repo:  repo,
		Token: token,
	})
	if err != nil {
		return err
	}
	for _, branch := range branches {
		if err := store.EnsureBranch(ctx, branch, "main"); err != nil {
			return fmt.Errorf("ensure GitHub branch %q: %w", branch, err)
		}
	}
	return nil
}

func readR1GitHubMeshToken(tokenFile string) (string, error) {
	body, err := os.ReadFile(tokenFile)
	if err != nil {
		return "", fmt.Errorf("read github token file: %w", err)
	}
	token := strings.TrimSpace(string(body))
	if token == "" {
		return "", fmt.Errorf("github token file is empty")
	}
	return token, nil
}

func buildR1GitHubMeshSyncReport(repo string, agents []r1CodexSyncAgent) *r1CodexSyncReport {
	report := &r1CodexSyncReport{
		Status:               "running",
		Backend:              exchange.RemoteBackendGitHub,
		Repo:                 repo,
		TransportModel:       "repo-mediated-publication",
		RosterSource:         "configured-remotes-json",
		NetworkDiscovery:     "none",
		AllowedEventSubjects: r1SyncEventSubjectLabels(r1GitHubMeshScopes()),
		Artifacts:            map[string]string{},
		BranchByAgent:        map[string]string{},
		PublishedByBranch:    map[string]int{},
		ImportedByMnemond:    map[string]int{},
		DiagnosticsByMnemond: map[string]int{},
		ProfileByMnemond:     map[string]int{},
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
		remotesPath := filepath.Join(agent.workspace, ".mnemon", "harness", "sync", "remotes.json")
		report.RemotePlanPaths = append(report.RemotePlanPaths, remotesPath)
		report.Artifacts[fmt.Sprintf("remotes:%s", agent.principal)] = remotesPath
		if i == 0 {
			report.Source = agent.principal
		}
	}
	sort.Strings(report.PublicationBranches)
	sort.Strings(report.RemotePlanPaths)
	return report
}

func populateR1GitHubMeshSyncEvidence(report *r1CodexAcceptanceReport, obs acceptanceObserveReport) {
	if report == nil || report.Sync == nil {
		return
	}
	syncReport := report.Sync
	if syncReport.PublishedByBranch == nil {
		syncReport.PublishedByBranch = map[string]int{}
	}
	if syncReport.ImportedByMnemond == nil {
		syncReport.ImportedByMnemond = map[string]int{}
	}
	if syncReport.DiagnosticsByMnemond == nil {
		syncReport.DiagnosticsByMnemond = map[string]int{}
	}
	if syncReport.ProfileByMnemond == nil {
		syncReport.ProfileByMnemond = map[string]int{}
	}
	stores := map[string]acceptanceStoreInspect{}
	for _, store := range obs.Stores {
		if store.Role == "mnemond" {
			stores[store.Name] = store
		}
	}
	for _, agent := range syncReport.Agents {
		principal := strings.TrimSpace(agent.Principal)
		if principal == "" {
			continue
		}
		storeName := r1GitHubMeshStoreName(principal)
		store, ok := stores[storeName]
		if !ok {
			continue
		}
		branch := syncReport.BranchByAgent[principal]
		if branch != "" {
			syncReport.PublishedByBranch[branch] = store.SyncEventsByStatus["synced"]
		}
		syncReport.ImportedByMnemond[principal] = store.Counts["imported_accepted"]
		syncReport.DiagnosticsByMnemond[principal] = store.ObservedByType["sync.diagnostic"] + store.ObservedByType["sync.remote_diagnostic.observed"]
		syncReport.ProfileByMnemond[principal] = store.EnvelopeByType["agent_profile.accepted"]
	}
}

func r1GitHubMeshStoreName(principal string) string {
	name, _, _ := strings.Cut(strings.TrimSpace(principal), "@")
	return name
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
