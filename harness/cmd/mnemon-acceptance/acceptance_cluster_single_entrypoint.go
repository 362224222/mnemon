package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/spf13/cobra"
)

var (
	acceptanceClusterScenario     string
	acceptanceClusterSeed         int64
	acceptanceClusterWakeCycles   int
	acceptanceClusterWakeInterval time.Duration
	acceptanceClusterEntrypoint   string
)

const r1ClusterWorkerWakePrompt = `Check your Mnemon context. If there is governed work for you, act on it through
your own Local Mnemon and record durable progress. If there is no work for you,
answer "no governed work".`

var acceptanceR1ClusterSingleEntrypointCmd = &cobra.Command{
	Use:   "r1-cluster-single-entrypoint",
	Short: "Run productization phase-1 single-entrypoint cluster validation",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runR1ClusterSingleEntrypointAcceptance(cmd.Context(), r1ClusterSingleEntrypointOptions{
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
			Scenario:     acceptanceClusterScenario,
			Seed:         acceptanceClusterSeed,
			WakeCycles:   acceptanceClusterWakeCycles,
			WakeInterval: acceptanceClusterWakeInterval,
			Entrypoint:   acceptanceClusterEntrypoint,
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("R1 cluster single-entrypoint acceptance status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceR1ClusterSingleEntrypointCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of Codex appservers")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real model turns that write governed R1 cluster events")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per real agent turn")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().StringVar(&acceptanceClusterScenario, "scenario", "project-validation", "phase-1 scenario: project-validation or seeded-defect")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().Int64Var(&acceptanceClusterSeed, "seed", 0, "random seed for entrypoint selection; defaults to current time")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().IntVar(&acceptanceClusterWakeCycles, "wake-cycles", 4, "generic worker wake cycles")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().DurationVar(&acceptanceClusterWakeInterval, "wake-interval", 3*time.Second, "delay between worker wake cycles")
	acceptanceR1ClusterSingleEntrypointCmd.Flags().StringVar(&acceptanceClusterEntrypoint, "entrypoint", "", "explicit entrypoint principal; empty chooses by seed")
	rootCmd.AddCommand(acceptanceR1ClusterSingleEntrypointCmd)
}

type r1ClusterSingleEntrypointOptions struct {
	r1CodexAcceptanceOptions
	Scenario     string
	Seed         int64
	WakeCycles   int
	WakeInterval time.Duration
	Entrypoint   string
}

type r1RunnerContractReport struct {
	ProfileBootstrapPrompts             int                    `json:"profile_bootstrap_prompts"`
	BusinessTaskPrompts                 int                    `json:"business_task_prompts"`
	WorkerWakePrompts                   int                    `json:"worker_wake_prompts"`
	DirectWorkerBusinessPrompts         int                    `json:"direct_worker_business_prompts"`
	IntegrationPrompts                  int                    `json:"integration_prompts"`
	ManualEventWrites                   int                    `json:"manual_event_writes"`
	EntrypointProgressBeforeIntegration int                    `json:"entrypoint_progress_before_integration"`
	EntrypointProgressAfterIntegration  int                    `json:"entrypoint_progress_after_integration"`
	SyncSettleSeconds                   int                    `json:"sync_settle_seconds,omitempty"`
	WorkerWakePrompt                    string                 `json:"worker_wake_prompt"`
	EntryBusinessPrompt                 string                 `json:"entry_business_prompt,omitempty"`
	IntegrationPrompt                   string                 `json:"integration_prompt,omitempty"`
	PromptAudit                         []r1RunnerPromptReport `json:"prompt_audit,omitempty"`
	WorkerWakeErrors                    []string               `json:"worker_wake_errors,omitempty"`
}

type r1RunnerPromptReport struct {
	Index     int    `json:"index"`
	Principal string `json:"principal"`
	Kind      string `json:"kind"`
	Prompt    string `json:"prompt"`
}

type r1ClusterParticipantReport struct {
	Principal   string         `json:"principal"`
	Roles       []string       `json:"roles"`
	EventCounts map[string]int `json:"event_counts"`
}

type r1ClusterFindingReport struct {
	Kind     string `json:"kind"`
	Summary  string `json:"summary"`
	Evidence string `json:"evidence"`
	Resolved bool   `json:"resolved"`
}

func runR1ClusterSingleEntrypointAcceptance(ctx context.Context, opts r1ClusterSingleEntrypointOptions) (r1CodexAcceptanceReport, error) {
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
	if opts.WakeCycles <= 0 {
		opts.WakeCycles = 4
	}
	if opts.WakeInterval <= 0 {
		opts.WakeInterval = 3 * time.Second
	}
	if opts.Scenario == "" {
		opts.Scenario = "project-validation"
	}
	if opts.Scenario != "project-validation" && opts.Scenario != "seeded-defect" {
		return r1CodexAcceptanceReport{}, fmt.Errorf("unsupported cluster scenario %q", opts.Scenario)
	}
	if opts.Seed == 0 {
		opts.Seed = time.Now().UnixNano()
	}
	started := time.Now().UTC().Truncate(time.Second)
	runRoot := opts.RunRoot
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "r1-cluster-single-entrypoint", started.Format("20060102T150405Z"))
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
		Scenario:          opts.Scenario,
		Seed:              opts.Seed,
		AgentTurns:        opts.AgentTurns,
		LedgerCounts:      map[string]int{},
		DerivedEventAudit: map[string]int{},
		Artifacts:         map[string]string{},
		Raw:               map[string]json.RawMessage{},
		RunnerContract: &r1RunnerContractReport{
			EntrypointProgressBeforeIntegration: -1,
			EntrypointProgressAfterIntegration:  -1,
			WorkerWakePrompt:                    r1ClusterWorkerWakePrompt,
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
	binDir, err := installAcceptanceHarnessBinary(runRoot)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	hub, err := startR1SyncHub(runRoot, opts.Agents)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	defer hub.close()
	sourceCodexHome := resolveSourceCodexHome(opts.CodexHome)
	sourceRoot, _ := filepath.Abs(".")
	report.Artifacts["codex_home_source"] = sourceCodexHome
	report.Artifacts["project_snapshot_source"] = sourceRoot
	report.Artifacts["hub_db"] = filepath.Join(runRoot, "hub", "hub.db")
	report.Artifacts["hub_audit"] = hub.AuditPath

	agents, err := setupR1CodexSyncAgents(ctx, runRoot, binDir, hub, opts.Agents, sourceCodexHome)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	defer stopR1CodexSyncAgents(agents)
	for i := range agents {
		if err := copyR1ClusterProjectSnapshot(sourceRoot, agents[i].workspace, runRoot); err != nil {
			addR1Error(&report, fmt.Errorf("%s: copy project snapshot: %w", agents[i].principal, err))
			report.Status = "blocked"
			return report, err
		}
	}
	if opts.Scenario == "seeded-defect" {
		rel, err := applyR1ClusterSeededDefect(agents)
		if err != nil {
			addR1Error(&report, err)
			report.Status = "blocked"
			return report, err
		}
		report.Artifacts["seeded_defect"] = rel
	}

	report.Topology = buildR1ProdSimTopology(agents)
	addR1Assertion(&report, "cluster strict per-hostagent mnemond topology", prodSimStrictTopology(report.Topology), fmt.Sprintf("%+v", report.Topology))
	for _, agent := range agents {
		report.Artifacts["mnemond:"+agent.principal] = prodSimMnemondPath(agent)
		report.Artifacts["render_audit:"+agent.principal] = agent.renderAuditPath
	}
	syncReport := &r1CodexSyncReport{
		Status:               "running",
		HubURL:               hub.URL,
		AllowedEventSubjects: hub.AllowedEventSubjects,
		Agents:               []r1CodexAgentReport{},
		Artifacts: map[string]string{
			"hub_db":    report.Artifacts["hub_db"],
			"hub_audit": hub.AuditPath,
		},
	}
	report.Sync = syncReport

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
		syncReport.Agents = append(syncReport.Agents, agentReport)
		report.Agents = append(report.Agents, agentReport)
		if raw != nil {
			report.Raw[agents[i].principal+":hooks"] = raw
		}
	}
	addR1Assertion(&report, "cluster 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))
	addR1ClusterHookAssertions(&report, opts.r1CodexAcceptanceOptions)
	if !opts.AgentTurns {
		addR1Assertion(&report, "cluster real agent turns requested", false, "rerun with --agent-turns")
		report.Status = "failed"
		return report, fmt.Errorf("R1 cluster single-entrypoint acceptance requires --agent-turns")
	}

	run := prodSimRun{
		ctx:    ctx,
		opts:   r1ProdSimAcceptanceOptions{r1CodexAcceptanceOptions: opts.r1CodexAcceptanceOptions},
		report: &report,
		agents: agents,
		runID:  started.Format("150405"),
	}
	report.RunnerContract.ProfileBootstrapPrompts = len(agents)
	if err := run.bootstrapProfiles(); err != nil {
		addR1Error(&report, err)
	}

	entryIndex, err := chooseR1ClusterEntrypoint(agents, opts.Entrypoint, opts.Seed)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	entrypoint := &agents[entryIndex]
	report.Entrypoint = entrypoint.principal
	report.Starter = entrypoint.principal
	syncReport.Source = entrypoint.principal
	addR1Assertion(&report, "cluster entrypoint selected by seed or explicit flag", entrypoint.principal != "", fmt.Sprintf("entrypoint=%s seed=%d explicit=%q", entrypoint.principal, opts.Seed, opts.Entrypoint))

	businessPrompt := r1ClusterBusinessTaskPrompt(opts.Scenario)
	report.RunnerContract.EntryBusinessPrompt = businessPrompt
	report.RunnerContract.BusinessTaskPrompts++
	recordR1ClusterPrompt(report.RunnerContract, entrypoint.principal, "business_task", businessPrompt)
	answer, err := runR1Turn(&entrypoint.r1CodexAgent, businessPrompt, opts.TurnTimeout)
	appendSyncAgentAnswer(syncReport, entrypoint.principal, answer)
	if err != nil {
		addR1Assertion(&report, "cluster entrypoint accepts single business prompt", false, err.Error())
		addR1Error(&report, err)
	} else {
		addR1Assertion(&report, "cluster entrypoint accepts single business prompt", true, truncateR1Cluster(answer, 300))
	}
	waitForLedgerCount(entrypoint.localURL, entrypoint.r1CodexAgent, "assignment", 2, 60*time.Second)

	for cycle := 1; cycle <= opts.WakeCycles; cycle++ {
		if ctx.Err() != nil {
			addR1Error(&report, ctx.Err())
			break
		}
		for i := range agents {
			if i == entryIndex {
				continue
			}
			worker := &agents[i]
			report.RunnerContract.WorkerWakePrompts++
			recordR1ClusterPrompt(report.RunnerContract, worker.principal, "worker_wake", r1ClusterWorkerWakePrompt)
			answer, err := runR1Turn(&worker.r1CodexAgent, r1ClusterWorkerWakePrompt, opts.TurnTimeout)
			appendSyncAgentAnswer(syncReport, worker.principal, answer)
			if err != nil {
				report.RunnerContract.WorkerWakeErrors = append(report.RunnerContract.WorkerWakeErrors, fmt.Sprintf("cycle %d %s: %v", cycle, worker.principal, err))
			}
		}
		obs, err := observeAcceptanceRun(runRoot, 1000)
		if err == nil && r1ClusterProgressReady(r1ClusterActorEventCounts(obs), entrypoint.principal) {
			break
		}
		if cycle == opts.WakeCycles {
			break
		}
		select {
		case <-ctx.Done():
			addR1Error(&report, ctx.Err())
			cycle = opts.WakeCycles
		case <-time.After(opts.WakeInterval):
		}
	}

	report.RunnerContract.SyncSettleSeconds = int((10 * time.Second).Seconds())
	waitR1ClusterAcceptedEventSettle(runRoot, 10*time.Second, 2*time.Second)
	if obs, err := observeAcceptanceRun(runRoot, 1000); err == nil {
		report.RunnerContract.EntrypointProgressBeforeIntegration = r1ClusterActorEventCounts(obs)[entrypoint.principal]["progress_digest"]
	} else {
		addR1Error(&report, fmt.Errorf("pre-integration observe: %w", err))
	}
	integrationPrompt := r1ClusterIntegrationPrompt()
	report.RunnerContract.IntegrationPrompt = integrationPrompt
	report.RunnerContract.IntegrationPrompts++
	recordR1ClusterPrompt(report.RunnerContract, entrypoint.principal, "integration", integrationPrompt)
	answer, err = runR1Turn(&entrypoint.r1CodexAgent, integrationPrompt, opts.TurnTimeout)
	appendSyncAgentAnswer(syncReport, entrypoint.principal, answer)
	if err != nil {
		addR1Assertion(&report, "cluster entrypoint final integration turn completes", false, err.Error())
		addR1Error(&report, err)
	} else {
		addR1Assertion(&report, "cluster entrypoint final integration turn completes", true, truncateR1Cluster(answer, 300))
	}
	waitForLedgerCount(entrypoint.localURL, entrypoint.r1CodexAgent, "progress_digest", 3, 30*time.Second)

	if client, err := access.NewSyncClient(hub.URL, access.SyncClientConfig{Token: entrypoint.replicaToken}); err == nil {
		syncReport.HubStatus, err = client.SyncStatus()
		if err != nil {
			addR1Assertion(&report, "cluster mnemonhub status readable", false, err.Error())
		} else {
			addR1Assertion(&report, "cluster mnemonhub exchanges accepted events", syncReport.HubStatus.HubEventsReceived > 0 && syncReport.HubStatus.HubEventsServed > 0, fmt.Sprintf("received=%d served=%d", syncReport.HubStatus.HubEventsReceived, syncReport.HubStatus.HubEventsServed))
		}
	} else {
		addR1Assertion(&report, "cluster mnemonhub status readable", false, err.Error())
	}

	report.LedgerCounts = countR1Ledger(entrypoint.localURL, entrypoint.r1CodexAgent)
	report.DerivedEventAudit = prodSimDerivedAudit(agents)
	obs, obsErr := observeAcceptanceRun(runRoot, 1000)
	if obsErr == nil {
		report.Observability = &obs
	} else {
		addR1Error(&report, obsErr)
	}
	actorCounts := r1ClusterActorEventCounts(obs)
	report.RunnerContract.EntrypointProgressAfterIntegration = actorCounts[entrypoint.principal]["progress_digest"]
	report.Participants = r1ClusterParticipants(actorCounts, entrypoint.principal)
	finalAnswer := latestR1ClusterAgentAnswer(syncReport, entrypoint.principal)
	report.Findings = []r1ClusterFindingReport{r1ClusterFindingFromAnswer(finalAnswer, report.LedgerCounts)}

	addR1ClusterAuditAssertions(&report, syncReport, actorCounts, finalAnswer, opts.WakeCycles)
	if report.Observability != nil {
		addR1Assertion(&report, "cluster observability sees strict topology", report.Observability.Topology.Mode == "per-hostagent-mnemond" && !report.Observability.Topology.SharedMnemond, fmt.Sprintf("mode=%s shared=%t mnemond=%d hub=%d", report.Observability.Topology.Mode, report.Observability.Topology.SharedMnemond, report.Observability.Topology.MnemondStores, report.Observability.Topology.MnemonhubStores))
		ok, detail := acceptedR2PayloadShapeAssertion(*report.Observability)
		addR1Assertion(&report, "cluster accepted event payloads are R2 nested", ok, detail)
	} else {
		addR1Assertion(&report, "cluster observability sees strict topology", false, "observe report unavailable")
		addR1Assertion(&report, "cluster accepted event payloads are R2 nested", false, "observe report unavailable")
	}

	scenarioOK := len(report.Errors) == 0 && allR1AssertionsPassed(report.Assertions)
	report.Scenarios = append(report.Scenarios, r1TaskSimScenarioReport{
		Name:   "cluster_single_entrypoint",
		Status: statusFromBool(scenarioOK),
		Actors: r1ClusterParticipantPrincipals(report.Participants),
		Evidence: map[string]any{
			"entrypoint":    entrypoint.principal,
			"seed":          opts.Seed,
			"ledger_counts": report.LedgerCounts,
		},
	})
	syncReport.Status = statusFromBool(scenarioOK)
	if scenarioOK {
		report.Status = "ok"
		return report, nil
	}
	report.Status = "failed"
	return report, fmt.Errorf("R1 cluster single-entrypoint acceptance failed")
}

func addR1ClusterHookAssertions(report *r1CodexAcceptanceReport, opts r1CodexAcceptanceOptions) {
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
	addR1Assertion(report, "cluster preflight hooks discovered and remind", allHooks, "each appserver lists R1 hooks and manual lifecycle reminder succeeds")
	hookTrustApproved := allTrusted || strings.Contains(opts.Command, "--dangerously-bypass-hook-trust")
	hookTrustDetail := "trust status must be trusted or managed for generic lifecycle hook proof"
	if !allTrusted && hookTrustApproved {
		hookTrustDetail = "project hooks list as untrusted, but this appserver invocation used --dangerously-bypass-hook-trust as explicit operator approval"
	}
	addR1Assertion(report, "cluster preflight project hooks approved", hookTrustApproved, hookTrustDetail)
}

func addR1ClusterAuditAssertions(report *r1CodexAcceptanceReport, syncReport *r1CodexSyncReport, actorCounts map[string]map[string]int, finalAnswer string, wakeCycles int) {
	entrypoint := report.Entrypoint
	workerProgress := r1ClusterWorkerProgressActors(actorCounts, entrypoint)
	nonProfileParticipants := r1ClusterNonProfileParticipantCount(actorCounts)
	addR1Assertion(report, "cluster exactly one business task prompt before worker wakes", report.RunnerContract != nil && report.RunnerContract.BusinessTaskPrompts == 1 && r1ClusterBusinessBeforeWake(report.RunnerContract), fmt.Sprintf("business=%d wake=%d", report.RunnerContract.BusinessTaskPrompts, report.RunnerContract.WorkerWakePrompts))
	addR1Assertion(report, "cluster non-entrypoint prompts are generic wakes only", report.RunnerContract != nil && report.RunnerContract.DirectWorkerBusinessPrompts == 0 && r1ClusterWorkerPromptsGeneric(report.RunnerContract), fmt.Sprintf("worker_wake_prompts=%d direct_worker_business=%d", report.RunnerContract.WorkerWakePrompts, report.RunnerContract.DirectWorkerBusinessPrompts))
	addR1Assertion(report, "cluster runner wakes every non-entrypoint through generic prompt", report.RunnerContract != nil && r1ClusterWokeAllNonEntrypoints(report.RunnerContract, report.Agents, entrypoint), fmt.Sprintf("worker_wake_prompts=%d agents=%d entrypoint=%s", report.RunnerContract.WorkerWakePrompts, len(report.Agents), entrypoint))
	addR1Assertion(report, "cluster at least three hostagents participate through accepted events", nonProfileParticipants >= 3, fmt.Sprintf("participants=%d actor_counts=%v", nonProfileParticipants, actorCounts))
	addR1Assertion(report, "cluster entrypoint emits coordination events", actorCounts[entrypoint]["teamwork_signal"] >= 1 && actorCounts[entrypoint]["assignment"] >= 2, fmt.Sprintf("entrypoint=%s counts=%v", entrypoint, actorCounts[entrypoint]))
	addR1Assertion(report, "cluster entrypoint records project intent or integration progress", actorCounts[entrypoint]["project_intent"] >= 1 || actorCounts[entrypoint]["progress_digest"] >= 1, fmt.Sprintf("entrypoint=%s counts=%v", entrypoint, actorCounts[entrypoint]))
	addR1Assertion(report, "cluster entrypoint records final integration progress", report.RunnerContract.EntrypointProgressBeforeIntegration >= 0 && report.RunnerContract.EntrypointProgressAfterIntegration > report.RunnerContract.EntrypointProgressBeforeIntegration, fmt.Sprintf("entrypoint_progress_before=%d after=%d", report.RunnerContract.EntrypointProgressBeforeIntegration, report.RunnerContract.EntrypointProgressAfterIntegration))
	addR1Assertion(report, "cluster workers act because of Mnemon context", len(workerProgress) >= 2 && report.RunnerContract.DirectWorkerBusinessPrompts == 0, fmt.Sprintf("worker_progress_actors=%v", workerProgress))
	addR1Assertion(report, "cluster at least two non-entrypoint progress_digest actors", len(workerProgress) >= 2, fmt.Sprintf("worker_progress_actors=%v", workerProgress))
	addR1Assertion(report, "cluster entrypoint reads worker progress and returns integrated answer", report.LedgerCounts["progress_digest"] >= 2 && r1ClusterFinalAnswerCitesEvidence(finalAnswer), fmt.Sprintf("progress_digest=%d final=%s", report.LedgerCounts["progress_digest"], truncateR1Cluster(finalAnswer, 400)))
	addR1Assertion(report, "cluster mnemonhub moves accepted event subjects only", r1SyncEventSubjectsOnlyAccepted(syncReport.AllowedEventSubjects) && syncReport.HubStatus.HubEventsReceived > 0 && syncReport.HubStatus.HubEventsServed > 0, fmt.Sprintf("event_subjects=%v received=%d served=%d", syncReport.AllowedEventSubjects, syncReport.HubStatus.HubEventsReceived, syncReport.HubStatus.HubEventsServed))
	addR1Assertion(report, "cluster no assignment_status/assignment_expired invented", report.LedgerCounts["assignment_status"] == 0 && report.LedgerCounts["assignment_expired"] == 0, fmt.Sprintf("assignment_status=%d assignment_expired=%d", report.LedgerCounts["assignment_status"], report.LedgerCounts["assignment_expired"]))
	addR1Assertion(report, "cluster no manual event repair", report.RunnerContract != nil && report.RunnerContract.ManualEventWrites == 0, fmt.Sprintf("manual_event_writes=%d", report.RunnerContract.ManualEventWrites))
	addR1Assertion(report, "cluster wake cycles bounded by runner contract", wakeCycles > 0 && report.RunnerContract.WorkerWakePrompts <= wakeCycles*(len(report.Agents)-1), fmt.Sprintf("wake_prompts=%d wake_cycles=%d agents=%d", report.RunnerContract.WorkerWakePrompts, wakeCycles, len(report.Agents)))
}

func r1ClusterBusinessTaskPrompt(scenario string) string {
	extra := ""
	if scenario == "seeded-defect" {
		extra = "\nThis run may include a seeded repository defect. Treat it as ordinary project state: discover it from inspection or tests, then coordinate any fix or review through Mnemon."
	}
	return `You are the entrypoint for a Mnemon agent cluster.

Using Mnemon as the only cluster coordination channel, organize the cluster to
test this repository in a realistic way:

1. Establish the validation objective.
2. Inspect the current project state in this workspace.
3. Delegate at least two distinct validation or review tasks to other agents.
4. Have workers report findings through Mnemon.
5. Identify at least one concrete issue, risk, or gap if one exists.
6. Analyze the issue and either fix it, propose a minimal fix, or explain why no
   code change is justified.
7. Ask for independent review when useful.
8. Integrate the final result and answer the user with evidence.

Read available agent profiles through your own Local Mnemon before assigning
work. Choose assignees yourself from Mnemon context. Use the standard governed
events project_intent, teamwork_signal, assignment, and progress_digest as the
durable coordination channel, following the managed GUIDE and observe skill.
Do not message workers directly. Do not wait for worker output in this turn:
create the coordination graph, then answer briefly with the events you recorded.` + extra
}

func r1ClusterIntegrationPrompt() string {
	return `Read your Mnemon context through your own Local Mnemon and integrate the cluster work for the user.
Use only Mnemon events as worker evidence. Do not use runner wake answers as evidence.
If the cluster result is ready, record a final progress_digest through your own Local Mnemon.
Answer with participants, event-backed evidence, concrete issue/risk/gap or no-defect rationale, fix/proposed fix/no-code-change decision, and remaining risk.`
}

func chooseR1ClusterEntrypoint(agents []r1CodexSyncAgent, explicit string, seed int64) (int, error) {
	if len(agents) == 0 {
		return -1, fmt.Errorf("no agents available")
	}
	explicit = strings.TrimSpace(explicit)
	if explicit != "" {
		for i := range agents {
			if agents[i].principal == explicit {
				return i, nil
			}
		}
		return -1, fmt.Errorf("entrypoint %q is not one of the appservers", explicit)
	}
	rng := rand.New(rand.NewSource(seed))
	return rng.Intn(len(agents)), nil
}

func recordR1ClusterPrompt(contract *r1RunnerContractReport, principal, kind, prompt string) {
	if contract == nil {
		return
	}
	contract.PromptAudit = append(contract.PromptAudit, r1RunnerPromptReport{
		Index:     len(contract.PromptAudit) + 1,
		Principal: principal,
		Kind:      kind,
		Prompt:    truncateR1Cluster(prompt, 2000),
	})
}

func r1ClusterBusinessBeforeWake(contract *r1RunnerContractReport) bool {
	if contract == nil {
		return false
	}
	businessSeen := 0
	for _, prompt := range contract.PromptAudit {
		switch prompt.Kind {
		case "business_task":
			businessSeen++
		case "worker_wake":
			return businessSeen == 1
		}
	}
	return businessSeen == 1
}

func r1ClusterWorkerPromptsGeneric(contract *r1RunnerContractReport) bool {
	if contract == nil {
		return false
	}
	for _, prompt := range contract.PromptAudit {
		if prompt.Kind == "worker_wake" && prompt.Prompt != r1ClusterWorkerWakePrompt {
			return false
		}
	}
	return true
}

func r1ClusterWokeAllNonEntrypoints(contract *r1RunnerContractReport, agents []r1CodexAgentReport, entrypoint string) bool {
	if contract == nil {
		return false
	}
	woke := map[string]bool{}
	for _, prompt := range contract.PromptAudit {
		if prompt.Kind == "worker_wake" {
			woke[prompt.Principal] = true
		}
	}
	workers := 0
	for _, agent := range agents {
		if agent.Principal == entrypoint {
			continue
		}
		workers++
		if !woke[agent.Principal] {
			return false
		}
	}
	return workers > 0
}

func waitR1ClusterAcceptedEventSettle(runRoot string, timeout, stableFor time.Duration) {
	deadline := time.Now().Add(timeout)
	lastCount := -1
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		obs, err := observeAcceptanceRun(runRoot, 1000)
		if err == nil {
			count := r1ClusterAcceptedEventCount(obs)
			if count != lastCount {
				lastCount = count
				stableSince = time.Now()
			} else if time.Since(stableSince) >= stableFor {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func r1ClusterAcceptedEventCount(obs acceptanceObserveReport) int {
	count := 0
	for _, ev := range obs.CrossEvents {
		if ev.Status == "accepted" {
			count++
		}
	}
	return count
}

func r1ClusterActorEventCounts(obs acceptanceObserveReport) map[string]map[string]int {
	out := map[string]map[string]int{}
	for _, ev := range obs.CrossEvents {
		if ev.Status != "accepted" || ev.Actor == "" {
			continue
		}
		kind := r1ClusterKindFromEventSubject(ev.EventSubject)
		if kind == "" {
			continue
		}
		if out[ev.Actor] == nil {
			out[ev.Actor] = map[string]int{}
		}
		out[ev.Actor][kind]++
	}
	return out
}

func r1ClusterKindFromEventSubject(subject string) string {
	if idx := strings.Index(subject, "/"); idx > 0 {
		return subject[:idx]
	}
	if idx := strings.Index(subject, ":"); idx > 0 {
		return subject[:idx]
	}
	return ""
}

func r1ClusterProgressReady(actorCounts map[string]map[string]int, entrypoint string) bool {
	return actorCounts[entrypoint]["teamwork_signal"] >= 1 &&
		actorCounts[entrypoint]["assignment"] >= 2 &&
		len(r1ClusterWorkerProgressActors(actorCounts, entrypoint)) >= 2
}

func r1ClusterWorkerProgressActors(actorCounts map[string]map[string]int, entrypoint string) []string {
	var out []string
	for actor, counts := range actorCounts {
		if actor == entrypoint {
			continue
		}
		if counts["progress_digest"] > 0 {
			out = append(out, actor)
		}
	}
	sort.Strings(out)
	return out
}

func r1ClusterNonProfileParticipantCount(actorCounts map[string]map[string]int) int {
	count := 0
	for _, kinds := range actorCounts {
		for kind, n := range kinds {
			if kind != "agent_profile" && n > 0 {
				count++
				break
			}
		}
	}
	return count
}

func r1ClusterParticipants(actorCounts map[string]map[string]int, entrypoint string) []r1ClusterParticipantReport {
	var principals []string
	for principal := range actorCounts {
		principals = append(principals, principal)
	}
	sort.Strings(principals)
	out := make([]r1ClusterParticipantReport, 0, len(principals))
	for _, principal := range principals {
		counts := actorCounts[principal]
		var roles []string
		if principal == entrypoint {
			roles = append(roles, "entrypoint")
		}
		if counts["project_intent"] > 0 || counts["teamwork_signal"] > 0 || counts["assignment"] > 0 {
			roles = append(roles, "coordinator")
		}
		if counts["progress_digest"] > 0 && principal != entrypoint {
			roles = append(roles, "worker")
		}
		if counts["agent_profile"] > 0 {
			roles = append(roles, "profiled")
		}
		sort.Strings(roles)
		out = append(out, r1ClusterParticipantReport{Principal: principal, Roles: roles, EventCounts: counts})
	}
	return out
}

func r1ClusterParticipantPrincipals(participants []r1ClusterParticipantReport) []string {
	out := make([]string, 0, len(participants))
	for _, p := range participants {
		out = append(out, p.Principal)
	}
	sort.Strings(out)
	return out
}

func latestR1ClusterAgentAnswer(report *r1CodexSyncReport, principal string) string {
	if report == nil {
		return ""
	}
	for _, agent := range report.Agents {
		if agent.Principal != principal || len(agent.FinalAnswers) == 0 {
			continue
		}
		return agent.FinalAnswers[len(agent.FinalAnswers)-1]
	}
	return ""
}

func r1ClusterFinalAnswerCitesEvidence(answer string) bool {
	lower := strings.ToLower(answer)
	if strings.TrimSpace(lower) == "" {
		return false
	}
	for _, needle := range []string{"event", "mnemon", "assignment", "progress", "evidence", "agent"} {
		if strings.Contains(lower, needle) {
			return true
		}
	}
	return false
}

func r1ClusterFindingFromAnswer(answer string, counts map[string]int) r1ClusterFindingReport {
	kind := "unknown"
	lower := strings.ToLower(answer)
	switch {
	case strings.Contains(lower, "no defect") || strings.Contains(lower, "no concrete defect") || strings.Contains(lower, "no code change"):
		kind = "no-defect"
	case strings.Contains(lower, "defect") || strings.Contains(lower, "bug") || strings.Contains(lower, "issue"):
		kind = "issue"
	case strings.Contains(lower, "risk") || strings.Contains(lower, "gap"):
		kind = "risk"
	}
	resolved := strings.Contains(lower, "fixed") ||
		strings.Contains(lower, "resolved") ||
		strings.Contains(lower, "no code change") ||
		strings.Contains(lower, "applied the reviewed minimal fix") ||
		strings.Contains(lower, "applied the minimal fix") ||
		strings.Contains(lower, "applied fix")
	return r1ClusterFindingReport{
		Kind:     kind,
		Summary:  truncateR1Cluster(strings.TrimSpace(answer), 800),
		Evidence: fmt.Sprintf("ledger_counts=%v", counts),
		Resolved: resolved,
	}
}

func copyR1ClusterProjectSnapshot(sourceRoot, workspace, runRoot string) error {
	sourceRoot, err := filepath.Abs(sourceRoot)
	if err != nil {
		return err
	}
	workspace, err = filepath.Abs(workspace)
	if err != nil {
		return err
	}
	runRoot, _ = filepath.Abs(runRoot)
	return filepath.WalkDir(sourceRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(sourceRoot, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if pathWithin(runRoot, path) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		parts := splitPath(rel)
		if len(parts) == 0 {
			return nil
		}
		if d.IsDir() && r1ClusterSkipSnapshotDir(parts[0], d.Name()) {
			return filepath.SkipDir
		}
		if d.Type()&os.ModeSymlink != 0 {
			return nil
		}
		dst := filepath.Join(workspace, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		if !d.Type().IsRegular() || r1ClusterSkipSnapshotFile(rel, d.Name()) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return copyRegularFile(path, dst, info.Mode().Perm())
	})
}

func pathWithin(root, path string) bool {
	if root == "" {
		return false
	}
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func r1ClusterSkipSnapshotDir(first, name string) bool {
	switch first {
	case ".git", ".testdata", ".mnemon-dev", ".mnemon", ".codex", ".claude", ".openclaw", "node_modules":
		return true
	}
	switch name {
	case ".git", ".mnemon", ".codex", ".claude", ".openclaw", "node_modules":
		return true
	}
	return false
}

func r1ClusterSkipSnapshotFile(rel, name string) bool {
	if strings.HasPrefix(rel, ".") {
		switch name {
		case ".DS_Store":
			return true
		}
	}
	switch name {
	case "mnemon", "mnemon-harness", "coverage.out":
		return true
	}
	return strings.HasSuffix(name, ".test") || strings.HasSuffix(name, ".tmp")
}

func applyR1ClusterSeededDefect(agents []r1CodexSyncAgent) (string, error) {
	rel := "phase1_seeded_defect_test.go"
	body := `package main

import "testing"

func TestPhase1SeededRepositoryValidation(t *testing.T) {
	t.Fatalf("seeded phase-1 validation defect: replace this failing fixture with a passing assertion")
}
`
	for _, agent := range agents {
		if err := os.WriteFile(filepath.Join(agent.workspace, rel), []byte(body), 0o644); err != nil {
			return rel, fmt.Errorf("%s: write seeded defect: %w", agent.principal, err)
		}
	}
	return rel, nil
}

func truncateR1Cluster(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	if n <= 15 {
		return s[:n]
	}
	return s[:n] + "...(truncated)"
}
