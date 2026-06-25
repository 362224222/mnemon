package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var acceptanceTaskSimScenarios []string

var acceptanceR1TaskSimCmd = &cobra.Command{
	Use:   "r1-task-sim",
	Short: "Run R1 simulated real-task acceptance with real Codex appservers",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runR1TaskSimAcceptance(cmd.Context(), r1TaskSimAcceptanceOptions{
			r1CodexAcceptanceOptions: r1CodexAcceptanceOptions{
				RunRoot:     acceptanceRunRoot,
				Command:     acceptanceCommand,
				CodexHome:   acceptanceCodexHome,
				Agents:      acceptanceAgents,
				AgentTurns:  acceptanceAgentTurns,
				SyncArm:     acceptanceSyncArm,
				TurnTimeout: acceptanceTurnTimeout,
				Stdout:      cmd.OutOrStdout(),
				Stderr:      cmd.ErrOrStderr(),
			},
			Scenarios: acceptanceTaskSimScenarios,
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("R1 task simulation acceptance status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceR1TaskSimCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceR1TaskSimCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceR1TaskSimCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceR1TaskSimCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of Codex appservers")
	acceptanceR1TaskSimCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real model turns that write governed R1 task events")
	acceptanceR1TaskSimCmd.Flags().BoolVar(&acceptanceSyncArm, "sync-arm", false, "run the cross-workspace sync/import scenario")
	acceptanceR1TaskSimCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per real agent turn")
	acceptanceR1TaskSimCmd.Flags().StringArrayVar(&acceptanceTaskSimScenarios, "scenario", nil, "scenario to run; repeatable")
	acceptanceCmd.AddCommand(acceptanceR1TaskSimCmd)
}

type r1TaskSimAcceptanceOptions struct {
	r1CodexAcceptanceOptions
	Scenarios []string
}

type r1TaskSimScenarioReport struct {
	Name     string         `json:"name"`
	Status   string         `json:"status"`
	Actors   []string       `json:"actors,omitempty"`
	Evidence map[string]any `json:"evidence,omitempty"`
}

type taskSimRun struct {
	ctx    context.Context
	opts   r1TaskSimAcceptanceOptions
	report *r1CodexAcceptanceReport
	agents []r1CodexAgent
	runID  string
}

func runR1TaskSimAcceptance(ctx context.Context, opts r1TaskSimAcceptanceOptions) (r1CodexAcceptanceReport, error) {
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
	started := time.Now().UTC().Truncate(time.Second)
	runRoot := opts.RunRoot
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "r1-task-sim", started.Format("20060102T150405Z"))
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
	report.Artifacts["local_workspace"] = localWorkspace
	report.Artifacts["render_audit"] = filepath.Join(localWorkspace, ".mnemon", "harness", "local", "render-audit.jsonl")
	agents, loaded, err := setupR1CodexAgents(runRoot, binDir, report.LocalAddr, opts.Agents, sourceCodexHome)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
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
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
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
	addR1Assertion(&report, "task-sim 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))
	if !opts.AgentTurns {
		addR1Assertion(&report, "task-sim real agent turns requested", false, "rerun with --agent-turns")
		report.Status = "failed"
		return report, fmt.Errorf("R1 task simulation requires --agent-turns")
	}
	runID := started.Format("150405")
	sim := taskSimRun{ctx: ctx, opts: opts, report: &report, agents: agents, runID: runID}
	for _, name := range taskSimScenarioNames(opts.Scenarios) {
		if err := sim.runScenario(name); err != nil {
			addR1Error(&report, err)
		}
	}
	if taskSimHasScenario(opts.Scenarios, "cross-workspace-integration") {
		for i := range agents {
			agents[i].server.Close()
		}
		if opts.SyncArm {
			if err := runR1CodexSyncScenario(ctx, opts.r1CodexAcceptanceOptions, runRoot, binDir, sourceCodexHome, &report); err != nil {
				addR1Error(&report, err)
				report.Scenarios = append(report.Scenarios, r1TaskSimScenarioReport{
					Name:   "cross-workspace-integration",
					Status: "failed",
					Evidence: map[string]any{
						"error": err.Error(),
					},
				})
			} else {
				report.Scenarios = append(report.Scenarios, r1TaskSimScenarioReport{
					Name:   "cross-workspace-integration",
					Status: "ok",
					Actors: []string{report.Sync.Source, report.Sync.Target},
					Evidence: map[string]any{
						"hub_events_received": report.Sync.HubStatus.HubEventsReceived,
						"hub_events_served":   report.Sync.HubStatus.HubEventsServed,
						"target_assignment":   report.Sync.TargetLedger["assignment"],
						"source_progress":     report.Sync.SourceLedger["progress_digest"],
					},
				})
			}
		} else {
			addR1Assertion(&report, "task-sim cross-workspace sync arm requested", false, "rerun with --sync-arm")
			report.Scenarios = append(report.Scenarios, r1TaskSimScenarioReport{Name: "cross-workspace-integration", Status: "blocked"})
		}
	}
	report.LedgerCounts = countR1Ledger(report.LocalAddr, agents[0])
	report.DerivedEventAudit = countR1DerivedEventAudit(report.Artifacts["render_audit"])
	addR1Assertion(&report, "task-sim no assignment_status/assignment_expired", report.LedgerCounts["assignment_status"] == 0 && report.LedgerCounts["assignment_expired"] == 0, fmt.Sprintf("assignment_status=%d assignment_expired=%d", report.LedgerCounts["assignment_status"], report.LedgerCounts["assignment_expired"]))
	addR1Assertion(&report, "task-sim derived event audit has provenance", report.DerivedEventAudit["with_provenance"] > 0 && report.DerivedEventAudit["with_body_digest"] > 0 && report.DerivedEventAudit["with_audit_id"] > 0, fmt.Sprintf("%+v", report.DerivedEventAudit))
	if allR1AssertionsPassed(report.Assertions) && len(report.Errors) == 0 && allTaskSimScenariosOK(report.Scenarios, opts.Scenarios) {
		report.Status = "ok"
		return report, nil
	}
	report.Status = "failed"
	return report, fmt.Errorf("R1 task simulation acceptance failed")
}

func taskSimScenarioNames(selected []string) []string {
	if len(selected) == 0 {
		return []string{"bugfix-review", "split-feature", "failing-test-repair", "conflict-rework"}
	}
	out := make([]string, 0, len(selected))
	for _, name := range selected {
		if name != "cross-workspace-integration" {
			out = append(out, name)
		}
	}
	return out
}

func taskSimHasScenario(selected []string, name string) bool {
	if len(selected) == 0 {
		return true
	}
	for _, item := range selected {
		if item == name {
			return true
		}
	}
	return false
}

func allTaskSimScenariosOK(scenarios []r1TaskSimScenarioReport, selected []string) bool {
	want := map[string]bool{}
	for _, name := range append(taskSimScenarioNames(selected), "cross-workspace-integration") {
		if name == "cross-workspace-integration" && !taskSimHasScenario(selected, name) {
			continue
		}
		want[name] = false
	}
	for _, scenario := range scenarios {
		if scenario.Status == "ok" {
			want[scenario.Name] = true
		}
	}
	for _, ok := range want {
		if !ok {
			return false
		}
	}
	return true
}

func (s taskSimRun) runScenario(name string) error {
	switch name {
	case "bugfix-review":
		return s.runBugfixReview()
	case "split-feature":
		return s.runSplitFeature()
	case "failing-test-repair":
		return s.runFailingTestRepair()
	case "conflict-rework":
		return s.runConflictRework()
	default:
		addR1Assertion(s.report, "task-sim known scenario "+name, false, "unknown scenario")
		s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{Name: name, Status: "blocked"})
		return fmt.Errorf("unknown task simulation scenario %q", name)
	}
}

func (s taskSimRun) runBugfixReview() error {
	starter, impl, reviewer := s.agents[0], s.agents[1], s.agents[2]
	fixID := "sim-bugfix-" + s.runID
	reviewID := "sim-review-" + s.runID
	if err := s.emitAssignment(&starter, fixID, impl.principal, "task-sim/bugfix-review", "Fix the simulated failing parser edge case and report exact evidence.", "progress_digest with fix evidence"); err != nil {
		return err
	}
	if err := s.waitAndAct(&impl, fixID, "progress-"+fixID, "Implemented simulated parser bugfix after reproducing failing case.", "bugfix artifact: parser_test.go::TestEdgeCase now passes"); err != nil {
		return err
	}
	if err := s.emitAssignment(&starter, reviewID, reviewer.principal, "task-sim/bugfix-review", "Review the simulated bugfix evidence and report acceptance or blockers.", "progress_digest with review evidence"); err != nil {
		return err
	}
	if err := s.waitAndAct(&reviewer, reviewID, "progress-"+reviewID, "Reviewed simulated bugfix and accepted the evidence chain.", "review artifact: diff and test evidence referenced by prior progress"); err != nil {
		return err
	}
	counts := countR1Ledger(s.report.LocalAddr, starter)
	passed := counts["assignment"] >= 2 && counts["progress_digest"] >= 2
	addR1Assertion(s.report, "task-sim bugfix-review passes", passed, fmt.Sprintf("assignment=%d progress_digest=%d", counts["assignment"], counts["progress_digest"]))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "bugfix-review",
		Status: statusFromBool(passed),
		Actors: []string{starter.principal, impl.principal, reviewer.principal},
		Evidence: map[string]any{
			"fix_assignment":    fixID,
			"review_assignment": reviewID,
			"assignment":        counts["assignment"],
			"progress_digest":   counts["progress_digest"],
		},
	})
	if !passed {
		return fmt.Errorf("bugfix-review did not produce expected event chain")
	}
	return nil
}

func (s taskSimRun) runSplitFeature() error {
	starter, a, b := s.agents[0], s.agents[3], s.agents[4]
	apiID := "sim-split-api-" + s.runID
	uiID := "sim-split-ui-" + s.runID
	if err := s.emitTeamworkSignal(&starter, "sim-signal-split-"+s.runID, "task-sim/split-feature", "Split a medium feature into API and presentation work."); err != nil {
		return err
	}
	if err := s.emitAssignment(&starter, apiID, a.principal, "task-sim/split-feature/api", "Implement the simulated API half and report evidence.", "progress_digest with API evidence"); err != nil {
		return err
	}
	if err := s.emitAssignment(&starter, uiID, b.principal, "task-sim/split-feature/presentation", "Implement the simulated presentation half and report evidence.", "progress_digest with presentation evidence"); err != nil {
		return err
	}
	if err := s.waitAndAct(&a, apiID, "progress-"+apiID, "Completed simulated API half of split feature.", "artifact: API contract checklist"); err != nil {
		return err
	}
	if err := s.waitAndAct(&b, uiID, "progress-"+uiID, "Completed simulated presentation half of split feature.", "artifact: presentation checklist"); err != nil {
		return err
	}
	counts := countR1Ledger(s.report.LocalAddr, starter)
	passed := counts["teamwork_signal"] >= 1 && counts["assignment"] >= 4 && counts["progress_digest"] >= 4
	addR1Assertion(s.report, "task-sim split-feature passes", passed, fmt.Sprintf("teamwork_signal=%d assignment=%d progress_digest=%d", counts["teamwork_signal"], counts["assignment"], counts["progress_digest"]))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "split-feature",
		Status: statusFromBool(passed),
		Actors: []string{starter.principal, a.principal, b.principal},
		Evidence: map[string]any{
			"api_assignment":  apiID,
			"ui_assignment":   uiID,
			"teamwork_signal": counts["teamwork_signal"],
			"assignment":      counts["assignment"],
			"progress_digest": counts["progress_digest"],
		},
	})
	if !passed {
		return fmt.Errorf("split-feature did not produce expected event chain")
	}
	return nil
}

func (s taskSimRun) runFailingTestRepair() error {
	starter, assignee := s.agents[1], s.agents[2]
	assignmentID := "sim-repair-" + s.runID
	if err := s.emitAssignment(&starter, assignmentID, assignee.principal, "task-sim/failing-test-repair", "Observe a simulated failing test, repair it, and report evidence.", "progress_digest with failing test and repair evidence"); err != nil {
		return err
	}
	if err := s.waitAndAct(&assignee, assignmentID, "progress-"+assignmentID, "Observed failing test TestTaskRepair, repaired the simulated defect, and reran it.", "artifact: TestTaskRepair failed before and passed after repair"); err != nil {
		return err
	}
	counts := countR1Ledger(s.report.LocalAddr, starter)
	passed := counts["assignment"] >= 5 && counts["progress_digest"] >= 5
	addR1Assertion(s.report, "task-sim failing-test-repair passes", passed, fmt.Sprintf("assignment=%d progress_digest=%d", counts["assignment"], counts["progress_digest"]))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "failing-test-repair",
		Status: statusFromBool(passed),
		Actors: []string{starter.principal, assignee.principal},
		Evidence: map[string]any{
			"assignment":      assignmentID,
			"ledger_assign":   counts["assignment"],
			"progress_digest": counts["progress_digest"],
		},
	})
	if !passed {
		return fmt.Errorf("failing-test-repair did not produce expected event chain")
	}
	return nil
}

func (s taskSimRun) runConflictRework() error {
	starter, left, right, resolver := s.agents[0], s.agents[1], s.agents[2], s.agents[3]
	leftID := "sim-conflict-left-" + s.runID
	rightID := "sim-conflict-right-" + s.runID
	reworkID := "sim-conflict-rework-" + s.runID
	if err := s.emitAssignment(&starter, leftID, left.principal, "task-sim/conflict-rework/shared", "Make simulated change A to the shared component and report assumptions.", "progress_digest with change A evidence"); err != nil {
		return err
	}
	if err := s.emitAssignment(&starter, rightID, right.principal, "task-sim/conflict-rework/shared", "Make simulated change B to the shared component and report assumptions.", "progress_digest with change B evidence"); err != nil {
		return err
	}
	if err := s.waitAndAct(&left, leftID, "progress-"+leftID, "Completed simulated shared-component change A; notes overlap risk.", "artifact: change A touches shared event presentation path"); err != nil {
		return err
	}
	if err := s.waitAndAct(&right, rightID, "progress-"+rightID, "Completed simulated shared-component change B; notes overlap risk.", "artifact: change B touches same event presentation path"); err != nil {
		return err
	}
	if err := s.emitAssignment(&starter, reworkID, resolver.principal, "task-sim/conflict-rework/resolve", "Resolve the two overlapping simulated changes and report final integration evidence.", "progress_digest with conflict resolution evidence"); err != nil {
		return err
	}
	if err := s.waitAndAct(&resolver, reworkID, "progress-"+reworkID, "Resolved simulated overlap by integrating both changes into one event-flow outcome.", "artifact: conflict resolution notes reference change A and change B"); err != nil {
		return err
	}
	counts := countR1Ledger(s.report.LocalAddr, starter)
	passed := counts["assignment"] >= 8 && counts["progress_digest"] >= 8
	addR1Assertion(s.report, "task-sim conflict-rework passes", passed, fmt.Sprintf("assignment=%d progress_digest=%d", counts["assignment"], counts["progress_digest"]))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "conflict-rework",
		Status: statusFromBool(passed),
		Actors: []string{starter.principal, left.principal, right.principal, resolver.principal},
		Evidence: map[string]any{
			"left_assignment":  leftID,
			"right_assignment": rightID,
			"rework":           reworkID,
			"assignment":       counts["assignment"],
			"progress_digest":  counts["progress_digest"],
		},
	})
	if !passed {
		return fmt.Errorf("conflict-rework did not produce expected event chain")
	}
	return nil
}

func (s taskSimRun) emitTeamworkSignal(agent *r1CodexAgent, signalID, scope, statement string) error {
	payload := taskSimJSON(map[string]any{
		"signal_id":    signalID,
		"scope":        scope,
		"statement":    statement,
		"why_teamwork": "task simulation requires multiple hostagents to coordinate through events",
		"ttl":          "30m",
		"evidence":     "r1-task-sim",
	})
	prompt := fmt.Sprintf(`Emit teamwork_signal.write_candidate.observed for the task simulation.
Use external id signal-%s and payload:
%s
After the command succeeds, answer "signal %s written".`, signalID, payload, signalID)
	answer, err := runR1Turn(agent, prompt, s.opts.TurnTimeout)
	appendAgentAnswer(s.report, agent.principal, answer)
	if err != nil {
		return fmt.Errorf("%s signal %s: %w", agent.principal, signalID, err)
	}
	waitForLedgerCount(s.report.LocalAddr, *agent, "teamwork_signal", 1, 10*time.Second)
	return nil
}

func (s taskSimRun) emitAssignment(agent *r1CodexAgent, assignmentID, assignee, scope, expectedWork, expectedFeedback string) error {
	payload := taskSimJSON(map[string]any{
		"assignment_id":     assignmentID,
		"assignee":          assignee,
		"scope":             scope,
		"expected_work":     expectedWork,
		"expected_feedback": expectedFeedback,
		"ttl":               "20m",
		"evidence":          "r1-task-sim",
	})
	prompt := fmt.Sprintf(`Emit assignment.write_candidate.observed for the task simulation.
Use external id assignment-%s and payload:
%s
After the command succeeds, answer "assignment %s written".`, assignmentID, payload, assignmentID)
	answer, err := runR1Turn(agent, prompt, s.opts.TurnTimeout)
	appendAgentAnswer(s.report, agent.principal, answer)
	if err != nil {
		return fmt.Errorf("%s assignment %s: %w", agent.principal, assignmentID, err)
	}
	waitForLedgerCount(s.report.LocalAddr, *agent, "assignment", 1, 10*time.Second)
	return nil
}

func (s taskSimRun) waitAndAct(agent *r1CodexAgent, assignmentID, externalID, summary, evidence string) error {
	presentation, ok := waitForR1DerivedEventPresentation(s.report.LocalAddr, agent.token, []string{"[mnemon:work]", assignmentID}, 60*time.Second)
	addR1Assertion(s.report, "task-sim "+assignmentID+" reaches assignee derived event", ok, presentation.Body)
	if !ok {
		return fmt.Errorf("%s did not receive assignment %s as derived event", agent.principal, assignmentID)
	}
	payload := taskSimJSON(map[string]any{
		"assignment_ref":  assignmentID,
		"scope":           "task-sim",
		"summary":         summary,
		"evidence":        evidence,
		"changed_context": "simulated real task advanced through observed event",
		"suggested_next":  "starter should integrate or assign follow-up work",
	})
	prompt := fmt.Sprintf(`Act on assignment %s from your derived-event presentation.
Emit progress_digest.write_candidate.observed with external id %s and payload:
%s
After the command succeeds, answer "progress %s written".`, assignmentID, externalID, payload, assignmentID)
	answer, err := runR1Turn(agent, prompt, s.opts.TurnTimeout)
	appendAgentAnswer(s.report, agent.principal, answer)
	if err != nil {
		return fmt.Errorf("%s progress %s: %w", agent.principal, assignmentID, err)
	}
	waitForLedgerCount(s.report.LocalAddr, *agent, "progress_digest", 1, 10*time.Second)
	return nil
}

func taskSimJSON(v map[string]any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func statusFromBool(ok bool) string {
	if ok {
		return "ok"
	}
	return "failed"
}
