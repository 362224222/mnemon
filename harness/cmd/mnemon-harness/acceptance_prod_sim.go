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

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
	"github.com/spf13/cobra"
)

var acceptanceR1ProdSimCmd = &cobra.Command{
	Use:   "r1-prod-sim",
	Short: "Run production-like R1 acceptance with per-hostagent mnemond instances",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runR1ProdSimAcceptance(cmd.Context(), r1ProdSimAcceptanceOptions{
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
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("R1 production-like simulation acceptance status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceR1ProdSimCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceR1ProdSimCmd.Flags().StringVar(&acceptanceCommand, "command", "codex --dangerously-bypass-hook-trust", "Codex CLI command")
	acceptanceR1ProdSimCmd.Flags().StringVar(&acceptanceCodexHome, "codex-home-source", "", "source CODEX_HOME to copy auth/config from")
	acceptanceR1ProdSimCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of Codex appservers")
	acceptanceR1ProdSimCmd.Flags().BoolVar(&acceptanceAgentTurns, "agent-turns", false, "run real model turns that write governed R1 production-like events")
	acceptanceR1ProdSimCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per real agent turn")
	acceptanceCmd.AddCommand(acceptanceR1ProdSimCmd)
}

type r1ProdSimAcceptanceOptions struct {
	r1CodexAcceptanceOptions
}

type prodSimRun struct {
	ctx    context.Context
	opts   r1ProdSimAcceptanceOptions
	report *r1CodexAcceptanceReport
	agents []r1CodexSyncAgent
	runID  string
}

func runR1ProdSimAcceptance(ctx context.Context, opts r1ProdSimAcceptanceOptions) (r1CodexAcceptanceReport, error) {
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
		runRoot = filepath.Join(".testdata", "r1-prod-sim", started.Format("20060102T150405Z"))
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
	hub, err := startR1SyncHub(runRoot, opts.Agents)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	defer hub.close()
	sourceCodexHome := resolveSourceCodexHome(opts.CodexHome)
	report.Artifacts["codex_home_source"] = sourceCodexHome
	report.Artifacts["hub_db"] = filepath.Join(runRoot, "hub", "hub.db")
	report.Artifacts["hub_audit"] = hub.AuditPath

	agents, err := setupR1CodexSyncAgents(ctx, runRoot, binDir, hub, opts.Agents, sourceCodexHome)
	if err != nil {
		addR1Error(&report, err)
		report.Status = "blocked"
		return report, err
	}
	defer stopR1CodexSyncAgents(agents)
	report.Topology = buildR1ProdSimTopology(agents)
	addR1Assertion(&report, "prod-sim strict per-hostagent mnemond topology", prodSimStrictTopology(report.Topology), fmt.Sprintf("%+v", report.Topology))
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
	addR1Assertion(&report, "prod-sim 5/5 appservers start/init", len(report.Agents) == opts.Agents, fmt.Sprintf("started=%d requested=%d", len(report.Agents), opts.Agents))
	if !opts.AgentTurns {
		addR1Assertion(&report, "prod-sim real agent turns requested", false, "rerun with --agent-turns")
		report.Status = "failed"
		return report, fmt.Errorf("R1 production-like simulation requires --agent-turns")
	}

	run := prodSimRun{ctx: ctx, opts: opts, report: &report, agents: agents, runID: started.Format("150405")}
	if err := run.bootstrapProfiles(); err != nil {
		addR1Error(&report, err)
	}
	if err := run.runSplitWork(); err != nil {
		addR1Error(&report, err)
	}
	if err := run.runDependencyHandoff(); err != nil {
		addR1Error(&report, err)
	}
	if err := run.runBlockerRework(); err != nil {
		addR1Error(&report, err)
	}
	if err := run.runTTLPausedAgent(); err != nil {
		addR1Error(&report, err)
	}
	if err := run.runRestartNoDuplicateAction(); err != nil {
		addR1Error(&report, err)
	}

	client, err := access.NewSyncClient(hub.URL, access.SyncClientConfig{Token: hub.Tokens[0]})
	if err == nil {
		syncReport.HubStatus, err = client.SyncStatus()
	}
	if err != nil {
		addR1Assertion(&report, "prod-sim mnemonhub status readable", false, err.Error())
	} else {
		addR1Assertion(&report, "prod-sim mnemonhub exchanges accepted events", syncReport.HubStatus.HubEventsReceived > 0 && syncReport.HubStatus.HubEventsServed > 0, fmt.Sprintf("received=%d served=%d", syncReport.HubStatus.HubEventsReceived, syncReport.HubStatus.HubEventsServed))
	}
	if len(agents) > 0 {
		report.LedgerCounts = countR1Ledger(agents[0].localURL, agents[0].r1CodexAgent)
	}
	report.DerivedEventAudit = prodSimDerivedAudit(agents)
	if obs, err := observeAcceptanceRun(runRoot, 1000); err == nil {
		report.Observability = &obs
		addR1Assertion(&report, "prod-sim observability sees strict topology", obs.Topology.Mode == "per-hostagent-mnemond" && !obs.Topology.SharedMnemond, fmt.Sprintf("mode=%s shared=%t mnemond=%d hub=%d", obs.Topology.Mode, obs.Topology.SharedMnemond, obs.Topology.MnemondStores, obs.Topology.MnemonhubStores))
	} else {
		addR1Assertion(&report, "prod-sim observability sees strict topology", false, err.Error())
	}
	syncReport.Status = statusFromBool(len(report.Errors) == 0 && allR1AssertionsPassed(report.Assertions) && allProdSimScenariosOK(report.Scenarios))
	if syncReport.Status == "ok" {
		report.Status = "ok"
		return report, nil
	}
	report.Status = "failed"
	return report, fmt.Errorf("R1 production-like simulation acceptance failed")
}

func (s prodSimRun) bootstrapProfiles() error {
	for i := range s.agents {
		agent := &s.agents[i]
		payload := taskSimJSON(map[string]any{
			"rule": map[string]any{
				"actor":        agent.principal,
				"availability": "available",
				"ttl":          "30m",
			},
			"narrative": map[string]any{
				"focus":              fmt.Sprintf("production-like acceptance node %s", agent.principal),
				"context_advantages": []string{"isolated local mnemond", "sync/import visibility", "real Codex appserver turn"},
				"summary":            fmt.Sprintf("%s is available for production-like Mnemon teamwork validation.", agent.principal),
			},
		})
		prompt := fmt.Sprintf(`Emit exactly one agent_profile.write_candidate.observed event through your own Local Mnemon.
Use external id prod-profile-%s-%s and payload:
%s
After the command succeeds, answer "profile written".`, s.runID, prodSafeID(agent.principal), payload)
		answer, err := runR1Turn(&agent.r1CodexAgent, prompt, s.opts.TurnTimeout)
		appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
		if err != nil {
			addR1Assertion(s.report, "prod-sim profile emitted "+agent.principal, false, err.Error())
			return err
		}
		waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "agent_profile", 1, 20*time.Second)
		counts := countR1Ledger(agent.localURL, agent.r1CodexAgent)
		addR1Assertion(s.report, "prod-sim local profile accepted "+agent.principal, counts["agent_profile"] >= 1, fmt.Sprintf("agent_profile=%d", counts["agent_profile"]))
	}
	allVisible := true
	for i := range s.agents {
		agent := s.agents[i]
		waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "agent_profile", len(s.agents), 90*time.Second)
		counts := countR1Ledger(agent.localURL, agent.r1CodexAgent)
		if counts["agent_profile"] < len(s.agents) {
			allVisible = false
		}
	}
	addR1Assertion(s.report, "prod-sim profiles converge through mnemonhub", allVisible, fmt.Sprintf("agents=%d", len(s.agents)))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{Name: "bootstrap_profiles", Status: statusFromBool(allVisible)})
	if !allVisible {
		return fmt.Errorf("profiles did not converge through mnemonhub")
	}
	return nil
}

func (s prodSimRun) runSplitWork() error {
	starter, a, b, reviewer := &s.agents[2], &s.agents[0], &s.agents[1], &s.agents[3]
	s.report.Sync.Source = starter.principal
	sigID := "prod-signal-" + s.runID
	if err := s.emitTeamworkSignal(starter, sigID, "prod-sim/split-work", "Split a production-like validation task across multiple isolated mnemond nodes."); err != nil {
		return err
	}
	assignments := []struct {
		id       string
		agent    *r1CodexSyncAgent
		work     string
		evidence string
	}{
		{"prod-parser-" + s.runID, a, "Fix the simulated parser edge case and report file/test evidence.", "artifact: parser edge-case test passes"},
		{"prod-feature-" + s.runID, b, "Add the simulated feature behavior and report exact evidence.", "artifact: feature behavior checklist passes"},
		{"prod-review-" + s.runID, reviewer, "Review the simulated integration risk and report acceptance or blocker evidence.", "artifact: integration review checklist"},
	}
	for _, item := range assignments {
		if err := s.emitAssignment(starter, item.id, item.agent.principal, "prod-sim/split-work", item.work, "progress_digest with concrete evidence", "20m"); err != nil {
			return err
		}
	}
	for _, item := range assignments {
		if err := s.waitAndAct(item.agent, item.id, "progress-"+item.id, "Completed "+item.work, item.evidence); err != nil {
			return err
		}
		if _, ok := waitForR1DerivedEventPresentation(starter.localURL, starter.token, []string{"[mnemon:integrate]", item.id}, 90*time.Second); !ok {
			addR1Assertion(s.report, "prod-sim split-work integrate "+item.id, false, "starter did not receive integrate derived event")
			return fmt.Errorf("starter did not receive integrate for %s", item.id)
		}
	}
	counts := countR1Ledger(starter.localURL, starter.r1CodexAgent)
	passed := counts["teamwork_signal"] >= 1 && counts["assignment"] >= 3 && counts["progress_digest"] >= 3
	addR1Assertion(s.report, "prod-sim split work passes", passed, fmt.Sprintf("teamwork_signal=%d assignment=%d progress_digest=%d", counts["teamwork_signal"], counts["assignment"], counts["progress_digest"]))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "split_work",
		Status: statusFromBool(passed),
		Actors: []string{starter.principal, a.principal, b.principal, reviewer.principal},
		Evidence: map[string]any{
			"teamwork_signal": sigID,
			"assignment":      counts["assignment"],
			"progress_digest": counts["progress_digest"],
		},
	})
	if !passed {
		return fmt.Errorf("split work did not produce expected event chain")
	}
	return nil
}

func (s prodSimRun) runDependencyHandoff() error {
	starter, assignee := &s.agents[2], &s.agents[1]
	assignmentID := "prod-handoff-" + s.runID
	if err := s.emitAssignment(starter, assignmentID, assignee.principal, "prod-sim/dependency-handoff", "Use prior progress events to complete the dependent simulated integration step.", "progress_digest with dependent integration evidence", "20m"); err != nil {
		return err
	}
	if err := s.waitAndAct(assignee, assignmentID, "progress-"+assignmentID, "Completed dependent handoff after seeing the synced assignment.", "artifact: dependent integration step references previous progress"); err != nil {
		return err
	}
	presentation, ok := waitForR1DerivedEventPresentation(starter.localURL, starter.token, []string{"[mnemon:integrate]", assignmentID}, 90*time.Second)
	addR1Assertion(s.report, "prod-sim dependency handoff returns integrate", ok, presentation.Body)
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "dependency_handoff",
		Status: statusFromBool(ok),
		Actors: []string{starter.principal, assignee.principal},
		Evidence: map[string]any{
			"assignment": assignmentID,
		},
	})
	if !ok {
		return fmt.Errorf("dependency handoff did not return integrate event")
	}
	return nil
}

func (s prodSimRun) runBlockerRework() error {
	starter, blocked, repair := &s.agents[2], &s.agents[3], &s.agents[0]
	blockID := "prod-blocker-" + s.runID
	repairID := "prod-rework-" + s.runID
	if err := s.emitAssignment(starter, blockID, blocked.principal, "prod-sim/blocker", "Attempt the simulated risky change and report blocker evidence if it cannot pass.", "progress_digest with blocker evidence", "20m"); err != nil {
		return err
	}
	if err := s.waitAndAct(blocked, blockID, "progress-"+blockID, "Blocked on simulated risky change because the first validation still fails.", "artifact: failing validation output captured for rework"); err != nil {
		return err
	}
	if _, ok := waitForR1DerivedEventPresentation(starter.localURL, starter.token, []string{"[mnemon:integrate]", blockID}, 90*time.Second); !ok {
		addR1Assertion(s.report, "prod-sim blocker reaches starter", false, "starter did not see blocker progress")
		return fmt.Errorf("starter did not see blocker progress")
	}
	if err := s.emitAssignment(starter, repairID, repair.principal, "prod-sim/blocker-rework", "Repair the blocked validation with narrower scope and report passing evidence.", "progress_digest with repair evidence", "20m"); err != nil {
		return err
	}
	if err := s.waitAndAct(repair, repairID, "progress-"+repairID, "Repaired the blocked simulated validation with narrower scope.", "artifact: rework validation passes"); err != nil {
		return err
	}
	presentation, ok := waitForR1DerivedEventPresentation(starter.localURL, starter.token, []string{"[mnemon:integrate]", repairID}, 90*time.Second)
	addR1Assertion(s.report, "prod-sim blocker rework completes", ok, presentation.Body)
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "blocker_rework",
		Status: statusFromBool(ok),
		Actors: []string{starter.principal, blocked.principal, repair.principal},
		Evidence: map[string]any{
			"blocker_assignment": blockID,
			"repair_assignment":  repairID,
		},
	})
	if !ok {
		return fmt.Errorf("blocker rework did not complete")
	}
	return nil
}

func (s prodSimRun) runTTLPausedAgent() error {
	starter, paused := &s.agents[2], &s.agents[4]
	assignmentID := "prod-ttl-" + s.runID
	if err := s.emitAssignment(starter, assignmentID, paused.principal, "prod-sim/ttl-paused", "This assignment is intentionally left without progress to validate derived expiry.", "progress_digest only if completed", "1s"); err != nil {
		return err
	}
	time.Sleep(2 * time.Second)
	presentation, ok := waitForR1DerivedEventPresentation(starter.localURL, starter.token, []string{"[mnemon:expired]", assignmentID}, 30*time.Second)
	counts := countR1Ledger(starter.localURL, starter.r1CodexAgent)
	passed := ok && counts["assignment_status"] == 0 && counts["assignment_expired"] == 0
	addR1Assertion(s.report, "prod-sim TTL paused agent derives expiry only", passed, fmt.Sprintf("expired=%t assignment_status=%d assignment_expired=%d body=%s", ok, counts["assignment_status"], counts["assignment_expired"], presentation.Body))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "ttl_paused_agent",
		Status: statusFromBool(passed),
		Actors: []string{starter.principal, paused.principal},
		Evidence: map[string]any{
			"assignment": assignmentID,
		},
	})
	if !passed {
		return fmt.Errorf("TTL paused agent did not derive expiry correctly")
	}
	return nil
}

func (s prodSimRun) runRestartNoDuplicateAction() error {
	if len(s.agents) < 4 {
		return fmt.Errorf("restart scenario requires at least four agents")
	}
	before, err := observeAcceptanceRun(s.report.RunRoot, 10)
	if err != nil {
		addR1Assertion(s.report, "prod-sim restart inspect before", false, err.Error())
		return err
	}
	beforeAccepted := prodSimAcceptedTotal(before)
	beforeHubRemote := prodSimHubRemoteTotal(before)
	agent := &s.agents[3]
	if agent.server != nil {
		agent.server.Close()
		agent.server = nil
	}
	if err := startR1CodexAppserver(&agent.r1CodexAgent, s.opts.Command); err != nil {
		addR1Assertion(s.report, "prod-sim restart appserver", false, err.Error())
		return err
	}
	agentReport, _, err := initializeR1CodexAgent(&agent.r1CodexAgent, s.opts.TurnTimeout)
	if err != nil {
		addR1Assertion(s.report, "prod-sim restart appserver", false, err.Error())
		return err
	}
	appendSyncAgentAnswer(s.report.Sync, agent.principal, "restarted thread "+agentReport.ThreadID)
	time.Sleep(2 * time.Second)
	after, err := observeAcceptanceRun(s.report.RunRoot, 10)
	if err != nil {
		addR1Assertion(s.report, "prod-sim restart inspect after", false, err.Error())
		return err
	}
	afterAccepted := prodSimAcceptedTotal(after)
	afterHubRemote := prodSimHubRemoteTotal(after)
	passed := beforeAccepted == afterAccepted && beforeHubRemote == afterHubRemote
	addR1Assertion(s.report, "prod-sim appserver restart does not duplicate governed events", passed, fmt.Sprintf("accepted_before=%d accepted_after=%d hub_remote_before=%d hub_remote_after=%d", beforeAccepted, afterAccepted, beforeHubRemote, afterHubRemote))
	s.report.Scenarios = append(s.report.Scenarios, r1TaskSimScenarioReport{
		Name:   "duplicate_pull_restart",
		Status: statusFromBool(passed),
		Actors: []string{agent.principal},
		Evidence: map[string]any{
			"accepted_before":   beforeAccepted,
			"accepted_after":    afterAccepted,
			"hub_remote_before": beforeHubRemote,
			"hub_remote_after":  afterHubRemote,
		},
	})
	if !passed {
		return fmt.Errorf("appserver restart changed governed event counts")
	}
	return nil
}

func (s prodSimRun) emitTeamworkSignal(agent *r1CodexSyncAgent, signalID, scope, statement string) error {
	payload := taskSimJSON(map[string]any{
		"rule": map[string]any{
			"signal_id": signalID,
			"scope":     scope,
			"ttl":       "30m",
		},
		"narrative": map[string]any{
			"statement":    statement,
			"why_teamwork": "production-like validation requires multiple isolated hostagents",
		},
		"refs": map[string]any{"evidence_refs": []string{"r1-prod-sim"}},
	})
	prompt := fmt.Sprintf(`Emit teamwork_signal.write_candidate.observed through your own Local Mnemon.
Use external id signal-%s and payload:
%s
After the command succeeds, answer "signal %s written".`, signalID, payload, signalID)
	answer, err := runR1Turn(&agent.r1CodexAgent, prompt, s.opts.TurnTimeout)
	appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
	if err != nil {
		addR1Assertion(s.report, "prod-sim signal emitted "+signalID, false, err.Error())
		return err
	}
	waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "teamwork_signal", 1, 20*time.Second)
	return nil
}

func (s prodSimRun) emitAssignment(agent *r1CodexSyncAgent, assignmentID, assignee, scope, expectedWork, expectedFeedback, ttl string) error {
	payload := taskSimJSON(map[string]any{
		"rule": map[string]any{
			"assignment_id": assignmentID,
			"assignee":      assignee,
			"scope":         scope,
			"ttl":           ttl,
		},
		"narrative": map[string]any{
			"expected_work":     expectedWork,
			"expected_feedback": expectedFeedback,
		},
		"refs": map[string]any{"evidence_refs": []string{"r1-prod-sim"}},
	})
	prompt := fmt.Sprintf(`Emit assignment.write_candidate.observed through your own Local Mnemon.
Use external id assignment-%s and payload:
%s
Do not message the assignee directly. After the command succeeds, answer "assignment %s written".`, assignmentID, payload, assignmentID)
	answer, err := runR1Turn(&agent.r1CodexAgent, prompt, s.opts.TurnTimeout)
	appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
	if err != nil {
		addR1Assertion(s.report, "prod-sim assignment emitted "+assignmentID, false, err.Error())
		return err
	}
	waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "assignment", 1, 20*time.Second)
	return nil
}

func (s prodSimRun) waitAndAct(agent *r1CodexSyncAgent, assignmentID, externalID, summary, evidence string) error {
	presentation, ok := waitForR1DerivedEventPresentation(agent.localURL, agent.token, []string{"[mnemon:work]", assignmentID}, 90*time.Second)
	addR1Assertion(s.report, "prod-sim "+assignmentID+" reaches assignee local mnemond", ok, presentation.Body)
	if !ok {
		return fmt.Errorf("%s did not receive assignment %s through local mnemond", agent.principal, assignmentID)
	}
	payload := taskSimJSON(map[string]any{
		"rule": map[string]any{
			"assignment_ref": assignmentID,
			"scope":          "prod-sim",
			"feedback_kind":  "progress",
		},
		"narrative": map[string]any{
			"summary":         summary,
			"changed_context": []string{"production-like task advanced through local observed event"},
			"suggested_next":  "starter should integrate or assign follow-up work",
		},
		"refs": map[string]any{"evidence_refs": []string{evidence}},
	})
	prompt := fmt.Sprintf(`Act on assignment %s from your local derived-event presentation.
Emit progress_digest.write_candidate.observed through your own Local Mnemon with external id %s and payload:
%s
After the command succeeds, answer "progress %s written".`, assignmentID, externalID, payload, assignmentID)
	answer, err := runR1Turn(&agent.r1CodexAgent, prompt, s.opts.TurnTimeout)
	appendSyncAgentAnswer(s.report.Sync, agent.principal, answer)
	if err != nil {
		addR1Assertion(s.report, "prod-sim progress emitted "+assignmentID, false, err.Error())
		return err
	}
	waitForLedgerCount(agent.localURL, agent.r1CodexAgent, "progress_digest", 1, 20*time.Second)
	return nil
}

func buildR1ProdSimTopology(agents []r1CodexSyncAgent) *r1AcceptanceTopologyReport {
	out := &r1AcceptanceTopologyReport{
		Mode:               "per-hostagent-mnemond",
		Agents:             len(agents),
		MnemondInstances:   len(agents),
		MnemonhubInstances: 1,
		SharedMnemond:      false,
		AgentMnemondMap:    map[string]string{},
	}
	for _, agent := range agents {
		out.AgentMnemondMap[agent.principal] = prodSimMnemondPath(agent)
	}
	return out
}

func prodSimStrictTopology(top *r1AcceptanceTopologyReport) bool {
	if top == nil || top.Mode != "per-hostagent-mnemond" || top.SharedMnemond || top.MnemonhubInstances != 1 || top.Agents < 5 || top.MnemondInstances != top.Agents {
		return false
	}
	seen := map[string]bool{}
	for _, path := range top.AgentMnemondMap {
		if strings.TrimSpace(path) == "" || seen[path] {
			return false
		}
		seen[path] = true
	}
	return len(seen) == top.Agents
}

func prodSimMnemondPath(agent r1CodexSyncAgent) string {
	return filepath.Join(agent.workspace, runtime.DefaultStorePath)
}

func prodSimDerivedAudit(agents []r1CodexSyncAgent) map[string]int {
	out := map[string]int{}
	for _, agent := range agents {
		for key, value := range countR1DerivedEventAudit(agent.renderAuditPath) {
			out[key] += value
		}
	}
	return out
}

func prodSimAcceptedTotal(report acceptanceObserveReport) int {
	total := 0
	for _, store := range report.Stores {
		if store.Role == "mnemond" {
			total += store.Counts["event_envelopes"]
		}
	}
	return total
}

func prodSimHubRemoteTotal(report acceptanceObserveReport) int {
	total := 0
	for _, store := range report.Stores {
		if store.Role == "mnemonhub" {
			total += store.Counts["sync_remote_events"]
		}
	}
	return total
}

func allProdSimScenariosOK(scenarios []r1TaskSimScenarioReport) bool {
	want := map[string]bool{
		"bootstrap_profiles":     false,
		"split_work":             false,
		"dependency_handoff":     false,
		"blocker_rework":         false,
		"ttl_paused_agent":       false,
		"duplicate_pull_restart": false,
	}
	for _, scenario := range scenarios {
		if scenario.Status == "ok" {
			if _, ok := want[scenario.Name]; ok {
				want[scenario.Name] = true
			}
		}
	}
	for _, ok := range want {
		if !ok {
			return false
		}
	}
	return true
}

func prodSafeID(s string) string {
	replacer := strings.NewReplacer("@", "-", "/", "-", ":", "-", ".", "-")
	return replacer.Replace(s)
}
