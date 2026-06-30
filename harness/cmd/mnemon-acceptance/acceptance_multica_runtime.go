package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
	"github.com/spf13/cobra"
)

var (
	acceptanceMulticaBin                string
	acceptanceMulticaProfile            string
	acceptanceMulticaServerURL          string
	acceptanceMulticaWorkspaceID        string
	acceptanceMulticaRegistry           string
	acceptanceMulticaAssigneePrincipal  string
	acceptanceMulticaTaskCase           string
	acceptanceMulticaIssueTitle         string
	acceptanceMulticaIssueDescription   string
	acceptanceMulticaWait               time.Duration
	acceptanceMulticaPoll               time.Duration
	acceptanceMulticaRequireIngest      bool
	acceptanceMulticaRequireSurfaceFlow bool
	acceptanceMulticaMinParticipants    int
	acceptanceMulticaMinActiveAgents    int
)

var acceptanceMulticaRuntimeCmd = &cobra.Command{
	Use:   "multica-runtime-prod-sim",
	Short: "Run a real Multica issue-to-runtime acceptance check",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runMulticaRuntimeProdSimAcceptance(cmd.Context(), multicaRuntimeProdSimOptions{
			RunRoot:            acceptanceRunRoot,
			MulticaBin:         acceptanceMulticaBin,
			Profile:            acceptanceMulticaProfile,
			ServerURL:          acceptanceMulticaServerURL,
			WorkspaceID:        acceptanceMulticaWorkspaceID,
			RegistryPath:       acceptanceMulticaRegistry,
			AssigneePrincipal:  acceptanceMulticaAssigneePrincipal,
			TaskCase:           acceptanceMulticaTaskCase,
			IssueTitle:         acceptanceMulticaIssueTitle,
			IssueDescription:   acceptanceMulticaIssueDescription,
			Wait:               acceptanceMulticaWait,
			Poll:               acceptanceMulticaPoll,
			RequireIngest:      acceptanceMulticaRequireIngest,
			RequireSurfaceFlow: acceptanceMulticaRequireSurfaceFlow,
			MinParticipants:    acceptanceMulticaMinParticipants,
			MinActiveAgents:    acceptanceMulticaMinActiveAgents,
			Stdout:             cmd.OutOrStdout(),
			Stderr:             cmd.ErrOrStderr(),
		})
		if report.ReportPath != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "acceptance report: %s\n", report.ReportPath)
		}
		if err != nil {
			return err
		}
		if report.Status != "ok" {
			return fmt.Errorf("Multica runtime prod-sim status: %s", report.Status)
		}
		return nil
	},
}

func init() {
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceRunRoot, "run-root", "", "acceptance run directory")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaBin, "multica-bin", multicaAcceptanceEnvDefault("MNEMON_MULTICA_BIN", ""), "Multica CLI path")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaProfile, "multica-profile", multicaAcceptanceEnvDefault("MNEMON_MULTICA_PROFILE", ""), "Multica CLI profile")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaServerURL, "multica-server-url", multicaAcceptanceEnvDefault("MNEMON_MULTICA_SERVER_URL", ""), "Multica server URL")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaWorkspaceID, "multica-workspace-id", multicaAcceptanceEnvDefault("MNEMON_MULTICA_WORKSPACE_ID", ""), "Multica workspace ID")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaRegistry, "registry", "", "Multica participant registry path")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaAssigneePrincipal, "assignee-principal", "planner@team", "Mnemon principal whose Multica agent receives the issue")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaTaskCase, "task-case", multicaAcceptanceTaskCaseR3Surface, "real Multica task case to create ("+strings.Join(multicaAcceptanceTaskCaseNames(), ", ")+")")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaIssueTitle, "issue-title", "", "Multica issue title")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaIssueDescription, "issue-description", "", "Multica issue description")
	acceptanceMulticaRuntimeCmd.Flags().DurationVar(&acceptanceMulticaWait, "wait", 10*time.Minute, "time to wait for Multica runtime evidence")
	acceptanceMulticaRuntimeCmd.Flags().DurationVar(&acceptanceMulticaPoll, "poll", 5*time.Second, "poll interval for Multica runs")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireIngest, "require-mnemon-ingest", true, "require run output to show recorded Mnemon ingest")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireSurfaceFlow, "require-surface-flow", false, "require R3 Multica surface evidence: provider-wrapper env, root run, readable metadata, and visible OA state")
	acceptanceMulticaRuntimeCmd.Flags().IntVar(&acceptanceMulticaMinParticipants, "min-participants", 5, "minimum Multica participant agents required for surface-flow acceptance")
	acceptanceMulticaRuntimeCmd.Flags().IntVar(&acceptanceMulticaMinActiveAgents, "min-active-agents", 3, "minimum distinct Multica agents with provider-wrapper readiness for surface-flow acceptance")
	rootCmd.AddCommand(acceptanceMulticaRuntimeCmd)
}

type multicaRuntimeProdSimOptions struct {
	RunRoot            string
	MulticaBin         string
	Profile            string
	ServerURL          string
	WorkspaceID        string
	RegistryPath       string
	AssigneePrincipal  string
	TaskCase           string
	IssueTitle         string
	IssueDescription   string
	Wait               time.Duration
	Poll               time.Duration
	RequireIngest      bool
	RequireSurfaceFlow bool
	MinParticipants    int
	MinActiveAgents    int
	Stdout             io.Writer
	Stderr             io.Writer
}

type multicaRuntimeProdSimReport struct {
	SchemaVersion     int                                   `json:"schema_version"`
	Status            string                                `json:"status"`
	StartedAt         string                                `json:"started_at"`
	FinishedAt        string                                `json:"finished_at"`
	RunRoot           string                                `json:"run_root"`
	ReportPath        string                                `json:"report_path"`
	WorkspaceID       string                                `json:"workspace_id"`
	RegistryPath      string                                `json:"registry_path"`
	TaskCase          string                                `json:"task_case,omitempty"`
	TaskExpectations  multicaAcceptanceTaskCaseExpectations `json:"task_expectations,omitempty"`
	ExecutionPlan     multicaAcceptanceExecutionPlan        `json:"execution_plan,omitempty"`
	Assignee          driver.MulticaParticipantRecord       `json:"assignee"`
	Participants      []driver.MulticaParticipantRecord     `json:"participants,omitempty"`
	Issue             driver.MulticaIssue                   `json:"issue"`
	RootMetadata      map[string]string                     `json:"root_metadata,omitempty"`
	ChildIssues       []driver.MulticaIssue                 `json:"child_issues,omitempty"`
	ChildMetadata     map[string]map[string]string          `json:"child_metadata,omitempty"`
	RootComments      []driver.MulticaComment               `json:"root_comments,omitempty"`
	ChildComments     map[string][]driver.MulticaComment    `json:"child_comments,omitempty"`
	Runs              []driver.MulticaIssueRun              `json:"runs,omitempty"`
	RunMessages       []driver.MulticaRunMessage            `json:"run_messages,omitempty"`
	MessageTypes      map[string]int                        `json:"message_types,omitempty"`
	ChildRuns         map[string][]driver.MulticaIssueRun   `json:"child_runs,omitempty"`
	ChildMessages     map[string][]driver.MulticaRunMessage `json:"child_messages,omitempty"`
	ChildMessageTypes map[string]int                        `json:"child_message_types,omitempty"`
	ActiveAgents      []string                              `json:"active_agents,omitempty"`
	FinalRoot         driver.MulticaIssue                   `json:"final_root,omitempty"`
	FinalChildren     []driver.MulticaIssue                 `json:"final_children,omitempty"`
	Assertions        []multicaRuntimeProdSimAssertion      `json:"assertions"`
	Errors            []string                              `json:"errors,omitempty"`
}

type multicaRuntimeProdSimAssertion struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Detail string `json:"detail,omitempty"`
}

func runMulticaRuntimeProdSimAcceptance(ctx context.Context, opts multicaRuntimeProdSimOptions) (multicaRuntimeProdSimReport, error) {
	started := time.Now().UTC()
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if opts.Wait <= 0 {
		opts.Wait = 10 * time.Minute
	}
	if opts.Poll <= 0 {
		opts.Poll = 5 * time.Second
	}
	if opts.MinParticipants <= 0 {
		opts.MinParticipants = 5
	}
	if opts.MinActiveAgents <= 0 {
		opts.MinActiveAgents = 3
	}
	requestedTaskCase := strings.TrimSpace(opts.TaskCase)
	if requestedTaskCase == "" {
		requestedTaskCase = multicaAcceptanceTaskCaseR3Surface
	}
	taskCase, taskCaseErr := multicaAcceptanceTaskCase(requestedTaskCase, started)
	runRoot := strings.TrimSpace(opts.RunRoot)
	if runRoot == "" {
		caseID := requestedTaskCase
		if taskCaseErr != nil {
			caseID = "invalid-task-case"
		} else {
			caseID = taskCase.ID
		}
		runRoot = filepath.Join(".testdata", "multica-runtime-prod-sim", multicaAcceptancePathSegment(caseID, "task-case"), started.Format("20060102T150405Z"))
	}
	absRunRoot, err := filepath.Abs(runRoot)
	if err != nil {
		return multicaRuntimeProdSimReport{}, err
	}
	runRoot = absRunRoot
	report := multicaRuntimeProdSimReport{
		SchemaVersion: 1,
		Status:        "ok",
		StartedAt:     started.Format(time.RFC3339),
		RunRoot:       runRoot,
		WorkspaceID:   strings.TrimSpace(opts.WorkspaceID),
		RegistryPath:  strings.TrimSpace(opts.RegistryPath),
		TaskCase:      requestedTaskCase,
	}
	if err := prepareR1AcceptanceRunRoot(runRoot); err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	if taskCaseErr != nil {
		return finishMulticaRuntimeProdSimReport(report, taskCaseErr)
	}
	report.TaskCase = taskCase.ID
	executionPlan, err := materializeMulticaAcceptanceExecutionPlan(runRoot, taskCase)
	if err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	report.ExecutionPlan = executionPlan
	var prereqErrs []error
	if cliPath, err := resolveMulticaAcceptanceCLI(opts.MulticaBin); err != nil {
		addMulticaProdSimAssertion(&report, "Multica CLI available", false, err.Error())
		prereqErrs = append(prereqErrs, err)
	} else {
		opts.MulticaBin = cliPath
		addMulticaProdSimAssertion(&report, "Multica CLI available", true, cliPath)
	}
	registryPath := strings.TrimSpace(opts.RegistryPath)
	if registryPath == "" {
		registryPath = filepath.Join(".", driver.MulticaDefaultRegistryRelPath)
	}
	registry, ok, err := driver.LoadMulticaRegistry(registryPath)
	if err != nil {
		addMulticaProdSimAssertion(&report, "Multica registry available", false, err.Error())
		prereqErrs = append(prereqErrs, err)
	} else if !ok {
		err := fmt.Errorf("Multica registry not found: %s", registryPath)
		addMulticaProdSimAssertion(&report, "Multica registry available", false, err.Error())
		prereqErrs = append(prereqErrs, err)
	} else {
		addMulticaProdSimAssertion(&report, "Multica registry available", true, registryPath)
	}
	if err := multicaProdSimPrerequisiteError(prereqErrs); err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	report.RegistryPath = registryPath
	report.Participants = registry.Participants
	if opts.RequireSurfaceFlow {
		addMulticaProdSimAssertion(&report, "surface-flow registry has participant team", len(registry.Participants) >= opts.MinParticipants, fmt.Sprintf("participants=%d min=%d", len(registry.Participants), opts.MinParticipants))
		if len(registry.Participants) < opts.MinParticipants {
			return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica surface-flow requires at least %d participants", opts.MinParticipants))
		}
	}
	assignee, err := selectMulticaAcceptanceAssignee(registry, opts.AssigneePrincipal)
	if err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	report.Assignee = assignee
	report.TaskExpectations = taskCase.Expectations
	if taskCase.Expectations.MinActiveAgents > opts.MinActiveAgents {
		opts.MinActiveAgents = taskCase.Expectations.MinActiveAgents
	}
	cli := driver.MulticaCLI{
		Command:     strings.TrimSpace(opts.MulticaBin),
		Profile:     strings.TrimSpace(opts.Profile),
		ServerURL:   strings.TrimSpace(opts.ServerURL),
		WorkspaceID: multicaAcceptanceFirstNonEmpty(strings.TrimSpace(opts.WorkspaceID), strings.TrimSpace(registry.WorkspaceID)),
		Env:         os.Environ(),
		Timeout:     30 * time.Second,
	}
	if opts.RequireSurfaceFlow {
		ok, detail, err := multicaSurfaceFlowProviderReady(ctx, cli, registry.Participants, opts.MinActiveAgents, assignee.Principal)
		addMulticaProdSimAssertion(&report, "surface-flow agents expose provider wrapper", ok, detail)
		if err != nil {
			return finishMulticaRuntimeProdSimReport(report, err)
		}
		if !ok {
			return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica surface-flow requires at least %d provider-wrapper participants and a ready root assignee", opts.MinActiveAgents))
		}
	}
	title := strings.TrimSpace(opts.IssueTitle)
	if title == "" {
		title = taskCase.Title
	}
	description := strings.TrimSpace(opts.IssueDescription)
	if description == "" {
		description = strings.TrimSpace(taskCase.Description + "\n\n" + renderMulticaAcceptanceExecutionPlan(executionPlan))
	}
	issue, err := cli.CreateIssue(ctx, driver.MulticaCreateIssueRequest{
		Title:          title,
		Description:    description,
		AssigneeID:     assignee.AgentID,
		AllowDuplicate: true,
	})
	if err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	report.Issue = issue
	addMulticaProdSimAssertion(&report, "issue created through Multica", issue.ID != "", issue.ID)
	addMulticaProdSimAssertion(&report, "issue assigned to Mnemon participant", assignee.AgentID != "", assignee.Principal)
	runtimeEvidenceWait := opts.Wait
	if opts.RequireSurfaceFlow && runtimeEvidenceWait > 2*time.Minute {
		runtimeEvidenceWait = 2 * time.Minute
	}
	runs, messages, err := waitMulticaRuntimeEvidence(ctx, cli, issue.ID, runtimeEvidenceWait, opts.Poll)
	report.Runs = runs
	report.RunMessages = messages
	report.MessageTypes = multicaRunMessageTypeCounts(messages)
	if err != nil {
		if !opts.RequireSurfaceFlow || len(runs) == 0 {
			addMulticaProdSimAssertion(&report, "Multica runtime produced run evidence", false, err.Error())
			return finishMulticaRuntimeProdSimReport(report, err)
		}
		addMulticaProdSimAssertion(&report, "Multica runtime produced run evidence", true, fmt.Sprintf("runs=%d messages=%d deferred_messages=%v", len(runs), len(messages), err))
	}
	combined := combinedMulticaRunMessages(messages)
	if err == nil {
		addMulticaProdSimAssertion(&report, "Multica runtime produced run evidence", len(runs) > 0 && len(messages) > 0, fmt.Sprintf("runs=%d messages=%d", len(runs), len(messages)))
	}
	if strings.TrimSpace(combined) != "" {
		addMulticaProdSimAssertion(&report, "runtime output names Mnemon runtime", strings.Contains(combined, "Mnemon Multica runtime handled issue"), combined)
		if opts.RequireIngest {
			addMulticaProdSimAssertion(&report, "runtime observed Multica surface input", strings.Contains(combined, "Multica surface input: observed"), combined)
		}
	}
	if opts.RequireSurfaceFlow {
		rootRuntimeActivity := multicaMessagesExposeRuntimeActivity(report.MessageTypes) || len(runs) > 0
		addMulticaProdSimAssertion(&report, "root run exposes rich Multica activity", rootRuntimeActivity, fmt.Sprintf("types=%+v runs=%d", report.MessageTypes, len(runs)))
		if err := collectMulticaSurfaceFlowEvidence(ctx, cli, opts, &report); err != nil {
			return finishMulticaRuntimeProdSimReport(report, err)
		}
	}
	if !multicaProdSimAssertionsPassed(report) {
		return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica runtime prod-sim assertions failed"))
	}
	return finishMulticaRuntimeProdSimReport(report, nil)
}

func selectMulticaAcceptanceAssignee(reg driver.MulticaRegistry, principal string) (driver.MulticaParticipantRecord, error) {
	principal = strings.TrimSpace(principal)
	if principal != "" {
		participant, ok := multicasurface.MulticaParticipantForPrincipal(reg, principal)
		if !ok {
			return driver.MulticaParticipantRecord{}, fmt.Errorf("participant principal %q not found in registry", principal)
		}
		if strings.TrimSpace(participant.AgentID) != "" {
			return participant, nil
		}
		return driver.MulticaParticipantRecord{}, fmt.Errorf("participant %s has no Multica agent id", participant.Principal)
	}
	if participant, ok := multicasurface.FirstMulticaParticipantWithAgentID(reg); ok {
		return participant, nil
	}
	return driver.MulticaParticipantRecord{}, fmt.Errorf("registry has no participant with a Multica agent id")
}

func multicaSurfaceFlowProviderReady(ctx context.Context, cli driver.MulticaCLI, participants []driver.MulticaParticipantRecord, minActive int, requiredPrincipal string) (bool, string, error) {
	if minActive < 1 {
		minActive = 1
	}
	requiredPrincipal = strings.TrimSpace(requiredPrincipal)
	active := 0
	requiredActive := requiredPrincipal == ""
	var details []string
	for _, participant := range participants {
		principal := strings.TrimSpace(participant.Principal)
		agentID := strings.TrimSpace(participant.AgentID)
		if principal == "" || agentID == "" {
			continue
		}
		env, err := cli.GetAgentEnv(ctx, agentID)
		if err != nil {
			return false, strings.Join(details, "; "), fmt.Errorf("read Multica agent env for %s: %w", principal, err)
		}
		ready, label := multicaSurfaceFlowParticipantReady(env)
		if ready {
			active++
		}
		if principal == requiredPrincipal {
			requiredActive = ready
		}
		details = append(details, fmt.Sprintf("%s=%s", principal, label))
	}
	ok := active >= minActive && requiredActive
	details = append(details, fmt.Sprintf("active=%d min=%d root_ready=%v", active, minActive, requiredActive))
	return ok, strings.Join(details, "; "), nil
}

func multicaSurfaceFlowParticipantReady(env map[string]string) (bool, string) {
	runtimeName := strings.TrimSpace(env["MNEMON_MULTICA_PROVIDER_RUNTIME"])
	runtimeReady := multicaProviderRuntimeCanDriveTeamwork(runtimeName)
	if !runtimeReady {
		return false, multicaProviderRuntimeReadinessLabel(runtimeName, false)
	}
	var missing []string
	if strings.TrimSpace(env["MNEMON_CONTROL_ADDR"]) == "" {
		missing = append(missing, "MNEMON_CONTROL_ADDR")
	}
	if strings.TrimSpace(env["MNEMON_MULTICA_PROVIDER_COMMAND"]) == "" {
		missing = append(missing, "MNEMON_MULTICA_PROVIDER_COMMAND")
	}
	if tokenFile := strings.TrimSpace(env["MNEMON_CONTROL_TOKEN_FILE"]); tokenFile != "" {
		if info, err := os.Stat(tokenFile); err != nil || info.IsDir() {
			missing = append(missing, "MNEMON_CONTROL_TOKEN_FILE")
		}
	}
	label := multicaProviderRuntimeReadinessLabel(runtimeName, true)
	if len(missing) > 0 {
		return false, label + " (missing " + strings.Join(missing, ", ") + ")"
	}
	return true, label
}

func multicaProviderRuntimeCanDriveTeamwork(runtimeName string) bool {
	switch strings.ToLower(strings.TrimSpace(runtimeName)) {
	case "codex", "codex-appserver":
		return true
	default:
		return false
	}
}

func multicaProviderRuntimeReadinessLabel(runtimeName string, ready bool) string {
	runtimeName = strings.TrimSpace(runtimeName)
	if runtimeName == "" {
		runtimeName = "missing"
	}
	if ready {
		return runtimeName
	}
	return runtimeName + " (not surface-flow capable)"
}

func collectMulticaSurfaceFlowEvidence(ctx context.Context, cli driver.MulticaCLI, opts multicaRuntimeProdSimOptions, report *multicaRuntimeProdSimReport) error {
	rootMeta, err := cli.ListIssueMetadata(ctx, report.Issue.ID)
	if err != nil {
		addMulticaProdSimAssertion(report, "root issue metadata readable", false, err.Error())
		return err
	}
	report.RootMetadata = rootMeta
	addMulticaProdSimAssertion(report, "root issue metadata readable", true, fmt.Sprintf("%+v", rootMeta))
	addMulticaProdSimAssertion(report, "root issue does not use legacy hub metadata", !multicaMetadataContainsLegacyHubKeys(rootMeta), fmt.Sprintf("%+v", rootMeta))

	latest, comments, err := waitMulticaSurfaceVisibility(ctx, cli, report.Issue.ID, opts.Wait, opts.Poll)
	report.FinalRoot = latest
	report.RootComments = comments
	if err != nil {
		addMulticaProdSimAssertion(report, "surface-flow root visibility readable", false, err.Error())
		return err
	}
	addMulticaProdSimAssertion(report, "surface-flow root visibility readable", latest.ID != "", fmt.Sprintf("status=%s comments=%d", latest.Status, len(comments)))
	if len(comments) > 0 {
		addMulticaProdSimAssertion(report, "surface-flow comments carry OA output", multicaCommentsContainFeedbackMarker(comments) || multicaCommentsContainMnemonUpdate(comments), fmt.Sprintf("comments=%d", len(comments)))
	}
	return nil
}

func multicaProdSimSnapshotContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil || ctx.Err() != nil {
		return context.WithTimeout(context.Background(), 15*time.Second)
	}
	return ctx, func() {}
}

func multicaProdSimPartialSnapshotDetail(reason, runErr, surfaceErr error, report *multicaRuntimeProdSimReport) string {
	parts := []string{fmt.Sprintf("reason=%v", reason)}
	if runErr != nil {
		parts = append(parts, "child_runs="+runErr.Error())
	}
	if surfaceErr != nil {
		parts = append(parts, "surface="+surfaceErr.Error())
	}
	parts = append(parts,
		fmt.Sprintf("child_runs=%d", len(report.ChildRuns)),
		fmt.Sprintf("active_agents=%v", report.ActiveAgents),
		fmt.Sprintf("final_children=%d", len(report.FinalChildren)),
		fmt.Sprintf("comments=%d", multicaCommentCount(report.ChildComments)),
	)
	return strings.Join(parts, "; ")
}

func waitMulticaRuntimeEvidence(ctx context.Context, cli driver.MulticaCLI, issueID string, wait, poll time.Duration) ([]driver.MulticaIssueRun, []driver.MulticaRunMessage, error) {
	deadline := time.Now().Add(wait)
	var lastRuns []driver.MulticaIssueRun
	for {
		runs, err := cli.ListIssueRuns(ctx, issueID)
		if err != nil {
			return lastRuns, nil, err
		}
		lastRuns = runs
		for _, run := range runs {
			if !multicaRunTerminal(run) {
				continue
			}
			messages, err := cli.ListRunMessages(ctx, run.ID, issueID)
			if err != nil {
				return runs, nil, err
			}
			if len(messages) > 0 {
				return runs, messages, nil
			}
		}
		if wait <= 0 || time.Now().After(deadline) {
			return lastRuns, nil, fmt.Errorf("timed out waiting for Multica runtime evidence on issue %s", issueID)
		}
		select {
		case <-ctx.Done():
			return lastRuns, nil, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func waitMulticaSurfaceVisibility(ctx context.Context, cli driver.MulticaCLI, issueID string, wait, poll time.Duration) (driver.MulticaIssue, []driver.MulticaComment, error) {
	deadline := time.Now().Add(wait)
	var lastRoot driver.MulticaIssue
	var lastComments []driver.MulticaComment
	for {
		root, err := cli.GetIssue(ctx, issueID)
		if err != nil {
			return lastRoot, lastComments, err
		}
		comments, err := cli.ListIssueComments(ctx, issueID)
		if err != nil {
			return root, lastComments, err
		}
		lastRoot = root
		lastComments = comments
		if root.ID != "" {
			return root, comments, nil
		}
		if wait <= 0 || time.Now().After(deadline) {
			return root, comments, fmt.Errorf("timed out waiting for Multica surface visibility on issue %s", issueID)
		}
		select {
		case <-ctx.Done():
			return root, comments, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func multicaMetadataContainsLegacyHubKeys(meta map[string]string) bool {
	for key, value := range meta {
		lowerKey := strings.ToLower(strings.TrimSpace(key))
		lowerValue := strings.ToLower(strings.TrimSpace(value))
		if lowerKey == "mnemon.hub_backend" || lowerKey == "mnemon.kind" {
			return true
		}
		if strings.Contains(lowerValue, "assignment_mailbox") || strings.Contains(lowerValue, "session_mailbox") {
			return true
		}
	}
	return false
}

func multicaRunTerminal(run driver.MulticaIssueRun) bool {
	if strings.TrimSpace(run.CompletedAt) != "" {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(run.Status)) {
	case "completed", "complete", "succeeded", "success", "done", "failed", "error", "errored", "cancelled", "canceled":
		return true
	default:
		return false
	}
}

func combinedMulticaRunMessages(messages []driver.MulticaRunMessage) string {
	var parts []string
	for _, message := range messages {
		if strings.TrimSpace(message.Content) != "" {
			parts = append(parts, strings.TrimSpace(message.Content))
		}
	}
	return strings.Join(parts, "\n")
}

func multicaRunMessageTypeCounts(messages []driver.MulticaRunMessage) map[string]int {
	out := map[string]int{}
	for _, message := range messages {
		typ := strings.TrimSpace(message.Type)
		if typ == "" {
			typ = "unknown"
		}
		out[typ]++
	}
	return out
}

func multicaMessagesExposeRuntimeActivity(types map[string]int) bool {
	if len(types) == 0 {
		return false
	}
	if types["tool_use"] > 0 && types["tool_result"] > 0 {
		return true
	}
	return types["commandExecution"] > 0 || types["exec_command"] > 0
}

func multicaCommentsContainMnemonUpdate(comments []driver.MulticaComment) bool {
	for _, comment := range comments {
		content := strings.ToLower(comment.Content)
		if strings.Contains(content, "mnemon 更新") || strings.Contains(content, "mnemon update") {
			return true
		}
	}
	return false
}

func multicaCommentsContainFeedbackMarker(comments []driver.MulticaComment) bool {
	for _, comment := range comments {
		content := strings.ToLower(comment.Content)
		if strings.Contains(content, "mnemon update: assignment feedback") && strings.Contains(content, "mnemon:event=") {
			return true
		}
	}
	return false
}

func multicaIssueStatuses(issues []driver.MulticaIssue) map[string]string {
	out := map[string]string{}
	for _, issue := range issues {
		out[firstNonEmptyString(issue.Identifier, issue.ID)] = issue.Status
	}
	return out
}

func multicaCommentCount(comments map[string][]driver.MulticaComment) int {
	n := 0
	for _, items := range comments {
		n += len(items)
	}
	return n
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func multicaActiveAgentIDs(runs []driver.MulticaIssueRun) map[string]bool {
	out := map[string]bool{}
	for _, run := range runs {
		if strings.TrimSpace(run.AgentID) != "" {
			out[run.AgentID] = true
		}
	}
	return out
}

func sortedMulticaActiveAgents(active map[string]bool) []string {
	var out []string
	for id := range active {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func addMulticaProdSimAssertion(report *multicaRuntimeProdSimReport, name string, passed bool, detail string) {
	report.Assertions = append(report.Assertions, multicaRuntimeProdSimAssertion{Name: name, Passed: passed, Detail: detail})
}

func resolveMulticaAcceptanceCLI(command string) (string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		command = "multica"
	}
	path, err := exec.LookPath(command)
	if err != nil {
		if command == "multica" {
			return "", fmt.Errorf("Multica CLI not found in PATH; pass --multica-bin or set MNEMON_MULTICA_BIN")
		}
		return "", fmt.Errorf("Multica CLI not executable %q: %w", command, err)
	}
	return path, nil
}

func multicaProdSimPrerequisiteError(errs []error) error {
	var messages []string
	for _, err := range errs {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("Multica runtime prod-sim prerequisites failed: %s", strings.Join(messages, "; "))
}

func multicaProdSimAssertionsPassed(report multicaRuntimeProdSimReport) bool {
	for _, assertion := range report.Assertions {
		if !assertion.Passed {
			return false
		}
	}
	return true
}

func finishMulticaRuntimeProdSimReport(report multicaRuntimeProdSimReport, err error) (multicaRuntimeProdSimReport, error) {
	report.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if err != nil {
		report.Status = "failed"
		report.Errors = append(report.Errors, err.Error())
	} else if !multicaProdSimAssertionsPassed(report) {
		report.Status = "failed"
	} else {
		report.Status = "ok"
	}
	path := filepath.Join(report.RunRoot, "acceptance-report.json")
	raw, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		return report, marshalErr
	}
	if writeErr := os.WriteFile(path, append(raw, '\n'), 0o644); writeErr != nil {
		return report, writeErr
	}
	report.ReportPath = path
	raw, marshalErr = json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		return report, marshalErr
	}
	if writeErr := os.WriteFile(path, append(raw, '\n'), 0o644); writeErr != nil {
		return report, writeErr
	}
	return report, err
}

func multicaAcceptanceEnvDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func multicaAcceptanceFirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
