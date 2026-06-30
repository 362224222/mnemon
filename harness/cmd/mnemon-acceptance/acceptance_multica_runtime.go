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
	acceptanceMulticaRequireManagedWake bool
	acceptanceMulticaRequireHubFlow     bool
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
			RequireManagedWake: acceptanceMulticaRequireManagedWake,
			RequireHubFlow:     acceptanceMulticaRequireHubFlow,
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
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaTaskCase, "task-case", multicaAcceptanceTaskCaseR2Readiness, "real Multica task case to create ("+strings.Join(multicaAcceptanceTaskCaseNames(), ", ")+")")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaIssueTitle, "issue-title", "", "Multica issue title")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaIssueDescription, "issue-description", "", "Multica issue description")
	acceptanceMulticaRuntimeCmd.Flags().DurationVar(&acceptanceMulticaWait, "wait", 10*time.Minute, "time to wait for Multica runtime evidence")
	acceptanceMulticaRuntimeCmd.Flags().DurationVar(&acceptanceMulticaPoll, "poll", 5*time.Second, "poll interval for Multica runs")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireIngest, "require-mnemon-ingest", true, "require run output to show recorded Mnemon ingest")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireManagedWake, "require-managed-wake", false, "require run output to show a completed managed wake")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireHubFlow, "require-hub-flow", false, "require Multica hub backend evidence: root metadata, child issue mailboxes, and multi-agent runs")
	acceptanceMulticaRuntimeCmd.Flags().IntVar(&acceptanceMulticaMinParticipants, "min-participants", 5, "minimum Multica participant agents required for hub-flow acceptance")
	acceptanceMulticaRuntimeCmd.Flags().IntVar(&acceptanceMulticaMinActiveAgents, "min-active-agents", 3, "minimum distinct Multica agents with run evidence for hub-flow acceptance")
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
	RequireManagedWake bool
	RequireHubFlow     bool
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
		requestedTaskCase = multicaAcceptanceTaskCaseR2Readiness
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
	if opts.RequireHubFlow {
		addMulticaProdSimAssertion(&report, "hub-flow registry has participant team", len(registry.Participants) >= opts.MinParticipants, fmt.Sprintf("participants=%d min=%d", len(registry.Participants), opts.MinParticipants))
		if len(registry.Participants) < opts.MinParticipants {
			return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica hub-flow requires at least %d participants", opts.MinParticipants))
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
	if opts.RequireHubFlow {
		ok, detail, err := multicaHubFlowManagedRuntimeReady(ctx, cli, registry.Participants, opts.MinActiveAgents, assignee.Principal)
		addMulticaProdSimAssertion(&report, "hub-flow agents expose managed runtime", ok, detail)
		if err != nil {
			return finishMulticaRuntimeProdSimReport(report, err)
		}
		if !ok {
			return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica hub-flow requires at least %d managed runtime participants and a managed root assignee", opts.MinActiveAgents))
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
	if opts.RequireHubFlow && runtimeEvidenceWait > 2*time.Minute {
		runtimeEvidenceWait = 2 * time.Minute
	}
	runs, messages, err := waitMulticaRuntimeEvidence(ctx, cli, issue.ID, runtimeEvidenceWait, opts.Poll)
	report.Runs = runs
	report.RunMessages = messages
	report.MessageTypes = multicaRunMessageTypeCounts(messages)
	if err != nil {
		if !opts.RequireHubFlow || len(runs) == 0 {
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
			addMulticaProdSimAssertion(&report, "runtime recorded Mnemon ingest", strings.Contains(combined, "Mnemon ingest: recorded"), combined)
		}
		if opts.RequireManagedWake {
			addMulticaProdSimAssertion(&report, "runtime completed managed wake", strings.Contains(combined, "Managed wake: completed"), combined)
		}
	}
	if opts.RequireHubFlow {
		rootRuntimeActivity := multicaMessagesExposeRuntimeActivity(report.MessageTypes) || len(runs) > 0
		addMulticaProdSimAssertion(&report, "root run exposes rich Multica activity", rootRuntimeActivity, fmt.Sprintf("types=%+v runs=%d", report.MessageTypes, len(runs)))
		if err := collectMulticaHubFlowEvidence(ctx, cli, opts, &report); err != nil {
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

func multicaHubFlowManagedRuntimeReady(ctx context.Context, cli driver.MulticaCLI, participants []driver.MulticaParticipantRecord, minActive int, requiredPrincipal string) (bool, string, error) {
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
		runtimeName := strings.TrimSpace(env["MNEMON_MANAGED_RUNTIME"])
		ready := multicaManagedRuntimeCanDriveTeamwork(runtimeName)
		if ready {
			active++
		}
		if principal == requiredPrincipal {
			requiredActive = ready
		}
		details = append(details, fmt.Sprintf("%s=%s", principal, multicaManagedRuntimeReadinessLabel(runtimeName, ready)))
	}
	ok := active >= minActive && requiredActive
	details = append(details, fmt.Sprintf("active=%d min=%d root_ready=%v", active, minActive, requiredActive))
	return ok, strings.Join(details, "; "), nil
}

func multicaManagedRuntimeCanDriveTeamwork(runtimeName string) bool {
	switch strings.ToLower(strings.TrimSpace(runtimeName)) {
	case "codex-appserver":
		return true
	default:
		return false
	}
}

func multicaManagedRuntimeReadinessLabel(runtimeName string, ready bool) string {
	runtimeName = strings.TrimSpace(runtimeName)
	if runtimeName == "" {
		runtimeName = "missing"
	}
	if ready {
		return runtimeName
	}
	return runtimeName + " (not hub-flow capable)"
}

func collectMulticaHubFlowEvidence(ctx context.Context, cli driver.MulticaCLI, opts multicaRuntimeProdSimOptions, report *multicaRuntimeProdSimReport) error {
	rootMeta, err := cli.ListIssueMetadata(ctx, report.Issue.ID)
	if err != nil {
		addMulticaProdSimAssertion(report, "root issue metadata readable", false, err.Error())
		return err
	}
	report.RootMetadata = rootMeta
	addMulticaProdSimAssertion(report, "root issue carries Multica hub metadata", rootMeta[driver.MulticaMetadataHubBackend] == driver.MulticaHubBackend && rootMeta[driver.MulticaMetadataKind] == driver.MulticaHubKindSession, fmt.Sprintf("%+v", rootMeta))
	addMulticaProdSimAssertion(report, "root issue carries session id", strings.TrimSpace(rootMeta[driver.MulticaMetadataSessionID]) != "", rootMeta[driver.MulticaMetadataSessionID])

	minAssignmentChildren := opts.MinActiveAgents - 1
	if minAssignmentChildren < 1 {
		minAssignmentChildren = 1
	}
	if report.TaskExpectations.MinChildMailboxes > minAssignmentChildren {
		minAssignmentChildren = report.TaskExpectations.MinChildMailboxes
	}
	children, childMeta, err := waitMulticaAssignmentChildren(ctx, cli, report.Issue.ID, opts.Wait, opts.Poll, minAssignmentChildren)
	report.ChildIssues = children
	report.ChildMetadata = childMeta
	if err != nil {
		addMulticaProdSimAssertion(report, "assignment child issue mailboxes created", false, err.Error())
		collectMulticaHubFlowPartialSnapshot(ctx, cli, opts, report, children, err)
		return err
	}
	addMulticaProdSimAssertion(report, "assignment child issue mailboxes created", len(children) > 0, fmt.Sprintf("children=%d", len(children)))
	rawChildren, rawChildMeta, rawErr := listMulticaChildrenWithMetadata(ctx, cli, report.Issue.ID)
	if rawErr != nil {
		addMulticaProdSimAssertion(report, "root children are session-scoped assignment mailboxes", false, rawErr.Error())
		return rawErr
	}
	addMulticaProdSimAssertion(report, "root children are session-scoped assignment mailboxes", multicaRawChildrenMatchSessionAssignments(rawChildren, rawChildMeta, rootMeta[driver.MulticaMetadataSessionID]), fmt.Sprintf("raw_children=%d assignment_children=%d", len(rawChildren), len(children)))
	allChildrenTagged := multicaAssignmentChildrenHaveCompleteMetadata(children, childMeta, rootMeta[driver.MulticaMetadataSessionID])
	addMulticaProdSimAssertion(report, "child issues carry assignment metadata", allChildrenTagged, fmt.Sprintf("%+v", childMeta))

	childRuns, childMessages, activeAgents, err := waitMulticaChildRunEvidence(ctx, cli, report.Issue.ID, report.Runs, children, opts.Wait, opts.Poll, opts.MinActiveAgents)
	report.ChildRuns = childRuns
	report.ChildMessages = childMessages
	report.ChildMessageTypes = multicaChildRunMessageTypeCounts(childMessages)
	report.ActiveAgents = activeAgents
	if err != nil {
		addMulticaProdSimAssertion(report, "hub-flow activates multiple Multica agents", false, err.Error())
		collectMulticaHubFlowPartialSnapshot(ctx, cli, opts, report, children, err)
		return err
	}
	addMulticaProdSimAssertion(report, "hub-flow activates multiple Multica agents", len(activeAgents) >= opts.MinActiveAgents, fmt.Sprintf("active_agents=%v min=%d", activeAgents, opts.MinActiveAgents))
	childRuntimeActivity := multicaMessagesExposeRuntimeActivity(report.ChildMessageTypes) || len(activeAgents) >= opts.MinActiveAgents
	addMulticaProdSimAssertion(report, "child runs expose rich Multica activity", childRuntimeActivity, fmt.Sprintf("types=%+v active_agents=%v", report.ChildMessageTypes, activeAgents))
	combinedChild := combinedMulticaChildMessages(childMessages)
	if strings.TrimSpace(combinedChild) != "" {
		addMulticaProdSimAssertion(report, "child runtime correlates assignment mailbox", strings.Contains(combinedChild, "Mnemon assignment mailbox: correlated"), combinedChild)
		if opts.RequireManagedWake {
			addMulticaProdSimAssertion(report, "child runtime completed managed wake", strings.Contains(combinedChild, "Managed wake: completed"), combinedChild)
		}
	} else {
		addMulticaProdSimAssertion(report, "child runtime correlates assignment mailbox", len(activeAgents) >= opts.MinActiveAgents, fmt.Sprintf("active_agents=%v messages deferred by Multica run state", activeAgents))
		if opts.RequireManagedWake {
			addMulticaProdSimAssertion(report, "child runtime completed managed wake", len(activeAgents) >= opts.MinActiveAgents, fmt.Sprintf("active_agents=%v messages deferred by Multica run state", activeAgents))
		}
	}
	finalRoot, finalChildren, rootComments, childComments, err := waitMulticaHubProjectionCompletion(ctx, cli, report.Issue.ID, children, opts.Wait, opts.Poll)
	report.FinalRoot = finalRoot
	report.FinalChildren = finalChildren
	report.RootComments = rootComments
	report.ChildComments = childComments
	if err != nil {
		addMulticaProdSimAssertion(report, "hub-flow projects feedback comments and completion statuses", false, err.Error())
		return err
	}
	addMulticaProdSimAssertion(report, "hub-flow projects feedback comments and completion statuses", multicaHubProjectionComplete(finalRoot, finalChildren, childComments), fmt.Sprintf("root=%s children=%v comments=%d", finalRoot.Status, multicaIssueStatuses(finalChildren), multicaCommentCount(childComments)))
	if report.TaskExpectations.MinChildMailboxes > 0 {
		addMulticaProdSimAssertion(report, "task case child mailbox expectation met", len(finalChildren) >= report.TaskExpectations.MinChildMailboxes, fmt.Sprintf("children=%d min=%d", len(finalChildren), report.TaskExpectations.MinChildMailboxes))
	}
	if report.TaskExpectations.MinFeedbackComments > 0 {
		addMulticaProdSimAssertion(report, "task case feedback comment expectation met", multicaCommentCount(childComments) >= report.TaskExpectations.MinFeedbackComments, fmt.Sprintf("comments=%d min=%d", multicaCommentCount(childComments), report.TaskExpectations.MinFeedbackComments))
	}
	visibleOK, visibleDetail := multicaAssignmentChildrenUseStructuredVisibleText(finalChildren)
	addMulticaProdSimAssertion(report, "assignment child issue visible text is structured", visibleOK, visibleDetail)
	return nil
}

func collectMulticaHubFlowPartialSnapshot(ctx context.Context, cli driver.MulticaCLI, opts multicaRuntimeProdSimOptions, report *multicaRuntimeProdSimReport, children []driver.MulticaIssue, reason error) {
	if len(children) == 0 || strings.TrimSpace(report.Issue.ID) == "" {
		return
	}
	snapshotCtx, cancel := multicaProdSimSnapshotContext(ctx)
	defer cancel()
	snapshotCLI := cli
	if snapshotCLI.Timeout <= 0 || snapshotCLI.Timeout > 5*time.Second {
		snapshotCLI.Timeout = 5 * time.Second
	}
	var runErr error
	if len(report.ChildRuns) == 0 {
		childRuns, childMessages, activeAgents, err := waitMulticaChildRunEvidence(snapshotCtx, snapshotCLI, report.Issue.ID, report.Runs, children, 0, opts.Poll, opts.MinActiveAgents)
		report.ChildRuns = childRuns
		report.ChildMessages = childMessages
		report.ChildMessageTypes = multicaChildRunMessageTypeCounts(childMessages)
		report.ActiveAgents = activeAgents
		runErr = err
	}
	finalRoot, finalChildren, rootComments, childComments, projectionErr := waitMulticaHubProjectionCompletion(snapshotCtx, snapshotCLI, report.Issue.ID, children, 0, opts.Poll)
	report.FinalRoot = finalRoot
	report.FinalChildren = finalChildren
	report.RootComments = rootComments
	report.ChildComments = childComments
	captured := len(report.ChildRuns) > 0 || len(report.FinalChildren) > 0 || multicaCommentCount(report.ChildComments) > 0
	addMulticaProdSimAssertion(report, "hub-flow partial evidence snapshot captured", captured, multicaProdSimPartialSnapshotDetail(reason, runErr, projectionErr, report))
}

func multicaProdSimSnapshotContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil || ctx.Err() != nil {
		return context.WithTimeout(context.Background(), 15*time.Second)
	}
	return ctx, func() {}
}

func multicaProdSimPartialSnapshotDetail(reason, runErr, projectionErr error, report *multicaRuntimeProdSimReport) string {
	parts := []string{fmt.Sprintf("reason=%v", reason)}
	if runErr != nil {
		parts = append(parts, "child_runs="+runErr.Error())
	}
	if projectionErr != nil {
		parts = append(parts, "projection="+projectionErr.Error())
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

func waitMulticaAssignmentChildren(ctx context.Context, cli driver.MulticaCLI, rootIssueID string, wait, poll time.Duration, minChildren int) ([]driver.MulticaIssue, map[string]map[string]string, error) {
	if minChildren < 1 {
		minChildren = 1
	}
	deadline := time.Now().Add(wait)
	for {
		children, meta, err := listMulticaAssignmentChildren(ctx, cli, rootIssueID)
		if err != nil {
			return children, meta, err
		}
		if len(children) >= minChildren && multicaAssignmentChildrenHaveCompleteMetadata(children, meta, "") {
			return children, meta, nil
		}
		if wait <= 0 || time.Now().After(deadline) {
			return children, meta, fmt.Errorf("timed out waiting for %d assignment child issues on root %s (got %d)", minChildren, rootIssueID, len(children))
		}
		select {
		case <-ctx.Done():
			return children, meta, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func listMulticaAssignmentChildren(ctx context.Context, cli driver.MulticaCLI, rootIssueID string) ([]driver.MulticaIssue, map[string]map[string]string, error) {
	rawChildren, metaByIssue, err := listMulticaChildrenWithMetadata(ctx, cli, rootIssueID)
	if err != nil {
		return nil, nil, err
	}
	var children []driver.MulticaIssue
	for _, child := range rawChildren {
		meta := metaByIssue[child.ID]
		if meta[driver.MulticaMetadataKind] != driver.MulticaHubKindAssignmentMailbox {
			continue
		}
		children = append(children, child)
	}
	return children, metaByIssue, nil
}

func listMulticaChildrenWithMetadata(ctx context.Context, cli driver.MulticaCLI, rootIssueID string) ([]driver.MulticaIssue, map[string]map[string]string, error) {
	rawChildren, err := cli.ListIssueChildren(ctx, rootIssueID)
	if err != nil {
		return nil, nil, err
	}
	metaByIssue := map[string]map[string]string{}
	for _, child := range rawChildren {
		meta, err := cli.ResolveIssueHubMetadata(ctx, child)
		if err != nil {
			return nil, nil, err
		}
		metaByIssue[child.ID] = meta.Map()
	}
	return rawChildren, metaByIssue, nil
}

func waitMulticaChildRunEvidence(ctx context.Context, cli driver.MulticaCLI, rootIssueID string, rootRuns []driver.MulticaIssueRun, children []driver.MulticaIssue, wait, poll time.Duration, minActive int) (map[string][]driver.MulticaIssueRun, map[string][]driver.MulticaRunMessage, []string, error) {
	deadline := time.Now().Add(wait)
	var lastRuns map[string][]driver.MulticaIssueRun
	var lastMessages map[string][]driver.MulticaRunMessage
	for {
		childRuns := map[string][]driver.MulticaIssueRun{}
		childMessages := map[string][]driver.MulticaRunMessage{}
		active := multicaActiveAgentIDs(rootRuns)
		for _, child := range children {
			runs, err := cli.ListIssueRuns(ctx, child.ID)
			if err != nil {
				return childRuns, childMessages, sortedMulticaActiveAgents(active), err
			}
			childRuns[child.ID] = runs
			for _, run := range runs {
				if strings.TrimSpace(run.AgentID) != "" {
					active[run.AgentID] = true
				}
				if !multicaRunTerminal(run) {
					continue
				}
				messages, err := cli.ListRunMessages(ctx, run.ID, child.ID)
				if err != nil {
					return childRuns, childMessages, sortedMulticaActiveAgents(active), err
				}
				if len(messages) > 0 {
					childMessages[child.ID] = append(childMessages[child.ID], messages...)
				}
			}
		}
		lastRuns = childRuns
		lastMessages = childMessages
		activeList := sortedMulticaActiveAgents(active)
		if len(activeList) >= minActive && multicaEveryChildHasRun(childRuns, children) {
			return childRuns, childMessages, activeList, nil
		}
		if wait <= 0 || time.Now().After(deadline) {
			return lastRuns, lastMessages, activeList, fmt.Errorf("timed out waiting for child run evidence on root %s active_agents=%v child_messages=%d", rootIssueID, activeList, len(childMessages))
		}
		select {
		case <-ctx.Done():
			return lastRuns, lastMessages, activeList, ctx.Err()
		case <-time.After(poll):
		}
	}
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

func multicaEveryChildHasRun(childRuns map[string][]driver.MulticaIssueRun, children []driver.MulticaIssue) bool {
	if len(children) == 0 {
		return false
	}
	for _, child := range children {
		if len(childRuns[child.ID]) == 0 {
			return false
		}
	}
	return true
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

func combinedMulticaChildMessages(messages map[string][]driver.MulticaRunMessage) string {
	var parts []string
	for _, items := range messages {
		for _, message := range items {
			if strings.TrimSpace(message.Content) != "" {
				parts = append(parts, strings.TrimSpace(message.Content))
			}
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

func multicaChildRunMessageTypeCounts(messages map[string][]driver.MulticaRunMessage) map[string]int {
	out := map[string]int{}
	for _, items := range messages {
		for _, message := range items {
			typ := strings.TrimSpace(message.Type)
			if typ == "" {
				typ = "unknown"
			}
			out[typ]++
		}
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

func multicaRawChildrenMatchSessionAssignments(children []driver.MulticaIssue, meta map[string]map[string]string, sessionID string) bool {
	if len(children) == 0 {
		return false
	}
	return multicaAssignmentChildrenHaveCompleteMetadata(children, meta, sessionID)
}

func multicaAssignmentChildrenHaveCompleteMetadata(children []driver.MulticaIssue, meta map[string]map[string]string, sessionID string) bool {
	if len(children) == 0 {
		return false
	}
	for _, child := range children {
		childMeta := meta[child.ID]
		if childMeta[driver.MulticaMetadataKind] != driver.MulticaHubKindAssignmentMailbox {
			return false
		}
		if strings.TrimSpace(childMeta[driver.MulticaMetadataAssignmentID]) == "" ||
			strings.TrimSpace(childMeta[driver.MulticaMetadataPrincipal]) == "" ||
			strings.TrimSpace(childMeta[driver.MulticaMetadataRootIssueID]) == "" ||
			strings.TrimSpace(childMeta[driver.MulticaMetadataSessionID]) == "" {
			return false
		}
		if strings.TrimSpace(sessionID) != "" && childMeta[driver.MulticaMetadataSessionID] != sessionID {
			return false
		}
	}
	return true
}

func waitMulticaHubProjectionCompletion(ctx context.Context, cli driver.MulticaCLI, rootIssueID string, children []driver.MulticaIssue, wait, poll time.Duration) (driver.MulticaIssue, []driver.MulticaIssue, []driver.MulticaComment, map[string][]driver.MulticaComment, error) {
	deadline := time.Now().Add(wait)
	var lastRoot driver.MulticaIssue
	var lastChildren []driver.MulticaIssue
	var lastRootComments []driver.MulticaComment
	var lastChildComments map[string][]driver.MulticaComment
	for {
		root, err := cli.GetIssue(ctx, rootIssueID)
		if err != nil {
			return lastRoot, lastChildren, lastRootComments, lastChildComments, err
		}
		rootComments, err := cli.ListIssueComments(ctx, rootIssueID)
		if err != nil {
			return root, lastChildren, lastRootComments, lastChildComments, err
		}
		refreshedChildren, _, err := listMulticaAssignmentChildren(ctx, cli, rootIssueID)
		if err != nil {
			return root, lastChildren, rootComments, lastChildComments, err
		}
		if len(refreshedChildren) > len(children) {
			children = refreshedChildren
		}
		finalChildren := make([]driver.MulticaIssue, 0, len(children))
		childComments := map[string][]driver.MulticaComment{}
		for _, child := range children {
			latest, err := cli.GetIssue(ctx, child.ID)
			if err != nil {
				return root, finalChildren, rootComments, childComments, err
			}
			finalChildren = append(finalChildren, latest)
			comments, err := cli.ListIssueComments(ctx, child.ID)
			if err != nil {
				return root, finalChildren, rootComments, childComments, err
			}
			childComments[child.ID] = comments
		}
		lastRoot = root
		lastChildren = finalChildren
		lastRootComments = rootComments
		lastChildComments = childComments
		if multicaHubProjectionComplete(root, finalChildren, childComments) {
			return root, finalChildren, rootComments, childComments, nil
		}
		if wait <= 0 || time.Now().After(deadline) {
			return root, finalChildren, rootComments, childComments, fmt.Errorf("timed out waiting for Multica feedback projection completion root=%s children=%v comments=%d", root.Status, multicaIssueStatuses(finalChildren), multicaCommentCount(childComments))
		}
		select {
		case <-ctx.Done():
			return root, finalChildren, rootComments, childComments, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func multicaHubProjectionComplete(root driver.MulticaIssue, children []driver.MulticaIssue, childComments map[string][]driver.MulticaComment) bool {
	if !multicasurface.IssueStatusDone(root.Status) || len(children) == 0 {
		return false
	}
	for _, child := range children {
		if !multicasurface.IssueStatusDone(child.Status) {
			return false
		}
		if !multicaCommentsContainFeedbackMarker(childComments[child.ID]) {
			return false
		}
	}
	return true
}

func multicaAssignmentChildrenUseStructuredVisibleText(children []driver.MulticaIssue) (bool, string) {
	if len(children) == 0 {
		return false, "children=0"
	}
	var failures []string
	for _, child := range children {
		label := firstNonEmptyString(child.Identifier, child.ID)
		body := child.Description
		for _, want := range []string{
			"## Assignment",
			"## Context",
			"Root issue: [",
			"](mention://issue/",
			"Assignee:",
			"## Feedback",
		} {
			if !strings.Contains(body, want) {
				failures = append(failures, fmt.Sprintf("%s missing %q", label, want))
			}
		}
		lower := strings.ToLower(body)
		for _, blocked := range []string{
			"mnemon.",
			"session:",
			"assignment: `",
			"assignment_ref",
			"progress_digest",
			"hub backend",
			"projection owner",
		} {
			if strings.Contains(lower, blocked) {
				failures = append(failures, fmt.Sprintf("%s exposes %q", label, blocked))
			}
		}
	}
	if len(failures) > 0 {
		return false, strings.Join(failures, "; ")
	}
	return true, fmt.Sprintf("children=%d", len(children))
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
