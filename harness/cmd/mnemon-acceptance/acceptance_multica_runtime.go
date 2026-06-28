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

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/spf13/cobra"
)

var (
	acceptanceMulticaBin                string
	acceptanceMulticaProfile            string
	acceptanceMulticaServerURL          string
	acceptanceMulticaWorkspaceID        string
	acceptanceMulticaRegistry           string
	acceptanceMulticaAssigneePrincipal  string
	acceptanceMulticaIssueTitle         string
	acceptanceMulticaIssueDescription   string
	acceptanceMulticaWait               time.Duration
	acceptanceMulticaPoll               time.Duration
	acceptanceMulticaRequireIngest      bool
	acceptanceMulticaRequireManagedWake bool
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
			IssueTitle:         acceptanceMulticaIssueTitle,
			IssueDescription:   acceptanceMulticaIssueDescription,
			Wait:               acceptanceMulticaWait,
			Poll:               acceptanceMulticaPoll,
			RequireIngest:      acceptanceMulticaRequireIngest,
			RequireManagedWake: acceptanceMulticaRequireManagedWake,
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
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaIssueTitle, "issue-title", "", "Multica issue title")
	acceptanceMulticaRuntimeCmd.Flags().StringVar(&acceptanceMulticaIssueDescription, "issue-description", "", "Multica issue description")
	acceptanceMulticaRuntimeCmd.Flags().DurationVar(&acceptanceMulticaWait, "wait", 10*time.Minute, "time to wait for Multica runtime evidence")
	acceptanceMulticaRuntimeCmd.Flags().DurationVar(&acceptanceMulticaPoll, "poll", 5*time.Second, "poll interval for Multica runs")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireIngest, "require-mnemon-ingest", true, "require run output to show recorded Mnemon ingest")
	acceptanceMulticaRuntimeCmd.Flags().BoolVar(&acceptanceMulticaRequireManagedWake, "require-managed-wake", false, "require run output to show a completed managed wake")
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
	IssueTitle         string
	IssueDescription   string
	Wait               time.Duration
	Poll               time.Duration
	RequireIngest      bool
	RequireManagedWake bool
	Stdout             io.Writer
	Stderr             io.Writer
}

type multicaRuntimeProdSimReport struct {
	SchemaVersion int                              `json:"schema_version"`
	Status        string                           `json:"status"`
	StartedAt     string                           `json:"started_at"`
	FinishedAt    string                           `json:"finished_at"`
	RunRoot       string                           `json:"run_root"`
	ReportPath    string                           `json:"report_path"`
	WorkspaceID   string                           `json:"workspace_id"`
	RegistryPath  string                           `json:"registry_path"`
	Assignee      driver.MulticaParticipantRecord  `json:"assignee"`
	Issue         driver.MulticaIssue              `json:"issue"`
	Runs          []driver.MulticaIssueRun         `json:"runs,omitempty"`
	RunMessages   []driver.MulticaRunMessage       `json:"run_messages,omitempty"`
	Assertions    []multicaRuntimeProdSimAssertion `json:"assertions"`
	Errors        []string                         `json:"errors,omitempty"`
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
	runRoot := strings.TrimSpace(opts.RunRoot)
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "multica-runtime-prod-sim", started.Format("20060102T150405Z"))
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
	}
	if err := prepareR1AcceptanceRunRoot(runRoot); err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	registryPath := strings.TrimSpace(opts.RegistryPath)
	if registryPath == "" {
		registryPath = filepath.Join(".", driver.MulticaDefaultRegistryRelPath)
	}
	registry, ok, err := driver.LoadMulticaRegistry(registryPath)
	if err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	if !ok {
		return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica registry not found: %s", registryPath))
	}
	report.RegistryPath = registryPath
	assignee, err := selectMulticaAcceptanceAssignee(registry, opts.AssigneePrincipal)
	if err != nil {
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	report.Assignee = assignee
	cli := driver.MulticaCLI{
		Command:     strings.TrimSpace(opts.MulticaBin),
		Profile:     strings.TrimSpace(opts.Profile),
		ServerURL:   strings.TrimSpace(opts.ServerURL),
		WorkspaceID: multicaAcceptanceFirstNonEmpty(strings.TrimSpace(opts.WorkspaceID), strings.TrimSpace(registry.WorkspaceID)),
		Env:         os.Environ(),
		Timeout:     30 * time.Second,
	}
	title := strings.TrimSpace(opts.IssueTitle)
	if title == "" {
		title = "Mnemon Multica runtime prod-sim " + started.Format("150405")
	}
	description := strings.TrimSpace(opts.IssueDescription)
	if description == "" {
		description = "Validate that a Multica issue assigned to a Mnemon participant enters the Mnemon runtime path and reports product-visible progress."
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
	runs, messages, err := waitMulticaRuntimeEvidence(ctx, cli, issue.ID, opts.Wait, opts.Poll)
	report.Runs = runs
	report.RunMessages = messages
	if err != nil {
		addMulticaProdSimAssertion(&report, "Multica runtime produced run evidence", false, err.Error())
		return finishMulticaRuntimeProdSimReport(report, err)
	}
	combined := combinedMulticaRunMessages(messages)
	addMulticaProdSimAssertion(&report, "Multica runtime produced run evidence", len(runs) > 0 && len(messages) > 0, fmt.Sprintf("runs=%d messages=%d", len(runs), len(messages)))
	addMulticaProdSimAssertion(&report, "runtime output names Mnemon runtime", strings.Contains(combined, "Mnemon Multica runtime handled issue"), combined)
	if opts.RequireIngest {
		addMulticaProdSimAssertion(&report, "runtime recorded Mnemon ingest", strings.Contains(combined, "Mnemon ingest: recorded"), combined)
	}
	if opts.RequireManagedWake {
		addMulticaProdSimAssertion(&report, "runtime completed managed wake", strings.Contains(combined, "Managed wake: completed"), combined)
	}
	if !multicaProdSimAssertionsPassed(report) {
		return finishMulticaRuntimeProdSimReport(report, fmt.Errorf("Multica runtime prod-sim assertions failed"))
	}
	return finishMulticaRuntimeProdSimReport(report, nil)
}

func selectMulticaAcceptanceAssignee(reg driver.MulticaRegistry, principal string) (driver.MulticaParticipantRecord, error) {
	principal = strings.TrimSpace(principal)
	for _, participant := range reg.Participants {
		if principal != "" && participant.Principal == principal {
			if strings.TrimSpace(participant.AgentID) == "" {
				return driver.MulticaParticipantRecord{}, fmt.Errorf("participant %s has no Multica agent id", participant.Principal)
			}
			return participant, nil
		}
	}
	if principal != "" {
		return driver.MulticaParticipantRecord{}, fmt.Errorf("participant principal %q not found in registry", principal)
	}
	for _, participant := range reg.Participants {
		if strings.TrimSpace(participant.AgentID) != "" {
			return participant, nil
		}
	}
	return driver.MulticaParticipantRecord{}, fmt.Errorf("registry has no participant with a Multica agent id")
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

func addMulticaProdSimAssertion(report *multicaRuntimeProdSimReport, name string, passed bool, detail string) {
	report.Assertions = append(report.Assertions, multicaRuntimeProdSimAssertion{Name: name, Passed: passed, Detail: detail})
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
