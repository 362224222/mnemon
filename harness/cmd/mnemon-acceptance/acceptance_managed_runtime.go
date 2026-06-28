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

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/spf13/cobra"
)

var (
	acceptanceManagedExchange        string
	acceptanceManagedMnemondBin      string
	acceptanceManagedGitHubRepo      string
	acceptanceManagedGitHubTokenFile string
)

var acceptanceManagedRuntimeCmd = &cobra.Command{
	Use:   "managed-runtime",
	Short: "Run managed-runtime seed-and-observe acceptance",
	RunE: func(cmd *cobra.Command, args []string) error {
		report, err := runManagedRuntimeAcceptance(cmd.Context(), managedRuntimeAcceptanceOptions{
			RunRoot:         acceptanceRunRoot,
			Agents:          acceptanceAgents,
			Exchange:        acceptanceManagedExchange,
			MnemondBin:      acceptanceManagedMnemondBin,
			GitHubRepo:      acceptanceManagedGitHubRepo,
			GitHubTokenFile: acceptanceManagedGitHubTokenFile,
			TurnTimeout:     acceptanceTurnTimeout,
			Stdout:          cmd.OutOrStdout(),
			Stderr:          cmd.ErrOrStderr(),
			Wake:            managedRuntimeMnemondDryRunWake,
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
	acceptanceManagedRuntimeCmd.Flags().IntVar(&acceptanceAgents, "agents", 5, "number of managed agent nodes")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedExchange, "exchange", "mnemonhub", "exchange mode: mnemonhub or github")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedMnemondBin, "mnemond-bin", "mnemond", "mnemond binary used for product-path wake checks")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedGitHubRepo, "github-repo", "mnemon-dev/mnemon-teamwork-example", "GitHub Remote Workspace repository (owner/name)")
	acceptanceManagedRuntimeCmd.Flags().StringVar(&acceptanceManagedGitHubTokenFile, "github-token-file", "", "GitHub token file for real GitHub exchange validation")
	acceptanceManagedRuntimeCmd.Flags().DurationVar(&acceptanceTurnTimeout, "turn-timeout", 5*time.Minute, "timeout per managed wake check")
	rootCmd.AddCommand(acceptanceManagedRuntimeCmd)
}

type managedRuntimeAcceptanceOptions struct {
	RunRoot         string
	Agents          int
	Exchange        string
	MnemondBin      string
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
	StartedAt     string                           `json:"started_at"`
	FinishedAt    string                           `json:"finished_at"`
	RunRoot       string                           `json:"run_root"`
	ReportPath    string                           `json:"report_path"`
	Agents        []managedRuntimeAgentReport      `json:"agents"`
	Assertions    []managedRuntimeAssertion        `json:"assertions"`
	PromptAudit   []managedRuntimePromptAuditEntry `json:"prompt_audit"`
	Errors        []string                         `json:"errors,omitempty"`
}

type managedRuntimeAgentReport struct {
	Principal string `json:"principal"`
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
	if opts.Wake == nil {
		opts.Wake = managedRuntimeMnemondDryRunWake
	}
	if opts.Agents <= 0 {
		opts.Agents = 5
	}
	exchangeMode := strings.TrimSpace(opts.Exchange)
	if exchangeMode == "" {
		exchangeMode = "mnemonhub"
	}
	runRoot := opts.RunRoot
	if runRoot == "" {
		runRoot = filepath.Join(".testdata", "managed-runtime", started.Format("20060102T150405Z"))
	}
	report := managedRuntimeAcceptanceReport{
		SchemaVersion: 1,
		Status:        "ok",
		Layer:         "managed_runtime_acceptance",
		RunnerRole:    "seed_and_observe",
		Exchange:      exchangeMode,
		GitHubRepo:    strings.TrimSpace(opts.GitHubRepo),
		StartedAt:     started.Format(time.RFC3339),
		RunRoot:       runRoot,
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
