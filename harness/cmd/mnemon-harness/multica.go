package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/spf13/cobra"
)

var (
	multicaBin         string
	multicaProfile     string
	multicaServerURL   string
	multicaWorkspaceID string
	multicaTimeout     time.Duration
	multicaJSON        bool

	multicaIssueID      string
	multicaIssueJSON    string
	multicaScope        string
	multicaTTL          string
	multicaWhyTeamwork  string
	multicaEvidenceRefs []string
	multicaContextRefs  []string
	multicaDryRun       bool

	multicaLocalAddr      string
	multicaLocalPrincipal string
	multicaLocalToken     string
	multicaLocalTokenFile string

	multicaCommentContent string
	multicaCommentFile    string
	multicaCommentStdin   bool
	multicaCommentTitle   string
	multicaCommentEvents  []string
)

var multicaCmd = &cobra.Command{
	Use:   "multica",
	Short: "Bridge Multica issues and comments with Local Mnemon",
}

var multicaProbeCmd = &cobra.Command{
	Use:   "probe",
	Short: "Report local Multica CLI and service readiness",
	RunE:  runMulticaProbe,
}

var multicaImportIssueCmd = &cobra.Command{
	Use:   "import-issue",
	Short: "Import one Multica issue as a Mnemon teamwork signal",
	RunE:  runMulticaImportIssue,
}

var multicaProjectCommentCmd = &cobra.Command{
	Use:   "project-comment",
	Short: "Write a Mnemon update as a Multica issue comment",
	RunE:  runMulticaProjectComment,
}

type multicaProbeReport struct {
	Command          string                      `json:"command"`
	Profile          string                      `json:"profile,omitempty"`
	ServerURL        string                      `json:"server_url,omitempty"`
	WorkspaceID      string                      `json:"workspace_id,omitempty"`
	Version          *driver.MulticaVersion      `json:"version,omitempty"`
	VersionError     string                      `json:"version_error,omitempty"`
	AuthStatus       string                      `json:"auth_status,omitempty"`
	AuthError        string                      `json:"auth_error,omitempty"`
	DaemonStatus     *driver.MulticaDaemonStatus `json:"daemon_status,omitempty"`
	DaemonError      string                      `json:"daemon_error,omitempty"`
	IntegrationReady bool                        `json:"integration_ready"`
}

func runMulticaProbe(cmd *cobra.Command, args []string) error {
	cli := multicaCLI()
	report := multicaProbeReport{
		Command:     multicaCLICommand(cli),
		Profile:     strings.TrimSpace(multicaProfile),
		ServerURL:   strings.TrimSpace(multicaServerURL),
		WorkspaceID: strings.TrimSpace(multicaWorkspaceID),
	}
	if version, err := cli.Version(cmd.Context()); err != nil {
		report.VersionError = err.Error()
	} else {
		report.Version = &version
	}
	if status, err := cli.AuthStatus(cmd.Context()); err != nil {
		report.AuthStatus = status
		report.AuthError = err.Error()
	} else {
		report.AuthStatus = status
	}
	if daemon, err := cli.DaemonStatus(cmd.Context()); err != nil {
		report.DaemonStatus = &daemon
		report.DaemonError = err.Error()
	} else {
		report.DaemonStatus = &daemon
	}
	authenticated := report.AuthStatus != "" && report.AuthError == "" && !strings.Contains(strings.ToLower(report.AuthStatus), "not authenticated")
	daemonRunning := report.DaemonStatus != nil && strings.EqualFold(strings.TrimSpace(report.DaemonStatus.Status), "running")
	report.IntegrationReady = report.Version != nil && authenticated && daemonRunning
	if multicaJSON {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	if report.Version != nil {
		fmt.Fprintf(cmd.OutOrStdout(), "Multica CLI: %s (%s)\n", report.Version.Version, report.Command)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "Multica CLI: unavailable (%s)\n", report.VersionError)
	}
	if report.AuthStatus != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Auth: %s\n", report.AuthStatus)
	}
	if report.DaemonStatus != nil && strings.TrimSpace(report.DaemonStatus.Status) != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Daemon: %s\n", report.DaemonStatus.Status)
	} else if report.DaemonError != "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Daemon: unavailable (%s)\n", report.DaemonError)
	}
	if !report.IntegrationReady {
		fmt.Fprintln(cmd.OutOrStdout(), "Live Multica writes need an authenticated Multica profile.")
	}
	return nil
}

func runMulticaImportIssue(cmd *cobra.Command, args []string) error {
	issue, err := loadMulticaIssue(cmd.Context(), cmd.InOrStdin())
	if err != nil {
		return err
	}
	draft, err := driver.BuildMulticaIssueTeamworkSignal(issue, driver.MulticaIssueSignalOptions{
		Scope:        multicaScope,
		TTL:          multicaTTL,
		WhyTeamwork:  multicaWhyTeamwork,
		EvidenceRefs: multicaEvidenceRefs,
		ContextRefs:  multicaContextRefs,
	})
	if err != nil {
		return err
	}
	if multicaDryRun {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(draft)
	}
	client, principal, err := multicaLocalClient()
	if err != nil {
		return err
	}
	rec, err := client.IngestObserve(contract.ActorID(principal), contract.ObservationEnvelope{
		ExternalID: draft.ExternalID,
		Event: contract.Event{
			Type:    draft.EventType,
			Payload: draft.Payload,
		},
	})
	if err != nil {
		return fmt.Errorf("Local Mnemon observe failed: %w", err)
	}
	if multicaJSON {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(map[string]any{
			"event_type":  draft.EventType,
			"external_id": draft.ExternalID,
			"receipt":     rec,
		})
	}
	fmt.Fprintf(cmd.OutOrStdout(), "imported issue %s as %s seq=%d dup=%v ticked=%v\n", issue.ID, draft.EventType, rec.Seq, rec.Dup, rec.Ticked)
	return nil
}

func runMulticaProjectComment(cmd *cobra.Command, args []string) error {
	if strings.TrimSpace(multicaIssueID) == "" {
		return fmt.Errorf("--issue-id is required")
	}
	content, err := readMulticaTextInput(cmd.InOrStdin(), multicaCommentContent, multicaCommentFile, multicaCommentStdin)
	if err != nil {
		return err
	}
	body := driver.FormatMulticaProjectionComment(multicaCommentTitle, content, multicaCommentEvents)
	comment, err := multicaCLI().AddIssueComment(cmd.Context(), multicaIssueID, body)
	if err != nil {
		return err
	}
	if multicaJSON {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(comment)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "commented issue %s comment=%s\n", multicaIssueID, comment.ID)
	return nil
}

func loadMulticaIssue(ctx context.Context, stdin io.Reader) (driver.MulticaIssue, error) {
	switch {
	case strings.TrimSpace(multicaIssueJSON) != "":
		if multicaIssueJSON == "-" {
			return driver.DecodeMulticaIssue(stdin)
		}
		f, err := os.Open(multicaIssueJSON)
		if err != nil {
			return driver.MulticaIssue{}, err
		}
		defer f.Close()
		return driver.DecodeMulticaIssue(f)
	case strings.TrimSpace(multicaIssueID) != "":
		return multicaCLI().GetIssue(ctx, multicaIssueID)
	default:
		return driver.MulticaIssue{}, fmt.Errorf("one of --issue-id or --issue-json is required")
	}
}

func multicaLocalClient() (*access.Client, string, error) {
	addr := strings.TrimSpace(multicaLocalAddr)
	if addr == "" {
		return nil, "", fmt.Errorf("--addr is required")
	}
	token := strings.TrimSpace(multicaLocalToken)
	if strings.TrimSpace(multicaLocalTokenFile) != "" {
		data, err := os.ReadFile(multicaLocalTokenFile)
		if err != nil {
			return nil, "", fmt.Errorf("read --token-file: %w", err)
		}
		token = strings.TrimSpace(string(data))
	}
	if token != "" {
		return access.NewClientWithToken(addr, token), strings.TrimSpace(multicaLocalPrincipal), nil
	}
	principal := strings.TrimSpace(multicaLocalPrincipal)
	if principal == "" {
		return nil, "", fmt.Errorf("--principal is required when --token is not provided")
	}
	return access.NewClient(addr, contract.ActorID(principal)), principal, nil
}

func multicaCLI() driver.MulticaCLI {
	return driver.MulticaCLI{
		Command:     strings.TrimSpace(multicaBin),
		Profile:     strings.TrimSpace(multicaProfile),
		ServerURL:   strings.TrimSpace(multicaServerURL),
		WorkspaceID: strings.TrimSpace(multicaWorkspaceID),
		Timeout:     multicaTimeout,
		Env:         os.Environ(),
	}
}

func multicaCLICommand(cli driver.MulticaCLI) string {
	if strings.TrimSpace(cli.Command) != "" {
		return strings.TrimSpace(cli.Command)
	}
	return driver.MulticaDefaultCommand
}

func readMulticaTextInput(stdin io.Reader, inline, path string, useStdin bool) (string, error) {
	sources := 0
	if strings.TrimSpace(inline) != "" {
		sources++
	}
	if strings.TrimSpace(path) != "" {
		sources++
	}
	if useStdin {
		sources++
	}
	if sources != 1 {
		return "", fmt.Errorf("exactly one of --content, --content-file, or --content-stdin is required")
	}
	if strings.TrimSpace(inline) != "" {
		return strings.TrimSpace(inline), nil
	}
	if strings.TrimSpace(path) != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(data)), nil
	}
	data, err := io.ReadAll(stdin)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func init() {
	multicaCmd.PersistentFlags().StringVar(&multicaBin, "multica-bin", envDefault("MNEMON_MULTICA_BIN", ""), "Multica CLI path")
	multicaCmd.PersistentFlags().StringVar(&multicaProfile, "multica-profile", envDefault("MNEMON_MULTICA_PROFILE", ""), "Multica CLI profile")
	multicaCmd.PersistentFlags().StringVar(&multicaServerURL, "multica-server-url", envDefault("MNEMON_MULTICA_SERVER_URL", ""), "Multica server URL")
	multicaCmd.PersistentFlags().StringVar(&multicaWorkspaceID, "multica-workspace-id", envDefault("MNEMON_MULTICA_WORKSPACE_ID", ""), "Multica workspace ID")
	multicaCmd.PersistentFlags().DurationVar(&multicaTimeout, "multica-timeout", 30*time.Second, "Multica CLI timeout")
	multicaCmd.PersistentFlags().BoolVar(&multicaJSON, "json", false, "emit JSON")

	multicaImportIssueCmd.Flags().StringVar(&multicaIssueID, "issue-id", "", "Multica issue ID")
	multicaImportIssueCmd.Flags().StringVar(&multicaIssueJSON, "issue-json", "", "read a Multica issue JSON file, or '-' for stdin")
	multicaImportIssueCmd.Flags().StringVar(&multicaScope, "scope", "multica/teamwork", "Mnemon teamwork scope")
	multicaImportIssueCmd.Flags().StringVar(&multicaTTL, "ttl", "30m", "Mnemon teamwork signal TTL")
	multicaImportIssueCmd.Flags().StringVar(&multicaWhyTeamwork, "why-teamwork", "", "natural-language reason this issue needs teamwork")
	multicaImportIssueCmd.Flags().StringArrayVar(&multicaEvidenceRefs, "evidence", nil, "evidence reference; may be repeated")
	multicaImportIssueCmd.Flags().StringArrayVar(&multicaContextRefs, "context-ref", nil, "context reference; may be repeated")
	multicaImportIssueCmd.Flags().StringVar(&multicaLocalAddr, "addr", envDefault("MNEMON_CONTROL_ADDR", "http://127.0.0.1:8787"), "Local Mnemon URL")
	multicaImportIssueCmd.Flags().StringVar(&multicaLocalPrincipal, "principal", envDefault("MNEMON_CONTROL_PRINCIPAL", ""), "local principal")
	multicaImportIssueCmd.Flags().StringVar(&multicaLocalToken, "token", envDefault("MNEMON_CONTROL_TOKEN", ""), "Local Mnemon bearer token")
	multicaImportIssueCmd.Flags().StringVar(&multicaLocalTokenFile, "token-file", envDefault("MNEMON_CONTROL_TOKEN_FILE", ""), "read Local Mnemon bearer token from a file")
	multicaImportIssueCmd.Flags().BoolVar(&multicaDryRun, "dry-run", false, "print the Mnemon observed draft without writing")

	multicaProjectCommentCmd.Flags().StringVar(&multicaIssueID, "issue-id", "", "Multica issue ID")
	multicaProjectCommentCmd.Flags().StringVar(&multicaCommentContent, "content", "", "comment body")
	multicaProjectCommentCmd.Flags().StringVar(&multicaCommentFile, "content-file", "", "read comment body from a file")
	multicaProjectCommentCmd.Flags().BoolVar(&multicaCommentStdin, "content-stdin", false, "read comment body from stdin")
	multicaProjectCommentCmd.Flags().StringVar(&multicaCommentTitle, "title", "", "short Mnemon update title")
	multicaProjectCommentCmd.Flags().StringArrayVar(&multicaCommentEvents, "event", nil, "Mnemon event marker; may be repeated")

	multicaCmd.AddCommand(multicaProbeCmd, multicaImportIssueCmd, multicaProjectCommentCmd)
	multicaCmd.GroupID = groupSpine
	rootCmd.AddCommand(multicaCmd)
}
