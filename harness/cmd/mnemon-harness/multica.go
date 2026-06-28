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

	multicaProvisionRegistry         string
	multicaProvisionProjectRoot      string
	multicaProvisionProfileName      string
	multicaProvisionRuntimeCommand   string
	multicaProvisionRuntimePath      string
	multicaProvisionAgentPrefix      string
	multicaProvisionRestartDaemon    bool
	multicaProvisionWait             time.Duration
	multicaProvisionControlAddr      string
	multicaProvisionControlToken     string
	multicaProvisionControlTokenFile string
	multicaProvisionManagedRuntime   string
	multicaProvisionManagedCommand   string
	multicaProvisionManagedWorkspace string
	multicaProvisionManagedTimeout   time.Duration
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

var multicaProvisionCmd = &cobra.Command{
	Use:   "provision",
	Short: "Provision Multica runtime profile and Mnemon participant agents",
	RunE:  runMulticaProvision,
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

type multicaProvisionReport struct {
	WorkspaceID     string                            `json:"workspace_id"`
	RuntimeProfile  driver.MulticaRuntimeProfile      `json:"runtime_profile"`
	Runtime         driver.MulticaRuntime             `json:"runtime"`
	Participants    []driver.MulticaParticipantRecord `json:"participants"`
	RegistryPath    string                            `json:"registry_path"`
	RestartedDaemon bool                              `json:"restarted_daemon"`
	CreatedProfile  bool                              `json:"created_profile"`
	CreatedAgents   []string                          `json:"created_agents,omitempty"`
	RestoredAgents  []string                          `json:"restored_agents,omitempty"`
	UpdatedAgents   []string                          `json:"updated_agents,omitempty"`
	UpdatedEnv      []string                          `json:"updated_env,omitempty"`
	Warnings        []string                          `json:"warnings,omitempty"`
}

func runMulticaProbe(cmd *cobra.Command, args []string) error {
	ctx := multicaCommandContext(cmd)
	cli := multicaCLI()
	report := multicaProbeReport{
		Command:     multicaCLICommand(cli),
		Profile:     strings.TrimSpace(multicaProfile),
		ServerURL:   strings.TrimSpace(multicaServerURL),
		WorkspaceID: strings.TrimSpace(multicaWorkspaceID),
	}
	if version, err := cli.Version(ctx); err != nil {
		report.VersionError = err.Error()
	} else {
		report.Version = &version
	}
	if status, err := cli.AuthStatus(ctx); err != nil {
		report.AuthStatus = status
		report.AuthError = err.Error()
	} else {
		report.AuthStatus = status
	}
	if daemon, err := cli.DaemonStatus(ctx); err != nil {
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
	ctx := multicaCommandContext(cmd)
	issue, err := loadMulticaIssue(ctx, cmd.InOrStdin())
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
	comment, err := multicaCLI().AddIssueComment(multicaCommandContext(cmd), multicaIssueID, body)
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

func runMulticaProvision(cmd *cobra.Command, args []string) error {
	ctx := multicaCommandContext(cmd)
	workspaceID := strings.TrimSpace(multicaWorkspaceID)
	if workspaceID == "" {
		return fmt.Errorf("--multica-workspace-id is required for provision")
	}
	if strings.TrimSpace(multicaProvisionControlToken) != "" && strings.TrimSpace(multicaProvisionControlTokenFile) != "" {
		return fmt.Errorf("--mnemon-control-token and --mnemon-control-token-file are mutually exclusive")
	}
	cli := multicaCLI()
	if err := ensureMulticaReady(ctx, cli); err != nil {
		return err
	}
	profileName := strings.TrimSpace(multicaProvisionProfileName)
	if profileName == "" {
		profileName = "mnemon-runtime"
	}
	runtimeCommand := strings.TrimSpace(multicaProvisionRuntimeCommand)
	if runtimeCommand == "" {
		runtimeCommand = "mnemon-multica-runtime"
	}
	registryPath := driver.MulticaRegistryPath(multicaProvisionProjectRoot, multicaProvisionRegistry)
	report := multicaProvisionReport{WorkspaceID: workspaceID, RegistryPath: registryPath}
	profile, created, err := ensureMulticaRuntimeProfile(ctx, cli, profileName, runtimeCommand)
	if err != nil {
		return err
	}
	report.RuntimeProfile = profile
	report.CreatedProfile = created
	if strings.TrimSpace(multicaProvisionRuntimePath) != "" {
		if err := cli.SetRuntimeProfilePath(ctx, profile.ID, multicaProvisionRuntimePath); err != nil {
			return err
		}
	}
	if multicaProvisionRestartDaemon {
		if _, err := cli.Run(ctx, []string{"daemon", "restart"}, ""); err != nil {
			return err
		}
		report.RestartedDaemon = true
	}
	runtime, err := waitMulticaRuntimeForProfile(ctx, cli, profile.ID, multicaProvisionWait)
	if err != nil {
		return err
	}
	report.Runtime = runtime
	participants := driver.DefaultMulticaParticipantRecords(multicaProvisionAgentPrefix)
	agents, err := cli.ListAgents(ctx, true)
	if err != nil {
		return err
	}
	for _, participant := range participants {
		agent, action, err := ensureMulticaParticipantAgent(ctx, cli, agents, runtime.ID, participant)
		if err != nil {
			return err
		}
		participant.AgentID = agent.ID
		report.Participants = driver.UpsertMulticaParticipantRecord(report.Participants, participant)
		switch action {
		case "created":
			report.CreatedAgents = append(report.CreatedAgents, agent.ID)
		case "restored":
			report.RestoredAgents = append(report.RestoredAgents, agent.ID)
		case "updated":
			report.UpdatedAgents = append(report.UpdatedAgents, agent.ID)
		}
		agents = upsertMulticaAgent(agents, agent)
	}
	if err := driver.SaveMulticaRegistry(registryPath, driver.MulticaRegistry{
		SchemaVersion:    1,
		WorkspaceID:      workspaceID,
		RuntimeProfileID: profile.ID,
		RuntimeID:        runtime.ID,
		Participants:     report.Participants,
	}); err != nil {
		return err
	}
	for _, participant := range report.Participants {
		updated, err := ensureMulticaParticipantEnv(ctx, cli, participant, registryPath, workspaceID)
		if err != nil {
			return err
		}
		if updated {
			report.UpdatedEnv = append(report.UpdatedEnv, participant.AgentID)
		}
	}
	if !multicaProvisionRestartDaemon && strings.TrimSpace(multicaProvisionRuntimePath) != "" && created {
		report.Warnings = append(report.Warnings, "if the runtime did not appear, restart the Multica daemon so the per-machine runtime path is loaded")
	}
	if strings.TrimSpace(multicaProvisionControlAddr) == "" {
		report.Warnings = append(report.Warnings, "participant env does not include MNEMON_CONTROL_ADDR; runtime turns can complete but will skip local mnemond ingest")
	}
	if multicaJSON {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "provisioned Multica runtime %s with %d Mnemon participants\n", report.Runtime.ID, len(report.Participants))
	fmt.Fprintf(cmd.OutOrStdout(), "registry: %s\n", report.RegistryPath)
	return nil
}

func ensureMulticaReady(ctx context.Context, cli driver.MulticaCLI) error {
	if _, err := cli.Version(ctx); err != nil {
		return err
	}
	auth, err := cli.AuthStatus(ctx)
	if err != nil {
		return err
	}
	if auth == "" || strings.Contains(strings.ToLower(auth), "not authenticated") {
		return fmt.Errorf("Multica profile is not authenticated")
	}
	status, err := cli.DaemonStatus(ctx)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(status.Status), "running") {
		return fmt.Errorf("Multica daemon is not running")
	}
	return nil
}

func ensureMulticaRuntimeProfile(ctx context.Context, cli driver.MulticaCLI, profileName, runtimeCommand string) (driver.MulticaRuntimeProfile, bool, error) {
	profiles, err := cli.ListRuntimeProfiles(ctx)
	if err != nil {
		return driver.MulticaRuntimeProfile{}, false, err
	}
	for _, profile := range profiles {
		if profile.DisplayName == profileName && profile.ProtocolFamily == "codex" {
			return profile, false, nil
		}
	}
	profile, err := cli.CreateRuntimeProfile(ctx, driver.MulticaCreateRuntimeProfileRequest{
		DisplayName:    profileName,
		Description:    "Mnemon teamwork runtime adapter",
		ProtocolFamily: "codex",
		CommandName:    runtimeCommand,
	})
	return profile, true, err
}

func waitMulticaRuntimeForProfile(ctx context.Context, cli driver.MulticaCLI, profileID string, wait time.Duration) (driver.MulticaRuntime, error) {
	deadline := time.Now().Add(wait)
	for {
		runtimes, err := cli.ListRuntimes(ctx)
		if err != nil {
			return driver.MulticaRuntime{}, err
		}
		for _, runtime := range runtimes {
			if runtime.ProfileID == profileID && strings.EqualFold(runtime.Status, "online") {
				return runtime, nil
			}
		}
		if wait <= 0 || time.Now().After(deadline) {
			return driver.MulticaRuntime{}, fmt.Errorf("no online Multica runtime found for profile %s", profileID)
		}
		select {
		case <-ctx.Done():
			return driver.MulticaRuntime{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func ensureMulticaParticipantAgent(ctx context.Context, cli driver.MulticaCLI, agents []driver.MulticaAgent, runtimeID string, participant driver.MulticaParticipantRecord) (driver.MulticaAgent, string, error) {
	req := driver.MulticaCreateAgentRequest{
		Name:               participant.AgentName,
		Description:        "Mnemon teamwork participant: " + participant.Principal,
		Instructions:       multicaParticipantInstructions(participant),
		RuntimeID:          runtimeID,
		Visibility:         "private",
		MaxConcurrentTasks: 1,
	}
	for _, agent := range agents {
		if agent.Name != participant.AgentName {
			continue
		}
		if strings.TrimSpace(agent.ArchivedAt) != "" {
			restored, err := cli.RestoreAgent(ctx, agent.ID)
			if err != nil {
				return driver.MulticaAgent{}, "", err
			}
			updated, err := cli.UpdateAgent(ctx, restored.ID, req)
			if err != nil {
				return driver.MulticaAgent{}, "", err
			}
			return updated, "restored", nil
		}
		if agent.RuntimeID != runtimeID || agent.Description != req.Description || agent.Instructions != req.Instructions {
			updated, err := cli.UpdateAgent(ctx, agent.ID, req)
			if err != nil {
				return driver.MulticaAgent{}, "", err
			}
			return updated, "updated", nil
		}
		return agent, "reused", nil
	}
	agent, err := cli.CreateAgent(ctx, req)
	if err != nil {
		return driver.MulticaAgent{}, "", err
	}
	return agent, "created", nil
}

func ensureMulticaParticipantEnv(ctx context.Context, cli driver.MulticaCLI, participant driver.MulticaParticipantRecord, registryPath, workspaceID string) (bool, error) {
	if strings.TrimSpace(participant.AgentID) == "" {
		return false, fmt.Errorf("participant %s is missing a Multica agent id", participant.Principal)
	}
	existing, err := cli.GetAgentEnv(ctx, participant.AgentID)
	if err != nil {
		return false, fmt.Errorf("read Multica agent env %s: %w", participant.AgentID, err)
	}
	merged := cloneStringMap(existing)
	for key, value := range multicaParticipantRuntimeEnv(cli, participant, registryPath, workspaceID) {
		merged[key] = value
	}
	if sameStringMap(existing, merged) {
		return false, nil
	}
	if _, err := cli.SetAgentEnv(ctx, participant.AgentID, merged); err != nil {
		return false, fmt.Errorf("write Multica agent env %s: %w", participant.AgentID, err)
	}
	return true, nil
}

func multicaParticipantRuntimeEnv(cli driver.MulticaCLI, participant driver.MulticaParticipantRecord, registryPath, workspaceID string) map[string]string {
	env := map[string]string{}
	addStringEnv(env, "MNEMON_HUB_BACKEND", driver.MulticaHubBackend)
	addStringEnv(env, "MNEMON_MULTICA_REGISTRY", registryPath)
	addStringEnv(env, "MNEMON_MULTICA_WORKSPACE_ID", workspaceID)
	addStringEnv(env, "MNEMON_MULTICA_BIN", multicaCLICommand(cli))
	addStringEnv(env, "MNEMON_MULTICA_PROFILE", multicaProfile)
	addStringEnv(env, "MNEMON_MULTICA_SERVER_URL", multicaServerURL)
	addStringEnv(env, "MNEMON_CONTROL_ADDR", multicaProvisionControlAddr)
	addStringEnv(env, "MNEMON_CONTROL_TOKEN", multicaProvisionControlToken)
	addStringEnv(env, "MNEMON_CONTROL_TOKEN_FILE", multicaProvisionControlTokenFile)
	addStringEnv(env, "MNEMON_CONTROL_PRINCIPAL", participant.Principal)
	addStringEnv(env, "MNEMON_MANAGED_RUNTIME", multicaProvisionManagedRuntime)
	addStringEnv(env, "MNEMON_MANAGED_COMMAND", multicaProvisionManagedCommand)
	addStringEnv(env, "MNEMON_MANAGED_WORKSPACE", multicaProvisionManagedWorkspace)
	if multicaProvisionManagedTimeout > 0 {
		env["MNEMON_MANAGED_TURN_TIMEOUT"] = multicaProvisionManagedTimeout.String()
	}
	return env
}

func addStringEnv(env map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		env[key] = value
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	out := map[string]string{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func multicaParticipantInstructions(participant driver.MulticaParticipantRecord) string {
	return strings.Join([]string{
		"You are the Multica-visible identity for Mnemon principal " + participant.Principal + ".",
		"Treat Multica issues as product-facing task surfaces; Mnemon events remain the teamwork protocol source of truth.",
		"When the Mnemon runtime wakes you, rely on Mnemon-rendered context rather than copying issue metadata into your own prompt.",
	}, "\n")
}

func upsertMulticaAgent(agents []driver.MulticaAgent, next driver.MulticaAgent) []driver.MulticaAgent {
	for i := range agents {
		if agents[i].ID == next.ID {
			agents[i] = next
			return agents
		}
	}
	return append(agents, next)
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

func multicaCommandContext(cmd *cobra.Command) context.Context {
	if cmd != nil && cmd.Context() != nil {
		return cmd.Context()
	}
	return context.Background()
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

	multicaProvisionCmd.Flags().StringVar(&multicaProvisionRegistry, "registry", "", "Multica registry path")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionProjectRoot, "project-root", ".", "project root for the default registry path")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionProfileName, "runtime-profile-name", "mnemon-runtime", "Multica runtime profile display name")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionRuntimeCommand, "runtime-command", "mnemon-multica-runtime", "runtime executable name registered with Multica")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionRuntimePath, "runtime-path", "", "absolute local executable path for the runtime profile")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionAgentPrefix, "agent-prefix", "mnemon", "Multica participant agent name prefix")
	multicaProvisionCmd.Flags().BoolVar(&multicaProvisionRestartDaemon, "restart-daemon", false, "restart the local Multica daemon after setting the runtime path")
	multicaProvisionCmd.Flags().DurationVar(&multicaProvisionWait, "wait", 30*time.Second, "time to wait for the runtime to appear online")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionControlAddr, "mnemon-control-addr", envDefault("MNEMON_CONTROL_ADDR", ""), "Local Mnemon URL injected into participant runtime env")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionControlToken, "mnemon-control-token", envDefault("MNEMON_CONTROL_TOKEN", ""), "Local Mnemon bearer token injected into participant runtime env")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionControlTokenFile, "mnemon-control-token-file", envDefault("MNEMON_CONTROL_TOKEN_FILE", ""), "Local Mnemon bearer token file injected into participant runtime env")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionManagedRuntime, "managed-runtime", envDefault("MNEMON_MANAGED_RUNTIME", ""), "managed agent runtime injected into participant env (noop or codex-appserver)")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionManagedCommand, "managed-command", envDefault("MNEMON_MANAGED_COMMAND", ""), "managed runtime command injected into participant env")
	multicaProvisionCmd.Flags().StringVar(&multicaProvisionManagedWorkspace, "managed-workspace", envDefault("MNEMON_MANAGED_WORKSPACE", ""), "managed runtime workspace injected into participant env")
	multicaProvisionCmd.Flags().DurationVar(&multicaProvisionManagedTimeout, "managed-turn-timeout", 0, "managed runtime turn timeout injected into participant env")

	multicaCmd.AddCommand(multicaProbeCmd, multicaProvisionCmd, multicaImportIssueCmd, multicaProjectCommentCmd)
	multicaCmd.GroupID = groupSpine
	rootCmd.AddCommand(multicaCmd)
}
