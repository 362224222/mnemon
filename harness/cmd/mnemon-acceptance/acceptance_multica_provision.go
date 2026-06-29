package main

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/spf13/cobra"
)

var (
	acceptanceMulticaProvisionHarnessCommand     string
	acceptanceMulticaProvisionMulticaBin         string
	acceptanceMulticaProvisionProfile            string
	acceptanceMulticaProvisionServerURL          string
	acceptanceMulticaProvisionWorkspaceID        string
	acceptanceMulticaProvisionRegistry           string
	acceptanceMulticaProvisionProjectRoot        string
	acceptanceMulticaProvisionProfileName        string
	acceptanceMulticaProvisionRuntimeCommand     string
	acceptanceMulticaProvisionRuntimePath        string
	acceptanceMulticaProvisionAgentPrefix        string
	acceptanceMulticaProvisionRestartDaemon      bool
	acceptanceMulticaProvisionWait               time.Duration
	acceptanceMulticaProvisionControlAddr        string
	acceptanceMulticaProvisionControlToken       string
	acceptanceMulticaProvisionControlTokenFile   string
	acceptanceMulticaProvisionInjectedHarnessBin string
	acceptanceMulticaProvisionManagedRuntime     string
	acceptanceMulticaProvisionManagedCommand     string
	acceptanceMulticaProvisionManagedWorkspace   string
	acceptanceMulticaProvisionManagedTimeout     time.Duration
)

type acceptanceCommandRunner func(ctx context.Context, command string, args []string, stdout, stderr io.Writer) error

var runAcceptanceMulticaProvisionHarness acceptanceCommandRunner = execAcceptanceCommand

var acceptanceMulticaProvisionCmd = &cobra.Command{
	Use:   "multica-provision",
	Short: "Run test-only Multica runtime and agent provisioning",
	RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(acceptanceMulticaProvisionWorkspaceID) == "" {
			return fmt.Errorf("--multica-workspace-id is required")
		}
		command := strings.TrimSpace(acceptanceMulticaProvisionHarnessCommand)
		if command == "" {
			command = "mnemon-harness"
		}
		return runAcceptanceMulticaProvisionHarness(
			cmd.Context(),
			command,
			buildAcceptanceMulticaProvisionArgs(),
			cmd.OutOrStdout(),
			cmd.ErrOrStderr(),
		)
	},
}

func init() {
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionHarnessCommand, "mnemon-harness-bin", "mnemon-harness", "mnemon-harness command used for the hidden provisioning bridge")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionMulticaBin, "multica-bin", multicaAcceptanceEnvDefault("MNEMON_MULTICA_BIN", ""), "Multica CLI path")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionProfile, "multica-profile", multicaAcceptanceEnvDefault("MNEMON_MULTICA_PROFILE", ""), "Multica CLI profile")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionServerURL, "multica-server-url", multicaAcceptanceEnvDefault("MNEMON_MULTICA_SERVER_URL", ""), "Multica server URL")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionWorkspaceID, "multica-workspace-id", multicaAcceptanceEnvDefault("MNEMON_MULTICA_WORKSPACE_ID", ""), "Multica workspace ID")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionRegistry, "registry", "", "Multica participant registry path")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionProjectRoot, "project-root", ".", "project root for the default registry path")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionProfileName, "runtime-profile-name", "mnemon-runtime", "Multica runtime profile display name")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionRuntimeCommand, "runtime-command", driver.MulticaRuntimeCommandName, "runtime executable name registered with Multica")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionRuntimePath, "runtime-path", "", "absolute local executable path for the runtime profile")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionAgentPrefix, "agent-prefix", "mnemon", "Multica participant agent name prefix")
	acceptanceMulticaProvisionCmd.Flags().BoolVar(&acceptanceMulticaProvisionRestartDaemon, "restart-daemon", false, "restart the local Multica daemon after setting the runtime path")
	acceptanceMulticaProvisionCmd.Flags().DurationVar(&acceptanceMulticaProvisionWait, "wait", 30*time.Second, "time to wait for the runtime to appear online")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionControlAddr, "mnemon-control-addr", multicaAcceptanceEnvDefault("MNEMON_CONTROL_ADDR", ""), "Local Mnemon URL injected into participant runtime env")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionControlToken, "mnemon-control-token", multicaAcceptanceEnvDefault("MNEMON_CONTROL_TOKEN", ""), "Local Mnemon bearer token injected into participant runtime env")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionControlTokenFile, "mnemon-control-token-file", multicaAcceptanceEnvDefault("MNEMON_CONTROL_TOKEN_FILE", ""), "Local Mnemon bearer token file injected into participant runtime env")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionInjectedHarnessBin, "harness-bin", multicaAcceptanceEnvDefault("MNEMON_HARNESS_BIN", ""), "mnemon-harness executable injected into participant runtime env")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionManagedRuntime, "managed-runtime", multicaAcceptanceEnvDefault("MNEMON_MANAGED_RUNTIME", ""), "managed agent runtime injected into participant env")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionManagedCommand, "managed-command", multicaAcceptanceEnvDefault("MNEMON_MANAGED_COMMAND", ""), "managed runtime command injected into participant env")
	acceptanceMulticaProvisionCmd.Flags().StringVar(&acceptanceMulticaProvisionManagedWorkspace, "managed-workspace", multicaAcceptanceEnvDefault("MNEMON_MANAGED_WORKSPACE", ""), "managed runtime workspace injected into participant env")
	acceptanceMulticaProvisionCmd.Flags().DurationVar(&acceptanceMulticaProvisionManagedTimeout, "managed-turn-timeout", 0, "managed runtime turn timeout injected into participant env")
	rootCmd.AddCommand(acceptanceMulticaProvisionCmd)
}

func buildAcceptanceMulticaProvisionArgs() []string {
	args := []string{"multica"}
	args = appendFlag(args, "--multica-bin", acceptanceMulticaProvisionMulticaBin)
	args = appendFlag(args, "--multica-profile", acceptanceMulticaProvisionProfile)
	args = appendFlag(args, "--multica-server-url", acceptanceMulticaProvisionServerURL)
	args = appendFlag(args, "--multica-workspace-id", acceptanceMulticaProvisionWorkspaceID)
	args = append(args, "--json", "provision", "--acceptance-bridge")
	args = appendFlag(args, "--registry", acceptanceMulticaProvisionRegistry)
	args = appendFlag(args, "--project-root", acceptanceMulticaProvisionProjectRoot)
	args = appendFlag(args, "--runtime-profile-name", acceptanceMulticaProvisionProfileName)
	args = appendFlag(args, "--runtime-command", acceptanceMulticaProvisionRuntimeCommand)
	args = appendFlag(args, "--runtime-path", acceptanceMulticaProvisionRuntimePath)
	args = appendFlag(args, "--agent-prefix", acceptanceMulticaProvisionAgentPrefix)
	if acceptanceMulticaProvisionRestartDaemon {
		args = append(args, "--restart-daemon")
	}
	if acceptanceMulticaProvisionWait > 0 {
		args = appendFlag(args, "--wait", acceptanceMulticaProvisionWait.String())
	}
	args = appendFlag(args, "--mnemon-control-addr", acceptanceMulticaProvisionControlAddr)
	args = appendFlag(args, "--mnemon-control-token", acceptanceMulticaProvisionControlToken)
	args = appendFlag(args, "--mnemon-control-token-file", acceptanceMulticaProvisionControlTokenFile)
	args = appendFlag(args, "--harness-bin", acceptanceMulticaProvisionInjectedHarnessBin)
	args = appendFlag(args, "--managed-runtime", acceptanceMulticaProvisionManagedRuntime)
	args = appendFlag(args, "--managed-command", acceptanceMulticaProvisionManagedCommand)
	args = appendFlag(args, "--managed-workspace", acceptanceMulticaProvisionManagedWorkspace)
	if acceptanceMulticaProvisionManagedTimeout > 0 {
		args = appendFlag(args, "--managed-turn-timeout", acceptanceMulticaProvisionManagedTimeout.String())
	}
	return args
}

func appendFlag(args []string, flag, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return args
	}
	return append(args, flag, value)
}

func execAcceptanceCommand(ctx context.Context, command string, args []string, stdout, stderr io.Writer) error {
	proc := exec.CommandContext(ctx, command, args...)
	proc.Stdout = stdout
	proc.Stderr = stderr
	return proc.Run()
}
