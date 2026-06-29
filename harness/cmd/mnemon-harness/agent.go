package main

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	"github.com/spf13/cobra"
)

var (
	agentRoot        string
	agentConfigPath  string
	agentPrincipal   string
	agentDisplayName string
	agentRole        string
	agentRuntimeKind string
	agentRuntimeMode string
)

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Manage harness agents",
}

var agentAddCmd = &cobra.Command{
	Use:   "add --principal PRINCIPAL",
	Short: "Add one harness agent",
	RunE:  runAgentAdd,
}

var agentListCmd = &cobra.Command{
	Use:   "list",
	Short: "List harness agents",
	RunE:  runAgentList,
}

func init() {
	agentCmd.PersistentFlags().StringVar(&agentRoot, "root", ".", "project root")
	agentCmd.PersistentFlags().StringVar(&agentConfigPath, "config", "", "harness product config path")
	_ = agentCmd.PersistentFlags().MarkHidden("config")
	agentAddCmd.Flags().StringVar(&agentPrincipal, "principal", "", "agent principal")
	agentAddCmd.Flags().StringVar(&agentDisplayName, "display-name", "", "agent display name")
	agentAddCmd.Flags().StringVar(&agentRole, "role", "", "agent role")
	agentAddCmd.Flags().StringVar(&agentRuntimeKind, "runtime-kind", productconfig.RuntimeKindCodex, "host runtime kind")
	agentAddCmd.Flags().StringVar(&agentRuntimeMode, "runtime-mode", productconfig.RuntimeModeManagedOrHost, "host runtime mode")
	_ = agentAddCmd.Flags().MarkHidden("runtime-kind")
	_ = agentAddCmd.Flags().MarkHidden("runtime-mode")
	agentCmd.AddCommand(agentAddCmd, agentListCmd)
	agentCmd.GroupID = groupSpine
	rootCmd.AddCommand(agentCmd)
}

func runAgentAdd(cmd *cobra.Command, args []string) error {
	principal := strings.TrimSpace(agentPrincipal)
	if principal == "" {
		return fmt.Errorf("agent add requires --principal")
	}
	cfg, _, err := loadHarnessProductConfig(agentRoot, agentConfigPath)
	if err != nil {
		return err
	}
	next := productconfig.Participant{
		Principal:   principal,
		DisplayName: strings.TrimSpace(agentDisplayName),
		Role:        strings.TrimSpace(agentRole),
		HostRuntime: productconfig.HostRuntime{
			Kind: strings.TrimSpace(agentRuntimeKind),
			Mode: strings.TrimSpace(agentRuntimeMode),
		},
	}
	cfg.Participants = upsertProductParticipant(cfg.Participants, next)
	if len(cfg.Daemon.DriveSources) == 0 {
		cfg.Daemon.DriveSources = []string{productconfig.DriveManagedLocal}
	}
	path, err := saveHarnessProductConfig(agentRoot, agentConfigPath, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Agent: added %s\n", principal)
	fmt.Fprintf(cmd.OutOrStdout(), "Harness config: %s\n", path)
	return nil
}

func runAgentList(cmd *cobra.Command, args []string) error {
	cfg, path, err := loadHarnessProductConfig(agentRoot, agentConfigPath)
	if err != nil {
		return err
	}
	if len(cfg.Participants) == 0 {
		fmt.Fprintln(cmd.OutOrStdout(), "Agents: none configured")
		fmt.Fprintf(cmd.OutOrStdout(), "Harness config: %s\n", path)
		return nil
	}
	for _, participant := range cfg.Participants {
		label := participant.Principal
		if participant.DisplayName != "" {
			label += " (" + participant.DisplayName + ")"
		}
		if participant.Role != "" {
			label += " - " + participant.Role
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Agent: %s\n", label)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "Harness config: %s\n", path)
	return nil
}

func upsertProductParticipant(participants []productconfig.Participant, next productconfig.Participant) []productconfig.Participant {
	for i := range participants {
		if participants[i].Principal == next.Principal {
			participants[i] = next
			return participants
		}
	}
	return append(participants, next)
}
