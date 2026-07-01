package main

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	"github.com/spf13/cobra"
)

var (
	connectRoot           string
	connectConfigPath     string
	connectMulticaWS      string
	connectMulticaRuntime string
	connectMnemonhubURL   string
)

var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Configure harness external connections",
}

var connectMulticaCmd = &cobra.Command{
	Use:   "multica --workspace WORKSPACE",
	Short: "Connect Multica as a harness connection",
	RunE:  runConnectMultica,
}

var connectMnemonhubCmd = &cobra.Command{
	Use:   "mnemonhub --endpoint URL",
	Short: "Connect mnemonhub as a harness connection",
	RunE:  runConnectMnemonhub,
}

func init() {
	connectCmd.PersistentFlags().StringVar(&connectRoot, "root", ".", "project root")
	connectCmd.PersistentFlags().StringVar(&connectConfigPath, "config", "", "harness product config path")
	_ = connectCmd.PersistentFlags().MarkHidden("config")
	connectMulticaCmd.Flags().StringVar(&connectMulticaWS, "workspace", "", "Multica workspace")
	connectMulticaCmd.Flags().StringVar(&connectMulticaRuntime, "runtime-binary", productconfig.DefaultMulticaRuntimeBinary, "Multica runtime binary")
	_ = connectMulticaCmd.Flags().MarkHidden("runtime-binary")
	connectMnemonhubCmd.Flags().StringVar(&connectMnemonhubURL, "endpoint", "", "mnemonhub endpoint")
	connectCmd.AddCommand(connectMulticaCmd, connectMnemonhubCmd)
	connectCmd.GroupID = groupSpine
	rootCmd.AddCommand(connectCmd)
}

func runConnectMultica(cmd *cobra.Command, args []string) error {
	if strings.TrimSpace(connectMulticaWS) == "" {
		return fmt.Errorf("connect multica requires --workspace")
	}
	cfg, _, err := loadHarnessProductConfig(connectRoot, connectConfigPath)
	if err != nil {
		return err
	}
	cfg.Connections.Multica = productconfig.MulticaConnection{
		Enabled:       true,
		Workspace:     strings.TrimSpace(connectMulticaWS),
		RuntimeBinary: strings.TrimSpace(connectMulticaRuntime),
	}
	cfg.Daemon.InteractionWatchers = appendUniqueString(cfg.Daemon.InteractionWatchers, productconfig.ConnectionMultica)
	cfg.Daemon.DisplaySurfaces = appendUniqueString(cfg.Daemon.DisplaySurfaces, productconfig.ConnectionMultica)
	if cfg.Sessions.PrimaryActivationCarrier == "" {
		cfg.Sessions.PrimaryActivationCarrier = productconfig.ConnectionMultica
	}
	path, err := saveHarnessProductConfig(connectRoot, connectConfigPath, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Connection: multica ready")
	fmt.Fprintf(cmd.OutOrStdout(), "Harness config: %s\n", path)
	return nil
}

func runConnectMnemonhub(cmd *cobra.Command, args []string) error {
	if strings.TrimSpace(connectMnemonhubURL) == "" {
		return fmt.Errorf("connect mnemonhub requires --endpoint")
	}
	cfg, _, err := loadHarnessProductConfig(connectRoot, connectConfigPath)
	if err != nil {
		return err
	}
	cfg.Connections.Mnemonhub = productconfig.MnemonhubConnection{
		Enabled:  true,
		Endpoint: strings.TrimSpace(connectMnemonhubURL),
	}
	cfg.Daemon.InteractionWatchers = appendUniqueString(cfg.Daemon.InteractionWatchers, productconfig.ConnectionMnemonhub)
	path, err := saveHarnessProductConfig(connectRoot, connectConfigPath, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Connection: mnemonhub ready")
	fmt.Fprintf(cmd.OutOrStdout(), "Harness config: %s\n", path)
	return nil
}

func appendUniqueString(values []string, next string) []string {
	next = strings.TrimSpace(next)
	if next == "" {
		return values
	}
	for _, value := range values {
		if value == next {
			return values
		}
	}
	return append(values, next)
}
