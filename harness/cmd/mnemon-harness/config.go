package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	"github.com/spf13/cobra"
)

var (
	configRoot string
	configPath string
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Inspect harness product configuration",
}

var configValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate harness product configuration",
	RunE:  runConfigValidate,
}

var configOpenCmd = &cobra.Command{
	Use:   "open",
	Short: "Open harness product configuration",
	RunE:  runConfigOpen,
}

func init() {
	configCmd.PersistentFlags().StringVar(&configRoot, "root", ".", "project root")
	configCmd.PersistentFlags().StringVar(&configPath, "config", "", "harness product config path")
	_ = configCmd.PersistentFlags().MarkHidden("config")
	configCmd.AddCommand(configValidateCmd, configOpenCmd)
	configCmd.GroupID = groupSpine
	rootCmd.AddCommand(configCmd)
}

func runConfigValidate(cmd *cobra.Command, args []string) error {
	path := resolvedProductConfigPath()
	cfg, err := productconfig.Load(path)
	if err == nil {
		fmt.Fprintf(cmd.OutOrStdout(), "Harness config: valid %s\n", path)
		fmt.Fprintf(cmd.OutOrStdout(), "Participants: %d\n", len(cfg.Participants))
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	legacy, found, err := productconfig.FromLegacy(filepath.Clean(configRoot))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Harness config is not configured")
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Harness config: valid legacy bridge")
	fmt.Fprintf(cmd.OutOrStdout(), "Participants: %d\n", len(legacy.Participants))
	return nil
}

func runConfigOpen(cmd *cobra.Command, args []string) error {
	path := resolvedProductConfigPath()
	editor := strings.TrimSpace(os.Getenv("EDITOR"))
	if editor == "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Harness config: %s\n", path)
		return nil
	}
	open := exec.CommandContext(cmd.Context(), editor, path)
	open.Stdin = os.Stdin
	open.Stdout = cmd.OutOrStdout()
	open.Stderr = cmd.ErrOrStderr()
	return open.Run()
}

func resolvedProductConfigPath() string {
	return productconfig.DefaultPath(filepath.Clean(configRoot), configPath)
}
