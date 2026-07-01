package main

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	"github.com/spf13/cobra"
)

var doctorRoot string

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Check harness event-system readiness",
	RunE:  runDoctor,
}

func init() {
	doctorCmd.Flags().StringVar(&doctorRoot, "root", ".", "project root")
	doctorCmd.GroupID = groupSpine
	rootCmd.AddCommand(doctorCmd)
}

func runDoctor(cmd *cobra.Command, args []string) error {
	root := strings.TrimSpace(doctorRoot)
	if root == "" {
		root = "."
	}
	cfg, cfgStatus, cfgDetail := doctorProductConfig(root)
	fmt.Fprintln(cmd.OutOrStdout(), "Harness doctor")
	fmt.Fprintf(cmd.OutOrStdout(), "- Product config: %s", cfgStatus)
	if cfgDetail != "" {
		fmt.Fprintf(cmd.OutOrStdout(), " (%s)", cfgDetail)
	}
	fmt.Fprintln(cmd.OutOrStdout())
	if cfgStatus == "configured" || cfgStatus == "legacy bridge" {
		fmt.Fprintf(cmd.OutOrStdout(), "- Participants: %d\n", len(cfg.Participants))
		fmt.Fprintf(cmd.OutOrStdout(), "- Daemon roles: watchers=%d drive=%d surfaces=%d\n", len(cfg.Daemon.InteractionWatchers), len(cfg.Daemon.DriveSources), len(cfg.Daemon.DisplaySurfaces))
		fmt.Fprintf(cmd.OutOrStdout(), "- Connections: %s\n", doctorConnections(cfg))
	}
	fmt.Fprintf(cmd.OutOrStdout(), "- Local Mnemon config: %s\n", doctorLocalConfig(root))
	fmt.Fprintf(cmd.OutOrStdout(), "- Daemon snapshot: %s\n", doctorDaemonSnapshot(root))
	return nil
}

func doctorProductConfig(root string) (productconfig.Config, string, string) {
	cfg, err := productconfig.Load(productconfig.DefaultPath(root, ""))
	if err == nil {
		return cfg, "configured", ""
	}
	legacy, found, legacyErr := productconfig.FromLegacy(root)
	if legacyErr != nil {
		return productconfig.Config{}, "invalid", legacyErr.Error()
	}
	if found {
		return legacy, "legacy bridge", ""
	}
	return productconfig.Config{}, "missing", err.Error()
}

func doctorConnections(cfg productconfig.Config) string {
	var out []string
	if cfg.Connections.Multica.Enabled {
		out = append(out, "multica")
	}
	if cfg.Connections.Mnemonhub.Enabled {
		out = append(out, "mnemonhub")
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, ",")
}

func doctorLocalConfig(root string) string {
	if _, err := app.ReadLocalConfig(root); err != nil {
		return "missing"
	}
	return "configured"
}

func doctorDaemonSnapshot(root string) string {
	snapshot, ok, err := daemon.NewFileSnapshotStore(daemon.StatusSnapshotPath(root, "")).Load()
	if err != nil {
		return "invalid: " + err.Error()
	}
	if !ok {
		return "missing"
	}
	return fmt.Sprintf("workers=%d", len(snapshot.Workers))
}
