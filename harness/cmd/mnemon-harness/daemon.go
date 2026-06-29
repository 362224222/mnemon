package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
	"github.com/spf13/cobra"
)

var (
	daemonRoot                string
	daemonAddr                string
	daemonStorePath           string
	daemonBindingsPath        string
	daemonAllowNonLoopback    bool
	daemonIgnoreExternal      bool
	daemonAllowInsecureRemote bool
	daemonSyncInterval        time.Duration
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run and inspect the harness daemon",
}

var daemonStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the harness daemon",
	RunE:  runDaemonStart,
}

var daemonStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Show how to stop the harness daemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Fprintln(cmd.OutOrStdout(), "Harness daemon: stop the running process to shut down")
		return nil
	},
}

var daemonStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show harness daemon status",
	RunE:  runDaemonStatus,
}

func init() {
	daemonCmd.PersistentFlags().StringVar(&daemonRoot, "root", ".", "project root")
	daemonCmd.PersistentFlags().StringVar(&daemonStorePath, "store", "", "store path; defaults to the project Local Mnemon store")
	daemonStartCmd.Flags().StringVar(&daemonAddr, "addr", "127.0.0.1:8787", "listen address")
	daemonStartCmd.Flags().StringVar(&daemonBindingsPath, "bindings", "", "Agent Integration binding file")
	daemonStartCmd.Flags().DurationVar(&daemonSyncInterval, "sync-interval", 0, "sync worker cadence (0 = default 30s)")
	daemonStartCmd.Flags().BoolVar(&daemonAllowNonLoopback, "allow-nonloopback", false, "explicitly allow listening on a non-loopback address")
	daemonStartCmd.Flags().BoolVar(&daemonIgnoreExternal, "ignore-external", false, "ignore external event packages under .mnemon/loops")
	daemonStartCmd.Flags().BoolVar(&daemonAllowInsecureRemote, "allow-insecure-remote", false, "allow plaintext Remote Workspace endpoint when explicitly configured")
	_ = daemonStartCmd.Flags().MarkHidden("bindings")
	daemonCmd.AddCommand(daemonStartCmd, daemonStopCmd, daemonStatusCmd)
	daemonCmd.GroupID = groupSpine
	rootCmd.AddCommand(daemonCmd)
}

func runDaemonStart(cmd *cobra.Command, args []string) error {
	root := daemonProjectRoot()
	boot, err := app.ResolveLocalBoot(root, daemonStorePath, daemonBindingsPath)
	if err != nil {
		return err
	}
	addr := daemonAddr
	if !cmd.Flags().Changed("addr") {
		addr = app.ListenAddrFromEndpoint(boot.Config.Endpoint, daemonAddr)
	}
	if err := app.ValidateListenAddr(addr, daemonAllowNonLoopback); err != nil {
		return err
	}
	if err := writeConfiguredDaemonSnapshot(root, time.Now().UTC()); err != nil {
		return err
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Harness daemon: ready")
	fmt.Fprintln(cmd.OutOrStdout(), "Remote Workspace: "+app.RemoteWorkspaceStatus(root))
	return app.RunLocalHTTPServerWithBindings(cmd.Context(), addr, boot.StorePath, boot.Loaded, app.ServeOptions{
		Loops:               boot.Config.Loops,
		ProjectRoot:         root,
		IgnoreExternal:      daemonIgnoreExternal,
		AllowInsecureRemote: daemonAllowInsecureRemote,
		SyncInterval:        daemonSyncInterval,
	}, io.Discard)
}

func runDaemonStatus(cmd *cobra.Command, args []string) error {
	root := daemonProjectRoot()
	if cfg, err := productconfig.Load(productconfig.DefaultPath(root, "")); err == nil {
		fmt.Fprintln(cmd.OutOrStdout(), "Harness config: configured")
		writeDaemonRoleSummary(cmd.OutOrStdout(), cfg)
	} else if legacy, found, legacyErr := productconfig.FromLegacy(root); legacyErr == nil && found {
		fmt.Fprintln(cmd.OutOrStdout(), "Harness config: legacy bridge")
		writeDaemonRoleSummary(cmd.OutOrStdout(), legacy)
	} else {
		fmt.Fprintln(cmd.OutOrStdout(), "Harness config: not configured")
	}
	writeDaemonSnapshotSummary(cmd.OutOrStdout(), root)
	if cfg, err := app.ReadLocalConfig(root); err == nil {
		if _, ok := localServiceStatus(root, cfg, cfg.Principal); ok {
			fmt.Fprintln(cmd.OutOrStdout(), "Harness daemon: ready")
			return nil
		}
	}
	fmt.Fprintln(cmd.OutOrStdout(), "Harness daemon: not running")
	return nil
}

func writeDaemonRoleSummary(out io.Writer, cfg productconfig.Config) {
	summary := daemon.RoleSummary(cfg)
	fmt.Fprintf(out, "Harness daemon roles: watchers=%d drive=%d surfaces=%d\n", summary.InteractionWatchers, summary.DriveSources, summary.ProjectionSurfaces)
	details := daemon.RoleDetails(cfg)
	if len(details) == 0 {
		return
	}
	fmt.Fprintln(out, "Harness daemon role details:")
	for _, role := range details {
		fmt.Fprintf(out, "  - %s [%s]: %s=%s boundary=%s\n", role.WorkerName, role.Kind, role.Label, role.Value, role.Boundary)
	}
}

func writeConfiguredDaemonSnapshot(root string, now time.Time) error {
	cfg, ok, err := loadDaemonSnapshotConfig(root)
	if err != nil || !ok {
		return err
	}
	snapshot := daemon.ConfiguredSnapshot(cfg, now)
	if len(snapshot.Workers) == 0 {
		return nil
	}
	return daemon.NewFileSnapshotStore(daemon.StatusSnapshotPath(root, "")).Save(snapshot)
}

func loadDaemonSnapshotConfig(root string) (productconfig.Config, bool, error) {
	configPath := productconfig.DefaultPath(root, "")
	if cfg, err := productconfig.Load(configPath); err == nil {
		return cfg, true, nil
	} else if !os.IsNotExist(err) {
		return productconfig.Config{}, false, err
	} else if legacy, found, legacyErr := productconfig.FromLegacy(root); legacyErr == nil && found {
		return legacy, true, nil
	} else if legacyErr != nil {
		return productconfig.Config{}, false, legacyErr
	}
	return productconfig.Config{}, false, nil
}

func writeDaemonSnapshotSummary(out io.Writer, root string) {
	snapshot, ok, err := daemon.NewFileSnapshotStore(daemon.StatusSnapshotPath(root, "")).Load()
	if err != nil {
		fmt.Fprintf(out, "Harness daemon snapshot: unavailable (%v)\n", err)
		return
	}
	if !ok {
		return
	}
	fmt.Fprintf(out, "Harness daemon snapshot: workers=%d started=%s\n", len(snapshot.Workers), formatDaemonTime(snapshot.StartedAt))
	names := make([]string, 0, len(snapshot.Workers))
	for name := range snapshot.Workers {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		worker := snapshot.Workers[name]
		fmt.Fprintf(out, "  - %s [%s]: %s", name, worker.Kind, worker.Status)
		if worker.Message != "" {
			fmt.Fprintf(out, " (%s)", worker.Message)
		}
		if worker.Error != "" {
			fmt.Fprintf(out, " error=%s", worker.Error)
		}
		if !worker.UpdatedAt.IsZero() {
			fmt.Fprintf(out, " updated=%s", formatDaemonTime(worker.UpdatedAt))
		}
		fmt.Fprintln(out)
	}
}

func formatDaemonTime(ts time.Time) string {
	if ts.IsZero() {
		return "unknown"
	}
	return ts.UTC().Format(time.RFC3339)
}

func daemonProjectRoot() string {
	if daemonRoot == "" {
		return "."
	}
	return filepath.Clean(daemonRoot)
}
