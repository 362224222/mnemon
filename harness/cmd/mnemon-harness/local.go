package main

import (
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/spf13/cobra"
)

var (
	localRoot                string
	localAddr                string
	localStorePath           string
	localBindingsPath        string
	localAllowNonLoopback    bool
	localIgnoreExternal      bool
	localAllowInsecureRemote bool
	localSyncInterval        time.Duration
)

var localCmd = &cobra.Command{
	Use:   "local",
	Short: "Run and inspect Local Mnemon",
}

var localRunCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Local Mnemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		// The whole boot chain lives in internal/app (app/localboot.go) — shared with the
		// mnemond local governance daemon, so both mains stay behavior-identical.
		boot, err := app.ResolveLocalBoot(projectRoot(), localStorePath, localBindingsPath)
		if err != nil {
			return err
		}
		addr := localAddr
		if !cmd.Flags().Changed("addr") {
			addr = app.ListenAddrFromEndpoint(boot.Config.Endpoint, localAddr)
		}
		if err := app.ValidateListenAddr(addr, localAllowNonLoopback); err != nil {
			return err
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Local Mnemon: ready")
		fmt.Fprintln(cmd.OutOrStdout(), "Remote Workspace: "+app.RemoteWorkspaceStatus(projectRoot()))
		return app.RunLocalHTTPServerWithBindings(cmd.Context(), addr, boot.StorePath, boot.Loaded, app.ServeOptions{
			Loops:               boot.Config.Loops,
			ProjectRoot:         projectRoot(),
			IgnoreExternal:      localIgnoreExternal,
			AllowInsecureRemote: localAllowInsecureRemote,
			SyncInterval:        localSyncInterval,
		}, io.Discard)
	},
}

var localStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Local Mnemon status",
	RunE:  runLocalStatus,
}

var localStopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Show how to stop Local Mnemon",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Fprintln(cmd.OutOrStdout(), "Local Mnemon: stop the running local process to shut down")
		return nil
	},
}

func init() {
	localCmd.PersistentFlags().StringVar(&localRoot, "root", ".", "project root")
	localCmd.PersistentFlags().StringVar(&localStorePath, "store", "", "store path; defaults to the project Local Mnemon store")
	localRunCmd.Flags().StringVar(&localAddr, "addr", "127.0.0.1:8787", "listen address")
	localRunCmd.Flags().StringVar(&localBindingsPath, "bindings", "", "Agent Integration binding file")
	localRunCmd.Flags().DurationVar(&localSyncInterval, "sync-interval", 0, "sync worker cadence (0 = default 30s)")
	localRunCmd.Flags().BoolVar(&localAllowNonLoopback, "allow-nonloopback", false, "explicitly allow listening on a non-loopback address (T1: loopback-only by default)")
	localRunCmd.Flags().BoolVar(&localIgnoreExternal, "ignore-external", false, "boot the standard-only event package registry, ignoring external packages under .mnemon/loops (each ignored package is named on stderr)")
	localRunCmd.Flags().BoolVar(&localAllowInsecureRemote, "allow-insecure-remote", false, "let the background sync worker use a plaintext http:// Remote Workspace endpoint with a non-loopback host (T2: fail-closed by default)")
	_ = localRunCmd.Flags().MarkHidden("bindings")
	localCmd.AddCommand(localRunCmd, localStatusCmd, localStopCmd)
	localCmd.GroupID = groupSpine
	rootCmd.AddCommand(localCmd)
}

func runLocalStatus(cmd *cobra.Command, args []string) error {
	fmt.Fprintln(cmd.OutOrStdout(), "Local Mnemon: ready")
	fmt.Fprintf(cmd.OutOrStdout(), "Store: %s\n", app.ResolveLocalStorePath(projectRoot(), localStorePath))
	fmt.Fprintln(cmd.OutOrStdout(), "Mode: local")
	fmt.Fprintln(cmd.OutOrStdout(), "Remote Workspace: "+app.RemoteWorkspaceStatus(projectRoot()))
	return nil
}

func projectRoot() string {
	if localRoot == "" {
		return "."
	}
	return filepath.Clean(localRoot)
}
