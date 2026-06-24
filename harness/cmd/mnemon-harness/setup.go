package main

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/spf13/cobra"
)

var (
	setupRoot           string
	setupProjectRoot    string
	setupHost           string
	setupLoops          []string
	setupPrincipal      string
	setupControlURL     string
	setupActorKind      string
	setupUseToken       bool
	setupDryRun         bool
	setupThinRenderShim bool
)

// setup is the everyday install front door: it installs the static R1 host shim and wires the Local
// Mnemon channel artifacts a host agent uses. --loop enables optional event package scope; it does not
// project host assets on the R1 path.
var setupCmd = &cobra.Command{
	Use:   "setup --host HOST [--loop LOOP ...]",
	Short: "Install Agent Integration for a host",
	RunE: func(cmd *cobra.Command, args []string) error {
		_, err := app.New(setupRoot).Setup(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), app.SetupOptions{
			Host:           setupHost,
			Loops:          selectedSetupLoops(),
			ControlURL:     setupControlURL,
			Principal:      setupPrincipal,
			ActorKind:      setupActorKind,
			UseToken:       setupUseToken,
			TokenExplicit:  cmd.Flags().Changed("token"),
			ProjectRoot:    setupProjectRoot,
			DryRun:         setupDryRun,
			ThinRenderShim: setupThinRenderShim,
		})
		return err
	},
}

var setupStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Report Agent Integration setup health",
	RunE: func(cmd *cobra.Command, args []string) error {
		lines, err := app.New(setupRoot).SetupStatus(setupProjectRoot, setupPrincipal)
		if err != nil {
			return err
		}
		for _, l := range lines {
			fmt.Fprintln(cmd.OutOrStdout(), l)
		}
		return nil
	},
}

var setupUninstallCmd = &cobra.Command{
	Use:   "uninstall --host HOST --loop LOOP [--loop LOOP ...] --principal PRINCIPAL",
	Short: "Uninstall Agent Integration assets for a principal",
	RunE: func(cmd *cobra.Command, args []string) error {
		return app.New(setupRoot).SetupUninstall(cmd.Context(), cmd.OutOrStdout(), cmd.ErrOrStderr(), app.SetupOptions{
			Host:        setupHost,
			Loops:       selectedSetupLoops(),
			Principal:   setupPrincipal,
			ProjectRoot: setupProjectRoot,
		})
	},
}

func init() {
	setupCmd.PersistentFlags().StringVar(&setupRoot, "root", ".", "repository root containing harness declarations")
	setupCmd.PersistentFlags().StringVar(&setupProjectRoot, "project-root", "", "project root for Agent Integration artifacts (defaults to root)")
	setupCmd.PersistentFlags().StringVar(&setupHost, "host", "", "Agent Integration host id")
	setupCmd.PersistentFlags().StringArrayVar(&setupLoops, "loop", nil, "event package id to enable (e.g. assignment or an external package); may be repeated")
	setupCmd.PersistentFlags().StringVar(&setupPrincipal, "principal", "", "Agent Integration principal")

	setupCmd.Flags().StringVar(&setupControlURL, "control-url", "", "Local Mnemon endpoint URL")
	setupCmd.Flags().StringVar(&setupActorKind, "actor-kind", "host-agent", "agent kind: host-agent or control-agent")
	_ = setupCmd.Flags().MarkHidden("actor-kind")
	setupCmd.Flags().BoolVar(&setupUseToken, "token", true, "generate a local access token")
	setupCmd.Flags().BoolVar(&setupDryRun, "dry-run", false, "print changes without writing")
	setupCmd.Flags().BoolVar(&setupThinRenderShim, "thin-render-shim", true, "install R1 static render hooks")
	_ = setupCmd.Flags().MarkHidden("thin-render-shim")

	setupCmd.AddCommand(setupStatusCmd, setupUninstallCmd)
	setupCmd.GroupID = groupSpine
	rootCmd.AddCommand(setupCmd)
}

// selectedSetupLoops dedupes the repeated --loop flag.
func selectedSetupLoops() []string {
	seen := map[string]bool{}
	var loops []string
	for _, loop := range setupLoops {
		if loop == "" || seen[loop] {
			continue
		}
		seen[loop] = true
		loops = append(loops, loop)
	}
	return loops
}
