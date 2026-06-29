package main

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/ui"
	"github.com/spf13/cobra"
)

var towerDump bool

// towerCmd is the Agent Control Tower (P6, D5: TUI-only, command name `tower`) — the human-visible
// boundary over the agent field. It renders the four §3.3 pages (GOAL/FIELD/INBOX/LEDGER) read-only.
var towerCmd = &cobra.Command{
	Use:    "tower",
	Short:  "Agent Control Tower — the four-page human boundary over the agent field (GOAL/FIELD/INBOX/LEDGER)",
	Hidden: true,
	RunE:   runTower,
}

func init() {
	towerCmd.Flags().BoolVar(&towerDump, "dump", false, "render a one-shot read-only snapshot of the four pages and exit (headless/scriptable)")
	towerCmd.GroupID = groupAdvanced
	rootCmd.AddCommand(towerCmd)
}

// runTower assembles the read-only Tower view and renders it. READ-ONLY: it never writes or Ticks. It
// opens the local runtime directly (the facade needs cross-actor reads the per-actor channel cannot
// serve), so it requires the local daemon to be STOPPED — single-writer, S11. The live-while-serving
// Tower (a channel read-verb or in-daemon rendering) is a deployment decision deferred to P5/operator;
// `--dump` is the headless acceptance surface that works today.
func runTower(cmd *cobra.Command, args []string) error {
	root := projectRoot()
	boot, err := app.ResolveLocalBoot(root, "", "")
	if err != nil {
		return err
	}
	catalog := app.SyncImportCatalog(root, cmd.ErrOrStderr()) // the boot catalog (embedded + external packages)
	rt, err := app.OpenLocalRuntime(boot.StorePath, boot.Loaded, boot.Config.Loops, catalog)
	if err != nil {
		return fmt.Errorf("open Local Mnemon (the Tower needs exclusive store access — is the daemon running?): %w", err)
	}
	defer rt.Close()

	view, err := app.BuildTowerView(rt, boot.Loaded.Bindings)
	if err != nil {
		return err
	}
	// The interactive loop is a follow-up (presentation only — all state is in the pure TowerModel);
	// today both forms render the four-page snapshot, with --dump the explicit headless/scriptable mode.
	fmt.Fprint(cmd.OutOrStdout(), ui.NewTowerModel(view).RenderAll())
	return nil
}
