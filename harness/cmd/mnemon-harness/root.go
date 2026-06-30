package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var version = "dev"

var rootCmd = &cobra.Command{
	Use:               "mnemon-harness",
	Version:           version,
	Short:             "Experimental Mnemon event-system harness",
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	Long: "Configure the experimental Mnemon event-driven collaboration substrate: Agent Integration, " +
		"Local Mnemon, external connections, and daemon workers. Teamwork is a profile on top of this substrate.",
}

// Command groups are help-only: they change how `--help` lists verbs, never a
// verb path or behavior. Internal/debug commands stay callable but hidden from
// the ordinary product surface.
const (
	groupSpine    = "spine"
	groupAdvanced = "advanced"
)

func init() {
	rootCmd.AddGroup(
		&cobra.Group{ID: groupSpine, Title: "Product commands:"},
		&cobra.Group{ID: groupAdvanced, Title: "Internal/debug commands:"},
	)
	rootCmd.SetHelpCommandGroupID(groupAdvanced)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
