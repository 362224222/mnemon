package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
)

var version = "dev"

var rootCmd = &cobra.Command{
	Use:               "mnemon-acceptance",
	Version:           version,
	Short:             "Mnemon test-only acceptance runner",
	CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	Long: "Run controlled Mnemon acceptance scenarios. This command is test-only: " +
		"it starts, seeds, observes, and reports on product surfaces without defining runtime semantics.",
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
