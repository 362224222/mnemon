package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/cli"
)

var version = "dev"

const helpText = `mnemon-harness is the project-local client for mnemond-managed Teamwork.

Usage:
  mnemon-harness setup [--host auto|codex|claude-code]
  mnemon-harness status|doctor|eject
  mnemon-harness channel create|join|invite|status|remove|leave
  mnemon-harness hook check
  mnemon-harness agent current --json
  mnemon-harness agent resolve no-action|retry|reject --context PATH
  mnemon-harness teamwork offer|accept|decline|deliver|rework|close|cancel

Natural-language content is accepted only through --content-file PATH or stdin
with --content-file -. Use --help for this closed command catalog.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if exit := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); exit != 0 {
		os.Exit(exit)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		if _, err := io.WriteString(stdout, helpText); err != nil {
			return 1
		}
		return 0
	}

	switch args[0] {
	case "-h", "--help", "help":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemon-harness: help accepts no arguments")
			return 2
		}
		if _, err := io.WriteString(stdout, helpText); err != nil {
			return 1
		}
		return 0
	case "--version", "version":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemon-harness: version accepts no arguments")
			return 2
		}
		if _, err := fmt.Fprintf(stdout, "mnemon-harness version %s\n", version); err != nil {
			return 1
		}
		return 0
	case "hook", "agent", "teamwork":
		return cli.New(stdin, stdout, stderr).Run(ctx, args)
	default:
		fmt.Fprintf(stderr, "mnemon-harness: unknown command %q\n", args[0])
		return 2
	}
}
