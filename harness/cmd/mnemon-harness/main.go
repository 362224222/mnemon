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
  mnemon-harness setup [--host auto|codex|claude-code] [--project-root DIR]
  mnemon-harness status
  mnemon-harness eject [--host auto|codex|claude-code] [--project-root DIR]
  mnemon-harness --help
  mnemon-harness --version

Managed Agent commands are installed through the canonical Skill and Guide
and are intentionally absent from ordinary help.
`

type setupRunner func(context.Context, []string, io.Writer, io.Writer, string) int
type ejectRunner func(context.Context, []string, io.Writer, io.Writer, string) int
type statusRunner func(context.Context, []string, io.Writer, io.Writer, string) int

type commandRunners struct {
	setup  setupRunner
	eject  ejectRunner
	status statusRunner
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if exit := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); exit != 0 {
		os.Exit(exit)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return runWithCommandRunners(ctx, args, stdin, stdout, stderr,
		commandRunners{setup: cli.RunSetup, eject: cli.RunEject, status: cli.RunStatus})
}

func runWithSetup(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer,
	setup setupRunner,
) int {
	return runWithRunners(ctx, args, stdin, stdout, stderr, setup, cli.RunEject)
}

func runWithRunners(ctx context.Context, args []string, stdin io.Reader,
	stdout, stderr io.Writer, setup setupRunner, eject ejectRunner,
) int {
	return runWithCommandRunners(ctx, args, stdin, stdout, stderr,
		commandRunners{setup: setup, eject: eject, status: cli.RunStatus})
}

func runWithCommandRunners(ctx context.Context, args []string, stdin io.Reader,
	stdout, stderr io.Writer, runners commandRunners,
) int {
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
	case "setup":
		if runners.setup == nil {
			return 1
		}
		return runners.setup(ctx, args[1:], stdout, stderr, version)
	case "status":
		if runners.status == nil {
			return 1
		}
		return runners.status(ctx, args[1:], stdout, stderr, version)
	case "eject":
		if runners.eject == nil {
			return 1
		}
		return runners.eject(ctx, args[1:], stdout, stderr, version)
	case "hook", "agent", "teamwork":
		return cli.New(stdin, stdout, stderr).Run(ctx, args)
	default:
		fmt.Fprintf(stderr, "mnemon-harness: unknown command %q\n", args[0])
		return 2
	}
}
