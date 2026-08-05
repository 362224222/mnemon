package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/cli"
	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
)

var version = "dev"

const helpText = `mnemon-harness connects this workspace's Agent runtime to local mnemond authority.

Usage:
  mnemon-harness setup [--runtime pi] [--project-root DIR]
  mnemon-harness peer prepare --listen HOST:PORT --advertise HOST:PORT [--project-root DIR]
  mnemon-harness peer enroll --alias NAME [--project-root DIR] < peer-card.json
  mnemon-harness --help
  mnemon-harness --version

Agent action commands are installed through the project-local mnemond guide
and are intentionally absent from ordinary help.
`

type setupRunner func(context.Context, []string, io.Writer, io.Writer) int
type terminalRunner func(context.Context, []string, io.Reader, io.Writer, io.Writer) int
type peerRunner func(context.Context, []string, io.Reader, io.Writer, io.Writer) int

type commandRunners struct {
	setup    setupRunner
	terminal terminalRunner
	peer     peerRunner
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if exit := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); exit != 0 {
		os.Exit(exit)
	}
}

func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return runWithCommandRunners(ctx, args, stdin, stdout, stderr, commandRunners{
		setup: func(ctx context.Context, args []string, stdout, stderr io.Writer) int {
			return runSetup(ctx, args, stdout, stderr, productionSetupDependencies())
		},
		terminal: func(ctx context.Context, args []string, stdin io.Reader,
			stdout, stderr io.Writer,
		) int {
			return cli.New(stdin, stdout, stderr, daemon.Ensure).Run(ctx, args)
		},
		peer: func(ctx context.Context, args []string, stdin io.Reader,
			stdout, stderr io.Writer,
		) int {
			return runPeer(ctx, args, stdin, stdout, stderr, productionPeerDependencies())
		},
	})
}

func runWithCommandRunners(ctx context.Context, args []string, stdin io.Reader,
	stdout, stderr io.Writer, runners commandRunners,
) int {
	if ctx == nil || stdin == nil || stdout == nil || stderr == nil {
		return 1
	}
	if len(args) == 0 {
		return writeHelp(stdout)
	}
	switch args[0] {
	case "setup":
		if runners.setup == nil {
			return 1
		}
		return runners.setup(ctx, args[1:], stdout, stderr)
	case "peer":
		if runners.peer == nil {
			return 1
		}
		return runners.peer(ctx, args[1:], stdin, stdout, stderr)
	case "hook", "agent", "artifact":
		if runners.terminal == nil {
			return 1
		}
		return runners.terminal(ctx, args, stdin, stdout, stderr)
	default:
		return runMetaCommand(args, stdout, stderr)
	}
}

func runMetaCommand(args []string, stdout, stderr io.Writer) int {
	switch args[0] {
	case "-h", "--help", "help":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemon-harness: help accepts no arguments")
			return 2
		}
		return writeHelp(stdout)
	case "--version", "version":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemon-harness: version accepts no arguments")
			return 2
		}
		if _, err := fmt.Fprintf(stdout, "mnemon-harness version %s\n", version); err != nil {
			return 1
		}
		return 0
	default:
		fmt.Fprintf(stderr, "mnemon-harness: unknown command %q\n", args[0])
		return 2
	}
}

func writeHelp(stdout io.Writer) int {
	if _, err := io.WriteString(stdout, helpText); err != nil {
		return 1
	}
	return 0
}
