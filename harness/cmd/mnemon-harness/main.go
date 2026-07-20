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
  mnemon-harness channel create [NAME]
  mnemon-harness channel join [--file INVITE_FILE]
  mnemon-harness channel status [CHANNEL]
  mnemon-harness channel invite [--channel CHANNEL] [--expires DURATION] [--uses N]
  mnemon-harness channel invite [--channel CHANNEL] --close
  mnemon-harness channel remove [--channel CHANNEL] MEMBER
  mnemon-harness channel leave [CHANNEL]
  mnemon-harness status
  mnemon-harness doctor
  mnemon-harness eject [--host auto|codex|claude-code] [--project-root DIR]
  mnemon-harness --help
  mnemon-harness --version

Managed Agent commands are installed through the canonical Skill and Guide
and are intentionally absent from ordinary help.
`

type lifecycleRunner func(context.Context, []string, io.Writer, io.Writer, string) int
type setupRunner = lifecycleRunner
type ejectRunner = lifecycleRunner
type statusRunner = lifecycleRunner
type doctorRunner = lifecycleRunner
type resetRunner = lifecycleRunner
type channelRunner func(context.Context, []string, io.Reader, io.Writer, io.Writer, string) int

type commandRunners struct {
	setup   setupRunner
	eject   ejectRunner
	status  statusRunner
	doctor  doctorRunner
	reset   resetRunner
	channel channelRunner
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
		commandRunners{setup: cli.RunSetup, eject: cli.RunEject, status: cli.RunStatus,
			doctor: cli.RunDoctor, reset: cli.RunReset, channel: cli.RunChannel})
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
		commandRunners{setup: setup, eject: eject, status: cli.RunStatus, doctor: cli.RunDoctor})
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
	case "setup", "status", "doctor", "eject", "reset":
		return runLifecycleCommand(ctx, args, stdout, stderr, runners)
	case "channel", "hook", "agent", "teamwork":
		return runExtendedCommand(ctx, args, stdin, stdout, stderr, version, runners.channel)
	default:
		fmt.Fprintf(stderr, "mnemon-harness: unknown command %q\n", args[0])
		return 2
	}
}

func runLifecycleCommand(ctx context.Context, args []string, stdout, stderr io.Writer,
	runners commandRunners,
) int {
	var runner lifecycleRunner
	switch args[0] {
	case "setup":
		runner = runners.setup
	case "status":
		runner = runners.status
	case "doctor":
		runner = runners.doctor
	case "eject":
		runner = runners.eject
	case "reset":
		runner = runners.reset
	}
	if runner == nil {
		return 1
	}
	return runner(ctx, args[1:], stdout, stderr, version)
}

func runExtendedCommand(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer,
	version string, channel channelRunner,
) int {
	if args[0] == "channel" {
		if channel == nil {
			return 1
		}
		return channel(ctx, args[1:], stdin, stdout, stderr, version)
	}
	return cli.New(stdin, stdout, stderr).Run(ctx, args)
}
