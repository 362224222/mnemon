package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/internal/cli"
	"github.com/mnemon-dev/mnemon/internal/daemon"
)

var version = "dev"

const (
	gracefulShutdownBudget = 5 * time.Second
	helpText               = `mnemond manages one project-local Mnemon Agency authority.

Usage:
  mnemond setup [--runtime pi] [--project-root DIR]
  mnemond peer prepare --listen HOST:PORT --advertise HOST:PORT [--project-root DIR]
  mnemond peer enroll --alias NAME [--project-root DIR] < peer-card.json
  mnemond serve --state-dir DIR
  mnemond --help
  mnemond --version

Agent action commands are installed through the project-local mnemond guide
and are intentionally absent from ordinary help.
`
)

type setupRunner func(context.Context, []string, io.Writer, io.Writer) int
type terminalRunner func(context.Context, []string, io.Reader, io.Writer, io.Writer) int
type peerRunner func(context.Context, []string, io.Reader, io.Writer, io.Writer) int
type serveRunner func(context.Context, []string) error

type commandRunners struct {
	setup    setupRunner
	terminal terminalRunner
	peer     peerRunner
	serve    serveRunner
}

type daemonRuntime interface {
	Serve(context.Context) error
	Close(context.Context) error
}

type daemonOpener func(context.Context, string) (daemonRuntime, error)

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
		serve: func(ctx context.Context, args []string) error {
			return runServe(ctx, args, openDaemon)
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
	case "serve":
		if runners.serve == nil {
			return 1
		}
		if err := runners.serve(ctx, args[1:]); err != nil {
			_, _ = fmt.Fprintf(stderr, "mnemond: %v\n", err)
			return 1
		}
		return 0
	default:
		return runMetaCommand(args, stdout, stderr)
	}
}

func runMetaCommand(args []string, stdout, stderr io.Writer) int {
	switch args[0] {
	case "-h", "--help", "help":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemond: help accepts no arguments")
			return 2
		}
		return writeHelp(stdout)
	case "--version", "version":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemond: version accepts no arguments")
			return 2
		}
		if _, err := fmt.Fprintf(stdout, "mnemond version %s\n", version); err != nil {
			return 1
		}
		return 0
	default:
		fmt.Fprintf(stderr, "mnemond: unknown command %q\n", args[0])
		return 2
	}
}

func writeHelp(stdout io.Writer) int {
	if _, err := io.WriteString(stdout, helpText); err != nil {
		return 1
	}
	return 0
}

func openDaemon(ctx context.Context, stateDirectory string) (daemonRuntime, error) {
	return daemon.OpenProvisioned(ctx, stateDirectory)
}

func runServe(ctx context.Context, args []string, open daemonOpener) error {
	if ctx == nil || open == nil {
		return errors.New("mnemond serve is unavailable")
	}
	options, err := parseServeOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime, err := open(ctx, options.stateDirectory)
	if err != nil {
		return err
	}
	if runtime == nil {
		return errors.New("mnemond daemon opener returned no runtime")
	}
	serveErr := runtime.Serve(ctx)
	closeContext, cancel := context.WithTimeout(context.Background(), gracefulShutdownBudget)
	closeErr := runtime.Close(closeContext)
	cancel()
	return errors.Join(serveErr, closeErr)
}
