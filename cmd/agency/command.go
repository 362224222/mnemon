// Package agency implements the mnemon agency command tree.
//
// It owns only process composition. Canonical Agency state, admission, and the
// private Agent action terminal remain owned by internal packages.
package agency

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/internal/agencyclient"
	"github.com/mnemon-dev/mnemon/internal/daemon"
)

const (
	gracefulShutdownBudget = 5 * time.Second
	helpText               = `mnemon agency manages one project-local Mnemon Agency authority.

Usage:
  mnemon agency setup [--runtime pi] [--project-root DIR]
  mnemon agency peer prepare --listen HOST:PORT --advertise HOST:PORT [--project-root DIR]
  mnemon agency peer enroll --alias NAME [--project-root DIR] < peer-card.json
  mnemon agency serve --state-dir DIR
  mnemon agency --help
  mnemon agency --version

Agent action commands are installed through the project-local Mnemon Agency guide
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

// Run executes the Agency command subtree. Args begin after "mnemon agency".
// The caller owns signal handling and process exit.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer,
	version string,
) int {
	return runWithCommandRunners(ctx, args, stdin, stdout, stderr, version, commandRunners{
		setup: func(ctx context.Context, args []string, stdout, stderr io.Writer) int {
			return runSetup(ctx, args, stdout, stderr, productionSetupDependencies())
		},
		terminal: func(ctx context.Context, args []string, stdin io.Reader,
			stdout, stderr io.Writer,
		) int {
			return agencyclient.Run(ctx, args, stdin, stdout, stderr, daemon.Ensure)
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
	stdout, stderr io.Writer, version string, runners commandRunners,
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
			_, _ = fmt.Fprintf(stderr, "mnemon agency: %v\n", err)
			return 1
		}
		return 0
	default:
		return runMetaCommand(args, stdout, stderr, version)
	}
}

func runMetaCommand(args []string, stdout, stderr io.Writer, version string) int {
	switch args[0] {
	case "-h", "--help", "help":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemon agency: help accepts no arguments")
			return 2
		}
		return writeHelp(stdout)
	case "--version", "version":
		if len(args) != 1 {
			fmt.Fprintln(stderr, "mnemon agency: version accepts no arguments")
			return 2
		}
		if _, err := fmt.Fprintf(stdout, "mnemon agency version %s\n", version); err != nil {
			return 1
		}
		return 0
	default:
		fmt.Fprintf(stderr, "mnemon agency: unknown command %q\n", args[0])
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
		return errors.New("mnemon agency serve is unavailable")
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
		return errors.New("Mnemon Agency daemon opener returned no runtime")
	}
	serveErr := runtime.Serve(ctx)
	closeContext, cancel := context.WithTimeout(context.Background(), gracefulShutdownBudget)
	closeErr := runtime.Close(closeContext)
	cancel()
	return errors.Join(serveErr, closeErr)
}
