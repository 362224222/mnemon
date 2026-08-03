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

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
)

var version = "dev"

const (
	gracefulShutdownBudget = 5 * time.Second
	helpText               = `mnemond serves one already-provisioned R7 local authority.

Usage:
  mnemond serve --state-dir DIR --principal ID
  mnemond --help
  mnemond --version

Setup owns provisioning. mnemond strictly adopts the exact state directory and
Agent Principal supplied by its local launcher.
`
)

type daemonRuntime interface {
	Serve(context.Context) error
	Close(context.Context) error
}

type daemonOpener func(context.Context, string, agency.AgentPrincipalID) (daemonRuntime, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "mnemond: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithDaemon(ctx, args, stdout, stderr, openDaemon)
}

func runWithDaemon(ctx context.Context, args []string, stdout, stderr io.Writer,
	open daemonOpener,
) error {
	if ctx == nil || stdout == nil || stderr == nil || open == nil {
		return errors.New("mnemond command is unavailable")
	}
	_ = stderr
	if len(args) == 0 {
		_, err := io.WriteString(stdout, helpText)
		return err
	}
	switch args[0] {
	case "-h", "--help", "help":
		if len(args) != 1 {
			return errors.New("help accepts no arguments")
		}
		_, err := io.WriteString(stdout, helpText)
		return err
	case "--version", "version":
		if len(args) != 1 {
			return errors.New("version accepts no arguments")
		}
		_, err := fmt.Fprintf(stdout, "mnemond version %s\n", version)
		return err
	case "serve":
		return runServe(ctx, args[1:], open)
	default:
		return fmt.Errorf("unsupported command %q", args[0])
	}
}

func openDaemon(ctx context.Context, stateDirectory string,
	principal agency.AgentPrincipalID,
) (daemonRuntime, error) {
	return daemon.Open(ctx, stateDirectory, principal)
}

func runServe(ctx context.Context, args []string, open daemonOpener) error {
	options, err := parseServeOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime, err := open(ctx, options.stateDirectory, options.principal)
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
