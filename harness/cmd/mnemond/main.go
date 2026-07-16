package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

var version = "dev"

const helpText = `mnemond is the sole local controller for an R5 managed Agent.

It owns Node, Event, Work, Handling, and Artifact state.

Usage:
  mnemond
  mnemond serve [--project-root DIR]
  mnemond --help
  mnemond --version

Normal users do not need to start mnemond directly; mnemon-harness setup and
managed commands use the same bounded ensure path.
`

type daemonRuntime interface {
	Serve(context.Context) error
	Close() error
}

type daemonOpener func(context.Context, node.DaemonOptions) (daemonRuntime, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "mnemond: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithDaemon(ctx, args, stdout, stderr, func(ctx context.Context,
		options node.DaemonOptions,
	) (daemonRuntime, error) {
		verify, err := managedInstallationVerifier(options.Workspace)
		if err != nil {
			return nil, err
		}
		options.Install = verify
		return node.OpenDaemon(ctx, options)
	})
}

func managedInstallationVerifier(workspace string) (node.InstallationVerifier, error) {
	bundle, err := assets.Load()
	if err != nil {
		return nil, fmt.Errorf("load canonical managed assets: %w", err)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	return node.InstallationVerifierFunc(func(profile model.Profile) error {
		host := assets.Host(profile.Host())
		if !host.Valid() || profile.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
			return errors.New("active Profile does not select this canonical asset bundle")
		}
		return integration.VerifyHostProjection(workspace, nodeState, host, bundle)
	}), nil
}

func runWithDaemon(ctx context.Context, args []string, stdout, stderr io.Writer,
	open daemonOpener,
) error {
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
		projectRoot, err := parseServeProjectRoot(args[1:])
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		daemon, err := open(ctx, node.DaemonOptions{Workspace: projectRoot})
		if err != nil {
			return err
		}
		serveErr := daemon.Serve(ctx)
		return errors.Join(serveErr, daemon.Close())
	default:
		if len(args) != 1 {
			return fmt.Errorf("unsupported command %q", strings.Join(args, " "))
		}
		return fmt.Errorf("unsupported command %q", args[0])
	}
}

func parseServeProjectRoot(args []string) (string, error) {
	var requested string
	switch {
	case len(args) == 0:
		current, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve project root: %w", err)
		}
		requested = current
	case len(args) == 2 && args[0] == "--project-root":
		requested = args[1]
	case len(args) == 1 && strings.HasPrefix(args[0], "--project-root="):
		requested = strings.TrimPrefix(args[0], "--project-root=")
	default:
		return "", errors.New("serve accepts only --project-root DIR")
	}
	if strings.TrimSpace(requested) == "" {
		return "", errors.New("serve project root is empty")
	}
	absolute, err := filepath.Abs(requested)
	if err != nil {
		return "", fmt.Errorf("resolve project root: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve project root: %w", err)
	}
	info, err := os.Lstat(resolved)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("serve project root must be a real directory")
	}
	return filepath.Clean(resolved), nil
}
