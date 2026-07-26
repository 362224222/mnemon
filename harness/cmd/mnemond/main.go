package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

var version = "dev"

const gracefulShutdownBudget = 10 * time.Second

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
type nodeProvisioner func(context.Context, node.ProvisionOptions) (node.ProvisionResult, error)
type nodeActivator func(context.Context, node.ActivateOptions) (node.ActivateResult, error)
type nodeDeactivator func(context.Context, node.DeactivateOptions) (node.DeactivateResult, error)
type nodeInspector func(context.Context, string) (localapi.AuthoritySnapshot, error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, node.ErrOfflineAuthorityActive) {
			os.Exit(node.OfflineAuthorityActiveExitCode)
		}
		fmt.Fprintf(os.Stderr, "mnemond: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithNode(ctx, args, stdout, stderr, openManagedRuntime,
		provisionManagedNode, activateManagedNode, deactivateManagedNode, inspectManagedNode)
}

func runWithDaemon(ctx context.Context, args []string, stdout, stderr io.Writer,
	open daemonOpener,
) error {
	return runWithNode(ctx, args, stdout, stderr, open, node.Provision, activateManagedNode, node.Deactivate)
}

func runWithNode(ctx context.Context, args []string, stdout, stderr io.Writer,
	open daemonOpener, provision nodeProvisioner, activate nodeActivator, deactivate nodeDeactivator,
	inspectors ...nodeInspector,
) error {
	_ = stderr
	inspect, err := selectNodeInspector(inspectors)
	if err != nil {
		return err
	}

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
	case "initialize":
		return runInitialize(ctx, args[1:], stdout, provision)
	case "activate":
		return runActivate(ctx, args[1:], stdout, activate)
	case "deactivate":
		return runDeactivate(ctx, args[1:], stdout, deactivate)
	case "inspect":
		return runInspect(ctx, args[1:], stdout, inspect)
	case "confirm-offline":
		return runConfirmOffline(ctx, args[1:], stdout)
	default:
		if len(args) != 1 {
			return fmt.Errorf("unsupported command %q", strings.Join(args, " "))
		}
		return fmt.Errorf("unsupported command %q", args[0])
	}
}

func selectNodeInspector(inspectors []nodeInspector) (nodeInspector, error) {
	if len(inspectors) > 1 || len(inspectors) == 1 && inspectors[0] == nil {
		return nil, errors.New("mnemond command has invalid authority inspector composition")
	}
	if len(inspectors) == 1 {
		return inspectors[0], nil
	}
	return node.InspectAuthority, nil
}

func runServe(ctx context.Context, args []string, open daemonOpener) error {
	projectRoot, err := parseServeProjectRoot(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	daemon, err := open(ctx, node.DaemonOptions{Workspace: projectRoot,
		GracefulShutdownBudget: gracefulShutdownBudget})
	if err != nil {
		return err
	}
	serveErr := daemon.Serve(ctx)
	return errors.Join(serveErr, daemon.Close())
}

func runInitialize(ctx context.Context, args []string, stdout io.Writer,
	provision nodeProvisioner,
) error {
	options, err := parseInitializeOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := provision(ctx, options)
	if err != nil {
		return err
	}
	receipt := initializeReceipt{AssetRevision: result.Profile.ActiveAssetRevision(), Created: result.Created,
		Host: string(result.Profile.Host()), SchemaVersion: model.SchemaVersion, Status: "initialized"}
	return writeCanonicalReceipt(stdout, receipt, "initialization")
}

func runActivate(ctx context.Context, args []string, stdout io.Writer,
	activate nodeActivator,
) error {
	options, err := parseActivateOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := activate(ctx, options)
	if err != nil {
		return err
	}
	receipt := activateReceipt{AssetRevision: result.Profile.ActiveAssetRevision(), Changed: result.Changed,
		Host: string(result.Profile.Host()), SchemaVersion: model.SchemaVersion, Status: "active",
		UpdatedAt: result.Profile.UpdatedAt().UTC().Format(time.RFC3339Nano)}
	return writeCanonicalReceipt(stdout, receipt, "activation")
}

func runDeactivate(ctx context.Context, args []string, stdout io.Writer,
	deactivate nodeDeactivator,
) error {
	options, err := parseDeactivateOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	result, err := deactivate(ctx, options)
	if err != nil {
		return err
	}
	receipt := deactivateReceipt{AssetRevision: result.Profile.ActiveAssetRevision(), Changed: result.Changed,
		Host: string(result.Profile.Host()), SchemaVersion: model.SchemaVersion, Status: "inactive",
		UpdatedAt: result.Profile.UpdatedAt().UTC().Format(time.RFC3339Nano)}
	return writeCanonicalReceipt(stdout, receipt, "deactivation")
}

func runInspect(ctx context.Context, args []string, stdout io.Writer, inspect nodeInspector) error {
	projectRoot, err := parseInspectProjectRoot(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	snapshot, err := inspect(ctx, projectRoot)
	if err != nil {
		return err
	}
	receipt, err := localapi.NewAuthorityResponse(snapshot)
	if err != nil {
		return fmt.Errorf("encode authority inspection receipt: %w", err)
	}
	return writeCanonicalReceipt(stdout, receipt, "authority inspection")
}

func runConfirmOffline(ctx context.Context, args []string, stdout io.Writer) error {
	projectRoot, expected, err := parseConfirmOfflineOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	receipt, err := node.ConfirmOfflineAuthorityWithControl(ctx, projectRoot, expected,
		localapi.NodeRuntime{})
	if err != nil {
		return err
	}
	return writeCanonicalReceipt(stdout, receipt, "offline authority")
}

func writeCanonicalReceipt(stdout io.Writer, receipt any, name string) error {
	raw, err := model.CanonicalMarshal(receipt)
	if err != nil {
		return fmt.Errorf("encode %s receipt: %w", name, err)
	}
	_, err = stdout.Write(append(raw, '\n'))
	return err
}

type initializeReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Created       bool   `json:"created"`
	Host          string `json:"host"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
}

type activateReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Changed       bool   `json:"changed"`
	Host          string `json:"host"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	UpdatedAt     string `json:"updated_at"`
}

type deactivateReceipt struct {
	AssetRevision string `json:"asset_revision"`
	Changed       bool   `json:"changed"`
	Host          string `json:"host"`
	SchemaVersion int    `json:"schema_version"`
	Status        string `json:"status"`
	UpdatedAt     string `json:"updated_at"`
}
