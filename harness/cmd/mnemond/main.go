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
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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
	return runWithNode(ctx, args, stdout, stderr, func(ctx context.Context,
		options node.DaemonOptions,
	) (daemonRuntime, error) {
		verify, err := managedInstallationVerifier(options.Workspace)
		if err != nil {
			return nil, err
		}
		options.Install = verify
		return node.OpenManagedDaemon(ctx, options)
	}, node.Provision, activateManagedNode, node.Deactivate)
}

func activateManagedNode(ctx context.Context, options node.ActivateOptions) (node.ActivateResult, error) {
	verify, err := managedInstallationVerifier(options.Workspace)
	if err != nil {
		return node.ActivateResult{}, err
	}
	options.Install = verify
	return node.Activate(ctx, options)
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
	return runWithNode(ctx, args, stdout, stderr, open, node.Provision, activateManagedNode, node.Deactivate)
}

func runWithNode(ctx context.Context, args []string, stdout, stderr io.Writer,
	open daemonOpener, provision nodeProvisioner, activate nodeActivator, deactivate nodeDeactivator,
	inspectors ...nodeInspector,
) error {
	_ = stderr
	if len(inspectors) > 1 || len(inspectors) == 1 && inspectors[0] == nil {
		return errors.New("mnemond command has invalid authority inspector composition")
	}
	inspect := node.InspectAuthority
	if len(inspectors) == 1 {
		inspect = inspectors[0]
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
	case "initialize":
		options, err := parseInitializeOptions(args[1:])
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
		raw, err := model.CanonicalMarshal(receipt)
		if err != nil {
			return fmt.Errorf("encode initialization receipt: %w", err)
		}
		_, err = stdout.Write(append(raw, '\n'))
		return err
	case "activate":
		options, err := parseActivateOptions(args[1:])
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
		raw, err := model.CanonicalMarshal(receipt)
		if err != nil {
			return fmt.Errorf("encode activation receipt: %w", err)
		}
		_, err = stdout.Write(append(raw, '\n'))
		return err
	case "deactivate":
		options, err := parseDeactivateOptions(args[1:])
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
		raw, err := model.CanonicalMarshal(receipt)
		if err != nil {
			return fmt.Errorf("encode deactivation receipt: %w", err)
		}
		_, err = stdout.Write(append(raw, '\n'))
		return err
	case "inspect":
		projectRoot, err := parseInspectProjectRoot(args[1:])
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
		raw, err := model.CanonicalMarshal(receipt)
		if err != nil {
			return fmt.Errorf("encode authority inspection receipt: %w", err)
		}
		_, err = stdout.Write(append(raw, '\n'))
		return err
	case "confirm-offline":
		projectRoot, expected, err := parseConfirmOfflineOptions(args[1:])
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		receipt, err := node.ConfirmOfflineAuthority(ctx, projectRoot, expected)
		if err != nil {
			return err
		}
		raw, err := model.CanonicalMarshal(receipt)
		if err != nil {
			return fmt.Errorf("encode offline authority receipt: %w", err)
		}
		_, err = stdout.Write(append(raw, '\n'))
		return err
	default:
		if len(args) != 1 {
			return fmt.Errorf("unsupported command %q", strings.Join(args, " "))
		}
		return fmt.Errorf("unsupported command %q", args[0])
	}
}

func parseInspectProjectRoot(args []string) (string, error) {
	if len(args) != 2 || args[0] != "--project-root" || strings.TrimSpace(args[1]) == "" {
		return "", errors.New("inspect requires exactly --project-root DIR")
	}
	return resolveProjectRoot(args[1])
}

func parseConfirmOfflineOptions(args []string) (string, model.Digest, error) {
	expectedWire, remaining, err := takeRequiredManagedOption("confirm-offline",
		"--expected-authority-digest", args)
	if err != nil {
		return "", model.Digest{}, err
	}
	workspace, err := parseInspectProjectRoot(remaining)
	if err != nil {
		return "", model.Digest{}, errors.New(
			"confirm-offline requires --project-root and --expected-authority-digest")
	}
	expected, err := model.ParseDigest(expectedWire)
	if err != nil || expected.IsZero() || expected.String() != expectedWire {
		return "", model.Digest{}, errors.New(
			"confirm-offline expected authority digest must be canonical sha256")
	}
	return workspace, expected, nil
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

func parseInitializeOptions(args []string) (node.ProvisionOptions, error) {
	workspace, host, revision, err := parseManagedAuthorityOptions("initialize", args)
	if err != nil {
		return node.ProvisionOptions{}, err
	}
	return node.ProvisionOptions{Workspace: workspace, Host: host, AssetRevision: revision}, nil
}

func parseActivateOptions(args []string) (node.ActivateOptions, error) {
	expected, workspace, host, revision, err := parseExpectedManagedAuthorityOptions("activate", args)
	if err != nil {
		return node.ActivateOptions{}, err
	}
	return node.ActivateOptions{Workspace: workspace, Host: host, AssetRevision: revision,
		ExpectedUpdatedAt: expected}, nil
}

func parseDeactivateOptions(args []string) (node.DeactivateOptions, error) {
	expected, workspace, host, revision, err := parseExpectedManagedAuthorityOptions("deactivate", args)
	if err != nil {
		return node.DeactivateOptions{}, err
	}
	return node.DeactivateOptions{Workspace: workspace, Host: host, AssetRevision: revision,
		ExpectedUpdatedAt: expected}, nil
}

func parseExpectedManagedAuthorityOptions(command string,
	args []string,
) (time.Time, string, model.HostKind, string, error) {
	expectedWire, remaining, err := takeRequiredManagedOption(command, "--expected-updated-at", args)
	if err != nil {
		return time.Time{}, "", "", "", err
	}
	workspace, host, revision, err := parseManagedAuthorityOptions(command, remaining)
	if err != nil {
		return time.Time{}, "", "", "", err
	}
	expected, err := time.Parse(time.RFC3339Nano, expectedWire)
	if err != nil || expected.UnixNano() <= 0 || expected.UTC().Format(time.RFC3339Nano) != expectedWire {
		return time.Time{}, "", "", "", fmt.Errorf("%s expected update time must be canonical RFC3339Nano", command)
	}
	if !time.Unix(0, expected.UnixNano()).UTC().Equal(expected.UTC()) {
		return time.Time{}, "", "", "", fmt.Errorf("%s expected update time is outside the supported range", command)
	}
	return expected, workspace, host, revision, nil
}

func takeRequiredManagedOption(command, option string, args []string) (string, []string, error) {
	remaining := make([]string, 0, len(args))
	value := ""
	for index := 0; index < len(args); {
		if args[index] != option {
			remaining = append(remaining, args[index])
			index++
			continue
		}
		if value != "" || index+1 >= len(args) || strings.TrimSpace(args[index+1]) == "" {
			return "", nil, fmt.Errorf("%s %s must occur exactly once with a nonempty value", command, option)
		}
		value = args[index+1]
		index += 2
	}
	if value == "" {
		return "", nil, fmt.Errorf("%s requires %s", command, option)
	}
	return value, remaining, nil
}

func parseManagedAuthorityOptions(command string, args []string) (string, model.HostKind, string, error) {
	values := make(map[string]string, 3)
	for len(args) != 0 {
		if len(args) < 2 || (args[0] != "--project-root" && args[0] != "--host" && args[0] != "--asset-revision") {
			return "", "", "", fmt.Errorf("%s requires --project-root, --host and --asset-revision", command)
		}
		if _, duplicate := values[args[0]]; duplicate || strings.TrimSpace(args[1]) == "" {
			return "", "", "", fmt.Errorf("%s options must occur exactly once with nonempty values", command)
		}
		values[args[0]] = args[1]
		args = args[2:]
	}
	if len(values) != 3 {
		return "", "", "", fmt.Errorf("%s requires --project-root, --host and --asset-revision", command)
	}
	workspace, err := resolveProjectRoot(values["--project-root"])
	if err != nil {
		return "", "", "", err
	}
	host := model.HostKind(values["--host"])
	if _, ok := model.RuntimeForHost(host); !ok {
		return "", "", "", fmt.Errorf("%s Host must be codex or claude-code", command)
	}
	if _, err := model.ParseDigest(values["--asset-revision"]); err != nil {
		return "", "", "", fmt.Errorf("%s asset revision must be a sha256 digest", command)
	}
	return workspace, host, values["--asset-revision"], nil
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
	return resolveProjectRoot(requested)
}

func resolveProjectRoot(requested string) (string, error) {
	if strings.TrimSpace(requested) == "" {
		return "", errors.New("project root is empty")
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
