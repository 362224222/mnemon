package main

import (
	"context"
	"fmt"
	"io"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi/nodecontrol"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func openManagedRuntime(ctx context.Context,
	options node.DaemonOptions,
) (daemonRuntime, error) {
	installation, factory, err := managedRuntimeComponents(options.Workspace)
	if err != nil {
		return nil, err
	}
	options.Install = installation
	options.Credentials = nodecontrol.ProfileCredentials{}
	options.WakeAdapterFactory = factory
	options.Attachments = nodecontrol.NewAgentAttachmentFilesystem(filepath.Join(options.Workspace,
		".mnemon", "harness", "node"))
	options.Control = nodecontrol.Factory{}
	return node.OpenManagedDaemon(ctx, options)
}

func managedRuntimeComponents(workspace string) (*integration.ManagedInstallation,
	node.WakeAdapterFactory, error,
) {
	installation, err := integration.NewManagedInstallation(workspace)
	if err != nil {
		return nil, nil, err
	}
	factory, err := node.NewManagedWakeAdapterFactory(workspace, installation)
	if err != nil {
		return nil, nil, err
	}
	return installation, factory, nil
}

func activateManagedNode(ctx context.Context, options node.ActivateOptions) (node.ActivateResult, error) {
	installation, err := integration.NewManagedInstallation(options.Workspace)
	if err != nil {
		return node.ActivateResult{}, err
	}
	options.Install = installation
	options.Credentials = nodecontrol.ProfileCredentials{}
	return node.Activate(ctx, options)
}

func provisionManagedNode(ctx context.Context, options node.ProvisionOptions) (node.ProvisionResult, error) {
	options.Credentials = nodecontrol.ProfileCredentials{}
	return node.Provision(ctx, options)
}

func deactivateManagedNode(ctx context.Context, options node.DeactivateOptions) (node.DeactivateResult, error) {
	options.Credentials = nodecontrol.ProfileCredentials{}
	return node.Deactivate(ctx, options)
}

func inspectManagedNode(ctx context.Context, workspace string) (node.Authority, error) {
	return node.InspectAuthority(ctx, workspace, nodecontrol.ProfileCredentials{})
}

func runInspect(ctx context.Context, args []string, stdout io.Writer, inspect nodeInspector) error {
	projectRoot, err := parseInspectProjectRoot(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	authority, err := inspect(ctx, projectRoot)
	if err != nil {
		return err
	}
	wire, err := nodecontrol.AuthorityResponse(authority)
	if err != nil {
		return fmt.Errorf("encode authority inspection receipt: %w", err)
	}
	return writeCanonicalReceipt(stdout, wire, "authority inspection")
}

func runConfirmOffline(ctx context.Context, args []string, stdout io.Writer) error {
	projectRoot, expected, err := parseConfirmOfflineOptions(args)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	authority, err := node.ConfirmOfflineAuthority(ctx, projectRoot, expected,
		nodecontrol.ProfileCredentials{}, nodecontrol.RecoverControlSocket)
	if err != nil {
		return err
	}
	wire, err := nodecontrol.AuthorityResponse(authority)
	if err != nil {
		return fmt.Errorf("encode offline authority receipt: %w", err)
	}
	return writeCanonicalReceipt(stdout, wire, "offline authority")
}

func writeCanonicalReceipt(stdout io.Writer, value any, name string) error {
	raw, err := model.CanonicalMarshal(value)
	if err != nil {
		return fmt.Errorf("encode %s receipt: %w", name, err)
	}
	_, err = stdout.Write(append(raw, '\n'))
	return err
}
