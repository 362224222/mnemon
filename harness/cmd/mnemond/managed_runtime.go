package main

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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
	options.Control = localapi.NodeRuntime{}
	options.WakeAdapterFactory = factory
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

func provisionManagedNode(ctx context.Context, options node.ProvisionOptions) (node.ProvisionResult, error) {
	options.Credentials = localapi.NodeRuntime{}
	return node.Provision(ctx, options)
}

func activateManagedNode(ctx context.Context, options node.ActivateOptions) (node.ActivateResult, error) {
	installation, err := integration.NewManagedInstallation(options.Workspace)
	if err != nil {
		return node.ActivateResult{}, err
	}
	options.Install = installation
	options.Credentials = localapi.NodeRuntime{}
	return node.Activate(ctx, options)
}

func deactivateManagedNode(ctx context.Context, options node.DeactivateOptions) (node.DeactivateResult, error) {
	options.Credentials = localapi.NodeRuntime{}
	return node.Deactivate(ctx, options)
}

func inspectManagedNode(ctx context.Context, workspace string) (localapi.AuthoritySnapshot, error) {
	return node.InspectAuthorityWithCredentials(ctx, workspace, localapi.NodeRuntime{})
}
