package agency

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mnemon-dev/mnemon/internal/agency/attach"
	"github.com/mnemon-dev/mnemon/internal/daemon"
	"github.com/spf13/cobra"
)

const setupRuntimePi = "pi"

func setupCommand() *cobra.Command {
	runtime := &singleString{value: setupRuntimePi}
	projectRoot := new(singleString)
	command := &cobra.Command{
		Use:   "setup",
		Short: "Set up Agency for this project",
		Long:  "Provision project-local Agency state, ensure its daemon, and install the Pi integration.",
		Args:  cobra.NoArgs,
		RunE:  runSetup,
	}
	command.Flags().Var(runtime, "runtime", "Agent Runtime to integrate (pi)")
	command.Flags().Var(projectRoot, "project-root", "project root (default: current directory)")
	return command
}

func runSetup(command *cobra.Command, _ []string) error {
	runtime, err := command.Flags().GetString("runtime")
	if err != nil {
		return commandFailure{code: 1, err: fmt.Errorf("mnemon agency setup: %w", err)}
	}
	projectRoot, err := command.Flags().GetString("project-root")
	if err != nil {
		return commandFailure{code: 1, err: fmt.Errorf("mnemon agency setup: %w", err)}
	}
	if runtime != setupRuntimePi {
		return fmt.Errorf("mnemon agency setup: unsupported runtime %q", runtime)
	}
	if command.Flags().Changed("project-root") && strings.TrimSpace(projectRoot) == "" {
		return errors.New("mnemon agency setup: --project-root must not be empty")
	}
	if err := setupProject(command.Context(), projectRoot); err != nil {
		return commandFailure{code: 1, err: fmt.Errorf("mnemon agency setup: %w", err)}
	}
	if _, err := io.WriteString(command.OutOrStdout(),
		`{"schema":"mnemon.setup","status":"ready","version":1}`+"\n"); err != nil {
		return commandFailure{code: 1, err: fmt.Errorf("mnemon agency setup: %w", err)}
	}
	return nil
}

func setupProject(ctx context.Context, requestedRoot string) error {
	if requestedRoot == "" {
		var err error
		requestedRoot, err = os.Getwd()
		if err != nil {
			return err
		}
	}
	projectRoot, stateDirectory, err := daemon.ResolveProjectState(requestedRoot)
	if err != nil {
		return err
	}
	if err := ensureSetupDaemon(ctx, projectRoot, stateDirectory); err != nil {
		return err
	}
	_, err = attach.InstallPi(projectRoot)
	return err
}

func ensureSetupDaemon(ctx context.Context, projectRoot, stateDirectory string) error {
	firstErr := daemon.Ensure(ctx, stateDirectory)
	if firstErr == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	provisioned, provisionErr := daemon.Provision(ctx, projectRoot)
	if provisionErr != nil {
		return errors.Join(firstErr, provisionErr)
	}
	if provisioned.StateDirectory() != stateDirectory {
		return errors.New("provisioned node state does not match the resolved project")
	}
	if err := daemon.Ensure(ctx, stateDirectory); err != nil {
		return errors.Join(firstErr, err)
	}
	return nil
}
