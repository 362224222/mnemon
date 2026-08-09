package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mnemon-dev/mnemon/internal/attach"
	"github.com/mnemon-dev/mnemon/internal/daemon"
)

const setupRuntimePi = "pi"

type setupOptions struct {
	projectRoot string
	runtime     string
}

type setupDependencies struct {
	workingDirectory func() (string, error)
	resolveState     func(string) (string, string, error)
	ensure           func(context.Context, string) error
	provision        func(context.Context, string) (string, error)
	install          func(string) error
}

func productionSetupDependencies() setupDependencies {
	return setupDependencies{
		workingDirectory: os.Getwd,
		resolveState:     daemon.ResolveProjectState,
		ensure:           daemon.Ensure,
		provision: func(ctx context.Context, projectRoot string) (string, error) {
			result, err := daemon.Provision(ctx, projectRoot)
			return result.StateDirectory(), err
		},
		install: func(projectRoot string) error {
			_, err := attach.InstallPi(projectRoot)
			return err
		},
	}
}

func runSetup(ctx context.Context, args []string, stdout, stderr io.Writer,
	deps setupDependencies,
) int {
	if ctx == nil || stdout == nil || stderr == nil || !deps.available() {
		return 1
	}
	options, err := parseSetupOptions(args)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "mnemond setup: %v\n", err)
		return 2
	}
	requested := options.projectRoot
	if requested == "" {
		requested, err = deps.workingDirectory()
		if err != nil {
			return writeSetupFailure(stderr, err)
		}
	}
	projectRoot, stateDirectory, err := deps.resolveState(requested)
	if err != nil {
		return writeSetupFailure(stderr, err)
	}
	if err := ensureSetupDaemon(ctx, projectRoot, stateDirectory, deps); err != nil {
		return writeSetupFailure(stderr, err)
	}
	if err := deps.install(projectRoot); err != nil {
		return writeSetupFailure(stderr, err)
	}
	if _, err := io.WriteString(stdout,
		`{"schema":"mnemon.setup","status":"ready","version":1}`+"\n"); err != nil {
		return 1
	}
	return 0
}

func (deps setupDependencies) available() bool {
	return deps.workingDirectory != nil && deps.resolveState != nil && deps.ensure != nil &&
		deps.provision != nil && deps.install != nil
}

func ensureSetupDaemon(ctx context.Context, projectRoot, stateDirectory string,
	deps setupDependencies,
) error {
	firstErr := deps.ensure(ctx, stateDirectory)
	if firstErr == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	provisionedState, provisionErr := deps.provision(ctx, projectRoot)
	if provisionErr != nil {
		return errors.Join(firstErr, provisionErr)
	}
	if provisionedState != stateDirectory {
		return errors.New("provisioned node state does not match the resolved project")
	}
	if err := deps.ensure(ctx, stateDirectory); err != nil {
		return errors.Join(firstErr, err)
	}
	return nil
}

func parseSetupOptions(args []string) (setupOptions, error) {
	options := setupOptions{runtime: setupRuntimePi}
	seenRuntime := false
	seenRoot := false
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--runtime":
			if seenRuntime || index+1 >= len(args) {
				return setupOptions{}, errors.New("--runtime requires one value")
			}
			seenRuntime = true
			index++
			options.runtime = args[index]
		case "--project-root":
			if seenRoot || index+1 >= len(args) {
				return setupOptions{}, errors.New("--project-root requires one value")
			}
			seenRoot = true
			index++
			options.projectRoot = args[index]
		default:
			return setupOptions{}, fmt.Errorf("unsupported argument %q", args[index])
		}
	}
	if options.runtime != setupRuntimePi {
		return setupOptions{}, fmt.Errorf("unsupported runtime %q", options.runtime)
	}
	if seenRoot && options.projectRoot == "" {
		return setupOptions{}, errors.New("--project-root must not be empty")
	}
	return options, nil
}

func writeSetupFailure(stderr io.Writer, err error) int {
	if err == nil {
		err = errors.New("setup failed")
	}
	_, _ = fmt.Fprintf(stderr, "mnemond setup: %v\n", err)
	return 1
}
