package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

type statusControlClient interface {
	daemonHealthClient
	ReadStatus(context.Context) (localapi.StatusResponse, *localapi.APIError)
}

type statusDependencies struct {
	workingDirectory func() (string, error)
	newClient        func(string) (statusControlClient, error)
	ensureDaemon     func(context.Context, string, string, daemonHealthClient) *localapi.APIError
}

type statusApp struct {
	stdout io.Writer
	stderr io.Writer
	deps   statusDependencies
}

func productionStatusDependencies() statusDependencies {
	return statusDependencies{workingDirectory: os.Getwd,
		newClient: func(nodeState string) (statusControlClient, error) {
			return localapi.NewClient(nodeState)
		},
		ensureDaemon: ensureAgentDaemon}
}

// RunStatus is the public, read-only managed Agent observation command. It
// probes an already-running daemon before attempting the shared bounded ensure
// path so a reachable not_ready daemon can explain itself without a restart.
func RunStatus(ctx context.Context, args []string, stdout, stderr io.Writer, _ string) int {
	app := &statusApp{stdout: stdout, stderr: stderr, deps: productionStatusDependencies()}
	return app.run(ctx, args)
}

func (app *statusApp) run(ctx context.Context, args []string) int {
	if app == nil || ctx == nil || app.stdout == nil || app.stderr == nil ||
		app.deps.workingDirectory == nil || app.deps.newClient == nil || app.deps.ensureDaemon == nil {
		return 1
	}
	if len(args) != 0 {
		return app.writeError(localapi.NewAPIError(localapi.CodeInvalidArgument,
			"status accepts no arguments"))
	}
	workspace, nodeState, err := resolveManagedWorkspace(app.deps.workingDirectory)
	if err != nil {
		return app.writeError(localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"Mnemon Harness is not set up in this workspace"))
	}
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		if errors.Is(err, localapi.ErrUnsafeClientState) {
			return app.writeError(localapi.NewAPIError(localapi.CodeAuthenticationFailed,
				"Mnemon Harness client state is unavailable"))
		}
		return app.writeError(localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"mnemond local control is unavailable"))
	}

	response, apiErr := client.ReadStatus(ctx)
	if apiErr == nil {
		return app.writeResponse(response)
	}
	if apiErr.Code != localapi.CodeMnemondUnavailable {
		return app.writeError(apiErr)
	}
	if apiErr := app.deps.ensureDaemon(ctx, workspace, nodeState, client); apiErr != nil {
		return app.writeError(apiErr)
	}
	response, apiErr = client.ReadStatus(ctx)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	return app.writeResponse(response)
}

func (app *statusApp) writeResponse(response localapi.StatusResponse) int {
	raw, err := model.CanonicalMarshal(response)
	if err != nil {
		return 1
	}
	if _, err := app.stdout.Write(append(raw, '\n')); err != nil {
		return 1
	}
	return response.ExitStatus()
}

func (app *statusApp) writeError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = localapi.NewAPIError(localapi.CodeInternal, "internal status error")
	}
	if _, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message); err != nil {
		return 1
	}
	return apiErr.ExitStatus()
}
