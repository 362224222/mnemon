// Package agencycli implements the owner-local R7 Agent terminal.
//
// It owns only private client mechanics: one attachment proof, one Current
// operation, and captured candidate bindings. It does not interpret semantic
// Event kinds or duplicate mnemond admission policy.
package agencycli

import (
	"context"
	cryptorand "crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

type dependencies struct {
	workingDirectory func() (string, error)
	ensureDaemon     EnsureDaemonFunc
	newClient        func(string) (agencyClient, error)
	random           io.Reader
	clock            func() time.Time
}

// EnsureDaemonFunc is the narrow composition seam back to the existing
// bounded process ensure. Empty code means success. The string pair keeps the
// lifecycle implementation and its legacy error type outside this package.
type EnsureDaemonFunc func(context.Context, string, string) (code, message string)

// App is deliberately independent from the R5 cli.App. TryRun is the single
// coexistence router: a pre-existing R7 journal claims agent current; absence
// falls back to the still-active R5 command, while unsafe state fails closed.
type App struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	deps   dependencies
}

func New(stdin io.Reader, stdout, stderr io.Writer, ensure EnsureDaemonFunc) *App {
	return &App{stdin: stdin, stdout: stdout, stderr: stderr, deps: dependencies{
		workingDirectory: os.Getwd,
		ensureDaemon:     ensure,
		newClient: func(nodeState string) (agencyClient, error) {
			return newControlClient(nodeState)
		},
		random: cryptorand.Reader,
		clock:  time.Now,
	}}
}

// TryRun returns handled=false only for commands outside the R7 surface and
// for agent current when no R7 journal exists. A corrupt or unsafe journal is
// R7 state and therefore returns a fail-closed R7 error instead of falling
// through to R5.
func (app *App) TryRun(ctx context.Context, args []string) (bool, int) {
	command := classify(args)
	if command == commandOther {
		return false, 0
	}
	if app == nil || app.stdin == nil || app.stdout == nil || app.stderr == nil || ctx == nil {
		return true, 1
	}
	if !app.available(ctx) {
		return true, app.writeError(newControlError(codeInternal,
			"R7 Agent terminal is unavailable"))
	}
	if !validArguments(command, args) {
		return true, app.writeError(newControlError(codeInvalidArgument,
			"R7 Agent command requires its exact --json form"))
	}
	prepared, fallback, apiErr := app.prepare(ctx, command)
	if fallback {
		return false, 0
	}
	if apiErr != nil {
		return true, app.writeError(apiErr)
	}

	switch command {
	case commandAttach:
		return true, app.runAttach(ctx, prepared.store, prepared.client)
	case commandCurrent:
		return true, app.runCurrent(ctx, prepared.store, prepared.client)
	case commandSubmit:
		return true, app.runSubmit(ctx, prepared.store, prepared.client)
	case commandCapture:
		return true, app.runCapture(ctx, prepared.store, prepared.client)
	case commandStatus:
		return true, app.runStatus(ctx, prepared.client)
	default:
		return false, 0
	}
}

type preparedRun struct {
	store  *journalStore
	client agencyClient
}

func (app *App) available(ctx context.Context) bool {
	return app != nil && ctx != nil && app.deps.workingDirectory != nil && app.deps.ensureDaemon != nil &&
		app.deps.newClient != nil && app.deps.random != nil && app.deps.clock != nil
}

func (app *App) prepare(ctx context.Context, command commandKind) (
	preparedRun, bool, *controlError,
) {
	projectRoot, nodeState, err := resolveWorkspace(app.deps.workingDirectory)
	if err != nil {
		if command == commandCurrent {
			return preparedRun{}, true, nil
		}
		return preparedRun{}, false, newControlError(codeMnemondUnavailable,
			"Mnemon Harness is not set up in this workspace")
	}
	store := newJournalStore(nodeState, app.deps.random)
	if fallback, apiErr := currentFallback(command, store); fallback || apiErr != nil {
		return preparedRun{}, fallback, apiErr
	}
	if code, message := app.deps.ensureDaemon(ctx, projectRoot, nodeState); code != "" || message != "" {
		return preparedRun{}, false, newControlError(controlErrorCode(code), message)
	}
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		return preparedRun{}, false, classifyClientConstruction(err)
	}
	return preparedRun{store: store, client: client}, false, nil
}

func currentFallback(command commandKind, store *journalStore) (bool, *controlError) {
	if command != commandCurrent {
		return false, nil
	}
	exists, err := store.exists()
	if err != nil {
		return false, clientStateError()
	}
	return !exists, nil
}

type commandKind uint8

const (
	commandOther commandKind = iota
	commandAttach
	commandCurrent
	commandSubmit
	commandCapture
	commandStatus
)

func classify(args []string) commandKind {
	if len(args) < 2 {
		return commandOther
	}
	switch args[0] + "\x00" + args[1] {
	case "hook\x00attach":
		return commandAttach
	case "agent\x00current":
		return commandCurrent
	case "agent\x00submit":
		return commandSubmit
	case "artifact\x00capture":
		return commandCapture
	case "agency\x00status":
		return commandStatus
	default:
		return commandOther
	}
}

func validArguments(command commandKind, args []string) bool {
	return command != commandOther && len(args) == 3 && args[2] == "--json"
}

func resolveWorkspace(getwd func() (string, error)) (string, string, error) {
	cwd, err := getwd()
	if err != nil {
		return "", "", err
	}
	physical, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", "", err
	}
	current, err := filepath.Abs(physical)
	if err != nil {
		return "", "", err
	}
	for {
		nodeState := filepath.Join(current, ".mnemon", "harness", "node")
		info, statErr := os.Lstat(nodeState)
		if statErr == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return current, nodeState, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", "", errors.New("no configured Mnemon Harness workspace")
		}
		current = parent
	}
}

func classifyClientConstruction(err error) *controlError {
	if errors.Is(err, errUnsafeClientState) {
		return clientStateError()
	}
	return newControlError(codeMnemondUnavailable,
		"mnemond local Agency control is unavailable")
}

func clientStateError() *controlError {
	return newControlError(codeAuthenticationFailed,
		"R7 owner-private client state is unsafe")
}
