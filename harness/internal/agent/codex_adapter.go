package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const (
	WakeCue = "[mnemon:wake] Managed work is pending. Use the Mnemon Harness skill to process one Event."

	codexAdapterName        = "codex-app-server"
	codexClientName         = "mnemon-harness"
	codexClientVersion      = "r5"
	codexClientTitle        = "Mnemon Harness"
	codexHookStatus         = "Checking Mnemon Teamwork"
	codexSandboxMode        = "workspace-write"
	codexSandboxPolicyType  = "workspaceWrite"
	codexProtocolLineMax    = 1 << 20
	codexProtocolTotalMax   = 8 << 20
	codexProtocolMessageMax = 4096
	codexStderrMax          = 64 << 10
	codexTerminationMax     = 5 * time.Second
	codexTerminationPoll    = 10 * time.Millisecond
)

var ErrCodexWakeAdapter = errors.New("Codex managed wake adapter")

type codexAdapterFailure struct {
	stage string
	cause error
}

func (failure *codexAdapterFailure) Error() string {
	return ErrCodexWakeAdapter.Error() + ": " + failure.stage + ": " +
		codexAdapterErrorCategory(failure.cause)
}

func (failure *codexAdapterFailure) Unwrap() error { return ErrCodexWakeAdapter }

// Is preserves only the closed error categories callers need for control
// flow. The original callback/process/protocol cause remains private so an
// innocent errors.Unwrap or structured logger cannot bypass Error redaction.
func (failure *codexAdapterFailure) Is(target error) bool {
	switch target {
	case ErrCodexWakeAdapter:
		return true
	case context.Canceled, context.DeadlineExceeded, io.EOF,
		ErrRuntimeProcess, ErrRuntimeProcessLive, ErrRuntimeProcessUncertain:
		return errors.Is(failure.cause, target)
	default:
		return false
	}
}

type CodexLaunchEvidence struct {
	At         time.Time
	Diagnostic model.JSON
	RuntimeIDs model.JSON
}

type CodexWakeEvidence struct {
	At      time.Time
	Receipt model.JSON
}

type CodexWakeResult struct {
	At                time.Time
	LaunchAt          time.Time
	WakeAt            time.Time
	Diagnostic        model.JSON
	RuntimeIDs        model.JSON
	WakeReceipt       model.JSON
	CompletionReceipt model.JSON
	WakeDelivered     bool
	ProcessExited     bool
}

type CodexWakeCallbacks struct {
	RecordLaunch func(context.Context, CodexLaunchEvidence) error
	RecordWake   func(context.Context, CodexWakeEvidence) error
}

type CodexWakeRequest struct {
	RunAttachmentEnvironment string
	Callbacks                CodexWakeCallbacks
}

type CodexAdapterClock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type CodexProcessStartSpec struct {
	Executable  string
	Arguments   []string
	Directory   string
	Environment []string
}

type CodexProcess interface {
	PID() int
	Stdin() io.WriteCloser
	Stdout() io.ReadCloser
	Stderr() io.ReadCloser
	Signal(syscall.Signal) error
	Wait() error
}

type CodexProcessStarter interface {
	Start(CodexProcessStartSpec) (CodexProcess, error)
}

type CodexProcessIdentity struct {
	RuntimeIDs model.JSON
}

type CodexProcessIdentityProbe interface {
	Identify(int) (CodexProcessIdentity, error)
}

type CodexProcessTerminator interface {
	Observe(context.Context, model.JSON) error
	Terminate(context.Context, model.JSON) ([]string, error)
}

type CodexWakeAdapterOptions struct {
	Executable       string
	Workspace        string
	Environment      []string
	Starter          CodexProcessStarter
	Identity         CodexProcessIdentityProbe
	Clock            CodexAdapterClock
	Terminator       CodexProcessTerminator
	VerifyProjection func() error
	InterruptGrace   time.Duration
	ExitGrace        time.Duration
	SignalGrace      time.Duration
}

type CodexWakeAdapter struct {
	executable       string
	workspace        string
	environment      []string
	starter          CodexProcessStarter
	identity         CodexProcessIdentityProbe
	clock            CodexAdapterClock
	terminator       CodexProcessTerminator
	verifyProjection func() error
	interruptGrace   time.Duration
	exitGrace        time.Duration
	signalGrace      time.Duration
}

type codexProcessExit struct {
	exited  bool
	method  string
	signals []string
}

type wallCodexAdapterClock struct{}

func (wallCodexAdapterClock) Now() time.Time { return time.Now() }
func (wallCodexAdapterClock) After(duration time.Duration) <-chan time.Time {
	return time.After(duration)
}

func NewCodexWakeAdapter(options CodexWakeAdapterOptions) (*CodexWakeAdapter, error) {
	if options.Executable == "" || !filepath.IsAbs(options.Executable) ||
		filepath.Clean(options.Executable) != options.Executable {
		return nil, codexAdapterError("configure", errors.New("executable must be absolute and clean"))
	}
	if options.Workspace == "" || !filepath.IsAbs(options.Workspace) ||
		filepath.Clean(options.Workspace) != options.Workspace {
		return nil, codexAdapterError("configure", errors.New("workspace must be absolute and clean"))
	}
	environment, err := validateCodexBaseEnvironment(options.Environment)
	if err != nil {
		return nil, codexAdapterError("configure", err)
	}
	if options.VerifyProjection == nil {
		return nil, codexAdapterError("configure", errors.New("projection verifier is required"))
	}
	if options.Identity == nil || options.Terminator == nil {
		if err := checkSystemRuntimeProcessSupport(); err != nil {
			return nil, codexAdapterError("runtime readiness", err)
		}
	}
	if options.Starter == nil {
		options.Starter = execCodexProcessStarter{}
	}
	if options.Identity == nil {
		options.Identity = systemCodexProcessIdentityProbe{}
	}
	if options.Clock == nil {
		options.Clock = wallCodexAdapterClock{}
	}
	if options.Terminator == nil {
		options.Terminator = systemCodexProcessTerminator{}
	}
	if options.InterruptGrace == 0 {
		options.InterruptGrace = 2 * time.Second
	}
	if options.ExitGrace == 0 {
		options.ExitGrace = 2 * time.Second
	}
	if options.SignalGrace == 0 {
		options.SignalGrace = time.Second
	}
	if options.InterruptGrace < time.Millisecond || options.InterruptGrace > 30*time.Second ||
		options.ExitGrace < time.Millisecond || options.ExitGrace > 30*time.Second ||
		options.SignalGrace < time.Millisecond || options.SignalGrace > 30*time.Second {
		return nil, codexAdapterError("configure", errors.New("cleanup deadlines must be 1ms..30s"))
	}
	return &CodexWakeAdapter{executable: options.Executable, workspace: options.Workspace,
		environment: environment, starter: options.Starter, identity: options.Identity,
		clock: options.Clock, terminator: options.Terminator,
		verifyProjection: options.VerifyProjection,
		interruptGrace:   options.InterruptGrace, exitGrace: options.ExitGrace,
		signalGrace: options.SignalGrace}, nil
}

func (adapter *CodexWakeAdapter) Run(ctx context.Context,
	request CodexWakeRequest,
) (result CodexWakeResult, runErr error) {
	if adapter == nil || ctx == nil || request.Callbacks.RecordLaunch == nil ||
		request.Callbacks.RecordWake == nil {
		return result, codexAdapterError("run", errors.New("adapter, context and callbacks are required"))
	}
	attachment, err := validateCodexAttachmentEnvironment(request.RunAttachmentEnvironment)
	if err != nil {
		return result, codexAdapterError("run", err)
	}
	if err := ctx.Err(); err != nil {
		return adapter.settleNotStarted(result, codexAdapterError("run", err))
	}
	// This gate is deliberately inside every Run and immediately before
	// process creation. Daemon-start readiness is not sufficient: projected
	// Hook/Skill assets may drift while mnemond remains alive.
	if err := adapter.verifyProjection(); err != nil {
		return adapter.settleNotStarted(result, codexAdapterError("verify projection", err))
	}
	if err := ctx.Err(); err != nil {
		return adapter.settleNotStarted(result, codexAdapterError("run", err))
	}
	process, err := adapter.starter.Start(CodexProcessStartSpec{Executable: adapter.executable,
		Arguments: []string{"app-server", "--stdio"}, Directory: adapter.workspace,
		Environment: append(append([]string(nil), adapter.environment...), attachment)})
	if err != nil {
		return adapter.settleNotStarted(result, codexAdapterError("start", err))
	}
	if process == nil {
		return adapter.settleNotStarted(result,
			codexAdapterError("start", errors.New("starter returned no process")))
	}
	identity, err := adapter.identity.Identify(process.PID())
	if err != nil {
		failure := adapter.settleUnregisteredProcess(&result, process, "launch_identity_failed",
			codexAdapterError("identify", err))
		return result, failure
	}
	if _, err := validateCodexProcessIdentity(process.PID(), identity); err != nil {
		failure := adapter.settleUnregisteredProcess(&result, process, "launch_identity_failed",
			codexAdapterError("identify", err))
		return result, failure
	}
	session, err := newCodexProtocolSession(adapter, process)
	if err != nil {
		failure := adapter.settleRegisteredProcess(&result, process, identity.RuntimeIDs,
			"launch_contract_failed", codexAdapterError("start", err))
		return result, failure
	}
	session.runtimeIDs = identity.RuntimeIDs
	completedNormally := false
	defer func() {
		exit, cleanupErr := session.close(!completedNormally)
		result.ProcessExited = exit.exited
		if cleanupErr != nil {
			runErr = errors.Join(runErr, cleanupErr)
		}
		if !exit.exited {
			return
		}
		completionAt, atErr := adapter.trustedNow()
		if atErr != nil {
			runErr = errors.Join(runErr, codexAdapterError("completion evidence", atErr))
			return
		}
		result.At = completionAt
		status := "failed"
		if runErr == nil {
			status = "completed"
		} else if completedNormally && cleanupErr != nil {
			status = "cleanup_failed"
		}
		result.CompletionReceipt, atErr = codexCompletionReceipt(status, session.threadID,
			session.turnID, result.WakeDelivered, exit.method, exit.signals)
		if atErr != nil {
			runErr = errors.Join(runErr, codexAdapterError("completion evidence", atErr))
		}
	}()
	// Persist the exact process incarnation before sending any protocol
	// request. If mnemond dies before this callback commits, the child has not
	// received work and the closed stdin pipe is its only shutdown authority.
	// Once the callback commits, startup recovery has the boot/start-token
	// authority needed to prove the exact Runtime's exit or remain unready while
	// that orphan is still live.
	launchAt, err := adapter.trustedNow()
	if err != nil {
		return result, codexAdapterError("launch evidence", err)
	}
	diagnostic, runtimeIDs, err := codexLaunchJSON(adapter.executable, identity)
	if err != nil {
		return result, codexAdapterError("launch evidence", err)
	}
	result.LaunchAt, result.Diagnostic, result.RuntimeIDs = launchAt, diagnostic, runtimeIDs
	if err := request.Callbacks.RecordLaunch(ctx, CodexLaunchEvidence{At: launchAt,
		Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err != nil {
		return result, codexAdapterError("record launch", err)
	}

	initializeRaw, err := session.call(ctx, 1, "initialize", map[string]any{
		"clientInfo": map[string]any{"name": codexClientName, "version": codexClientVersion,
			"title": codexClientTitle},
	})
	if err != nil {
		return result, codexAdapterError("initialize", err)
	}
	if err := validateCodexInitializeResult(initializeRaw); err != nil {
		return result, codexAdapterError("initialize", err)
	}
	if err := session.notify("initialized", nil); err != nil {
		return result, codexAdapterError("initialize", err)
	}

	hooksRaw, err := session.call(ctx, 2, "hooks/list", map[string]any{
		"cwds": []string{adapter.workspace},
	})
	if err != nil {
		return result, codexAdapterError("list hooks", err)
	}
	hook, err := validateCodexManagedHook(adapter.workspace, hooksRaw)
	if err != nil {
		return result, codexAdapterError("list hooks", err)
	}
	threadRaw, err := session.call(ctx, 3, "thread/start", map[string]any{
		"cwd": adapter.workspace, "ephemeral": true, "approvalPolicy": "never",
		"approvalsReviewer": "user", "sandbox": codexSandboxMode,
	})
	if err != nil {
		return result, codexAdapterError("start thread", err)
	}
	threadID, err := decodeCodexThreadStart(threadRaw, adapter.workspace)
	if err != nil {
		return result, codexAdapterError("start thread", err)
	}
	session.threadID = threadID
	skillPath := filepath.Join(adapter.workspace, ".agents", "skills", "mnemon-harness", "SKILL.md")
	turnRaw, err := session.call(ctx, 4, "turn/start", map[string]any{
		"threadId": threadID,
		"input": []map[string]any{{"type": "text", "text": "$mnemon-harness"},
			{"type": "skill", "name": "mnemon-harness", "path": skillPath}},
		"cwd": adapter.workspace, "approvalPolicy": "never", "approvalsReviewer": "user",
		"sandboxPolicy": map[string]any{"type": codexSandboxPolicyType,
			"writableRoots": []string{}, "networkAccess": false,
			"excludeTmpdirEnvVar": false, "excludeSlashTmp": false},
	})
	if err != nil {
		return result, codexAdapterError("start turn", err)
	}
	turnID, err := decodeCodexTurnStart(turnRaw)
	if err != nil {
		return result, codexAdapterError("start turn", err)
	}
	session.turnID = turnID
	proof := codexHookProof{expected: hook, threadID: threadID, turnID: turnID}
	handleNotification := func(envelope codexRPCEnvelope) (bool, error) {
		if len(envelope.ID) != 0 {
			return false, codexAdapterError("run turn", errors.New("unexpected response"))
		}
		switch envelope.Method {
		case "hook/started", "hook/completed":
			if err := proof.observe(envelope); err != nil {
				return false, codexAdapterError("observe Hook", err)
			}
			if !proof.complete || result.WakeDelivered {
				return false, nil
			}
			wakeAt, err := adapter.trustedNow()
			if err != nil {
				return false, codexAdapterError("wake evidence", err)
			}
			receipt, err := proof.receipt()
			if err != nil {
				return false, codexAdapterError("wake evidence", err)
			}
			result.WakeAt, result.WakeReceipt = wakeAt, receipt
			if err := request.Callbacks.RecordWake(ctx, CodexWakeEvidence{At: wakeAt,
				Receipt: receipt}); err != nil {
				return false, codexAdapterError("record wake", err)
			}
			result.WakeDelivered = true
			return false, nil
		case "turn/completed":
			status, err := decodeCodexTurnCompleted(envelope.Params, threadID, turnID)
			if err != nil {
				return false, codexAdapterError("complete turn", err)
			}
			if !proof.complete || !result.WakeDelivered {
				return false, codexAdapterError("complete turn",
					errors.New("managed Hook proof is missing"))
			}
			if status != "completed" {
				return false, codexAdapterError("complete turn",
					fmt.Errorf("turn status is %s", status))
			}
			return true, nil
		default:
			return false, nil
		}
	}
	pendingCompleted := false
	for _, pending := range session.takePendingNotifications() {
		if pendingCompleted && pending.Method == "turn/completed" {
			return result, codexAdapterError("complete turn",
				errors.New("duplicate turn completion"))
		}
		completed, err := handleNotification(pending)
		if err != nil {
			return result, err
		}
		if completed {
			pendingCompleted = true
		}
	}
	if pendingCompleted {
		completedNormally = true
		return result, nil
	}

	for {
		envelope, err := session.next(ctx)
		if err != nil {
			return result, codexAdapterError("run turn", err)
		}
		completed, err := handleNotification(envelope)
		if err != nil {
			return result, err
		}
		if completed {
			completedNormally = true
			return result, nil
		}
	}
}

func (adapter *CodexWakeAdapter) settleNotStarted(result CodexWakeResult,
	failure error,
) (CodexWakeResult, error) {
	result.ProcessExited = true
	completionAt, atErr := adapter.trustedNow()
	if atErr != nil {
		return result, errors.Join(failure, codexAdapterError("completion evidence", atErr))
	}
	result.At = completionAt
	result.CompletionReceipt, atErr = codexCompletionReceipt("launch_failed", "", "", false,
		"not_started", nil)
	if atErr != nil {
		return result, errors.Join(failure, codexAdapterError("completion evidence", atErr))
	}
	return result, failure
}

func (adapter *CodexWakeAdapter) settleUnregisteredProcess(result *CodexWakeResult,
	process CodexProcess, status string, failure error,
) error {
	exit, cleanupErr := adapter.closeUnregisteredProcess(process)
	result.ProcessExited = exit.exited
	if cleanupErr != nil {
		failure = errors.Join(failure, codexAdapterError("cleanup unregistered process", cleanupErr))
	}
	if !exit.exited {
		return failure
	}
	completionAt, atErr := adapter.trustedNow()
	if atErr != nil {
		return errors.Join(failure, codexAdapterError("completion evidence", atErr))
	}
	result.At = completionAt
	result.CompletionReceipt, atErr = codexCompletionReceipt(status, "", "", false,
		exit.method, exit.signals)
	if atErr != nil {
		return errors.Join(failure, codexAdapterError("completion evidence", atErr))
	}
	return failure
}

func (adapter *CodexWakeAdapter) settleRegisteredProcess(result *CodexWakeResult,
	process CodexProcess, runtimeIDs model.JSON, status string, failure error,
) error {
	exit, cleanupErr := adapter.closeRegisteredProcess(process, runtimeIDs)
	result.ProcessExited = exit.exited
	if cleanupErr != nil {
		failure = errors.Join(failure, codexAdapterError("cleanup registered process", cleanupErr))
	}
	if !exit.exited {
		return failure
	}
	completionAt, atErr := adapter.trustedNow()
	if atErr != nil {
		return errors.Join(failure, codexAdapterError("completion evidence", atErr))
	}
	result.At = completionAt
	result.CompletionReceipt, atErr = codexCompletionReceipt(status, "", "", false,
		exit.method, exit.signals)
	if atErr != nil {
		return errors.Join(failure, codexAdapterError("completion evidence", atErr))
	}
	return failure
}

func (adapter *CodexWakeAdapter) closeRegisteredProcess(process CodexProcess,
	runtimeIDs model.JSON,
) (codexProcessExit, error) {
	exit := codexProcessExit{signals: make([]string, 0)}
	if process == nil {
		return exit, errors.New("registered process is missing")
	}
	for _, stream := range []io.Closer{process.Stdin(), process.Stdout(), process.Stderr()} {
		if stream != nil {
			_ = stream.Close()
		}
	}
	// Exact Runtime identity has already been validated. Retain the direct
	// child unreaped while the terminator proves the process group's exit;
	// otherwise Wait could release its PID/PGID before group authority is used.
	terminateCtx, cancel := context.WithTimeout(context.Background(),
		codexTerminationMax+adapter.signalGrace)
	signals, err := adapter.terminator.Terminate(terminateCtx, runtimeIDs)
	cancel()
	if err != nil {
		return exit, err
	}
	if len(signals) != 0 {
		if err := validateCodexTerminationSignals(signals); err != nil {
			return exit, err
		}
		exit.signals = append(exit.signals, signals...)
	}
	exit.method = "wait_without_signal"
	if len(exit.signals) != 0 {
		exit.method = "signal_assisted"
	}
	waited := make(chan error, 1)
	go func() { waited <- process.Wait() }()
	select {
	case err := <-waited:
		exit.exited = true
		if err != nil {
			var exitErr *exec.ExitError
			if len(exit.signals) == 0 || !errors.As(err, &exitErr) {
				return exit, err
			}
		}
		return exit, nil
	case <-adapter.clock.After(adapter.exitGrace):
		return exit, errors.New("registered process did not become waitable after exact termination proof")
	}
}

func (adapter *CodexWakeAdapter) closeUnregisteredProcess(process CodexProcess) (codexProcessExit, error) {
	if process == nil {
		return codexProcessExit{exited: true, method: "not_started", signals: []string{}}, nil
	}
	if stream := process.Stdin(); stream != nil {
		_ = stream.Close()
	}
	if stream := process.Stdout(); stream != nil {
		_ = stream.Close()
	}
	if stream := process.Stderr(); stream != nil {
		_ = stream.Close()
	}
	exit := codexProcessExit{method: "wait_without_signal", signals: make([]string, 0)}
	// Identity capture failed before the first protocol byte was sent, so this
	// direct child cannot hold managed work. Signal only the exact os.Process
	// handle—not its unvalidated numeric group—and do so before Wait can reap
	// and release the PID. This closes the only safe cleanup window for an
	// unregistered child that ignores stdio EOF.
	if err := process.Signal(syscall.SIGKILL); err == nil {
		exit.method = "signal_assisted"
		exit.signals = []string{"SIGKILL"}
	} else if !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return exit, fmt.Errorf("kill exact unregistered child: %w", err)
	}
	waited := make(chan error, 1)
	go func() { waited <- process.Wait() }()
	select {
	case err := <-waited:
		exit.exited = true
		if err != nil {
			var exitErr *exec.ExitError
			if len(exit.signals) == 0 || !errors.As(err, &exitErr) {
				return exit, err
			}
		}
		return exit, nil
	case <-adapter.clock.After(adapter.exitGrace):
		return exit, errors.New("unregistered process did not become waitable after exact kill")
	}
}

func (adapter *CodexWakeAdapter) trustedNow() (time.Time, error) {
	at := adapter.clock.Now().Round(0).UTC()
	if at.IsZero() || at.UnixNano() <= 0 || !time.Unix(0, at.UnixNano()).UTC().Equal(at) {
		return time.Time{}, errors.New("clock returned a non-canonical instant")
	}
	return at, nil
}

func codexAdapterError(stage string, err error) error {
	if err == nil {
		err = errors.New("unknown failure")
	}
	return &codexAdapterFailure{stage: stage, cause: err}
}

func codexAdapterErrorCategory(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, io.EOF):
		return "protocol_closed"
	case errors.Is(err, ErrRuntimeProcessLive):
		return "runtime_live"
	case errors.Is(err, ErrRuntimeProcessUncertain):
		return "runtime_identity_uncertain"
	case errors.Is(err, ErrRuntimeProcess):
		return "runtime_identity_invalid"
	default:
		return "failed"
	}
}

func validateCodexBaseEnvironment(environment []string) ([]string, error) {
	result := append([]string(nil), environment...)
	seen := make(map[string]struct{}, len(result))
	for _, entry := range result {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || name == "" || strings.ContainsAny(name, "\x00\r\n") ||
			strings.ContainsAny(entry, "\x00\r\n") {
			return nil, errors.New("base environment contains an invalid assignment")
		}
		if name == localapi.RunAttachmentEnv {
			return nil, errors.New("base environment contains a Run attachment")
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, errors.New("base environment contains a duplicate variable")
		}
		seen[name] = struct{}{}
	}
	return result, nil
}

func validateCodexAttachmentEnvironment(assignment string) (string, error) {
	name, value, ok := strings.Cut(assignment, "=")
	if !ok || name != localapi.RunAttachmentEnv || value == "" ||
		strings.ContainsAny(assignment, "\x00\r\n") || !filepath.IsAbs(value) ||
		filepath.Clean(value) != value {
		return "", errors.New("one exact Run attachment environment assignment is required")
	}
	return assignment, nil
}

func validateCodexProcessIdentity(pid int,
	identity CodexProcessIdentity,
) (runtimeProcessIDs, error) {
	ids, err := parseRuntimeProcessIDs(identity.RuntimeIDs)
	if err != nil {
		return runtimeProcessIDs{}, err
	}
	if pid <= 0 || ids.PID != pid || ids.PGID != pid || ids.SID != pid {
		return runtimeProcessIDs{}, errors.New(
			"process identity differs from the isolated Runtime leader")
	}
	return ids, nil
}

func codexLaunchJSON(executable string,
	identity CodexProcessIdentity,
) (model.JSON, model.JSON, error) {
	diagnostic, err := model.JSONFrom(struct {
		Adapter       string `json:"adapter"`
		Client        string `json:"client"`
		Executable    string `json:"executable"`
		Protocol      string `json:"protocol"`
		SchemaVersion int    `json:"schema_version"`
	}{codexAdapterName, codexClientName, executable, "jsonl-v2", 1})
	if err != nil {
		return model.JSON{}, model.JSON{}, err
	}
	return diagnostic, identity.RuntimeIDs, nil
}

func codexCompletionReceipt(status, threadID, turnID string, wake bool,
	exitMethod string, signals []string,
) (model.JSON, error) {
	if err := validateCodexCompletionAuthority(status, threadID, turnID, wake,
		exitMethod, signals); err != nil {
		return model.JSON{}, err
	}
	closedSignals := append([]string(nil), signals...)
	if closedSignals == nil {
		closedSignals = make([]string, 0)
	}
	receipt, err := model.JSONFrom(struct {
		Adapter       string   `json:"adapter"`
		ExitMethod    string   `json:"exit_method"`
		SchemaVersion int      `json:"schema_version"`
		Signals       []string `json:"signals"`
		Status        string   `json:"status"`
		ThreadID      string   `json:"thread_id,omitempty"`
		TurnID        string   `json:"turn_id,omitempty"`
		WakeDelivered bool     `json:"wake_delivered"`
	}{codexAdapterName, exitMethod, 1, closedSignals, status, threadID, turnID, wake})
	if err != nil {
		return model.JSON{}, err
	}
	return receipt, nil
}

func validateCodexCompletionAuthority(status, threadID, turnID string, wake bool,
	exitMethod string, signals []string,
) error {
	if len(threadID) > 256 || len(turnID) > 256 || !utf8.ValidString(threadID) ||
		!utf8.ValidString(turnID) || turnID != "" && threadID == "" ||
		wake && (threadID == "" || turnID == "") {
		return errors.New("completion identifiers and wake authority are inconsistent")
	}
	switch status {
	case "launch_failed":
		if threadID != "" || turnID != "" || wake || exitMethod != "not_started" {
			return errors.New("launch failure authority is inconsistent")
		}
	case "launch_identity_failed", "launch_contract_failed":
		if threadID != "" || turnID != "" || wake || exitMethod == "not_started" {
			return errors.New("registered launch failure authority is inconsistent")
		}
	case "failed":
		if exitMethod == "not_started" {
			return errors.New("managed failure cannot be not-started")
		}
	case "completed":
		if !wake || threadID == "" || turnID == "" || exitMethod != "wait_without_signal" {
			return errors.New("clean completion authority is inconsistent")
		}
	case "cleanup_failed":
		if !wake || threadID == "" || turnID == "" || exitMethod == "not_started" {
			return errors.New("cleanup failure authority is inconsistent")
		}
	default:
		return errors.New("completion status is outside the closed schema")
	}
	switch exitMethod {
	case "not_started", "wait_without_signal":
		if len(signals) != 0 {
			return errors.New("signal-free completion contains signals")
		}
	case "signal_assisted":
		if err := validateCodexTerminationSignals(signals); err != nil {
			return err
		}
	default:
		return errors.New("completion exit method is outside the closed schema")
	}
	return nil
}

func validateCodexTerminationSignals(signals []string) error {
	valid := len(signals) == 1 && signals[0] == "SIGKILL" ||
		len(signals) == 2 && signals[0] == "SIGSTOP" && signals[1] == "SIGKILL"
	if !valid {
		return errors.New("termination signals are outside the closed schema")
	}
	return nil
}

type execCodexProcessStarter struct{}

func (execCodexProcessStarter) Start(spec CodexProcessStartSpec) (CodexProcess, error) {
	command := exec.Command(spec.Executable, spec.Arguments...)
	command.Dir = spec.Directory
	command.Env = append([]string(nil), spec.Environment...)
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, err
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, err
	}
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, err
	}
	return &execCodexProcess{command: command, stdin: stdin, stdout: stdout, stderr: stderr}, nil
}

type execCodexProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
}

func (process *execCodexProcess) PID() int              { return process.command.Process.Pid }
func (process *execCodexProcess) Stdin() io.WriteCloser { return process.stdin }
func (process *execCodexProcess) Stdout() io.ReadCloser { return process.stdout }
func (process *execCodexProcess) Stderr() io.ReadCloser { return process.stderr }
func (process *execCodexProcess) Signal(signal syscall.Signal) error {
	return process.command.Process.Signal(signal)
}
func (process *execCodexProcess) Wait() error { return process.command.Wait() }

type systemCodexProcessTerminator struct{}

func (systemCodexProcessTerminator) Observe(ctx context.Context, runtimeIDs model.JSON) error {
	ids, err := parseRuntimeProcessIDs(runtimeIDs)
	if err != nil {
		return err
	}
	proof, err := observeOwnedSystemRuntimeProcess(ctx, ids)
	if err != nil {
		return err
	}
	return validateRuntimeProcessProof(proof)
}

func (systemCodexProcessTerminator) Terminate(ctx context.Context,
	runtimeIDs model.JSON,
) ([]string, error) {
	ids, err := parseRuntimeProcessIDs(runtimeIDs)
	if err != nil {
		return nil, err
	}
	proof, err := terminateOwnedSystemRuntimeProcess(ctx, ids)
	if err != nil {
		return nil, err
	}
	if err := validateRuntimeProcessProof(proof); err != nil {
		return nil, err
	}
	return append([]string(nil), proof.signals...), nil
}

type systemCodexProcessIdentityProbe struct{}

func (systemCodexProcessIdentityProbe) Identify(pid int) (CodexProcessIdentity, error) {
	_, runtimeIDs, err := captureRuntimeProcessIDs(pid)
	if err != nil {
		return CodexProcessIdentity{}, err
	}
	return CodexProcessIdentity{RuntimeIDs: runtimeIDs}, nil
}

type codexRPCEnvelope struct {
	JSONRPC *string         `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type codexProtocolEvent struct {
	envelope codexRPCEnvelope
	err      error
}

type codexProtocolSession struct {
	adapter        *CodexWakeAdapter
	process        CodexProcess
	stdin          io.WriteCloser
	stdout         io.ReadCloser
	stderr         io.ReadCloser
	events         chan codexProtocolEvent
	stopReader     chan struct{}
	waited         chan error
	stderrOverflow chan struct{}
	stdoutDone     chan struct{}
	stderrDone     chan struct{}
	writeMu        sync.Mutex
	closeOnce      sync.Once
	waitOnce       sync.Once
	waitMu         sync.Mutex
	waitDone       bool
	waitErr        error
	stdoutEOF      atomic.Bool
	stderrExceeded atomic.Bool
	runtimeIDs     model.JSON
	threadID       string
	turnID         string
	pending        []codexRPCEnvelope
}

func newCodexProtocolSession(adapter *CodexWakeAdapter,
	process CodexProcess,
) (*codexProtocolSession, error) {
	if process == nil || process.PID() <= 0 {
		return nil, errors.New("started process does not expose complete stdio")
	}
	stdin, stdout, stderr := process.Stdin(), process.Stdout(), process.Stderr()
	if stdin == nil || stdout == nil || stderr == nil {
		return nil, errors.New("started process does not expose complete stdio")
	}
	session := &codexProtocolSession{adapter: adapter, process: process, stdin: stdin,
		stdout: stdout, stderr: stderr,
		events: make(chan codexProtocolEvent, 1), stopReader: make(chan struct{}),
		waited: make(chan error, 1), stderrOverflow: make(chan struct{}, 1),
		stdoutDone: make(chan struct{}), stderrDone: make(chan struct{})}
	go session.readStdout(stdout)
	go session.readStderr(stderr)
	return session, nil
}

func (session *codexProtocolSession) readStdout(source io.ReadCloser) {
	defer close(session.stdoutDone)
	defer source.Close()
	reader := bufio.NewScanner(source)
	reader.Buffer(make([]byte, 4096), codexProtocolLineMax+1)
	total, messages := 0, 0
	for reader.Scan() {
		line := append([]byte(nil), reader.Bytes()...)
		total += len(line) + 1
		messages++
		var event codexProtocolEvent
		if len(line) == 0 || len(line) > codexProtocolLineMax || total > codexProtocolTotalMax ||
			messages > codexProtocolMessageMax || !utf8.Valid(line) {
			event.err = errors.New("protocol output exceeded a bound or is invalid UTF-8")
		} else {
			event.envelope, event.err = decodeCodexEnvelope(line)
		}
		select {
		case session.events <- event:
		case <-session.stopReader:
			return
		}
		if event.err != nil {
			return
		}
	}
	err := reader.Err()
	if err == nil {
		err = io.EOF
		session.stdoutEOF.Store(true)
	}
	select {
	case session.events <- codexProtocolEvent{err: err}:
	case <-session.stopReader:
	}
}

func (session *codexProtocolSession) readStderr(source io.ReadCloser) {
	defer close(session.stderrDone)
	defer source.Close()
	read, _ := io.Copy(io.Discard, io.LimitReader(source, codexStderrMax+1))
	if read > codexStderrMax {
		session.stderrExceeded.Store(true)
		session.stderrOverflow <- struct{}{}
		_, _ = io.Copy(io.Discard, source)
	}
}

func (session *codexProtocolSession) waitForProcessPipeDrain(duration time.Duration) (bool, error) {
	timer := session.adapter.clock.After(duration)
	stdoutDone, stderrDone := session.stdoutDone, session.stderrDone
	var result error
	forcedClose := false
	for stdoutDone != nil || stderrDone != nil {
		select {
		case <-session.stderrOverflow:
			result = errors.Join(result, errors.New("Codex stderr exceeded its bound"))
		case event := <-session.events:
			if event.err != nil && !errors.Is(event.err, io.EOF) {
				result = errors.Join(result, event.err)
			}
			if event.err == nil {
				session.pending = append(session.pending, event.envelope)
				if len(session.pending) > codexProtocolMessageMax {
					result = errors.Join(result,
						errors.New("cleanup notifications exceeded their bound"))
				}
			}
		case <-stdoutDone:
			stdoutDone = nil
		case <-stderrDone:
			stderrDone = nil
		case <-timer:
			if forcedClose {
				return false, errors.Join(result,
					errors.New("Codex process pipe readers did not stop after close"))
			}
			_ = session.stdout.Close()
			_ = session.stderr.Close()
			result = errors.Join(result, errors.New("Codex process pipes did not drain after exit proof"))
			forcedClose = true
			timer = session.adapter.clock.After(duration)
		}
	}
	if session.stderrExceeded.Load() {
		result = errors.Join(result, errors.New("Codex stderr exceeded its bound"))
	}
	return true, result
}

func decodeCodexEnvelope(line []byte) (codexRPCEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return codexRPCEnvelope{}, errors.New("protocol envelope is not an object")
	}
	var envelope codexRPCEnvelope
	seen := make(map[string]struct{})
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return codexRPCEnvelope{}, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return codexRPCEnvelope{}, errors.New("protocol envelope key is invalid")
		}
		if _, duplicate := seen[key]; duplicate {
			return codexRPCEnvelope{}, errors.New("protocol envelope has a duplicate field")
		}
		seen[key] = struct{}{}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return codexRPCEnvelope{}, err
		}
		switch key {
		case "jsonrpc":
			var version string
			if err := json.Unmarshal(raw, &version); err != nil {
				return codexRPCEnvelope{}, errors.New("protocol version is invalid")
			}
			envelope.JSONRPC = &version
		case "id":
			envelope.ID = raw
		case "method":
			if err := json.Unmarshal(raw, &envelope.Method); err != nil {
				return codexRPCEnvelope{}, errors.New("protocol method is invalid")
			}
		case "params":
			envelope.Params = raw
		case "result":
			envelope.Result = raw
		case "error":
			envelope.Error = raw
		default:
			// App-server envelopes are additive. Unknown fields carry no
			// authority and are intentionally discarded.
		}
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return codexRPCEnvelope{}, errors.New("protocol envelope is unterminated")
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return codexRPCEnvelope{}, errors.New("protocol envelope has trailing data")
	}
	if envelope.JSONRPC != nil && *envelope.JSONRPC != "2.0" {
		return codexRPCEnvelope{}, errors.New("protocol version is invalid")
	}
	hasID := len(envelope.ID) != 0
	if hasID && envelope.Method != "" {
		return codexRPCEnvelope{}, errors.New("server requests are not supported")
	}
	if hasID {
		if (len(envelope.Result) == 0) == (len(envelope.Error) == 0) || len(envelope.Params) != 0 {
			return codexRPCEnvelope{}, errors.New("response envelope is malformed")
		}
		return envelope, nil
	}
	if envelope.Method == "" || len(envelope.Result) != 0 || len(envelope.Error) != 0 {
		return codexRPCEnvelope{}, errors.New("notification envelope is malformed")
	}
	return envelope, nil
}

func (session *codexProtocolSession) send(value any) error {
	session.writeMu.Lock()
	defer session.writeMu.Unlock()
	encoder := json.NewEncoder(session.stdin)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(value)
}

func (session *codexProtocolSession) notify(method string, params any) error {
	value := map[string]any{"method": method}
	if params != nil {
		value["params"] = params
	}
	return session.send(value)
}

func (session *codexProtocolSession) call(ctx context.Context, id int, method string,
	params any,
) (json.RawMessage, error) {
	if err := session.send(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	for {
		envelope, err := session.next(ctx)
		if err != nil {
			return nil, err
		}
		if len(envelope.ID) == 0 {
			session.pending = append(session.pending, envelope)
			if len(session.pending) > codexProtocolMessageMax {
				return nil, errors.New("pending notifications exceeded their bound")
			}
			continue
		}
		responseID, err := decodeCodexResponseID(envelope.ID)
		if err != nil || responseID != id {
			return nil, errors.New("response ID is invalid or out of order")
		}
		if len(envelope.Error) != 0 {
			return nil, decodeCodexResponseError(envelope.Error)
		}
		return append(json.RawMessage(nil), envelope.Result...), nil
	}
}

func (session *codexProtocolSession) next(ctx context.Context) (codexRPCEnvelope, error) {
	select {
	case event := <-session.events:
		return event.envelope, event.err
	case <-session.stderrOverflow:
		return codexRPCEnvelope{}, errors.New("Codex stderr exceeded its bound")
	case <-ctx.Done():
		return codexRPCEnvelope{}, ctx.Err()
	}
}

func (session *codexProtocolSession) takePendingNotifications() []codexRPCEnvelope {
	result := append([]codexRPCEnvelope(nil), session.pending...)
	session.pending = nil
	return result
}

func decodeCodexResponseID(raw json.RawMessage) (int, error) {
	var value int
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil || value <= 0 {
		return 0, errors.New("response ID must be a positive integer")
	}
	return value, nil
}

func decodeCodexResponseError(raw json.RawMessage) error {
	if err := validateCodexJSONAuthority(raw); err != nil {
		return errors.New("server returned a malformed error")
	}
	var value struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data,omitempty"`
	}
	if err := json.Unmarshal(raw, &value); err != nil || value.Message == "" ||
		len(value.Message) > 1024 || !utf8.ValidString(value.Message) {
		return errors.New("server returned a malformed error")
	}
	category := "remote_error"
	switch value.Code {
	case -32700:
		category = "parse_error"
	case -32600:
		category = "invalid_request"
	case -32601:
		category = "method_not_found"
	case -32602:
		category = "invalid_params"
	case -32603:
		category = "internal_error"
	}
	return fmt.Errorf("server error %s (%d)", category, value.Code)
}

func validateCodexJSONAuthority(raw json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 64 {
			return errors.New("protocol payload nesting exceeds its bound")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, composite := token.(json.Delim)
		if !composite {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("protocol object key is invalid")
				}
				if _, duplicate := seen[key]; duplicate {
					return errors.New("protocol payload has a duplicate field")
				}
				seen[key] = struct{}{}
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
		default:
			return errors.New("protocol payload delimiter is invalid")
		}
		end, err := decoder.Token()
		if err != nil || end != matchingCodexJSONDelimiter(delimiter) {
			return errors.New("protocol payload is unterminated")
		}
		return nil
	}
	if err := walk(0); err != nil {
		return err
	}
	if token, err := decoder.Token(); err == nil || token != nil {
		return errors.New("protocol payload has trailing data")
	}
	return nil
}

func matchingCodexJSONDelimiter(start json.Delim) json.Delim {
	if start == '{' {
		return '}'
	}
	if start == '[' {
		return ']'
	}
	return 0
}

func (session *codexProtocolSession) setWait(err error) {
	session.waitMu.Lock()
	defer session.waitMu.Unlock()
	if !session.waitDone {
		session.waitDone, session.waitErr = true, err
	}
}

func (session *codexProtocolSession) startWait() {
	session.waitOnce.Do(func() {
		go func() {
			err := session.process.Wait()
			session.setWait(err)
			session.waited <- err
		}()
	})
}

func (session *codexProtocolSession) waitFor(duration time.Duration) (bool, error) {
	session.waitMu.Lock()
	if session.waitDone {
		err := session.waitErr
		session.waitMu.Unlock()
		return true, err
	}
	session.waitMu.Unlock()
	timer := session.adapter.clock.After(duration)
	for {
		select {
		case err := <-session.waited:
			session.setWait(err)
			return true, err
		case <-timer:
			session.waitMu.Lock()
			done, err := session.waitDone, session.waitErr
			session.waitMu.Unlock()
			return done, err
		case <-session.events:
		case <-session.stderrOverflow:
		}
	}
}

func (session *codexProtocolSession) waitForProtocolExit(duration time.Duration) (bool, error) {
	if session.stdoutEOF.Load() {
		return true, nil
	}
	timer := session.adapter.clock.After(duration)
	for {
		select {
		case event := <-session.events:
			if errors.Is(event.err, io.EOF) {
				return true, nil
			}
			if event.err != nil {
				return false, event.err
			}
			session.pending = append(session.pending, event.envelope)
			if len(session.pending) > codexProtocolMessageMax {
				return false, errors.New("cleanup notifications exceeded their bound")
			}
		case <-session.stderrOverflow:
			return false, errors.New("Codex stderr exceeded its bound")
		case <-timer:
			return session.stdoutEOF.Load(), nil
		}
	}
}

func (session *codexProtocolSession) observeRuntimeExit(duration time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	for {
		err := session.adapter.terminator.Observe(ctx, session.runtimeIDs)
		if err == nil {
			return true, nil
		}
		if ctx.Err() != nil || errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			// Observation owns no mutation authority. Exhausting its small grace
			// period is therefore a normal hand-off to the retained-child
			// Terminate path, not a reason to leak the process before Wait.
			return false, nil
		}
		if !errors.Is(err, ErrRuntimeProcessLive) {
			return false, err
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-session.adapter.clock.After(codexTerminationPoll):
		}
	}
}

func (session *codexProtocolSession) sendForCleanup(value any,
	duration time.Duration,
) error {
	written := make(chan error, 1)
	go func() { written <- session.send(value) }()
	timer := session.adapter.clock.After(duration)
	for {
		select {
		case err := <-written:
			return err
		case event := <-session.events:
			if event.err != nil && !errors.Is(event.err, io.EOF) {
				return event.err
			}
			if event.err == nil {
				session.pending = append(session.pending, event.envelope)
			}
		case <-session.stderrOverflow:
			return errors.New("Codex stderr exceeded its bound")
		case <-timer:
			return errors.New("Codex interrupt write exceeded its deadline")
		}
	}
}

func (session *codexProtocolSession) waitForInterruptedTurn(duration time.Duration) error {
	timer := session.adapter.clock.After(duration)
	for {
		var envelope codexRPCEnvelope
		if len(session.pending) != 0 {
			envelope = session.pending[0]
			session.pending = session.pending[1:]
		} else {
			select {
			case event := <-session.events:
				if event.err != nil {
					return event.err
				}
				envelope = event.envelope
			case <-session.stderrOverflow:
				return errors.New("Codex stderr exceeded its bound")
			case <-timer:
				return errors.New("turn interruption exceeded its deadline")
			}
		}
		if len(envelope.ID) != 0 {
			id, err := decodeCodexResponseID(envelope.ID)
			if err != nil || id != 5 {
				return errors.New("turn interrupt response ID is invalid")
			}
			if len(envelope.Error) != 0 {
				return decodeCodexResponseError(envelope.Error)
			}
			continue
		}
		if envelope.Method != "turn/completed" {
			continue
		}
		status, err := decodeCodexTurnCompleted(envelope.Params, session.threadID, session.turnID)
		if err != nil {
			return err
		}
		if status != "interrupted" {
			return fmt.Errorf("interrupted turn completed with status %s", status)
		}
		return nil
	}
}

func (session *codexProtocolSession) close(interrupt bool) (codexProcessExit, error) {
	var result error
	exit := codexProcessExit{signals: make([]string, 0)}
	session.closeOnce.Do(func() {
		defer func() {
			close(session.stopReader)
			_ = session.stdout.Close()
			_ = session.stderr.Close()
		}()
		if interrupt && session.threadID != "" && session.turnID != "" {
			if err := session.sendForCleanup(map[string]any{"id": 5, "method": "turn/interrupt",
				"params": map[string]any{"threadId": session.threadID, "turnId": session.turnID}},
				session.adapter.interruptGrace); err != nil {
				result = errors.Join(result, codexAdapterError("interrupt", err))
			} else if err := session.waitForInterruptedTurn(session.adapter.interruptGrace); err != nil {
				result = errors.Join(result, codexAdapterError("interrupt", err))
			}
		}
		_ = session.stdin.Close()
		protocolExited, observeErr := session.waitForProtocolExit(session.adapter.exitGrace)
		if observeErr != nil {
			result = errors.Join(result, codexAdapterError("observe exit", observeErr))
		}

		// Wait has deliberately not started, even after protocol EOF: the direct
		// child therefore cannot be reaped and have its PID/PGID reused between
		// exact identity proof and termination. This active-owned path retains
		// the direct child as the process-group identity anchor on every OS.
		if protocolExited {
			observed, err := session.observeRuntimeExit(session.adapter.signalGrace)
			if err != nil {
				result = errors.Join(result, codexAdapterError("observe Runtime exit", err))
			} else if observed {
				exit.method = "wait_without_signal"
			}
		}
		if exit.method == "" {
			terminationBudget := codexTerminationMax + session.adapter.signalGrace
			terminateCtx, cancel := context.WithTimeout(context.Background(), terminationBudget)
			signals, terminateErr := session.adapter.terminator.Terminate(terminateCtx,
				session.runtimeIDs)
			cancel()
			if terminateErr != nil {
				result = errors.Join(result, codexAdapterError("terminate", terminateErr))
				return
			}
			if len(signals) != 0 {
				if err := validateCodexTerminationSignals(signals); err != nil {
					result = errors.Join(result, codexAdapterError("terminate", err))
					return
				}
			}
			exit.signals = append(exit.signals, signals...)
		}
		// StdoutPipe/StderrPipe require the readers to reach EOF before Wait;
		// Wait itself closes those pipes. Process-group exit proof exists here,
		// so bounded forced close is safe but is recorded as cleanup failure.
		drained, drainErr := session.waitForProcessPipeDrain(session.adapter.signalGrace)
		if drainErr != nil {
			result = errors.Join(result, codexAdapterError("pipe drain", drainErr))
		}
		if !drained {
			return
		}
		session.startWait()
		if didExit, err := session.waitFor(session.adapter.signalGrace); didExit {
			exit.exited = true
			if exit.method == "" {
				exit.method = "wait_without_signal"
			}
			if len(exit.signals) != 0 {
				exit.method = "signal_assisted"
			}
			if err != nil {
				var exitErr *exec.ExitError
				if len(exit.signals) == 0 || !errors.As(err, &exitErr) {
					result = errors.Join(result, codexAdapterError("wait", err))
				}
			}
			if len(exit.signals) != 0 {
				result = errors.Join(result, codexAdapterError("cleanup",
					errors.New("process required signal-assisted shutdown")))
			}
			return
		}
		result = errors.Join(result, codexAdapterError("wait",
			errors.New("process did not become waitable after exact termination proof")))
	})
	return exit, result
}

func validateCodexInitializeResult(raw json.RawMessage) error {
	if err := validateCodexJSONAuthority(raw); err != nil {
		return errors.New("initialize result is malformed")
	}
	var result struct {
		CodexHome      string `json:"codexHome"`
		PlatformFamily string `json:"platformFamily"`
		PlatformOS     string `json:"platformOs"`
		UserAgent      string `json:"userAgent"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || !filepath.IsAbs(result.CodexHome) ||
		result.PlatformFamily == "" || result.PlatformOS == "" || result.UserAgent == "" {
		return errors.New("initialize result lacks required fields")
	}
	return nil
}

type codexManagedHook struct {
	DisplayOrder  int64
	SourcePath    string
	StatusMessage string
}

func validateCodexManagedHook(workspace string, raw json.RawMessage) (codexManagedHook, error) {
	if err := validateCodexJSONAuthority(raw); err != nil {
		return codexManagedHook{}, errors.New("Codex Hook registration payload is malformed")
	}
	var result struct {
		Data []struct {
			CWD      string            `json:"cwd"`
			Errors   []json.RawMessage `json:"errors"`
			Warnings []json.RawMessage `json:"warnings"`
			Hooks    []struct {
				Command       *string `json:"command"`
				CurrentHash   string  `json:"currentHash"`
				DisplayOrder  int64   `json:"displayOrder"`
				Enabled       bool    `json:"enabled"`
				EventName     string  `json:"eventName"`
				HandlerType   string  `json:"handlerType"`
				IsManaged     bool    `json:"isManaged"`
				Key           string  `json:"key"`
				Source        string  `json:"source"`
				SourcePath    string  `json:"sourcePath"`
				StatusMessage *string `json:"statusMessage"`
				TimeoutSec    uint64  `json:"timeoutSec"`
				TrustStatus   string  `json:"trustStatus"`
			} `json:"hooks"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || len(result.Data) != 1 ||
		result.Data[0].CWD != workspace || result.Data[0].Errors == nil ||
		len(result.Data[0].Errors) != 0 || result.Data[0].Warnings == nil ||
		len(result.Data[0].Warnings) != 0 || result.Data[0].Hooks == nil {
		return codexManagedHook{}, errors.New("Codex did not expose one clean project Hook set")
	}
	sourcePath := filepath.Join(workspace, ".codex", "hooks.json")
	command := filepath.Join(workspace, ".codex", "hooks", "mnemon-harness", "hook.sh")
	var matched []codexManagedHook
	for _, hook := range result.Data[0].Hooks {
		if hook.SourcePath != sourcePath {
			continue
		}
		commandMatches := hook.Command != nil && *hook.Command == command
		statusMatches := hook.StatusMessage != nil && *hook.StatusMessage == codexHookStatus
		if !commandMatches && !statusMatches {
			continue
		}
		if hook.CurrentHash == "" || len(hook.CurrentHash) > 256 || hook.DisplayOrder < 0 ||
			hook.Key == "" || len(hook.Key) > 256 ||
			!hook.Enabled || hook.EventName != "userPromptSubmit" || hook.HandlerType != "command" ||
			hook.IsManaged || hook.Source != "project" || hook.TimeoutSec != 10 ||
			hook.TrustStatus != "trusted" || !commandMatches || !statusMatches {
			return codexManagedHook{}, errors.New("managed project Hook is invalid")
		}
		matched = append(matched, codexManagedHook{DisplayOrder: hook.DisplayOrder,
			SourcePath: hook.SourcePath, StatusMessage: *hook.StatusMessage})
	}
	if len(matched) != 1 {
		return codexManagedHook{}, errors.New("Codex did not expose exactly one managed project Hook")
	}
	return matched[0], nil
}

func decodeCodexThreadStart(raw json.RawMessage, workspace string) (string, error) {
	if err := validateCodexJSONAuthority(raw); err != nil {
		return "", errors.New("thread/start result is malformed")
	}
	var result struct {
		ApprovalPolicy    string `json:"approvalPolicy"`
		ApprovalsReviewer string `json:"approvalsReviewer"`
		CWD               string `json:"cwd"`
		Model             string `json:"model"`
		ModelProvider     string `json:"modelProvider"`
		Sandbox           struct {
			ExcludeSlashTmp     *bool     `json:"excludeSlashTmp"`
			ExcludeTmpdirEnvVar *bool     `json:"excludeTmpdirEnvVar"`
			NetworkAccess       *bool     `json:"networkAccess"`
			Type                string    `json:"type"`
			WritableRoots       *[]string `json:"writableRoots"`
		} `json:"sandbox"`
		Thread struct {
			CLIVersion    string            `json:"cliVersion"`
			CreatedAt     *int64            `json:"createdAt"`
			CWD           string            `json:"cwd"`
			Ephemeral     bool              `json:"ephemeral"`
			ID            string            `json:"id"`
			ModelProvider string            `json:"modelProvider"`
			Preview       *string           `json:"preview"`
			SessionID     string            `json:"sessionId"`
			Source        string            `json:"source"`
			Status        map[string]any    `json:"status"`
			Turns         []json.RawMessage `json:"turns"`
			UpdatedAt     *int64            `json:"updatedAt"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Thread.ID == "" ||
		len(result.Thread.ID) > 256 || !utf8.ValidString(result.Thread.ID) ||
		result.ApprovalPolicy != "never" || result.ApprovalsReviewer != "user" ||
		result.CWD != workspace || result.Model == "" || len(result.Model) > 256 ||
		result.ModelProvider == "" || len(result.ModelProvider) > 256 ||
		result.Sandbox.Type != codexSandboxPolicyType || result.Sandbox.NetworkAccess == nil ||
		*result.Sandbox.NetworkAccess || result.Sandbox.WritableRoots == nil ||
		len(*result.Sandbox.WritableRoots) != 0 || result.Sandbox.ExcludeSlashTmp == nil ||
		*result.Sandbox.ExcludeSlashTmp || result.Sandbox.ExcludeTmpdirEnvVar == nil ||
		*result.Sandbox.ExcludeTmpdirEnvVar || result.Thread.CWD != workspace ||
		!result.Thread.Ephemeral || result.Thread.SessionID != result.Thread.ID ||
		result.Thread.ModelProvider != result.ModelProvider || result.Thread.CLIVersion == "" ||
		len(result.Thread.CLIVersion) > 256 || result.Thread.Source == "" ||
		len(result.Thread.Source) > 256 || result.Thread.Preview == nil || *result.Thread.Preview != "" ||
		result.Thread.CreatedAt == nil || *result.Thread.CreatedAt <= 0 ||
		result.Thread.UpdatedAt == nil || *result.Thread.UpdatedAt < *result.Thread.CreatedAt ||
		result.Thread.Turns == nil || len(result.Thread.Turns) != 0 ||
		result.Thread.Status["type"] != "idle" {
		return "", errors.New("thread/start result lacks the requested managed authority")
	}
	return result.Thread.ID, nil
}

func decodeCodexTurnStart(raw json.RawMessage) (string, error) {
	if err := validateCodexJSONAuthority(raw); err != nil {
		return "", errors.New("turn/start result is malformed")
	}
	var result struct {
		Turn struct {
			ID     string            `json:"id"`
			Items  []json.RawMessage `json:"items"`
			Status string            `json:"status"`
		} `json:"turn"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.Turn.ID == "" ||
		len(result.Turn.ID) > 256 || !utf8.ValidString(result.Turn.ID) ||
		result.Turn.Items == nil || len(result.Turn.Items) != 0 || result.Turn.Status != "inProgress" {
		return "", errors.New("turn/start result lacks a valid in-progress turn")
	}
	return result.Turn.ID, nil
}

func decodeCodexTurnCompleted(raw json.RawMessage, threadID, turnID string) (string, error) {
	if err := validateCodexJSONAuthority(raw); err != nil {
		return "", errors.New("turn/completed payload is malformed")
	}
	var result struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			ID     string            `json:"id"`
			Items  []json.RawMessage `json:"items"`
			Status string            `json:"status"`
		} `json:"turn"`
	}
	if err := json.Unmarshal(raw, &result); err != nil || result.ThreadID != threadID ||
		result.Turn.ID != turnID || result.Turn.Items == nil {
		return "", errors.New("turn/completed authority differs")
	}
	switch result.Turn.Status {
	case "completed", "interrupted", "failed":
		return result.Turn.Status, nil
	default:
		return "", errors.New("turn/completed status is invalid")
	}
}

type codexHookRun struct {
	CompletedAt  *int64 `json:"completedAt"`
	DisplayOrder int64  `json:"displayOrder"`
	DurationMS   *int64 `json:"durationMs"`
	Entries      []struct {
		Kind string `json:"kind"`
		Text string `json:"text"`
	} `json:"entries"`
	EventName     string  `json:"eventName"`
	ExecutionMode string  `json:"executionMode"`
	HandlerType   string  `json:"handlerType"`
	ID            string  `json:"id"`
	Scope         string  `json:"scope"`
	Source        string  `json:"source"`
	SourcePath    string  `json:"sourcePath"`
	StartedAt     int64   `json:"startedAt"`
	Status        string  `json:"status"`
	StatusMessage *string `json:"statusMessage"`
}

type codexHookNotification struct {
	ThreadID string       `json:"threadId"`
	TurnID   *string      `json:"turnId"`
	Run      codexHookRun `json:"run"`
}

type codexHookProof struct {
	expected  codexManagedHook
	threadID  string
	turnID    string
	started   *codexHookRun
	completed *codexHookRun
	complete  bool
}

func (proof *codexHookProof) observe(envelope codexRPCEnvelope) error {
	if envelope.Method != "hook/started" && envelope.Method != "hook/completed" {
		return nil
	}
	if err := validateCodexJSONAuthority(envelope.Params); err != nil {
		return errors.New("Hook notification is malformed")
	}
	var notification codexHookNotification
	if err := json.Unmarshal(envelope.Params, &notification); err != nil {
		return errors.New("Hook notification is malformed")
	}
	run := notification.Run
	if run.SourcePath != proof.expected.SourcePath || run.DisplayOrder != proof.expected.DisplayOrder ||
		run.StatusMessage == nil || *run.StatusMessage != proof.expected.StatusMessage {
		return nil
	}
	if notification.ThreadID != proof.threadID || notification.TurnID == nil ||
		*notification.TurnID != proof.turnID || run.ID == "" || len(run.ID) > 256 ||
		!utf8.ValidString(run.ID) || run.EventName != "userPromptSubmit" ||
		run.ExecutionMode != "sync" || run.HandlerType != "command" || run.Scope != "turn" ||
		run.Source != "project" || run.StartedAt <= 0 {
		return errors.New("managed Hook notification authority differs")
	}
	if envelope.Method == "hook/started" {
		if proof.started != nil || run.Status != "running" || run.Entries == nil || len(run.Entries) != 0 ||
			run.CompletedAt != nil || run.DurationMS != nil {
			return errors.New("managed Hook start proof is invalid or duplicate")
		}
		copy := run
		proof.started = &copy
		return nil
	}
	if proof.started == nil || proof.completed != nil || run.Status != "completed" ||
		run.ID != proof.started.ID ||
		run.StartedAt != proof.started.StartedAt ||
		run.CompletedAt == nil || *run.CompletedAt < run.StartedAt || run.DurationMS == nil ||
		*run.DurationMS < 0 || len(run.Entries) != 1 || run.Entries[0].Kind != "context" ||
		run.Entries[0].Text != WakeCue {
		return errors.New("managed Hook completion proof is invalid, missing its start, or duplicate")
	}
	copy := run
	proof.completed, proof.complete = &copy, true
	return nil
}

func (proof *codexHookProof) receipt() (model.JSON, error) {
	if proof == nil || !proof.complete || proof.started == nil || proof.completed == nil {
		return model.JSON{}, errors.New("managed Hook proof is incomplete")
	}
	return model.JSONFrom(struct {
		Adapter         string `json:"adapter"`
		CompletedAt     int64  `json:"completed_at"`
		Cue             string `json:"cue"`
		DisplayOrder    int64  `json:"display_order"`
		EventName       string `json:"event_name"`
		ExecutionMode   string `json:"execution_mode"`
		HandlerType     string `json:"handler_type"`
		HookID          string `json:"hook_id"`
		SchemaVersion   int    `json:"schema_version"`
		Scope           string `json:"scope"`
		Source          string `json:"source"`
		SourcePath      string `json:"source_path"`
		StartObservedAt int64  `json:"start_observed_at"`
		StartedAt       int64  `json:"started_at"`
		StatusMessage   string `json:"status_message"`
		ThreadID        string `json:"thread_id"`
		TurnID          string `json:"turn_id"`
	}{codexAdapterName, *proof.completed.CompletedAt, WakeCue, proof.completed.DisplayOrder,
		proof.completed.EventName, proof.completed.ExecutionMode, proof.completed.HandlerType,
		proof.completed.ID, 1, proof.completed.Scope, proof.completed.Source,
		proof.completed.SourcePath, proof.started.StartedAt, proof.completed.StartedAt,
		*proof.completed.StatusMessage,
		proof.threadID, proof.turnID})
}
