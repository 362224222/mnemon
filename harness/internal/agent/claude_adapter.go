package agent

import (
	"context"
	"errors"
	"io"
)

const (
	claudeAdapterName     = "claude-cli"
	claudeClientName      = "mnemon-harness"
	claudeProtocolName    = "stream-json-v1"
	claudeManagedPrompt   = "/mnemon-harness\n"
	claudePermissionMode  = "dontAsk"
	claudeToolSurface     = "Bash,Read,Edit,Write,Glob,Grep"
	claudeEmptyMCPConfig  = `{"mcpServers":{}}`
	claudeStreamLineMax   = 1 << 20
	claudeStreamTotalMax  = 8 << 20
	claudeStreamEventsMax = 4096
	claudeStderrMax       = 64 << 10
)

var ErrClaudeWakeAdapter = errors.New("Claude managed wake adapter")

type claudeAdapterFailure struct {
	stage string
	cause error
}

func (failure *claudeAdapterFailure) Error() string {
	return ErrClaudeWakeAdapter.Error() + ": " + failure.stage + ": " +
		codexAdapterErrorCategory(failure.cause)
}

func (failure *claudeAdapterFailure) Unwrap() error { return ErrClaudeWakeAdapter }

func (failure *claudeAdapterFailure) Is(target error) bool {
	if target == ErrClaudeWakeAdapter {
		return true
	}
	switch target {
	case context.Canceled, context.DeadlineExceeded, io.EOF,
		ErrRuntimeProcess, ErrRuntimeProcessLive, ErrRuntimeProcessUncertain:
		return errors.Is(failure.cause, target)
	default:
		return false
	}
}

// ClaudeWakeAdapter runs one non-persistent Claude Code turn. It deliberately
// reuses the same durable request/result boundary as Codex; only the provider
// protocol and its receipts differ.
type ClaudeWakeAdapter struct {
	*managedRuntimeCore
}

func NewClaudeWakeAdapter(options CodexWakeAdapterOptions) (*ClaudeWakeAdapter, error) {
	core, stage, err := newManagedRuntimeCore(options)
	if err != nil {
		return nil, claudeAdapterError(stage, err)
	}
	return &ClaudeWakeAdapter{managedRuntimeCore: core}, nil
}

func (adapter *ClaudeWakeAdapter) Run(ctx context.Context,
	request CodexWakeRequest,
) (result CodexWakeResult, runErr error) {
	if err := validateClaudeRunRequest(adapter, ctx, request); err != nil {
		return result, err
	}
	attachment, err := validateCodexAttachmentEnvironment(request.RunAttachmentEnvironment)
	if err != nil {
		return result, claudeAdapterError("run", err)
	}
	if err := adapter.gateClaudeStart(ctx); err != nil {
		return adapter.settleClaudeNotStarted(result, err)
	}
	process, err := adapter.startClaudeProcess(attachment)
	if err != nil {
		return adapter.settleClaudeNotStarted(result, err)
	}
	identity, err := adapter.identity.Identify(process.PID())
	if err != nil {
		return result, adapter.settleClaudeUnregistered(&result, process,
			"launch_identity_failed", claudeAdapterError("identify", err))
	}
	if _, err := validateCodexProcessIdentity(process.PID(), identity); err != nil {
		return result, adapter.settleClaudeUnregistered(&result, process,
			"launch_identity_failed", claudeAdapterError("identify", err))
	}
	session, err := newClaudeStreamSession(adapter, process)
	if err != nil {
		return result, adapter.settleClaudeRegistered(&result, process, identity.RuntimeIDs,
			"launch_contract_failed", claudeAdapterError("start", err))
	}
	session.runtimeIDs = identity.RuntimeIDs
	proof := newClaudeStreamProof(adapter.workspace)
	completedNormally := false
	defer func() {
		adapter.finishClaudeRun(&result, session, proof, completedNormally, &runErr)
	}()
	if err := adapter.recordClaudeLaunch(ctx, request, identity, &result); err != nil {
		return result, err
	}
	if err := session.sendPrompt(); err != nil {
		return result, claudeAdapterError("send managed prompt", err)
	}
	if err := adapter.observeClaudeStream(ctx, request, session, proof, &result); err != nil {
		return result, err
	}
	completedNormally = true
	return result, nil
}

func validateClaudeRunRequest(adapter *ClaudeWakeAdapter, ctx context.Context,
	request CodexWakeRequest,
) error {
	if adapter == nil || adapter.managedRuntimeCore == nil || ctx == nil ||
		request.Callbacks.RecordLaunch == nil || request.Callbacks.RecordWake == nil {
		return claudeAdapterError("run", errors.New("adapter, context and callbacks are required"))
	}
	return nil
}

func (adapter *ClaudeWakeAdapter) gateClaudeStart(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return claudeAdapterError("run", err)
	}
	if err := adapter.verifyProjection(ctx); err != nil {
		return claudeAdapterError("verify projection", err)
	}
	if err := ctx.Err(); err != nil {
		return claudeAdapterError("run", err)
	}
	return nil
}

func (adapter *ClaudeWakeAdapter) startClaudeProcess(attachment string) (CodexProcess, error) {
	process, err := adapter.starter.Start(CodexProcessStartSpec{Executable: adapter.executable,
		Arguments: claudeRuntimeArguments(), Directory: adapter.workspace,
		Environment: append(append([]string(nil), adapter.environment...), attachment)})
	if err != nil {
		return nil, claudeAdapterError("start", err)
	}
	if process == nil {
		return nil, claudeAdapterError("start", errors.New("starter returned no process"))
	}
	return process, nil
}

func claudeRuntimeArguments() []string {
	return []string{"-p", "--input-format", "text", "--output-format", "stream-json",
		"--verbose", "--include-hook-events", "--no-session-persistence",
		"--setting-sources", "project", "--permission-mode", claudePermissionMode,
		"--tools", claudeToolSurface, "--allowedTools", claudeToolSurface,
		"--strict-mcp-config", "--mcp-config", claudeEmptyMCPConfig,
		"--prompt-suggestions", "false"}
}

func (adapter *ClaudeWakeAdapter) recordClaudeLaunch(ctx context.Context,
	request CodexWakeRequest, identity CodexProcessIdentity, result *CodexWakeResult,
) error {
	launchAt, err := adapter.trustedNow()
	if err != nil {
		return claudeAdapterError("launch evidence", err)
	}
	diagnostic, runtimeIDs, err := managedLaunchJSON(claudeAdapterName, claudeClientName,
		adapter.executable, claudeProtocolName, identity)
	if err != nil {
		return claudeAdapterError("launch evidence", err)
	}
	result.LaunchAt, result.Diagnostic, result.RuntimeIDs = launchAt, diagnostic, runtimeIDs
	if err := request.Callbacks.RecordLaunch(ctx, CodexLaunchEvidence{At: launchAt,
		Diagnostic: diagnostic, RuntimeIDs: runtimeIDs}); err != nil {
		return claudeAdapterError("record launch", err)
	}
	return nil
}

func (adapter *ClaudeWakeAdapter) observeClaudeStream(ctx context.Context,
	request CodexWakeRequest, session *claudeStreamSession, proof *claudeStreamProof,
	result *CodexWakeResult,
) error {
	for {
		line, err := session.next(ctx)
		if errors.Is(err, io.EOF) {
			if err := session.finishOutput(ctx); err != nil {
				return claudeAdapterError("complete stream", err)
			}
			return proof.validateComplete(true)
		}
		if err != nil {
			return claudeAdapterError("read stream", err)
		}
		wakeReady, err := proof.observe(line)
		if err != nil {
			return claudeAdapterError("observe stream", err)
		}
		if wakeReady && !result.WakeDelivered {
			if err := adapter.recordClaudeWake(ctx, request, proof, result); err != nil {
				return err
			}
		}
	}
}

func (adapter *ClaudeWakeAdapter) recordClaudeWake(ctx context.Context,
	request CodexWakeRequest, proof *claudeStreamProof, result *CodexWakeResult,
) error {
	wakeAt, err := adapter.trustedNow()
	if err != nil {
		return claudeAdapterError("wake evidence", err)
	}
	receipt, err := proof.wakeReceipt()
	if err != nil {
		return claudeAdapterError("wake evidence", err)
	}
	result.WakeAt, result.WakeReceipt, result.WakeDelivered = wakeAt, receipt, true
	if err := request.Callbacks.RecordWake(ctx, CodexWakeEvidence{At: wakeAt,
		Receipt: receipt}); err != nil {
		return claudeAdapterError("record wake", err)
	}
	return nil
}

func claudeAdapterError(stage string, err error) error {
	if err == nil {
		err = errors.New("unknown failure")
	}
	return &claudeAdapterFailure{stage: stage, cause: err}
}

func claudeSignalCleanupError(signals []string) error {
	if len(signals) == 0 {
		return nil
	}
	return errors.New("Claude Runtime required signal-assisted shutdown")
}

var _ WakeWorkerAdapter = (*ClaudeWakeAdapter)(nil)
