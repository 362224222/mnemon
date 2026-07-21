package agent

import (
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (adapter *ClaudeWakeAdapter) finishClaudeRun(result *CodexWakeResult,
	session *claudeStreamSession, proof *claudeStreamProof, completedNormally bool,
	runErr *error,
) {
	exit, cleanupErr := adapter.closeRegisteredProcess(session.process, session.runtimeIDs)
	cleanupErr = errors.Join(cleanupErr, claudeSignalCleanupError(exit.signals),
		session.closeReaders())
	result.ProcessExited = exit.exited
	if cleanupErr != nil {
		*runErr = errors.Join(*runErr, claudeAdapterError("cleanup", cleanupErr))
	}
	if !exit.exited {
		return
	}
	completionAt, err := adapter.trustedNow()
	if err != nil {
		*runErr = errors.Join(*runErr, claudeAdapterError("completion evidence", err))
		return
	}
	result.At = completionAt
	status := claudeCompletionStatus(completedNormally, cleanupErr, *runErr)
	threadID, turnID := proof.completionIDs()
	result.CompletionReceipt, err = managedCompletionReceipt(claudeAdapterName, status,
		threadID, turnID, result.WakeDelivered, exit.method, exit.signals)
	if err != nil {
		*runErr = errors.Join(*runErr, claudeAdapterError("completion evidence", err))
	}
}

func claudeCompletionStatus(completed bool, cleanupErr, runErr error) string {
	if completed && cleanupErr == nil && runErr == nil {
		return "completed"
	}
	if completed && cleanupErr != nil {
		return "cleanup_failed"
	}
	return "failed"
}

func (adapter *ClaudeWakeAdapter) settleClaudeNotStarted(result CodexWakeResult,
	failure error,
) (CodexWakeResult, error) {
	result.ProcessExited = true
	completionAt, err := adapter.trustedNow()
	if err != nil {
		return result, errors.Join(failure, claudeAdapterError("completion evidence", err))
	}
	result.At = completionAt
	result.CompletionReceipt, err = managedCompletionReceipt(claudeAdapterName, "launch_failed",
		"", "", false, "not_started", nil)
	if err != nil {
		return result, errors.Join(failure, claudeAdapterError("completion evidence", err))
	}
	return result, failure
}

func (adapter *ClaudeWakeAdapter) settleClaudeUnregistered(result *CodexWakeResult,
	process CodexProcess, status string, failure error,
) error {
	exit, cleanupErr := adapter.closeUnregisteredProcess(process)
	return adapter.completeClaudeSettlement(result, status, exit, failure, cleanupErr)
}

func (adapter *ClaudeWakeAdapter) settleClaudeRegistered(result *CodexWakeResult,
	process CodexProcess, runtimeIDs model.JSON, status string, failure error,
) error {
	exit, cleanupErr := adapter.closeRegisteredProcess(process, runtimeIDs)
	return adapter.completeClaudeSettlement(result, status, exit, failure, cleanupErr)
}

func (adapter *ClaudeWakeAdapter) completeClaudeSettlement(result *CodexWakeResult,
	status string, exit codexProcessExit, failure, cleanupErr error,
) error {
	result.ProcessExited = exit.exited
	if cleanupErr != nil {
		failure = errors.Join(failure, claudeAdapterError("cleanup", cleanupErr))
	}
	if !exit.exited {
		return failure
	}
	completionAt, err := adapter.trustedNow()
	if err != nil {
		return errors.Join(failure, claudeAdapterError("completion evidence", err))
	}
	result.At = completionAt
	result.CompletionReceipt, err = managedCompletionReceipt(claudeAdapterName, status,
		"", "", false, exit.method, exit.signals)
	if err != nil {
		return errors.Join(failure, claudeAdapterError("completion evidence", err))
	}
	return failure
}
