package agent

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func (worker *WakeWorker) runtimeExecutionContext(parent context.Context,
	run model.AgentRun,
) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	lease, _ := run.LeaseUntil()
	at, err := worker.trustedNow()
	if err != nil {
		runtimeCtx, cancel := context.WithCancel(parent)
		cancel()
		return runtimeCtx, cancel
	}
	remaining := lease.Sub(at)
	if remaining < 0 {
		remaining = 0
	}
	if remaining > worker.runtimeTimeout {
		remaining = worker.runtimeTimeout
	}
	runtimeCtx, cancel := context.WithTimeout(parent, remaining)
	return runtimeCtx, cancel
}

func (worker *WakeWorker) adapterName() (string, bool) {
	if worker.profile.Runtime() != model.RuntimeCodexAppServer {
		return "", false
	}
	return codexAdapterName, true
}
